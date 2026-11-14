mod args;
mod files;
mod inspect;
mod output;
mod progress;
mod query;
mod sort;

use std::io::{self, BufWriter, IsTerminal, Read, Write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, bail};
use args::{Args, Format};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use clap::Parser;
use output::{Borders, print_batch};
use parquet_reader::export::{BatchWriter, ExportFormat};
use parquet_reader::{
    FilterExpr, PAGE_ROWS, ParquetSource, Projection, SortBudget, SortOutput, SortSpec,
};
use progress::Progress;

/// Most rows the table format prints; it holds them all in memory to size columns.
const MAX_HEAD_ROWS: u64 = 1_000;
/// Rows shown in the table when `--head` is not given.
const DEFAULT_HEAD_ROWS: u64 = 10;
/// Rows read per step when streaming rows or counting matches: memory stays at
/// one batch of the projected (or, for `--count`, the filter's) columns.
const STREAM_BATCH_ROWS: u64 = PAGE_ROWS;

fn main() -> ExitCode {
    let args = Args::parse();
    let mut input = io::stdin().lock();
    let result = match &args.output {
        Some(path) => run_to_file(&args, path, &mut input, STREAM_BATCH_ROWS),
        None => {
            // `Stdout`, not its lock: the Parquet writer needs a `Send` output.
            let mut out = BufWriter::new(io::stdout());
            run(&args, &mut input, &mut out, STREAM_BATCH_ROWS)
                .and_then(|()| out.flush().context("write to standard output"))
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        // The reader went away (`cli … | head`): stop quietly, as `cat` does.
        Err(error) if is_broken_pipe(&error) => ExitCode::SUCCESS,
        Err(error) => {
            // Same text Rust prints for an error returned from `main`.
            let _ = writeln!(io::stderr(), "Error: {error:?}");
            ExitCode::FAILURE
        }
    }
}

/// [`run`] into `path`, which appears only once the output is complete.
fn run_to_file(
    args: &Args,
    path: &Path,
    input: &mut dyn Read,
    batch_rows: u64,
) -> anyhow::Result<()> {
    files::check_not_input(&args.file, path)?;
    files::write_atomically(path, |out| run(args, input, out, batch_rows))
}

/// The output format: `--format`, else `--output`'s extension, else the table.
fn output_format(args: &Args) -> anyhow::Result<Format> {
    match (args.format, &args.output) {
        (Some(format), _) => Ok(format),
        // `--count` prints a number whatever the file is called.
        (None, Some(path)) if !args.count => files::format_for_extension(path),
        (None, _) => Ok(Format::Table),
    }
}

/// Checks every argument against the file, then writes the output.
/// Nothing is written until the arguments are known to be valid and, for the
/// table, until its rows have been read. `input` is read only for `-`.
fn run(
    args: &Args,
    input: &mut dyn Read,
    out: &mut (impl Write + Send),
    batch_rows: u64,
) -> anyhow::Result<()> {
    let format = output_format(args)?;
    let borders = if args.ascii {
        Borders::Ascii
    } else {
        Borders::Unicode
    };
    // Rows the table holds: --head, or every statistics row up to the cap.
    let table_rows = if format == Format::Table && !args.count && !args.schema {
        let default = args.stats.then_some(MAX_HEAD_ROWS);
        Some(validate_head(args.head.or(default))?)
    } else {
        None
    };

    // Rows streamed to a terminal show their own progress; a note on the same
    // screen would only get in their way.
    let streams_to_terminal =
        format != Format::Table && args.output.is_none() && io::stdout().is_terminal();
    let progress = Progress::new(args.no_progress || streams_to_terminal);

    // Declared before the source so it is deleted after the source closes it.
    let stdin = (args.file == "-")
        .then(|| files::copy_stdin(input))
        .transpose()?;
    let opening = |share| {
        progress.share("Opening", share);
        false
    };
    let parquet = match &stdin {
        Some(copy) => {
            ParquetSource::open_cancellable(&copy.path, &opening).context("read standard input")?
        }
        None => ParquetSource::open_cancellable(&args.file, &opening)?,
    };
    progress.clear();
    let projection = parse_columns(args.columns.as_deref(), &parquet)?;
    let filter = args
        .filter
        .as_deref()
        .map(|text| parse_filter(&parquet, text))
        .transpose()?;
    let sort = args
        .sort
        .as_deref()
        .map(|text| sort::parse_sort(text, &parquet))
        .transpose()?;

    if args.count {
        let rows = count_rows(&parquet, filter.as_ref(), batch_rows, &progress)?;
        progress.clear();
        writeln!(out, "{rows}")?;
        return Ok(());
    }
    if args.schema {
        let batch = inspect::schema_batch(&parquet, &projection)?;
        return write_facts(&batch, "Schema", None, format, borders, out);
    }
    if args.stats {
        let batch = inspect::stats_batch(&parquet, &projection)?;
        let limit = table_rows.map_or(args.head, |rows| Some(rows as u64));
        return write_facts(&batch, "Statistics", limit, format, borders, out);
    }
    if args.profile {
        let profiles = profile_columns(
            &parquet,
            &projection,
            filter.as_ref(),
            batch_rows,
            &progress,
        )?;
        progress.clear();
        let batch = inspect::profile_batch(&profiles)?;
        return write_facts(&batch, "Profile", None, format, borders, out);
    }

    let output = RowOutput {
        format,
        table_rows,
        head: args.head,
        offset: args.offset,
        progress: &progress,
        borders,
    };

    if let Some(sql) = &args.sql {
        let dataset_path = if stdin.is_some() {
            Path::new("stdin")
        } else {
            Path::new(&args.file)
        };
        let tables = query::tables(Arc::new(parquet), dataset_path, &args.tables)?;
        let query = query::plan(sql, &tables)?;
        return output.write(&query.schema, "Result", None, out, |window, emit| {
            query.run(window, emit)
        });
    }

    // An empty window carries the projected schema.
    let schema = parquet.read_window(0, 0, &projection)?.schema();
    let rows = Rows {
        parquet: &parquet,
        filter: filter.as_ref(),
        sort: sort.as_deref(),
        projection: &projection,
        schema: &schema,
        offset: args.offset,
        batch_rows,
        sort_budget: SortBudget::default(),
        progress: &progress,
    };
    let summary = (!args.quiet).then_some(&parquet);
    output.write(&schema, "Preview", summary, out, |window, emit| {
        rows.for_each(window.left, emit)
    })
}

/// Where rows go: a table of at most `table_rows` rows, or a streamed format.
struct RowOutput<'a> {
    format: Format,
    table_rows: Option<usize>,
    head: Option<u64>,
    offset: u64,
    /// Blanked before the table is printed.
    progress: &'a Progress,
    borders: Borders,
}

impl RowOutput<'_> {
    /// Writes the rows `produce` hands out within the window of `--offset` and
    /// `--head` (or the table's row count). The table is printed only once
    /// every row is in, after `summary` if given.
    fn write(
        &self,
        schema: &SchemaRef,
        title: &str,
        summary: Option<&ParquetSource>,
        out: &mut (impl Write + Send),
        produce: impl FnOnce(
            Window,
            &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
        ) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let window = |left| Window {
            skip: self.offset,
            left,
        };
        let Some(head) = self.table_rows else {
            let mut writer = BatchWriter::new(export_format(self.format)?, out, schema.clone())?;
            produce(window(self.head.unwrap_or(u64::MAX)), &mut |batch| {
                writer.write(&batch)
            })?;
            return writer.finish();
        };

        let mut batches = Vec::new();
        produce(window(head as u64), &mut |batch| {
            batches.push(batch);
            Ok(())
        })?;
        let batch = arrow_select::concat::concat_batches(schema, &batches)?;

        self.progress.clear();
        if let Some(parquet) = summary {
            print_summary(parquet, out)?;
        }
        print_batch(
            &batch,
            title,
            self.offset.saturating_add(1),
            self.borders,
            out,
        )?;
        Ok(())
    }
}

/// Profiles `projection`'s columns in one scan of the rows `filter` keeps,
/// with the accumulators the app's Profile window uses. Memory stays at one
/// batch plus each column's bounded distinct values.
fn profile_columns(
    parquet: &ParquetSource,
    projection: &Projection,
    filter: Option<&FilterExpr>,
    batch_rows: u64,
    progress: &Progress,
) -> anyhow::Result<Vec<parquet_reader::profile::ColumnStats>> {
    let columns = projection.as_slice();
    if columns.len() > parquet_reader::profile::MAX_FILE_PROFILE_COLUMNS {
        bail!(
            "--profile covers at most {} columns and this has {}; choose some with --columns",
            parquet_reader::profile::MAX_FILE_PROFILE_COLUMNS,
            columns.len()
        );
    }
    let mut profiler = parquet_reader::profile::FileProfiler::new(parquet, columns)?;
    if !profiler.read_columns().is_empty() {
        let read = Projection::columns(profiler.read_columns().to_vec(), parquet.column_count())?;
        let schema = parquet.read_window(0, 0, &read)?.schema();
        Rows {
            parquet,
            filter,
            sort: None,
            projection: &read,
            schema: &schema,
            offset: 0,
            batch_rows,
            sort_budget: SortBudget::default(),
            progress,
        }
        .for_each(u64::MAX, &mut |batch| profiler.push_batch(&batch))?;
    }
    Ok(profiler.finish())
}

/// Writes a `--schema`, `--stats` or `--profile` batch, cut to `limit` rows.
fn write_facts(
    batch: &RecordBatch,
    title: &str,
    limit: Option<u64>,
    format: Format,
    borders: Borders,
    out: &mut (impl Write + Send),
) -> anyhow::Result<()> {
    let total = batch.num_rows();
    let shown = limit.map_or(total, |limit| {
        usize::try_from(limit).map_or(total, |limit| limit.min(total))
    });
    let batch = batch.slice(0, shown);

    if format == Format::Table {
        print_batch(&batch, title, 1, borders, out)?;
        if shown < total {
            writeln!(
                out,
                "{shown} of {total} rows shown; use --head (up to {MAX_HEAD_ROWS}) \
                 or --format csv for the rest"
            )?;
        }
        return Ok(());
    }
    let mut writer = BatchWriter::new(export_format(format)?, out, batch.schema())?;
    writer.write(&batch)?;
    writer.finish()
}

fn export_format(format: Format) -> anyhow::Result<ExportFormat> {
    Ok(match format {
        Format::Csv => ExportFormat::Csv,
        Format::Jsonl => ExportFormat::Json,
        Format::Parquet => ExportFormat::Parquet,
        Format::Arrow => ExportFormat::Arrow,
        Format::Table => bail!("the table is not a streamed format"),
    })
}

/// Skips the first `skip` rows of a run of batches, then keeps up to `left`.
struct Window {
    skip: u64,
    left: u64,
}

impl Window {
    fn is_done(&self) -> bool {
        self.left == 0
    }

    /// The part of `batch` inside the window, if any.
    fn take(&mut self, batch: &RecordBatch) -> Option<RecordBatch> {
        let rows = batch.num_rows();
        let skipped = usize::try_from(self.skip).map_or(rows, |skip| skip.min(rows));
        self.skip -= skipped as u64;
        let kept =
            usize::try_from(self.left).map_or(rows - skipped, |left| left.min(rows - skipped));
        self.left -= kept as u64;
        (kept > 0).then(|| batch.slice(skipped, kept))
    }
}

/// The rows to output: optionally filtered and sorted, after `offset` rows.
struct Rows<'a> {
    parquet: &'a ParquetSource,
    filter: Option<&'a FilterExpr>,
    sort: Option<&'a [SortSpec]>,
    projection: &'a Projection,
    /// Every batch handed out is given this schema, so a writer sees one
    /// schema whichever read produced the rows.
    schema: &'a SchemaRef,
    offset: u64,
    /// Rows read per step.
    batch_rows: u64,
    /// Rows and bytes a sort keeps in memory before it spills.
    sort_budget: SortBudget,
    /// Told the rows read so far.
    progress: &'a Progress,
}

impl Rows<'_> {
    /// Hands up to `limit` rows to `emit`, one batch of at most `batch_rows`
    /// rows (or a sort's page) at a time.
    fn for_each(
        &self,
        limit: u64,
        emit: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let total = self.parquet.row_count();
        match (self.sort, self.filter) {
            (Some(sort), filter) => self.sorted(sort, filter, limit, emit),
            (None, None) => {
                // The offset is where the reads start.
                let mut row = self.offset.min(total);
                let end = row.saturating_add(limit).min(total);
                while row < end {
                    let count = (end - row).min(self.batch_rows);
                    let batch = self
                        .parquet
                        .read_window(row, to_usize(count)?, self.projection)?;
                    self.deliver(batch, emit)?;
                    row += count;
                    self.progress.rows("Reading", row, total);
                }
                Ok(())
            }
            (None, Some(filter)) => {
                let mut window = Window {
                    skip: self.offset,
                    left: limit,
                };
                let mut next_row = 0;
                while !window.is_done() && next_row < total {
                    let read = self.parquet.read_filtered_from(
                        filter,
                        next_row,
                        to_usize(self.batch_rows)?,
                        self.projection,
                        &|scanned| {
                            self.progress.rows("Scanning", scanned, total);
                            false
                        },
                    )?;
                    if read.next_row <= next_row {
                        bail!("filtered read did not move past row {next_row}");
                    }
                    next_row = read.next_row;
                    if let Some(batch) = window.take(&read.batch) {
                        self.deliver(batch, emit)?;
                    }
                }
                Ok(())
            }
        }
    }

    /// Sorted rows through `read_sorted`: kept in memory up to the sort
    /// budget, spilled to temporary files past it (ADR 0005). With a limit,
    /// only the best `offset + limit` rows are kept while scanning.
    fn sorted(
        &self,
        sort: &[SortSpec],
        filter: Option<&FilterExpr>,
        limit: u64,
        emit: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let top = (limit != u64::MAX)
            .then(|| usize::try_from(self.offset.saturating_add(limit)).ok())
            .flatten();
        let output = self.parquet.read_sorted(
            filter,
            self.projection,
            sort,
            self.sort_budget,
            top,
            &|scanned| {
                self.progress
                    .rows("Sorting", scanned, self.parquet.row_count());
                false
            },
        )?;
        let mut window = Window {
            skip: self.offset,
            left: limit,
        };
        match output {
            SortOutput::InMemory(batches) => {
                for batch in &batches {
                    if window.is_done() {
                        break;
                    }
                    if let Some(batch) = window.take(batch) {
                        self.deliver(batch, emit)?;
                    }
                }
            }
            SortOutput::Spilled(mut merge) => {
                while !window.is_done() {
                    let (batches, finished) =
                        merge.next_rows(to_usize(self.batch_rows)?, &|| false)?;
                    for batch in &batches {
                        if let Some(batch) = window.take(batch) {
                            self.deliver(batch, emit)?;
                        }
                    }
                    if finished {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn deliver(
        &self,
        batch: RecordBatch,
        emit: &mut dyn FnMut(RecordBatch) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        emit(RecordBatch::try_new(
            self.schema.clone(),
            batch.columns().to_vec(),
        )?)
    }
}

/// The row count from the footer, or a scan that reads only the filter's columns.
fn count_rows(
    parquet: &ParquetSource,
    filter: Option<&FilterExpr>,
    batch_rows: u64,
    progress: &Progress,
) -> anyhow::Result<u64> {
    let Some(filter) = filter else {
        return Ok(parquet.row_count());
    };
    let total = parquet.row_count();
    let projection =
        Projection::columns(filter.columns(&parquet.schema())?, parquet.column_count())?;
    let mut matches = 0_u64;
    let mut next_row = 0;
    while next_row < total {
        let window = parquet.read_filtered_from(
            filter,
            next_row,
            to_usize(batch_rows)?,
            &projection,
            &|scanned| {
                progress.rows("Counting", scanned, total);
                false
            },
        )?;
        if window.next_row <= next_row {
            bail!("filtered read did not move past row {next_row}");
        }
        next_row = window.next_row;
        matches += window.batch.num_rows() as u64;
    }
    Ok(matches)
}

fn to_usize(value: u64) -> anyhow::Result<usize> {
    usize::try_from(value).with_context(|| format!("{value} rows do not fit in memory"))
}

fn print_summary(parquet: &ParquetSource, out: &mut impl Write) -> io::Result<()> {
    if parquet.is_folder() {
        writeln!(out, "Files: {}", parquet.file_count())?;
        let partitions = parquet.partition_column_names();
        if !partitions.is_empty() {
            writeln!(out, "Partitions: {}", partitions.join(", "))?;
        }
    }
    writeln!(out, "Rows: {}", parquet.row_count())?;
    writeln!(out, "Columns: {}", parquet.column_count())?;
    writeln!(out, "Row groups: {}", parquet.row_groups().len())?;
    writeln!(out)?;

    writeln!(out, "Schema:")?;
    for (i, field) in parquet.schema().fields().iter().enumerate() {
        writeln!(out, "{:>3} {:<20} {}", i, field.name(), field.data_type())?;
    }
    writeln!(out)?;

    for group in parquet.row_groups().iter().take(10) {
        if group.row_count == 0 {
            writeln!(out, "row group {:>3}: empty", group.index)?;
        } else {
            let last_row = group.first_row + group.row_count - 1;
            writeln!(
                out,
                "row group {:>3}: rows {}..={} ({} rows)",
                group.index, group.first_row, last_row, group.row_count
            )?;
        }
    }
    if parquet.row_groups().len() > 10 {
        writeln!(out, "...")?;
    }
    Ok(())
}

/// True when `error` comes from writing to a pipe whose reader has closed.
fn is_broken_pipe(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
    })
}

/// Rows the table prints: `--head`, at most [`MAX_HEAD_ROWS`].
fn validate_head(head: Option<u64>) -> anyhow::Result<usize> {
    let head = head.unwrap_or(DEFAULT_HEAD_ROWS);
    if head > MAX_HEAD_ROWS {
        bail!(
            "--head must be at most {MAX_HEAD_ROWS} for the table; \
             use --format csv, jsonl, parquet or arrow for more rows"
        );
    }
    to_usize(head)
}

fn parse_filter(parquet: &ParquetSource, text: &str) -> anyhow::Result<FilterExpr> {
    let filter = FilterExpr::parse(text)?;
    parquet.validate_filter(&filter)?;
    Ok(filter)
}

/// Columns by name or 0-based index, for `--columns`.
fn parse_columns(columns: Option<&str>, parquet: &ParquetSource) -> anyhow::Result<Projection> {
    let column_count = parquet.column_count();
    let Some(columns) = columns else {
        return Ok(Projection::all(column_count));
    };

    let mut result = Vec::new();
    for value in columns.split(',') {
        let value = value.trim();
        if value.is_empty() {
            bail!("empty column in --columns");
        }
        result.push(resolve_column(value, parquet)?);
    }

    Projection::columns(result, column_count)
}

/// One column by name or 0-based index. A column whose name looks like a
/// number is found by its name first.
fn resolve_column(value: &str, parquet: &ParquetSource) -> anyhow::Result<usize> {
    let column_count = parquet.column_count();
    let index = match parquet.schema().index_of(value) {
        Ok(index) => index,
        Err(_) => value.parse::<usize>().map_err(|_| {
            anyhow::anyhow!("unknown column {value:?}; give a column name or a 0-based index")
        })?,
    };
    if index >= column_count {
        bail!(
            "column index {index} is out of range; \
             file has {column_count} columns (0..{})",
            column_count.saturating_sub(1)
        );
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;

    /// Five rows, `id` 1..=5, `name` a..=e and `7` with one value, in row
    /// groups of two.
    fn fixture() -> (tempfile::TempDir, String) {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("t.parquet");
        write_fixture(&path);
        let path = path.to_string_lossy().into_owned();
        (folder, path)
    }

    fn write_fixture(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("7", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec![Some("x"), None, None, None, None])),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Six rows with ties and nulls in `group`, in row groups of two.
    fn sort_fixture() -> (tempfile::TempDir, String) {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("s.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("group", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    None,
                    Some("a"),
                    Some("b"),
                    None,
                    Some("a"),
                ])),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let path = path.to_string_lossy().into_owned();
        (folder, path)
    }

    fn parse(arguments: &[&str]) -> Args {
        Args::try_parse_from(std::iter::once("cli").chain(arguments.iter().copied())).unwrap()
    }

    /// Runs with two-row batches, so streams cross batch and row-group edges.
    fn output(arguments: &[&str]) -> String {
        let mut out = Vec::new();
        run(&parse(arguments), &mut io::empty(), &mut out, 2).unwrap();
        String::from_utf8(out).unwrap()
    }

    /// A pipe whose reader has gone away.
    struct ClosedPipe;

    impl Write for ClosedPipe {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn bounds_rows_held_for_the_table() {
        assert_eq!(validate_head(None).unwrap(), 10);
        assert_eq!(validate_head(Some(MAX_HEAD_ROWS)).unwrap(), 1_000);
        assert!(validate_head(Some(MAX_HEAD_ROWS + 1)).is_err());
    }

    #[test]
    fn prints_summary_and_filtered_preview() {
        let (_folder, path) = fixture();
        let text = output(&[&path, "--filter", "id >= 2", "--columns", "1"]);

        assert!(text.starts_with("Rows: 5\nColumns: 3\n"), "{text}");
        assert!(text.contains("  0 id                   Int64\n"), "{text}");
        assert!(text.contains("Preview: 4 rows × 1 column"), "{text}");
        assert!(text.contains("│   1 │ b    │"), "{text}");
    }

    #[test]
    fn quiet_table_skips_the_summary_and_numbers_rows_from_the_offset() {
        let (_folder, path) = fixture();
        let text = output(&[&path, "-q", "--offset", "3", "--columns", "name"]);

        assert!(text.starts_with("\nPreview: 2 rows × 1 column"), "{text}");
        assert!(text.contains("│   4 │ d    │"), "{text}");
        assert!(text.contains("│   5 │ e    │"), "{text}");
    }

    #[test]
    fn streams_csv_and_json_lines_with_offset_head_and_filter() {
        let (_folder, path) = fixture();
        assert_eq!(
            output(&[&path, "--format", "csv", "--columns", "name,id"]),
            "name,id\na,1\nb,2\nc,3\nd,4\ne,5\n"
        );
        assert_eq!(
            output(&[&path, "--format", "csv", "--offset", "1", "--head", "3"]),
            "id,name,7\n2,b,\n3,c,\n4,d,\n"
        );
        assert_eq!(
            output(&[
                &path,
                "--format",
                "jsonl",
                "--filter",
                "id != 2",
                "--offset",
                "1",
                "--head",
                "2",
                "--columns",
                "id",
            ]),
            "{\"id\":3}\n{\"id\":4}\n"
        );
        assert_eq!(
            output(&[&path, "--format", "csv", "--offset", "9"]),
            "id,name,7\n"
        );
        assert_eq!(
            output(&[&path, "--format", "csv", "--filter", "id > 9"]),
            "id,name,7\n"
        );
    }

    #[test]
    fn streams_parquet_and_arrow_that_open_again() {
        let (folder, path) = fixture();
        for (format, extension) in [("parquet", "parquet"), ("arrow", "arrow")] {
            let mut out = Vec::new();
            run(
                &parse(&[&path, "--format", format, "--filter", "id >= 3"]),
                &mut io::empty(),
                &mut out,
                2,
            )
            .unwrap();
            let written = folder.path().join(format!("out.{extension}"));
            std::fs::write(&written, out).unwrap();

            let source = ParquetSource::open(&written).unwrap();
            assert_eq!(source.row_count(), 3, "{format}");
            assert_eq!(source.schema().field(1).name(), "name", "{format}");
        }
    }

    #[test]
    fn counts_rows_from_metadata_or_by_scanning() {
        let (_folder, path) = fixture();
        assert_eq!(output(&[&path, "--count"]), "5\n");
        assert_eq!(output(&[&path, "--count", "--filter", "id >= 2"]), "4\n");
        assert_eq!(
            output(&[&path, "--count", "--filter", "name LIKE 'a%' OR id = 5"]),
            "2\n"
        );
        assert_eq!(output(&[&path, "--count", "--filter", "id > 9"]), "0\n");
    }

    #[test]
    fn rejects_bad_arguments_before_printing_anything() {
        let (_folder, path) = fixture();
        for arguments in [
            vec![path.as_str(), "--filter", "missing > 1"],
            vec![&path, "--filter", "id >> 1"],
            vec![&path, "--filter", "name > 'a' AND id = 'x'"],
            vec![&path, "--columns", "nope"],
            vec![&path, "--head", "1001"],
            vec![&path, "--format", "csv", "--filter", "missing > 1"],
            vec![&path, "--sort", "nope"],
            vec![&path, "--sort", "id,id:desc"],
            vec![&path, "--sort", "id:sideways"],
            vec![&path, "--sort", "id,"],
            vec![&path, "--stats", "--head", "1001"],
            vec![&path, "--schema", "--columns", "nope"],
        ] {
            let mut out = Vec::new();
            assert!(
                run(&parse(&arguments), &mut io::empty(), &mut out, 2).is_err(),
                "{arguments:?}"
            );
            assert!(out.is_empty(), "printed before failing on {arguments:?}");
        }
    }

    #[test]
    fn streams_more_than_the_table_limit() {
        let (_folder, path) = fixture();
        // --head over 1,000 is only refused for the table.
        let text = output(&[&path, "--format", "csv", "--head", "5000"]);
        assert_eq!(text.lines().count(), 6);
    }

    #[test]
    fn treats_a_closed_pipe_as_the_end_of_output() {
        let (_folder, path) = fixture();
        for format in ["table", "csv", "jsonl", "parquet", "arrow"] {
            for mode in [&[][..], &["--schema"], &["--stats"], &["--sort", "id:desc"]] {
                let mut arguments = vec![path.as_str(), "--format", format];
                arguments.extend_from_slice(mode);
                let error =
                    run(&parse(&arguments), &mut io::empty(), &mut ClosedPipe, 2).unwrap_err();
                assert!(is_broken_pipe(&error), "{arguments:?}: {error:?}");
            }
        }
        assert!(!is_broken_pipe(&anyhow::anyhow!("other")));
    }

    #[test]
    fn finds_columns_by_name_before_index() {
        let (_folder, path) = fixture();
        let parquet = ParquetSource::open(&path).unwrap();
        let columns = |text| parse_columns(Some(text), &parquet).map(|p| p.as_slice().to_vec());

        assert_eq!(columns("name, 0").unwrap(), [1, 0]);
        // "7" is a column name here, not index 7.
        assert_eq!(columns("7").unwrap(), [2]);
        assert!(columns("3").is_err());
        assert!(columns("nope").is_err());
        assert!(columns("0,").is_err());
        assert!(columns("id,0").is_err());
    }

    #[test]
    fn prints_the_schema_in_every_format() {
        let (_folder, path) = fixture();
        assert_eq!(
            output(&[&path, "--schema", "--format", "csv"]),
            "index,name,type,nullable\n0,id,Int64,false\n1,name,Utf8,false\n2,7,Utf8,true\n"
        );
        assert_eq!(
            output(&[&path, "--schema", "--format", "jsonl", "--columns", "7"]),
            "{\"index\":2,\"name\":\"7\",\"type\":\"Utf8\",\"nullable\":true}\n"
        );
        let table = output(&[&path, "--schema"]);
        assert!(table.starts_with("\nSchema: 3 rows × 4 columns"), "{table}");
        assert!(table.contains("│ id   │ Int64 │ false    │"), "{table}");
    }

    #[test]
    fn prints_row_group_statistics_without_reading_rows() {
        let (_folder, path) = fixture();
        assert_eq!(
            output(&[&path, "--stats", "--format", "csv", "--columns", "id,7"]),
            concat!(
                "row_group,column,rows,nulls,min,max,distinct\n",
                "0,id,2,0,1,2,\n",
                "0,7,2,1,x,x,\n",
                "1,id,2,0,3,4,\n",
                "1,7,2,2,,,\n",
                "2,id,1,0,5,5,\n",
                "2,7,1,1,,,\n",
            )
        );
        let table = output(&[&path, "--stats", "--head", "2"]);
        assert!(
            table.starts_with("\nStatistics: 2 rows × 7 columns"),
            "{table}"
        );
        assert!(
            table.ends_with(
                "2 of 9 rows shown; use --head (up to 1000) or --format csv for the rest\n"
            ),
            "{table}"
        );
        // Streamed formats show every row unless --head is given.
        let csv = output(&[&path, "--stats", "--format", "csv"]);
        assert_eq!(csv.lines().count(), 1 + 9);
    }

    #[test]
    fn sorts_with_nulls_last_ties_in_file_order_and_several_keys() {
        let (_folder, path) = sort_fixture();
        assert_eq!(
            output(&[&path, "--format", "csv", "--sort", "group"]),
            "id,group\n3,a\n6,a\n1,b\n4,b\n2,\n5,\n"
        );
        assert_eq!(
            output(&[&path, "--format", "csv", "--sort", "group:desc,id:desc"]),
            "id,group\n4,b\n1,b\n6,a\n3,a\n5,\n2,\n"
        );
        assert_eq!(
            output(&[&path, "--format", "csv", "--sort", "1:DESC , 0"]),
            "id,group\n1,b\n4,b\n3,a\n6,a\n2,\n5,\n"
        );
    }

    #[test]
    fn sorts_filtered_rows_with_offset_and_head_in_every_output() {
        let (_folder, path) = sort_fixture();
        let sorted = [
            &path[..],
            "--sort",
            "id:desc",
            "--filter",
            "group IS NOT NULL",
            "--offset",
            "1",
            "--head",
            "2",
        ];
        let mut csv = sorted.to_vec();
        csv.extend(["--format", "csv"]);
        assert_eq!(output(&csv), "id,group\n4,b\n3,a\n");

        let table = output(&sorted);
        assert!(table.contains("│   2 │    4 │ b     │"), "{table}");
        assert!(table.contains("│   3 │    3 │ a     │"), "{table}");
    }

    #[test]
    fn streams_a_sort_that_spills_to_disk() {
        let (_folder, path) = sort_fixture();
        let parquet = ParquetSource::open(&path).unwrap();
        let projection = Projection::all(2);
        let schema = parquet.read_window(0, 0, &projection).unwrap().schema();
        let sort = sort::parse_sort("group:desc,id", &parquet).unwrap();
        for (offset, limit, expected) in
            [(0, u64::MAX, vec![1, 4, 3, 6, 2, 5]), (1, 3, vec![4, 3, 6])]
        {
            let rows = Rows {
                parquet: &parquet,
                filter: None,
                sort: Some(&sort),
                projection: &projection,
                schema: &schema,
                offset,
                batch_rows: 2,
                // Two rows in memory: six rows spill into runs.
                sort_budget: SortBudget::new(2, 1 << 20).unwrap(),
                progress: &Progress::hidden(),
            };
            let mut ids = Vec::new();
            rows.for_each(limit, &mut |batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.extend(column.values().iter().copied());
                Ok(())
            })
            .unwrap();
            assert_eq!(ids, expected, "offset {offset}, limit {limit}");
        }
    }

    /// Runs with `stdin` as standard input.
    fn output_with_stdin(arguments: &[&str], stdin: &[u8]) -> String {
        let mut out = Vec::new();
        run(&parse(arguments), &mut &stdin[..], &mut out, 2).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn reads_csv_json_and_parquet_from_standard_input() {
        assert_eq!(
            output_with_stdin(&["-", "--format", "csv"], b"id,name\n1,a\n2,b\n"),
            "id,name\n1,a\n2,b\n"
        );
        assert_eq!(
            output_with_stdin(&["-", "--count"], b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n"),
            "3\n"
        );
        let (_folder, path) = fixture();
        let parquet = std::fs::read(&path).unwrap();
        assert_eq!(
            output_with_stdin(
                &[
                    "-",
                    "--format",
                    "csv",
                    "--filter",
                    "id > 3",
                    "--columns",
                    "name"
                ],
                &parquet
            ),
            "name\nd\ne\n"
        );

        let mut out = Vec::new();
        let error = run(&parse(&["-"]), &mut io::empty(), &mut out, 2).unwrap_err();
        assert!(format!("{error:#}").contains("standard input is empty"));
        assert!(out.is_empty());
    }

    #[test]
    fn converts_to_a_file_in_the_format_its_extension_names() {
        let (folder, path) = fixture();
        let target = |name: &str| folder.path().join(name);
        let convert = |arguments: &[&str]| {
            run_to_file(
                &parse(arguments),
                Path::new(arguments[2]),
                &mut io::empty(),
                2,
            )
        };

        let parquet = target("out.parquet");
        convert(&[
            &path,
            "-o",
            parquet.to_str().unwrap(),
            "--filter",
            "id >= 2",
        ])
        .unwrap();
        assert_eq!(ParquetSource::open(&parquet).unwrap().row_count(), 4);

        let csv = target("out.csv");
        convert(&[&path, "-o", csv.to_str().unwrap(), "--columns", "name"]).unwrap();
        assert_eq!(
            std::fs::read_to_string(&csv).unwrap(),
            "name\na\nb\nc\nd\ne\n"
        );

        // --format wins over the extension; --schema and --count write too.
        let text = target("schema.txt");
        convert(&[
            &path,
            "-o",
            text.to_str().unwrap(),
            "--schema",
            "--format",
            "jsonl",
        ])
        .unwrap();
        assert_eq!(std::fs::read_to_string(&text).unwrap().lines().count(), 3);
        let count = target("count.txt");
        convert(&[&path, "-o", count.to_str().unwrap(), "--count"]).unwrap();
        assert_eq!(std::fs::read_to_string(&count).unwrap(), "5\n");
    }

    #[test]
    fn leaves_no_file_when_the_output_cannot_be_written() {
        let (folder, path) = fixture();
        for (name, extra) in [
            // No format from this extension.
            ("out.tsv", None),
            // A bad filter fails after the temporary file was created.
            ("bad.csv", Some(["--filter", "missing > 1"])),
        ] {
            let output = folder.path().join(name);
            let mut arguments = vec![path.as_str(), "-o", output.to_str().unwrap()];
            arguments.extend(extra.iter().flatten());
            assert!(
                run_to_file(&parse(&arguments), &output, &mut io::empty(), 2).is_err(),
                "{name}"
            );
            assert!(!output.exists(), "{name}");
        }
        // Only the fixture is left: no temporary files.
        assert_eq!(std::fs::read_dir(folder.path()).unwrap().count(), 1);
    }

    #[test]
    fn refuses_to_overwrite_the_file_being_read() {
        let (_folder, path) = fixture();
        let before = std::fs::read(&path).unwrap();
        let arguments = [path.as_str(), "-o", &path, "--format", "parquet"];
        let error =
            run_to_file(&parse(&arguments), Path::new(&path), &mut io::empty(), 2).unwrap_err();
        assert!(
            format!("{error:#}").contains("is the file being read"),
            "{error:#}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    /// `labels.csv` next to the fixture: labels for ids 1 and 3.
    fn labels(folder: &tempfile::TempDir) -> String {
        let path = folder.path().join("labels.csv");
        std::fs::write(&path, "id,label\n1,one\n3,three\n").unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn runs_sql_with_group_by_on_the_file_by_either_name() {
        let (_folder, path) = fixture();
        let totals = output(&[
            &path,
            "--format",
            "csv",
            "--sql",
            "SELECT COUNT(*), SUM(id) FROM dataset WHERE id > 1",
        ]);
        assert_eq!(totals.lines().nth(1), Some("4,14"), "{totals}");
        let grouped = output(&[
            &path,
            "--format",
            "csv",
            "--sql",
            "SELECT `7`, COUNT(*) FROM dataset GROUP BY `7`",
        ]);
        let mut groups = grouped.lines().skip(1).collect::<Vec<_>>();
        groups.sort_unstable();
        assert_eq!(groups, [",4", "x,1"], "{grouped}");
        // The file's own name works as well as `dataset`.
        assert_eq!(
            output(&[
                &path,
                "--format",
                "csv",
                "--sql",
                "SELECT name FROM t WHERE id >= 4 ORDER BY id DESC",
            ]),
            "name\ne\nd\n"
        );
        let table = output(&[&path, "--sql", "SELECT id, name FROM dataset LIMIT 2"]);
        assert!(table.starts_with("\nResult: 2 rows × 2 columns"), "{table}");
        assert!(!table.contains("Rows: 5"), "no file summary: {table}");
    }

    #[test]
    fn joins_other_files_named_by_table() {
        let (folder, path) = fixture();
        let labels = labels(&folder);
        let join = "SELECT t.name, l.label FROM dataset AS t \
                    JOIN labels AS l ON t.id = l.id ORDER BY t.id";
        assert_eq!(
            output(&[&path, "--format", "csv", "--table", &labels, "--sql", join]),
            "name,label\na,one\nc,three\n"
        );
        // NAME=PATH names it; a subquery can use it too.
        let named = format!("lookup={labels}");
        assert_eq!(
            output(&[
                &path,
                "--format",
                "csv",
                "--table",
                &named,
                "--sql",
                "SELECT name FROM dataset WHERE id NOT IN (SELECT id FROM lookup) ORDER BY id",
            ]),
            "name\nb\nd\ne\n"
        );
    }

    #[test]
    fn cuts_sql_results_with_offset_and_head_and_writes_files() {
        let (folder, path) = fixture();
        let sql = "SELECT id FROM dataset ORDER BY id DESC";
        assert_eq!(
            output(&[
                &path, "--format", "csv", "--offset", "1", "--head", "2", "--sql", sql
            ]),
            "id\n4\n3\n"
        );
        let table = output(&[&path, "--offset", "3", "--sql", sql]);
        assert!(table.contains("│   4 │    2 │"), "{table}");

        let parquet = folder.path().join("result.parquet");
        let arguments = [path.as_str(), "-o", parquet.to_str().unwrap(), "--sql", sql];
        run_to_file(&parse(&arguments), &parquet, &mut io::empty(), 2).unwrap();
        assert_eq!(ParquetSource::open(&parquet).unwrap().row_count(), 5);
    }

    #[test]
    fn reports_sql_errors_before_printing_anything() {
        let (folder, path) = fixture();
        let labels = labels(&folder);
        let taken = format!("dataset={labels}");
        for arguments in [
            vec![path.as_str(), "--sql", "SELEC id FROM dataset"],
            vec![&path, "--sql", "SELECT nope FROM dataset"],
            vec![&path, "--sql", "SELECT id FROM missing"],
            vec![
                &path,
                "--sql",
                "SELECT id FROM dataset",
                "--table",
                "nope.csv",
            ],
            vec![&path, "--sql", "SELECT id FROM dataset", "--table", &taken],
            vec![
                &path,
                "--format",
                "csv",
                "--sql",
                "SELECT id FROM dataset UNION SELECT id FROM dataset",
            ],
        ] {
            let mut out = Vec::new();
            assert!(
                run(&parse(&arguments), &mut io::empty(), &mut out, 2).is_err(),
                "{arguments:?}"
            );
            assert!(out.is_empty(), "printed before failing on {arguments:?}");
        }
        // --table needs --sql, and --sql replaces the row options.
        let parses = |arguments: &[&str]| {
            Args::try_parse_from(std::iter::once("cli").chain(arguments.iter().copied())).is_ok()
        };
        assert!(!parses(&[&path, "--table", &labels]));
        assert!(!parses(&[&path, "--sql", "SELECT 1", "--filter", "id > 1"]));
        assert!(!parses(&[&path, "--sql", "SELECT 1", "--count"]));
    }

    #[test]
    fn runs_sql_on_standard_input() {
        assert_eq!(
            output_with_stdin(
                &[
                    "-",
                    "--format",
                    "csv",
                    "--sql",
                    "SELECT b FROM stdin WHERE a = 2"
                ],
                b"a,b\n1,x\n2,y\n"
            ),
            "b\ny\n"
        );
    }

    const PROFILE_HEADER: &str =
        "column,type,rows,nulls,distinct,distinct_capped,min,max,mean,top_value,top_count\n";

    #[test]
    fn profiles_every_column_or_the_chosen_ones_in_any_format() {
        let (_folder, path) = fixture();
        assert_eq!(
            output(&[&path, "--profile", "--format", "csv"]),
            format!(
                "{PROFILE_HEADER}id,Int64,5,0,5,false,1,5,3,1,1\n\
                 name,Utf8,5,0,5,false,a,e,,a,1\n\
                 7,Utf8,5,4,1,false,x,x,,x,1\n"
            )
        );
        // Only matching rows, and only --columns.
        assert_eq!(
            output(&[
                &path,
                "--profile",
                "--format",
                "csv",
                "--columns",
                "id",
                "--filter",
                "id >= 4",
            ]),
            format!("{PROFILE_HEADER}id,Int64,2,0,2,false,4,5,4.5,4,1\n")
        );
        let table = output(&[&path, "--profile"]);
        assert!(
            table.starts_with("\nProfile: 3 rows × 11 columns"),
            "{table}"
        );
    }

    #[test]
    fn profiles_wide_value_ranges_and_nested_columns_within_their_limits() {
        use arrow_array::{ListArray, types::Int32Type};

        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("wide.parquet");
        let rows = parquet_reader::profile::FILE_PROFILE_DISTINCT_VALUES as i64 + 100;
        let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(
            (0..rows).map(|row| (row % 2 == 0).then(|| vec![Some(1)])),
        );
        let batch = RecordBatch::try_from_iter(vec![
            (
                "n",
                Arc::new(Int64Array::from_iter_values(0..rows)) as arrow_array::ArrayRef,
            ),
            ("tags", Arc::new(tags)),
        ])
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let path = path.to_string_lossy().into_owned();

        let csv = output(&[&path, "--profile", "--format", "csv"]);
        let lines = csv.lines().collect::<Vec<_>>();
        // Distinct values stop at the cap; min, max and mean still cover every
        // row; the most frequent value is left out as it would be a guess.
        assert_eq!(
            lines[1],
            format!("n,Int64,{rows},0,1000,true,0,{},549.5,,", rows - 1)
        );
        // A list column: its type and what the footer says, nothing measured.
        let footer_nulls = ParquetSource::open(&path).unwrap().metadata().columns()[1]
            .row_group_statistics
            .iter()
            .map(|stats| stats.as_ref().and_then(|stats| stats.null_count))
            .sum::<Option<u64>>()
            .map_or(String::new(), |nulls| nulls.to_string());
        assert_eq!(
            lines[2],
            format!("tags,List(Int32),{rows},{footer_nulls},0,false,,,,,"),
            "{csv}"
        );
    }

    #[test]
    fn refuses_profiles_wider_than_the_limit_before_printing() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("many.parquet");
        let columns = parquet_reader::profile::MAX_FILE_PROFILE_COLUMNS + 1;
        let batch = RecordBatch::try_from_iter((0..columns).map(|column| {
            (
                format!("c{column}"),
                Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef,
            )
        }))
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let path = path.to_string_lossy().into_owned();

        let mut out = Vec::new();
        let error = run(&parse(&[&path, "--profile"]), &mut io::empty(), &mut out, 2).unwrap_err();
        assert!(format!("{error:#}").contains("--columns"), "{error:#}");
        assert!(out.is_empty());
        assert_eq!(
            output(&[&path, "--profile", "--columns", "c0,c1", "--format", "csv"])
                .lines()
                .count(),
            3
        );

        let parses = |arguments: &[&str]| {
            Args::try_parse_from(std::iter::once("cli").chain(arguments.iter().copied())).is_ok()
        };
        assert!(!parses(&[&path, "--profile", "--stats"]));
        assert!(!parses(&[&path, "--profile", "--sort", "c0"]));
        assert!(!parses(&[
            &path,
            "--profile",
            "--sql",
            "SELECT c0 FROM dataset"
        ]));
    }
}
