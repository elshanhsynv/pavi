mod args;
mod output;

use std::io::{self, BufWriter, Write};
use std::process::ExitCode;

use anyhow::{Context, bail};
use args::{Args, Format};
use clap::Parser;
use output::print_batch;
use parquet_reader::export::{BatchWriter, ExportFormat};
use parquet_reader::{FilterExpr, PAGE_ROWS, ParquetSource, Projection};

/// Most rows the table format prints; it holds them all in memory to size columns.
const MAX_HEAD_ROWS: u64 = 1_000;
/// Rows shown in the table when `--head` is not given.
const DEFAULT_HEAD_ROWS: u64 = 10;
/// Rows read per step when streaming rows or counting matches: memory stays at
/// one batch of the projected (or, for `--count`, the filter's) columns.
const STREAM_BATCH_ROWS: u64 = PAGE_ROWS;

fn main() -> ExitCode {
    let args = Args::parse();
    // `Stdout`, not its lock: the Parquet writer needs a `Send` output.
    let mut out = BufWriter::new(io::stdout());
    let result = run(&args, &mut out, STREAM_BATCH_ROWS)
        .and_then(|()| out.flush().context("write to standard output"));

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

/// Checks every argument against the file, then writes the output.
/// Nothing is written until the arguments are known to be valid.
fn run(args: &Args, out: &mut (impl Write + Send), batch_rows: u64) -> anyhow::Result<()> {
    let table_head = match args.format {
        Format::Table if !args.count => Some(validate_head(args.head)?),
        _ => None,
    };

    let parquet = ParquetSource::open(&args.file)?;
    let projection = parse_columns(args.columns.as_deref(), &parquet)?;
    let filter = args
        .filter
        .as_deref()
        .map(|text| parse_filter(&parquet, text))
        .transpose()?;

    if args.count {
        let rows = count_rows(&parquet, filter.as_ref(), batch_rows)?;
        writeln!(out, "{rows}")?;
        return Ok(());
    }

    let Some(head) = table_head else {
        let format = export_format(args.format)?;
        let limit = args.head.unwrap_or(u64::MAX);
        return stream_rows(
            &parquet,
            filter.as_ref(),
            &projection,
            args.offset,
            limit,
            batch_rows,
            // An empty window carries the projected schema.
            BatchWriter::new(
                format,
                out,
                parquet.read_window(0, 0, &projection)?.schema(),
            )?,
        );
    };

    if !args.quiet {
        print_summary(&parquet, out)?;
    }

    let batch = if let Some(filter) = &filter {
        parquet.read_filtered_window(filter, args.offset, head, &projection)?
    } else {
        let first_row = args.offset.min(parquet.row_count());
        parquet.read_window(first_row, head, &projection)?
    };

    print_batch(&batch, args.offset.saturating_add(1), out)?;
    Ok(())
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

/// Writes up to `limit` rows after skipping `offset` (matching) rows, one
/// batch of at most `batch_rows` rows at a time.
fn stream_rows<W: Write + Send>(
    parquet: &ParquetSource,
    filter: Option<&FilterExpr>,
    projection: &Projection,
    offset: u64,
    limit: u64,
    batch_rows: u64,
    mut writer: BatchWriter<W>,
) -> anyhow::Result<()> {
    let total = parquet.row_count();
    match filter {
        None => {
            let mut row = offset.min(total);
            let end = row.saturating_add(limit).min(total);
            while row < end {
                let count = (end - row).min(batch_rows);
                writer.write(&parquet.read_window(row, to_usize(count)?, projection)?)?;
                row += count;
            }
        }
        Some(filter) => {
            let mut next_row = 0;
            let mut skip = offset;
            let mut left = limit;
            while left > 0 && next_row < total {
                let wanted = skip.saturating_add(left).min(batch_rows);
                let window = parquet.read_filtered_from(
                    filter,
                    next_row,
                    to_usize(wanted)?,
                    projection,
                    &|_| false,
                )?;
                if window.next_row <= next_row {
                    bail!("filtered read did not move past row {next_row}");
                }
                next_row = window.next_row;

                let mut batch = window.batch;
                let skipped = skip.min(batch.num_rows() as u64);
                skip -= skipped;
                let kept = (batch.num_rows() as u64 - skipped).min(left);
                batch = batch.slice(to_usize(skipped)?, to_usize(kept)?);
                left -= kept;
                if batch.num_rows() > 0 {
                    writer.write(&batch)?;
                }
            }
        }
    }
    writer.finish()
}

/// The row count from the footer, or a scan that reads only the filter's columns.
fn count_rows(
    parquet: &ParquetSource,
    filter: Option<&FilterExpr>,
    batch_rows: u64,
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
            &|_| false,
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
        writeln!(out, "{:>3} {:<20} {:?}", i, field.name(), field.data_type())?;
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

/// Columns by name or 0-based index. A column whose name looks like a
/// number is found by its name first.
fn parse_columns(columns: Option<&str>, parquet: &ParquetSource) -> anyhow::Result<Projection> {
    let column_count = parquet.column_count();
    let Some(columns) = columns else {
        return Ok(Projection::all(column_count));
    };
    let schema = parquet.schema();

    let mut result = Vec::new();
    for value in columns.split(',') {
        let value = value.trim();
        if value.is_empty() {
            bail!("empty column in --columns");
        }

        let index = match schema.index_of(value) {
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
        result.push(index);
    }

    Projection::columns(result, column_count)
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

    /// Five rows, `id` 1..=5 and `name` a..=e, in row groups of two.
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

    fn parse(arguments: &[&str]) -> Args {
        Args::try_parse_from(std::iter::once("cli").chain(arguments.iter().copied())).unwrap()
    }

    /// Runs with two-row batches, so streams cross batch and row-group edges.
    fn output(arguments: &[&str]) -> String {
        let mut out = Vec::new();
        run(&parse(arguments), &mut out, 2).unwrap();
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
        ] {
            let mut out = Vec::new();
            assert!(
                run(&parse(&arguments), &mut out, 2).is_err(),
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
            let error = run(&parse(&[&path, "--format", format]), &mut ClosedPipe, 2).unwrap_err();
            assert!(is_broken_pipe(&error), "{format}: {error:?}");
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
}
