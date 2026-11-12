mod args;
mod output;

use std::io::{self, BufWriter, Write};
use std::process::ExitCode;

use anyhow::Context;
use args::Args;
use clap::Parser;
use output::print_batch;
use parquet_reader::{FilterExpr, ParquetSource, Projection};

const MAX_HEAD_ROWS: usize = 1_000;

fn main() -> ExitCode {
    let args = Args::parse();
    let mut out = BufWriter::new(io::stdout().lock());
    let result =
        run(&args, &mut out).and_then(|()| out.flush().context("write to standard output"));

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

/// Checks every argument against the file, then prints the summary and preview.
/// Nothing is written until the arguments are known to be valid.
fn run(args: &Args, out: &mut impl Write) -> anyhow::Result<()> {
    let head = validate_head(args.head)?;

    let parquet = ParquetSource::open(&args.file)?;
    let projection = parse_columns(args.columns.as_deref(), parquet.column_count())?;
    let filter = args
        .filter
        .as_deref()
        .map(|text| parse_filter(&parquet, text))
        .transpose()?;

    print_summary(&parquet, out)?;

    let batch = if let Some(filter) = &filter {
        parquet.read_filtered_window(filter, 0, head, &projection)?
    } else {
        parquet.read_window(0, head, &projection)?
    };

    print_batch(&batch, out)?;
    Ok(())
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

fn validate_head(head: usize) -> anyhow::Result<usize> {
    if head > MAX_HEAD_ROWS {
        anyhow::bail!("--head must be at most {MAX_HEAD_ROWS}");
    }
    Ok(head)
}

fn parse_filter(parquet: &ParquetSource, text: &str) -> anyhow::Result<FilterExpr> {
    let filter = FilterExpr::parse(text)?;
    parquet.validate_filter(&filter)?;
    Ok(filter)
}

fn parse_columns(columns: Option<&str>, column_count: usize) -> anyhow::Result<Projection> {
    let Some(columns) = columns else {
        return Ok(Projection::all(column_count));
    };

    let mut result = Vec::new();

    for value in columns.split(',') {
        let value = value.trim();

        if value.is_empty() {
            anyhow::bail!("empty column index in --columns");
        }

        let index: usize = value
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid column index: {value:?}"))?;

        if index >= column_count {
            anyhow::bail!(
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

    use super::*;

    fn write_fixture(path: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn args(file: &Path, filter: Option<&str>, columns: Option<&str>) -> Args {
        Args {
            file: file.to_string_lossy().into_owned(),
            head: 10,
            columns: columns.map(str::to_owned),
            filter: filter.map(str::to_owned),
        }
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
    fn bounds_user_requested_preview_rows() {
        assert_eq!(validate_head(MAX_HEAD_ROWS).unwrap(), MAX_HEAD_ROWS);
        assert!(validate_head(MAX_HEAD_ROWS + 1).is_err());
    }

    #[test]
    fn prints_summary_and_filtered_preview() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("t.parquet");
        write_fixture(&path);

        let mut out = Vec::new();
        run(&args(&path, Some("id >= 2"), Some("1")), &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();

        assert!(text.starts_with("Rows: 3\nColumns: 2\n"), "{text}");
        assert!(text.contains("Preview: 2 rows × 1 column"), "{text}");
        assert!(
            text.contains("│ b    │") && text.contains("│ c    │"),
            "{text}"
        );
    }

    #[test]
    fn rejects_a_bad_filter_before_printing_anything() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("t.parquet");
        write_fixture(&path);

        for filter in ["missing > 1", "id >> 1", "name > 'a' AND id = 'x'"] {
            let mut out = Vec::new();
            assert!(run(&args(&path, Some(filter), None), &mut out).is_err());
            assert!(out.is_empty(), "printed before failing on {filter:?}");
        }
    }

    #[test]
    fn treats_a_closed_pipe_as_the_end_of_output() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("t.parquet");
        write_fixture(&path);

        let error = run(&args(&path, None, None), &mut ClosedPipe).unwrap_err();
        assert!(is_broken_pipe(&error));
        assert!(!is_broken_pipe(&anyhow::anyhow!("other")));
    }

    #[test]
    fn checks_column_indexes() {
        assert_eq!(parse_columns(Some("1, 0"), 2).unwrap().as_slice(), &[1, 0]);
        assert!(parse_columns(Some("2"), 2).is_err());
        assert!(parse_columns(Some("x"), 2).is_err());
        assert!(parse_columns(Some("0,"), 2).is_err());
        assert!(parse_columns(Some("0,0"), 2).is_err());
    }
}
