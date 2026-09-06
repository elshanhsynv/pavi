mod args;
mod output;

use args::Args;
use clap::Parser;
use output::print_batch;
use parquet_reader::{FilterExpr, ParquetSource, Projection};

const MAX_HEAD_ROWS: usize = 1_000;

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let head = validate_head(args.head)?;

    let parquet = ParquetSource::open(&args.file)?;
    let projection = parse_columns(args.columns.as_deref(), parquet.column_count())?;

    println!("Rows: {}", parquet.row_count());
    println!("Columns: {}", parquet.column_count());
    println!("Row groups: {}", parquet.row_groups().len());

    println!();

    println!("Schema:");

    for (i, field) in parquet.schema().fields().iter().enumerate() {
        println!("{:>3} {:<20} {:?}", i, field.name(), field.data_type());
    }

    println!();

    for group in parquet.row_groups().iter().take(10) {
        if group.row_count == 0 {
            println!("row group {:>3}: empty", group.index);
        } else {
            let last_row = group.first_row + group.row_count - 1;
            println!(
                "row group {:>3}: rows {}..={} ({} rows)",
                group.index, group.first_row, last_row, group.row_count
            );
        }
    }

    if parquet.row_groups().len() > 10 {
        println!("...");
    }

    let batch = if let Some(filter) = args.filter.as_deref() {
        let filter = FilterExpr::parse(filter)?;
        parquet.read_filtered_window(&filter, 0, head, &projection)?
    } else {
        parquet.read_window(0, head, &projection)?
    };

    print_batch(&batch);

    Ok(())
}

fn validate_head(head: usize) -> anyhow::Result<usize> {
    if head > MAX_HEAD_ROWS {
        anyhow::bail!("--head must be at most {MAX_HEAD_ROWS}");
    }
    Ok(head)
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
    use super::*;

    #[test]
    fn bounds_user_requested_preview_rows() {
        assert_eq!(validate_head(MAX_HEAD_ROWS).unwrap(), MAX_HEAD_ROWS);
        assert!(validate_head(MAX_HEAD_ROWS + 1).is_err());
    }
}
