use clap::Parser;
use parquet_reader::{FilterExpr, ParquetSource, unsupported_sort_message, value::format_cell};
use arrow_array::RecordBatch;

#[derive(Parser)]
struct Args {
    file: String,

    #[arg(long, default_value_t = 10)]
    head: usize,

    #[arg(long)]
    columns: Option<String>,

    #[arg(long)]
    filter: Option<String>,

    #[arg(long)]
    sort: Option<usize>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let parquet = ParquetSource::open(&args.file)?;
    let columns = parse_columns(args.columns.as_deref())?;

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
        println!(
            "row group {:>3}: rows {}..{} ({})",
            group.index,
            group.first_row,
            group.first_row + group.row_count,
            group.row_count
        );
    }

    if parquet.row_groups().len() > 10 {
        println!("...");
    }

    if args.sort.is_some() {
        println!();
        println!("{}", unsupported_sort_message());
    }

    let batch = if let Some(filter) = args.filter.as_deref() {
        let filter = FilterExpr::parse(filter)?;
        parquet.read_filtered_window(&filter, 0, args.head, &columns)?
    } else {
        parquet.read_window(0, args.head, &columns)?
    };

    print_batch(&batch);

    Ok(())
}

fn parse_columns(columns: Option<&str>) -> anyhow::Result<Vec<usize>> {
    let Some(columns) = columns else {
        return Ok(Vec::new());
    };

    columns
        .split(',')
        .map(|column| Ok(column.trim().parse()?))
        .collect()
}

const MIN_COLUMN_WIDTH: usize = 4;
const MAX_COLUMN_WIDTH: usize = 32;

fn print_batch(batch: &RecordBatch) {
    let rows = batch.num_rows();
    let columns = batch.num_columns();

    println!();
    println!("Preview: {rows} rows × {columns} columns");
    println!();

    if columns == 0 {
        println!("(no columns)");
        return;
    }

    let headers: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect();

    let cells: Vec<Vec<String>> = (0..rows)
        .map(|row| {
            (0..columns)
                .map(|column| format_cell(batch.column(column).as_ref(), row))
                .collect()
        })
        .collect();

    // First column is the row number.
    let mut widths = Vec::with_capacity(columns + 1);

    let row_number_width = rows.to_string().len().max(1).max(3);

    widths.push(row_number_width);

    for column in 0..columns {
        let header_width = headers[column].chars().count();

        let value_width = cells
            .iter()
            .map(|row| row[column].chars().count())
            .max()
            .unwrap_or(0);

        widths.push(
            header_width
                .max(value_width)
                .clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH),
        );
    }

    // Top.
    print!("┌");
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));

        if i + 1 == widths.len() {
            print!("┐");
        } else {
            print!("┬");
        }
    }
    println!();

    // Header.
    print!("│ {:>width$} │", "#", width = widths[0]);

    for (header, &width) in headers.iter().zip(&widths[1..]) {
        print!(" {:<width$} │", header, width = width);
    }

    println!();

    // Separator.
    print!("├");
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));

        if i + 1 == widths.len() {
            print!("┤");
        } else {
            print!("┼");
        }
    }
    println!();

    // Data.
    for (row_index, row) in cells.iter().enumerate() {
        print!("│ {:>width$} │", row_index + 1, width = widths[0]);

        for (value, &width) in row.iter().zip(&widths[1..]) {
            print!(" {:<width$} │", truncate(value, width), width = width);
        }

        println!();
    }

    // Bottom.
    print!("└");
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));

        if i + 1 == widths.len() {
            print!("┘");
        } else {
            print!("┴");
        }
    }
    println!();
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_owned();
    }

    if width <= 1 {
        return "…".to_owned();
    }

    let mut result: String = value.chars().take(width - 1).collect();

    result.push('…');
    result
}
