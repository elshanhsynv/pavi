use clap::Parser;
use parquet_reader::ParquetSource;

#[derive(Parser)]
struct Args {
    file: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let parquet = ParquetSource::open(args.file)?;

    println!("Rows: {}", parquet.row_count());

    println!();

    println!("Columns:");

    for (i, field) in parquet.schema().fields().iter().enumerate() {
        println!("{:>3} {:<20} {:?}", i, field.name(), field.data_type());
    }

    Ok(())
}
