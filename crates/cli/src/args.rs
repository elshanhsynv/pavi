use clap::{Parser, ValueEnum};

const EXIT_CODES: &str = "\
Exit codes:
  0  success, also when the reader of standard output stops early (`| head`)
  1  the file, a column, the filter or the output failed
  2  invalid arguments";

/// Preview or stream the rows of a Parquet file, a folder of Parquet files,
/// a CSV, JSON or Arrow file, or an https/s3/gs/az URL.
///
/// By default prints a summary (row count, schema, row groups) and the first
/// rows as a table. With --format csv, jsonl, parquet or arrow it writes only
/// the rows, all of them unless --head is given, to standard output.
#[derive(Parser)]
#[command(name = "cli", version, about, after_help = EXIT_CODES)]
pub struct Args {
    /// File, folder or URL to read
    pub file: String,

    /// Number of rows to output [default: 10 for the table, at most 1,000;
    /// every row for the other formats]
    #[arg(long, value_name = "N")]
    pub head: Option<u64>,

    /// Skip this many rows first (with --filter: this many matching rows)
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub offset: u64,

    /// Columns to output, comma-separated, by name or 0-based index
    /// (e.g. "id,name" or "0,3"); a name wins over an index
    #[arg(long, value_name = "LIST")]
    pub columns: Option<String>,

    /// Only rows that match, e.g. "DEP_DELAY > 60 AND ORIGIN IN ('JFK', 'LGA')"
    #[arg(long, value_name = "EXPR")]
    pub filter: Option<String>,

    /// Output format
    #[arg(long, value_enum, default_value_t = Format::Table)]
    pub format: Format,

    /// Print only the table, without the summary and schema
    #[arg(long, short)]
    pub quiet: bool,

    /// Print only the number of rows (that match --filter, if given)
    #[arg(long, conflicts_with_all = ["head", "offset", "columns", "format", "quiet"])]
    pub count: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Format {
    /// Summary and a box-drawn table (for people)
    Table,
    /// CSV with a header line; nulls are empty fields
    Csv,
    /// One JSON object per line
    #[value(alias = "json")]
    Jsonl,
    /// A Parquet file
    Parquet,
    /// An Arrow IPC file (Feather v2)
    Arrow,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn describes_every_argument() {
        Args::command().debug_assert();
        for argument in Args::command().get_arguments() {
            assert!(argument.get_help().is_some(), "{}", argument.get_id());
        }
    }

    #[test]
    fn count_excludes_output_options() {
        assert!(Args::try_parse_from(["cli", "f", "--count", "--head", "3"]).is_err());
        assert!(Args::try_parse_from(["cli", "f", "--count", "--filter", "a > 1"]).is_ok());
        let args = Args::try_parse_from(["cli", "f", "--format", "json"]).unwrap();
        assert_eq!(args.format, Format::Jsonl);
    }
}
