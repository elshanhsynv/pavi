use std::path::PathBuf;

use clap::builder::styling::{AnsiColor, Effects, Style, Styles};
use clap::{ArgAction, Parser, ValueEnum};

/// Help colours, in cargo's style. They are dropped when the output is not a
/// terminal, and follow `NO_COLOR` and `CLICOLOR_FORCE`.
pub const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .usage(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .literal(AnsiColor::Cyan.on_default().effects(Effects::BOLD))
    .placeholder(AnsiColor::Cyan.on_default())
    .valid(AnsiColor::Cyan.on_default().effects(Effects::BOLD))
    .invalid(AnsiColor::Yellow.on_default().effects(Effects::BOLD))
    .error(AnsiColor::Red.on_default().effects(Effects::BOLD));

/// Help is wrapped to the terminal, but no wider than this, so lines stay
/// easy to read on a wide screen.
const MAX_HELP_WIDTH: usize = 100;

/// Preview, convert, query and profile Parquet, CSV, JSON and Arrow files.
///
/// By default pavi-cli prints a short summary of FILE (rows, columns, schema,
/// row groups) and its first 10 rows as a table. Choose rows with --columns,
/// --filter, --sort, --head and --offset; send them anywhere with --format and
/// --output; ask for facts about the file instead with --count, --schema,
/// --stats or --profile; or run SQL with --sql.
///
/// Large files are fine: rows are read a batch at a time, and only the table
/// keeps its rows (at most 1,000) in memory.
#[derive(Parser)]
#[command(
    name = "pavi-cli",
    bin_name = "pavi-cli",
    version,
    about,
    long_about,
    styles = STYLES,
    max_term_width = MAX_HELP_WIDTH,
    disable_help_flag = true,
    disable_version_flag = true,
    after_help = short_footer(),
    after_long_help = long_footer(),
)]
pub struct Args {
    /// File, folder or URL to read, or - for standard input
    ///
    /// A Parquet, CSV, TSV, JSON or Arrow file; a folder of Parquet files, read
    /// as one table (folders named key=value become columns); an https://,
    /// s3://, gs:// or az:// URL; or - for standard input, which is saved to a
    /// temporary file and told apart by its first bytes.
    #[arg(value_name = "FILE")]
    pub file: String,

    /// Read FILE as parquet, csv, json or arrow, whatever its name says
    ///
    /// For a CSV saved as .txt, say, or standard input that starts like another format.
    ///   parquet  a Parquet file
    ///   csv      comma, tab, semicolon or pipe separated, with a header line
    ///   json     JSON Lines, or one array of objects
    ///   arrow    an Arrow IPC file or stream
    #[arg(
        long,
        value_enum,
        value_name = "FORMAT",
        hide_possible_values = true,
        help_heading = "Input",
        verbatim_doc_comment
    )]
    pub input_format: Option<InputFormat>,

    /// Only these columns, by name or 0-based number: -c id,name
    ///
    /// Comma-separated, in the order to output. A name is tried before a
    /// number, so a column called 7 is found by its name.
    #[arg(short, long, value_name = "COLUMNS", help_heading = "Choosing rows")]
    pub columns: Option<String>,

    /// Only rows that match: -f "DEP_DELAY > 60 AND ORIGIN = 'JFK'"
    ///
    /// Tests joined by AND / OR, with brackets: = != < <= > >= contains,
    /// [NOT] IN (a, b), [NOT] BETWEEN a AND b, [NOT] LIKE 'a%', IS [NOT] NULL.
    /// Quote a value that holds spaces, commas, brackets or keywords.
    #[arg(short, long, value_name = "EXPR", help_heading = "Choosing rows")]
    pub filter: Option<String>,

    /// Sort by columns, each :asc or :desc: -s DEP_DELAY:desc,FL_DATE
    ///
    /// Up to 8 columns, by name or number; ascending unless :desc; nulls last.
    /// Works with --filter. Sorts larger than memory spill to temporary files.
    #[arg(short, long, value_name = "KEYS", help_heading = "Choosing rows")]
    pub sort: Option<String>,

    /// How many rows to output [default: 10 in the table, every row otherwise]
    ///
    /// The table shows at most 1,000 rows; the other formats stream any number.
    #[arg(short = 'n', long, value_name = "N", help_heading = "Choosing rows")]
    pub head: Option<u64>,

    /// Skip this many rows first (matching rows, with --filter)
    #[arg(
        long,
        value_name = "N",
        default_value_t = 0,
        hide_default_value = true,
        help_heading = "Choosing rows"
    )]
    pub offset: u64,

    /// table, csv, tsv, jsonl, parquet or arrow [default: table]
    ///
    /// With -o, the default is the format its extension names.
    /// Every format but the table writes only the rows (all of them unless --head is given).
    ///   table    a summary and a table, for reading
    ///   csv      comma-separated, with a header line; nulls are empty
    ///   tsv      tab-separated, with a header line; nulls are empty
    ///   jsonl    one JSON object per line (also: json)
    ///   parquet  a Parquet file
    ///   arrow    an Arrow IPC file (Feather v2)
    #[arg(
        long,
        value_enum,
        value_name = "FORMAT",
        hide_possible_values = true,
        help_heading = "Output",
        verbatim_doc_comment
    )]
    pub format: Option<Format>,

    /// Write to a file instead of standard output: -o flights.parquet
    ///
    /// The file appears only once it is complete (it is written next to the
    /// target, then renamed). An existing file is replaced, but never the file
    /// being read. The format comes from the extension (.csv .tsv .jsonl .json
    /// .parquet .arrow .feather) unless --format is given.
    #[arg(short, long, value_name = "PATH", help_heading = "Output")]
    pub output: Option<PathBuf>,

    /// Show only the table, without the file summary above it
    #[arg(short, long, help_heading = "Output")]
    pub quiet: bool,

    /// Draw tables with + - | instead of box-drawing lines
    ///
    /// For consoles and code pages that can't show box-drawing characters.
    /// Null cells then read NULL instead of ∅.
    #[arg(long, help_heading = "Output")]
    pub ascii: bool,

    /// Never show the progress line
    ///
    /// It appears on standard error, only on a terminal, once opening,
    /// scanning, sorting or a query has taken over a second, and is erased
    /// before any output.
    #[arg(long, help_heading = "Output")]
    pub no_progress: bool,

    /// Print only the number of rows (that match --filter)
    ///
    /// Without --filter the count comes from the footer, without reading data.
    #[arg(
        long,
        help_heading = "Facts instead of rows",
        conflicts_with_all = ["head", "offset", "columns", "format", "quiet", "sort"]
    )]
    pub count: bool,

    /// Print each column's number, name, type and nullability
    #[arg(
        long,
        help_heading = "Facts instead of rows",
        conflicts_with_all = ["stats", "count", "sort", "filter", "offset", "head", "quiet"]
    )]
    pub schema: bool,

    /// Print each row group's statistics, without reading any data
    ///
    /// Rows, nulls, min, max and distinct count per row group and column (or
    /// per --columns), from the file's footer. The table shows the first
    /// 1,000; --head shows fewer, the other formats show all.
    #[arg(
        long,
        help_heading = "Facts instead of rows",
        conflicts_with_all = ["count", "sort", "filter", "offset", "quiet"]
    )]
    pub stats: bool,

    /// Profile each column: nulls, distinct values, min, max, mean, …
    ///
    /// One scan of every column (or of --columns, at most 256), as the app's
    /// Profile window does: rows, nulls, distinct values (exact up to 1,000),
    /// min, max, mean, text lengths, and the most frequent value when every
    /// distinct value was counted. With --filter, only matching rows.
    #[arg(
        long,
        help_heading = "Facts instead of rows",
        conflicts_with_all = [
            "count", "schema", "stats", "sql", "sort", "offset", "head", "quiet"
        ]
    )]
    pub profile: bool,

    /// Run a SQL query; FILE is the table `dataset`
    ///
    /// The same SQL as the app's editor: WHERE, ORDER BY, LIMIT, GROUP BY with
    /// HAVING, COUNT / SUM / AVG / MIN / MAX and COUNT(DISTINCT …), SELECT
    /// DISTINCT, AS aliases, calculations (arithmetic, CASE, CAST, UPPER, YEAR,
    /// …), INNER and LEFT joins, and subqueries. FILE can also be named by its
    /// file name (flights.parquet → flights). --head and --offset cut the
    /// result.
    #[arg(
        long,
        value_name = "QUERY",
        help_heading = "SQL",
        conflicts_with_all = ["filter", "sort", "columns", "count", "schema", "stats", "quiet"]
    )]
    pub sql: Option<String>,

    /// Another file for --sql to query or join; repeat for more
    ///
    /// NAME=PATH names it; a bare PATH is named after the file, as the app
    /// names its tabs (airports.csv → airports).
    #[arg(
        long = "table",
        value_name = "[NAME=]PATH",
        help_heading = "SQL",
        requires = "sql"
    )]
    pub tables: Vec<String>,

    /// Print help (--help shows more, with examples)
    #[arg(short, long, action = ArgAction::Help, help_heading = "Help")]
    help: Option<bool>,

    /// Print the version
    #[arg(short = 'V', long, action = ArgAction::Version, help_heading = "Help")]
    version: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Format {
    /// A summary and a table, for reading
    Table,
    /// Comma-separated, with a header line; nulls are empty
    Csv,
    /// Tab-separated, with a header line; nulls are empty
    Tsv,
    /// One JSON object per line
    #[value(alias = "json")]
    Jsonl,
    /// A Parquet file
    Parquet,
    /// An Arrow IPC file (Feather v2)
    Arrow,
}

/// How to read FILE when its extension (or, for -, its first bytes) would
/// mislead.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum InputFormat {
    /// A Parquet file
    Parquet,
    /// Comma, tab, semicolon or pipe separated, with a header line
    Csv,
    /// JSON Lines, or one array of objects
    Json,
    /// An Arrow IPC file or stream
    Arrow,
}

impl InputFormat {
    pub fn file_format(self) -> parquet_reader::FileFormat {
        match self {
            Self::Parquet => parquet_reader::FileFormat::Parquet,
            Self::Csv => parquet_reader::FileFormat::Csv,
            Self::Json => parquet_reader::FileFormat::Json,
            Self::Arrow => parquet_reader::FileFormat::Arrow,
        }
    }
}

/// A few examples under `-h`.
fn short_footer() -> String {
    let mut text = heading("Examples:");
    for (command, what) in &EXAMPLES[..3] {
        text.push_str(&example(command, what));
    }
    text.push_str(&format!(
        "\n\nRun {} for details, more examples, exit codes and colours.",
        literal("pavi-cli --help")
    ));
    text
}

/// Every example, the exit codes and colours under `--help`.
fn long_footer() -> String {
    let mut text = heading("Examples:");
    for (command, what) in EXAMPLES {
        text.push_str(&example(command, what));
    }
    text.push_str("\n\n");
    text.push_str(&heading("Exit codes:"));
    for (code, meaning) in [
        (
            "0",
            "success, also when the reader of the output stops early (| head)",
        ),
        (
            "1",
            "the file, a column, the filter, the query or the output failed",
        ),
        ("2", "invalid arguments"),
    ] {
        text.push_str(&format!("\n  {}  {meaning}", literal(code)));
    }
    text.push('\n');
    text.push('\n');
    text.push_str(&heading("Colours:"));
    text.push_str(
        "\n  Used only on a terminal. NO_COLOR=1 turns them off; CLICOLOR_FORCE=1 keeps them\n  \
         in pipes.",
    );
    text
}

/// What each example does, shortest first; the first three show under `-h`.
const EXAMPLES: [(&str, &str); 10] = [
    ("pavi-cli flights.parquet", "summary and the first 10 rows"),
    (
        "pavi-cli flights.parquet -f \"DEP_DELAY > 60\" -s DEP_DELAY:desc -n 20",
        "the 20 worst delays",
    ),
    (
        "pavi-cli flights.csv -o flights.parquet",
        "convert CSV to Parquet",
    ),
    (
        "pavi-cli flights.parquet -c FL_DATE,DEP_DELAY --format csv | head",
        "two columns as CSV, into a pipe",
    ),
    (
        "pavi-cli flights.parquet --count -f \"DISTANCE > 1000\"",
        "how many rows match",
    ),
    (
        "pavi-cli flights.parquet --schema",
        "columns and their types",
    ),
    (
        "pavi-cli flights.parquet --profile",
        "nulls, distinct values, min, max …",
    ),
    (
        "pavi-cli flights.parquet --sql \"SELECT ORIGIN, COUNT(*) FROM flights GROUP BY ORIGIN\"",
        "SQL",
    ),
    (
        "pavi-cli flights.parquet --table airports.csv --sql \"SELECT … JOIN airports …\"",
        "join two files",
    ),
    (
        "curl -s https://…/data.parquet | pavi-cli - --count",
        "read standard input",
    ),
];

fn example(command: &str, what: &str) -> String {
    format!("\n  {}\n      {what}", literal(command))
}

fn heading(text: &str) -> String {
    styled(STYLES.get_header(), text)
}

fn literal(text: &str) -> String {
    styled(STYLES.get_literal(), text)
}

/// `text` with `style`'s escape codes; clap strips them where colour is off.
fn styled(style: &Style, text: &str) -> String {
    format!("{}{text}{}", style.render(), style.render_reset())
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
    fn groups_every_option_under_a_heading() {
        for argument in Args::command()
            .get_arguments()
            .filter(|argument| !argument.is_positional())
        {
            assert!(
                argument.get_help_heading().is_some(),
                "{} has no heading",
                argument.get_id()
            );
        }
    }

    #[test]
    fn keeps_short_help_to_one_line_per_option() {
        let help = Args::command().render_help().to_string();
        for line in help.lines() {
            assert!(
                line.chars().count() <= MAX_HELP_WIDTH,
                "line wider than the help: {line:?}"
            );
        }
        for heading in [
            "Input:",
            "Choosing rows:",
            "Output:",
            "Facts instead of rows:",
            "SQL:",
        ] {
            assert!(help.contains(heading), "{heading} missing in\n{help}");
        }
        assert!(help.contains("Examples:"), "{help}");
        let long = Args::command().render_long_help().to_string();
        assert!(long.contains("Exit codes:"), "{long}");
        assert!(long.contains("--table airports.csv"), "{long}");
    }

    #[test]
    fn takes_short_flags() {
        let args = Args::try_parse_from([
            "pavi-cli", "f", "-c", "a,b", "-f", "a > 1", "-s", "a:desc", "-n", "5",
        ])
        .unwrap();
        assert_eq!(args.columns.as_deref(), Some("a,b"));
        assert_eq!(args.filter.as_deref(), Some("a > 1"));
        assert_eq!(args.sort.as_deref(), Some("a:desc"));
        assert_eq!(args.head, Some(5));
    }

    #[test]
    fn count_excludes_output_options() {
        assert!(Args::try_parse_from(["cli", "f", "--count", "--head", "3"]).is_err());
        assert!(Args::try_parse_from(["cli", "f", "--count", "--filter", "a > 1"]).is_ok());
        let args = Args::try_parse_from(["cli", "f", "--format", "json"]).unwrap();
        assert_eq!(args.format, Some(Format::Jsonl));
    }

    #[test]
    fn inspection_modes_exclude_each_other_and_row_options() {
        let parses = |arguments: &[&str]| {
            Args::try_parse_from(std::iter::once("cli").chain(arguments.iter().copied())).is_ok()
        };
        assert!(!parses(&["f", "--schema", "--stats"]));
        assert!(!parses(&["f", "--schema", "--count"]));
        assert!(!parses(&["f", "--stats", "--count"]));
        assert!(!parses(&["f", "--schema", "--head", "2"]));
        assert!(!parses(&["f", "--stats", "--filter", "a > 1"]));
        assert!(!parses(&["f", "--stats", "--sort", "a"]));
        assert!(!parses(&["f", "--count", "--sort", "a"]));
        assert!(parses(&[
            "f",
            "--schema",
            "--columns",
            "a",
            "--format",
            "csv"
        ]));
        assert!(parses(&["f", "--stats", "--head", "5", "--columns", "a"]));
        assert!(parses(&[
            "f", "--sort", "a:desc", "--filter", "a > 1", "--offset", "2"
        ]));
    }
}
