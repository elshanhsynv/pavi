//! End-to-end tests: they run the built `pavi-cli` program as a user's shell
//! would, to check what only the real process shows: exit codes, what goes to
//! stdout and stderr, colour, a reader that stops early, standard input, and
//! files left on disk. Behaviour inside the program is tested next to the code
//! (`src/*.rs`); see docs/TESTING.md.

// This whole file is test code, helpers included; clippy.toml's
// allow-unwrap-in-tests only covers `#[test]` functions here.
#![allow(clippy::unwrap_used)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use parquet::arrow::ArrowWriter;

/// The program under test, built by Cargo for this test run.
const PAVI_CLI: &str = env!("CARGO_BIN_EXE_pavi-cli");

/// `pavi-cli` with a clean environment for colour and progress, so a test's
/// result doesn't depend on the shell it runs in.
fn pavi_cli() -> Command {
    let mut command = Command::new(PAVI_CLI);
    command
        .env_remove("CLICOLOR_FORCE")
        .env_remove("NO_COLOR")
        .env_remove("CLICOLOR");
    command
}

fn run(arguments: &[&str]) -> Output {
    pavi_cli().args(arguments).output().unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

/// A Parquet file of `rows` rows: `id` 0.., `name` "row <id>".
fn fixture(folder: &Path, rows: i64) -> PathBuf {
    let path = folder.join("rows.parquet");
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|id| format!("row {id}")),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

fn text(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn exits_0_on_success_1_on_failure_and_2_on_bad_arguments() {
    let folder = tempfile::tempdir().unwrap();
    let file = fixture(folder.path(), 5);

    let ok = run(&[text(&file), "--count"]);
    assert_eq!(ok.status.code(), Some(0), "{}", stderr(&ok));
    assert_eq!(stdout(&ok), "5\n");
    assert!(ok.stderr.is_empty(), "{}", stderr(&ok));

    let missing = run(&[text(&folder.path().join("missing.parquet"))]);
    assert_eq!(missing.status.code(), Some(1));
    assert!(missing.stdout.is_empty());
    assert!(
        stderr(&missing).starts_with("Error: open "),
        "{}",
        stderr(&missing)
    );

    let bad_filter = run(&[text(&file), "--filter", "nope > 1"]);
    assert_eq!(bad_filter.status.code(), Some(1));
    assert!(bad_filter.stdout.is_empty(), "printed before failing");

    for arguments in [
        &["--bogus"][..],
        &[],
        &[text(&file), "--count", "--head", "1"],
    ] {
        let usage = run(arguments);
        assert_eq!(usage.status.code(), Some(2), "{arguments:?}");
        assert!(stderr(&usage).contains("Usage: pavi-cli"), "{arguments:?}");
    }
}

#[test]
fn colours_errors_only_when_asked_on_a_pipe() {
    let folder = tempfile::tempdir().unwrap();
    let missing = folder.path().join("missing.parquet");

    // A pipe is not a terminal: no colour codes.
    let plain = run(&[text(&missing)]);
    assert!(!plain.stderr.contains(&0x1b), "{}", stderr(&plain));

    let forced = pavi_cli()
        .arg(text(&missing))
        .env("CLICOLOR_FORCE", "1")
        .output()
        .unwrap();
    assert!(
        stderr(&forced).starts_with("\u{1b}["),
        "{:?}",
        stderr(&forced)
    );

    // NO_COLOR wins over CLICOLOR_FORCE.
    let neither = pavi_cli()
        .arg(text(&missing))
        .env("CLICOLOR_FORCE", "1")
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(!neither.stderr.contains(&0x1b), "{}", stderr(&neither));
}

#[test]
fn stops_quietly_with_0_when_the_reader_closes_the_pipe() {
    let folder = tempfile::tempdir().unwrap();
    // Several MB of CSV: far more than a pipe holds, so pavi-cli is still
    // writing when the reader goes away.
    let file = fixture(folder.path(), 300_000);
    let mut child = pavi_cli()
        .args([text(&file), "--format", "csv"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Read the first bytes, as `| head -c 100` would, then close the pipe.
    let mut stdout = child.stdout.take().unwrap();
    let mut first = [0_u8; 100];
    stdout.read_exact(&mut first).unwrap();
    assert!(first.starts_with(b"id,name\n0,row 0\n"));
    drop(stdout);

    let status = child.wait().unwrap();
    let mut errors = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut errors)
        .unwrap();
    assert_eq!(status.code(), Some(0), "{errors}");
    assert!(errors.is_empty(), "{errors}");
}

#[test]
fn reads_standard_input() {
    let mut child = pavi_cli()
        .args(["-", "--count", "--filter", "a > 1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"a,b\n1,x\n2,y\n3,z\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "2\n");
}

#[test]
fn writes_output_files_only_when_complete() {
    let folder = tempfile::tempdir().unwrap();
    let file = fixture(folder.path(), 5);

    let csv = folder.path().join("out.csv");
    let written = run(&[
        text(&file),
        "-o",
        text(&csv),
        "--columns",
        "name",
        "-n",
        "2",
    ]);
    assert_eq!(written.status.code(), Some(0), "{}", stderr(&written));
    assert!(written.stdout.is_empty());
    assert_eq!(
        std::fs::read_to_string(&csv).unwrap(),
        "name\nrow 0\nrow 1\n"
    );

    // A failing run leaves neither the target nor a temporary file.
    let failed = folder.path().join("failed.csv");
    let failing = run(&[text(&file), "-o", text(&failed), "--filter", "nope > 1"]);
    assert_eq!(failing.status.code(), Some(1));
    assert!(!failed.exists());
    let mut names = std::fs::read_dir(folder.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["out.csv", "rows.parquet"]);

    // Never over the file being read.
    let over_input = run(&[text(&file), "-o", text(&file), "--format", "parquet"]);
    assert_eq!(over_input.status.code(), Some(1));
    assert!(stderr(&over_input).contains("is the file being read"));
}

#[test]
fn prints_help_version_completions_and_the_manual_page() {
    let help = run(&["-h"]);
    assert_eq!(help.status.code(), Some(0));
    for heading in ["Choosing rows:", "Output:", "Examples:"] {
        assert!(stdout(&help).contains(heading), "{heading}");
    }
    assert!(!help.stdout.contains(&0x1b), "colour codes in piped help");

    let version = run(&["--version"]);
    assert_eq!(
        stdout(&version).trim(),
        format!("pavi-cli {}", env!("CARGO_PKG_VERSION"))
    );

    let bash = run(&["--completions", "bash"]);
    assert_eq!(bash.status.code(), Some(0));
    assert!(
        stdout(&bash).contains("complete -F _pavi__cli"),
        "{}",
        stdout(&bash)
    );

    let man = run(&["--man"]);
    assert_eq!(man.status.code(), Some(0));
    assert!(stdout(&man).contains(".TH pavi-cli 1"));
}
