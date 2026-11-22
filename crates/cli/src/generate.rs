//! `--completions` and `--man`: shell completion scripts and the manual page,
//! generated from the same argument definitions as `--help`, so they never
//! drift from it.

use std::io::Write;

use anyhow::{Context, Result};
use clap::CommandFactory;
use clap_complete::Shell;

use crate::args::{self, Args};

/// The name the scripts complete and the manual page is about.
const BIN_NAME: &str = "pavi-cli";

/// Writes `shell`'s completion script for pavi-cli.
pub fn completions(shell: Shell, out: &mut impl Write) -> Result<()> {
    let mut command = Args::command();
    clap_complete::generate(shell, &mut command, BIN_NAME, out);
    Ok(())
}

/// Writes the manual page, section 1, in roff. Its EXTRA section holds the
/// examples, exit codes and colours of `--help`, without colour codes.
pub fn man(out: &mut impl Write) -> Result<()> {
    let command = Args::command()
        .after_help(None::<&str>)
        .after_long_help(args::long_footer_text(false));
    clap_mangen::Man::new(command)
        .render(out)
        .context("write the manual page")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(write: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> String {
        let mut out = Vec::new();
        write(&mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn writes_a_completion_script_for_each_shell() {
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::PowerShell,
            Shell::Elvish,
        ] {
            let script = text(|out| completions(shell, out));
            assert!(script.contains("pavi-cli"), "{shell}");
            // Without the dashes: fish writes `-l input-format`.
            for option in ["filter", "sql", "input-format", "completions"] {
                assert!(script.contains(option), "{shell} lacks {option}");
            }
        }
        // Values of an enum option complete too.
        let bash = text(|out| completions(Shell::Bash, out));
        assert!(bash.contains("parquet"), "{bash}");
    }

    #[test]
    fn writes_a_manual_page_without_colour_codes() {
        let page = text(man);
        assert!(page.starts_with(".ie \\n(.g .ds Aq"), "{page}");
        assert!(page.contains(".TH pavi-cli 1"), "{page}");
        for section in ["NAME", "SYNOPSIS", "DESCRIPTION", "OPTIONS"] {
            assert!(
                page.contains(&format!(".SH {section}")),
                "{section} missing"
            );
        }
        assert!(page.contains("Exit codes"), "footer missing");
        assert!(
            page.contains("pavi\\-cli flights.parquet"),
            "examples missing"
        );
        assert!(!page.contains('\u{1b}'), "colour codes in the man page");
    }
}
