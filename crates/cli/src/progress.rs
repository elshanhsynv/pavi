//! A one-line progress note on standard error while a long open or scan runs.
//!
//! Shown only when stderr is a terminal and the work has taken longer than
//! [`DELAY`], so quick runs, pipes and redirected logs never see it. The line
//! is rewritten in place with `\r` and blanked when the work ends, so it never
//! mixes with the output on stdout.

use std::cell::Cell;
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

/// Work shorter than this shows nothing.
const DELAY: Duration = Duration::from_secs(1);
/// Least time between two updates of the line.
const INTERVAL: Duration = Duration::from_millis(200);

pub struct Progress {
    enabled: bool,
    start: Instant,
    last: Cell<Option<Instant>>,
    /// Characters of the line on screen, to blank it.
    shown: Cell<usize>,
}

impl Progress {
    /// Progress for this run: on when stderr is a terminal, unless `off`.
    pub fn new(off: bool) -> Self {
        Self::with(!off && io::stderr().is_terminal())
    }

    /// Progress that never shows, for tests.
    #[cfg(test)]
    pub fn hidden() -> Self {
        Self::with(false)
    }

    fn with(enabled: bool) -> Self {
        Self {
            enabled,
            start: Instant::now(),
            last: Cell::new(None),
            shown: Cell::new(0),
        }
    }

    /// Reports `done` of `total` rows read so far.
    pub fn rows(&self, label: &str, done: u64, total: u64) {
        if self.due() {
            self.show(&rows_line(label, done, total));
        }
    }

    /// Reports a share (0 to 1) of an open: a folder's files, a download, a
    /// conversion.
    pub fn share(&self, label: &str, done: f64) {
        if self.due() {
            self.show(&format!("{label}: {:.0}%", done.clamp(0.0, 1.0) * 100.0));
        }
    }

    /// Blanks the line, if one is shown; call before writing anything else to
    /// the terminal.
    pub fn clear(&self) {
        let width = self.shown.replace(0);
        if width > 0 {
            let _ = write!(io::stderr(), "\r{}\r", " ".repeat(width));
        }
    }

    fn due(&self) -> bool {
        if !self.enabled {
            return false;
        }
        let now = Instant::now();
        if now.duration_since(self.start) < DELAY {
            return false;
        }
        if self
            .last
            .get()
            .is_some_and(|last| now.duration_since(last) < INTERVAL)
        {
            return false;
        }
        self.last.set(Some(now));
        true
    }

    fn show(&self, line: &str) {
        let width = line.chars().count();
        let padding = " ".repeat(self.shown.get().saturating_sub(width));
        // Errors writing a progress note are not worth stopping the work for.
        let _ = write!(io::stderr(), "\r{line}{padding}");
        let _ = io::stderr().flush();
        self.shown.set(width);
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.clear();
    }
}

/// "Scanning: 1,234,567 of 5,000,000 rows (24%)".
fn rows_line(label: &str, done: u64, total: u64) -> String {
    let done = done.min(total);
    let percent = if total == 0 {
        100
    } else {
        (u128::from(done) * 100 / u128::from(total)) as u64
    };
    format!(
        "{label}: {} of {} rows ({percent}%)",
        grouped(done),
        grouped(total)
    )
}

/// `1234567` → `1,234,567`.
fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut text = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            text.push(',');
        }
        text.push(digit);
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_rows_read_with_grouped_digits() {
        assert_eq!(
            rows_line("Scanning", 1_234_567, 5_000_000),
            "Scanning: 1,234,567 of 5,000,000 rows (24%)"
        );
        assert_eq!(rows_line("Sorting", 9, 0), "Sorting: 0 of 0 rows (100%)");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1_000), "1,000");
        assert_eq!(grouped(0), "0");
    }

    #[test]
    fn hidden_progress_never_reports() {
        let progress = Progress::hidden();
        assert!(!progress.due());
        progress.rows("Scanning", 1, 2);
        assert_eq!(progress.shown.get(), 0);
    }
}
