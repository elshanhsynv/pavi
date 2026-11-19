//! The log file and crash reports, kept in a `logs` folder beside the session
//! file (`session::session_dir`).
//!
//! The log (`pavi.log`) records what the app did and what failed: opens, query
//! and export errors, and every panic. It holds at most [`MAX_LOG_BYTES`]; when
//! full it becomes `pavi.old.log` and a new one starts. A panic on the UI thread,
//! which ends the app, also writes `crash-<time>.txt` with the backtrace; the
//! newest [`MAX_CRASH_REPORTS`] are kept, and the next start says one was written.
//! Paths are written without a URL's query string (`display_path`), which may
//! hold a token.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    panic::PanicHookInfo,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};

/// Bytes of `pavi.log` before it is moved to `pavi.old.log`.
pub(crate) const MAX_LOG_BYTES: u64 = 1024 * 1024;
/// Crash reports kept; older ones are deleted.
pub(crate) const MAX_CRASH_REPORTS: usize = 5;
const LOG_FILE: &str = "pavi.log";
const OLD_LOG_FILE: &str = "pavi.old.log";
const CRASH_PREFIX: &str = "crash-";

/// Where the log and crash reports go on this machine.
pub(crate) fn log_dir() -> PathBuf {
    let session = crate::session::SessionStore::default_path();
    session
        .parent()
        .map_or_else(|| std::env::temp_dir().join("PAVI"), Path::to_path_buf)
        .join("logs")
}

/// Writes log records to `pavi.log`: PAVI's own from `info` up, other crates'
/// (eframe, the graphics backend) from `warn` up.
struct FileLogger {
    dir: PathBuf,
    file: Mutex<Option<File>>,
}

impl FileLogger {
    fn open(&self) -> Option<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(LOG_FILE))
            .ok()
    }

    fn write_line(&self, line: &str) {
        // A panic while logging must not wait on the lock it holds.
        let Ok(mut slot) = self.file.try_lock() else {
            return;
        };
        if slot.is_none() {
            *slot = self.open();
        }
        let full = slot
            .as_ref()
            .and_then(|file| file.metadata().ok())
            .is_some_and(|metadata| metadata.len() + line.len() as u64 > MAX_LOG_BYTES);
        if full {
            *slot = None;
            let _ = fs::rename(self.dir.join(LOG_FILE), self.dir.join(OLD_LOG_FILE));
            *slot = self.open();
        }
        if let Some(file) = slot.as_mut() {
            let _ = file.write_all(line.as_bytes());
        }
    }
}

fn is_ours(target: &str) -> bool {
    ["pavi", "parquet_reader"]
        .iter()
        .any(|crate_name| target.starts_with(crate_name))
}

impl log::Log for FileLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        let floor = if is_ours(metadata.target()) {
            log::Level::Info
        } else {
            log::Level::Warn
        };
        metadata.level() <= floor
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            self.write_line(&format!(
                "{} {:<5} {}: {}\n",
                now_text(),
                record.level(),
                record.target(),
                record.args()
            ));
        }
    }

    fn flush(&self) {
        if let Ok(mut slot) = self.file.try_lock()
            && let Some(file) = slot.as_mut()
        {
            let _ = file.flush();
        }
    }
}

fn now_text() -> String {
    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Starts writing the log in `dir` and installs the panic hook. Called once, at
/// start. Without a writable folder the app runs without a log.
pub(crate) fn init(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    log::set_boxed_logger(Box::new(FileLogger {
        dir: dir.to_path_buf(),
        file: Mutex::new(None),
    }))
    .context("start the log")?;
    log::set_max_level(log::LevelFilter::Info);
    let crash_dir = dir.to_path_buf();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let thread = thread.name().unwrap_or("unnamed");
        log::error!("panic on thread {thread}: {}", panic_text(info));
        // Worker panics are caught and reported as errors; one on the UI thread
        // ends the app.
        if thread == "main" {
            let _ = write_crash_report(&crash_dir, info);
        }
        previous(info);
    }));
    Ok(())
}

/// The panic message and where it happened.
fn panic_text(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic");
    match info.location() {
        Some(location) => format!("{message} at {}:{}", location.file(), location.line()),
        None => message.to_owned(),
    }
}

/// The text of a crash report.
pub(crate) fn crash_report_text(panic: &str, backtrace: &str) -> String {
    format!(
        "PAVI {} closed unexpectedly.\n\
         Time: {}\n\
         System: {} {}\n\n\
         {panic}\n\n\
         Backtrace:\n{backtrace}\n",
        env!("CARGO_PKG_VERSION"),
        now_text(),
        std::env::consts::OS,
        std::env::consts::ARCH,
    )
}

fn write_crash_report(dir: &Path, info: &PanicHookInfo<'_>) -> Result<PathBuf> {
    let backtrace = std::backtrace::Backtrace::force_capture().to_string();
    let path = dir.join(format!(
        "{CRASH_PREFIX}{}.txt",
        chrono::Local::now().format("%Y%m%d-%H%M%S")
    ));
    fs::write(&path, crash_report_text(&panic_text(info), &backtrace))?;
    prune_crash_reports(dir);
    Ok(path)
}

/// Crash reports in `dir`, oldest first.
fn crash_reports(dir: &Path) -> Vec<(SystemTime, PathBuf)> {
    let mut reports = fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(CRASH_PREFIX)
                })
                .filter_map(|entry| Some((entry.metadata().ok()?.modified().ok()?, entry.path())))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    reports.sort();
    reports
}

/// Deletes all but the newest [`MAX_CRASH_REPORTS`].
pub(crate) fn prune_crash_reports(dir: &Path) {
    let reports = crash_reports(dir);
    let extra = reports.len().saturating_sub(MAX_CRASH_REPORTS);
    for (_, path) in reports.into_iter().take(extra) {
        let _ = fs::remove_file(path);
    }
}

/// The time (seconds since 1970) of the newest crash report in `dir` written
/// after `seen`, if there is one.
pub(crate) fn crash_since(dir: &Path, seen: u64) -> Option<u64> {
    crash_reports(dir)
        .last()
        .and_then(|(modified, _)| modified.duration_since(UNIX_EPOCH).ok())
        .map(|age| age.as_secs())
        .filter(|newest| *newest > seen)
}

/// Opens `dir` in the system's file manager.
pub(crate) fn open_folder(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    let program = match std::env::consts::OS {
        "windows" => "explorer",
        "macos" => "open",
        _ => "xdg-open",
    };
    std::process::Command::new(program)
        .arg(dir)
        .spawn()
        .with_context(|| format!("open {}", dir.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotates_the_log_when_it_is_full() {
        let directory = tempfile::tempdir().unwrap();
        let logger = FileLogger {
            dir: directory.path().to_path_buf(),
            file: Mutex::new(None),
        };
        let line = format!("{}\n", "x".repeat(1023));
        for _ in 0..(MAX_LOG_BYTES / 1024 + 10) {
            logger.write_line(&line);
        }
        let current = fs::metadata(directory.path().join(LOG_FILE)).unwrap().len();
        let old = fs::metadata(directory.path().join(OLD_LOG_FILE))
            .unwrap()
            .len();
        assert!(current <= MAX_LOG_BYTES && old <= MAX_LOG_BYTES);
        assert_eq!(current, 10 * 1024);
    }

    #[test]
    fn logs_our_info_and_only_others_warnings() {
        use log::Log;
        let directory = tempfile::tempdir().unwrap();
        let logger = FileLogger {
            dir: directory.path().to_path_buf(),
            file: Mutex::new(None),
        };
        let record = |target: &str, level, text: &str| {
            logger.log(
                &log::Record::builder()
                    .target(target)
                    .level(level)
                    .args(format_args!("{text}"))
                    .build(),
            );
        };
        record("pavi::app", log::Level::Info, "opened a file");
        record("wgpu_core", log::Level::Info, "adapter chatter");
        record("eframe", log::Level::Warn, "no GPU");
        record("pavi::app", log::Level::Debug, "noise");
        let text = fs::read_to_string(directory.path().join(LOG_FILE)).unwrap();
        assert!(text.contains("INFO  pavi::app: opened a file"));
        assert!(text.contains("WARN  eframe: no GPU"));
        assert!(!text.contains("adapter chatter") && !text.contains("noise"));
    }

    #[test]
    fn keeps_the_newest_crash_reports_and_finds_new_ones() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(crash_since(directory.path(), 0), None);
        for index in 0..MAX_CRASH_REPORTS + 3 {
            fs::write(
                directory
                    .path()
                    .join(format!("{CRASH_PREFIX}{index:02}.txt")),
                crash_report_text("boom at app.rs:1", "frames"),
            )
            .unwrap();
            // Distinct modification times.
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
        prune_crash_reports(directory.path());
        assert_eq!(crash_reports(directory.path()).len(), MAX_CRASH_REPORTS);
        let newest = crash_since(directory.path(), 0).unwrap();
        assert_eq!(crash_since(directory.path(), newest), None);
        let text = fs::read_to_string(&crash_reports(directory.path()).last().unwrap().1).unwrap();
        assert!(text.contains("closed unexpectedly") && text.contains("boom at app.rs:1"));
    }
}
