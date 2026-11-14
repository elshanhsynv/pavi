//! Files around the run: `-` (standard input copied to a temporary file) and
//! `--output` (written next to the target, then renamed into place).

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};

use crate::args::Format;

/// Most bytes read from standard input; the copy goes to the temporary folder.
/// Same cap as a remote download, which is also copied to disk first.
pub const MAX_STDIN_BYTES: u64 = parquet_reader::MAX_DOWNLOAD_BYTES;
/// Bytes read before choosing the input format.
const SNIFF_BYTES: usize = 4096;

/// Standard input saved to a temporary folder, deleted on drop. Drop it after
/// the `ParquetSource` that reads it: Windows cannot delete an open file.
pub struct StdinCopy {
    folder: PathBuf,
    pub path: PathBuf,
}

impl Drop for StdinCopy {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.folder);
    }
}

/// Copies `input` to a temporary file named for its format, so it can be
/// opened like any file (Parquet needs to seek to its footer).
pub fn copy_stdin(input: &mut dyn Read) -> Result<StdinCopy> {
    let mut prefix = Vec::with_capacity(SNIFF_BYTES);
    input
        .take(SNIFF_BYTES as u64)
        .read_to_end(&mut prefix)
        .context("read standard input")?;
    if prefix.is_empty() {
        bail!("standard input is empty");
    }
    let extension = sniff_extension(&prefix);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.subsec_nanos());
    let folder = std::env::temp_dir().join(format!("pavi-stdin-{}-{nanos}", std::process::id()));
    fs::create_dir(&folder)
        .with_context(|| format!("create temporary folder {}", folder.display()))?;
    let copy = StdinCopy {
        path: folder.join(format!("stdin.{extension}")),
        folder,
    };

    let mut file = BufWriter::new(
        File::create(&copy.path)
            .with_context(|| format!("create temporary file {}", copy.path.display()))?,
    );
    file.write_all(&prefix)?;
    let limit = MAX_STDIN_BYTES.saturating_sub(prefix.len() as u64);
    let copied = io::copy(&mut input.take(limit.saturating_add(1)), &mut file)
        .context("copy standard input to a temporary file")?;
    if copied > limit {
        bail!(
            "standard input is larger than {} GiB; save it to a file first",
            MAX_STDIN_BYTES / (1024 * 1024 * 1024)
        );
    }
    file.flush()?;
    Ok(copy)
}

/// The extension that makes `ParquetSource::open` read `prefix`'s format.
fn sniff_extension(prefix: &[u8]) -> &'static str {
    if prefix.starts_with(b"PAR1") {
        return "parquet";
    }
    if prefix.starts_with(b"ARROW1") {
        return "arrow";
    }
    // An Arrow IPC stream starts with a message's continuation marker.
    if prefix.starts_with(&[0xff, 0xff, 0xff, 0xff]) {
        return "arrows";
    }
    let text = prefix.strip_prefix(b"\xef\xbb\xbf").unwrap_or(prefix);
    match text.iter().find(|byte| !byte.is_ascii_whitespace()) {
        Some(b'{' | b'[') => "jsonl",
        _ => "csv",
    }
}

/// The output format `path`'s extension asks for.
pub fn format_for_extension(path: &Path) -> Result<Format> {
    let extension = path
        .extension()
        .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    Ok(match extension.as_str() {
        "csv" => Format::Csv,
        "jsonl" | "ndjson" | "json" => Format::Jsonl,
        "parquet" => Format::Parquet,
        "arrow" | "feather" | "ipc" => Format::Arrow,
        _ => bail!(
            "cannot tell the output format from {}; add --format csv, jsonl, parquet, arrow \
             or table",
            path.display()
        ),
    })
}

/// Fails when `output` is the input file: it is still being read while the
/// output is written, and replacing it would lose it.
pub fn check_not_input(input: &str, output: &Path) -> Result<()> {
    if input == "-" || parquet_reader::is_remote(Path::new(input)) {
        return Ok(());
    }
    if let (Ok(input), Ok(canonical)) = (fs::canonicalize(input), fs::canonicalize(output))
        && input == canonical
    {
        bail!(
            "--output {} is the file being read; write to another file",
            output.display()
        );
    }
    Ok(())
}

/// Runs `write` into a temporary file next to `path` and renames it to `path`
/// once everything is written and on disk. On any error the temporary file is
/// removed and `path` is left as it was.
pub fn write_atomically(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
) -> Result<()> {
    let temporary = temporary_path(path);
    let result = File::create(&temporary)
        .with_context(|| format!("create {}", temporary.display()))
        .and_then(|file| {
            let mut out = BufWriter::new(file);
            write(&mut out)?;
            let file = out.into_inner().map_err(io::IntoInnerError::into_error)?;
            file.sync_all()?;
            Ok(())
        })
        .and_then(|()| {
            fs::rename(&temporary, path).with_context(|| format!("write {}", path.display()))
        });
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// `<name>.pavi-cli-<pid>.tmp` in the target's folder, so the rename stays on
/// one disk (the app's export uses `<name>.pavi-<generation>.tmp`).
fn temporary_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map_or_else(|| "output".into(), |name| name.to_string_lossy());
    path.with_file_name(format!("{name}.pavi-cli-{}.tmp", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniffs_the_input_format_from_the_first_bytes() {
        assert_eq!(sniff_extension(b"PAR1\x15\x04"), "parquet");
        assert_eq!(sniff_extension(b"ARROW1\0\0"), "arrow");
        assert_eq!(sniff_extension(&[0xff, 0xff, 0xff, 0xff, 0x10]), "arrows");
        assert_eq!(sniff_extension(b"  \n{\"a\":1}"), "jsonl");
        assert_eq!(sniff_extension(b"\xef\xbb\xbf[{\"a\":1}]"), "jsonl");
        assert_eq!(sniff_extension(b"a,b\n1,2\n"), "csv");
    }

    #[test]
    fn chooses_the_output_format_from_the_extension() {
        let format = |name: &str| format_for_extension(Path::new(name)).ok();
        assert_eq!(format("out.CSV"), Some(Format::Csv));
        assert_eq!(format("out.json"), Some(Format::Jsonl));
        assert_eq!(format("dir/out.parquet"), Some(Format::Parquet));
        assert_eq!(format("out.feather"), Some(Format::Arrow));
        assert_eq!(format("out.tsv"), None);
        assert_eq!(format("out"), None);
    }

    #[test]
    fn keeps_the_old_file_when_writing_fails() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("out.csv");
        fs::write(&path, "old").unwrap();

        let error = write_atomically(&path, |out| {
            out.write_all(b"partial")?;
            bail!("the read failed")
        });
        assert!(error.is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "old");
        assert_eq!(
            fs::read_dir(folder.path()).unwrap().count(),
            1,
            "temp file left"
        );

        write_atomically(&path, |out| Ok(out.write_all(b"new")?)).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(fs::read_dir(folder.path()).unwrap().count(), 1);
    }

    #[test]
    fn deletes_the_stdin_copy_when_dropped() {
        let copy = copy_stdin(&mut &b"a,b\n1,2\n"[..]).unwrap();
        assert!(copy.path.ends_with("stdin.csv"));
        assert_eq!(fs::read_to_string(&copy.path).unwrap(), "a,b\n1,2\n");
        let folder = copy.folder.clone();
        drop(copy);
        assert!(!folder.exists());

        assert!(copy_stdin(&mut &b""[..]).is_err());
    }
}
