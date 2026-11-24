//! Noticing that the files a source was opened from changed on disk (ADR 0018).
//!
//! A source keeps each file's footer from the moment it opened, and reopens the
//! file by path for every read. If the file is rewritten in between, the old
//! footer would point into new bytes. So each local file's size and modification
//! time are recorded when it opens, every read compares them with the file it
//! just opened, and [`crate::ParquetSource::check_changed`] compares them with
//! what is on disk now.

use std::{fs::Metadata, path::Path, time::SystemTime};

/// A local file's size and modification time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
}

impl FileStamp {
    pub(crate) fn of(metadata: &Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }

    pub(crate) fn new(len: u64, modified: Option<SystemTime>) -> Self {
        Self { len, modified }
    }

    /// `None` when `path` still has this size and time, else what changed.
    pub(crate) fn check(&self, path: &Path) -> Option<FileChanged> {
        match std::fs::metadata(path) {
            Ok(metadata) if Self::of(&metadata) == *self => None,
            Ok(_) => Some(FileChanged::modified(path)),
            Err(_) => Some(FileChanged::removed(path)),
        }
    }
}

/// A folder's modification time, which changes when an entry is added to it,
/// removed from it or renamed in it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FolderStamp {
    pub(crate) path: std::path::PathBuf,
    pub(crate) modified: Option<SystemTime>,
}

impl FolderStamp {
    pub(crate) fn of(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            modified: std::fs::metadata(path)
                .ok()
                .and_then(|metadata| metadata.modified().ok()),
        }
    }

    pub(crate) fn check(&self) -> Option<FileChanged> {
        match std::fs::metadata(&self.path) {
            Ok(metadata) if metadata.modified().ok() == self.modified => None,
            Ok(_) => Some(FileChanged {
                message: format!(
                    "files were added to or removed from {} after it was opened",
                    self.path.display()
                ),
            }),
            Err(_) => Some(FileChanged::removed(&self.path)),
        }
    }
}

/// A file or folder a source was opened from is no longer what it was. Reads
/// fail with this error (find it with `error.downcast_ref::<FileChanged>()`),
/// and [`crate::ParquetSource::check_changed`] returns it. Opening the file
/// again reads what it holds now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileChanged {
    message: String,
}

impl FileChanged {
    pub(crate) fn modified(path: &Path) -> Self {
        Self {
            message: format!("{} changed on disk after it was opened", path.display()),
        }
    }

    pub(crate) fn removed(path: &Path) -> Self {
        Self {
            message: format!(
                "{} was removed or can no longer be read after it was opened",
                path.display()
            ),
        }
    }

    /// What changed, without the advice to open it again.
    pub fn summary(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for FileChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; open it again to read it as it is now", self.message)
    }
}

impl std::error::Error for FileChanged {}
