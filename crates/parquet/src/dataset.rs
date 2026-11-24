//! A folder of Parquet files read as one table (ADR 0006).
//!
//! The files are found by walking the folder, in name order, so the table's row
//! order is stable. Folders named `key=value` (Hive-style partitions, as Spark,
//! DuckDB, Polars and pyarrow write them) become extra columns holding that
//! value for every row below them.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow_array::{ArrayRef, Int64Array, StringArray, new_null_array};
use arrow_schema::{DataType, Field, Schema};
use parquet::{data_type::ByteArray, file::statistics::Statistics};

use crate::{change::FolderStamp, source::Cancelled};

/// Parquet files one folder dataset may hold. Each file's footer stays in memory.
pub const MAX_DATASET_FILES: usize = 4_096;
/// Folder levels below the dataset folder that are searched.
pub const MAX_FOLDER_DEPTH: usize = 16;
/// Directory entries looked at while searching, so opening a huge tree by
/// mistake (a drive root) ends quickly with an error.
pub const MAX_FOLDER_ENTRIES: usize = 100_000;
/// Footer metadata kept in memory for all the files of a dataset together.
pub const MAX_DATASET_METADATA_BYTES: usize = 256 * 1024 * 1024;

/// The partition value Hive writes for a null.
const HIVE_NULL: &str = "__HIVE_DEFAULT_PARTITION__";

/// A Parquet file found in a dataset folder, with the `key=value` folders above it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FoundFile {
    pub(crate) path: PathBuf,
    pub(crate) partitions: Vec<(String, Option<String>)>,
}

/// What a dataset folder holds: its Parquet files, and every folder searched,
/// with its modification time from just before it was listed.
#[derive(Debug)]
pub(crate) struct Listing {
    pub(crate) files: Vec<FoundFile>,
    pub(crate) folders: Vec<FolderStamp>,
}

/// A column made from partition folder names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartitionColumn {
    pub(crate) name: String,
    /// `Int64` when every value is a plain integer, otherwise `Utf8`.
    pub(crate) data_type: DataType,
}

/// Finds the Parquet files under `root`, in name order. Names starting with `.`
/// or `_` (`_SUCCESS`, `_delta_log`, hidden files) are skipped. Every file must
/// sit under the same partition keys, in the same order.
pub(crate) fn find_files(root: &Path, cancelled: &dyn Fn() -> bool) -> Result<Listing> {
    let mut listing = Listing {
        files: Vec::new(),
        folders: Vec::new(),
    };
    let mut entries = 0_usize;
    walk(root, 0, &[], &mut listing, &mut entries, cancelled)?;
    let files = &listing.files;
    let Some(first) = files.first() else {
        bail!(
            "{} holds no .parquet files (names starting with . or _ are skipped)",
            root.display()
        );
    };
    let keys = |file: &FoundFile| {
        file.partitions
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>()
    };
    let first_keys = keys(first);
    for (position, key) in first_keys.iter().enumerate() {
        if first_keys[..position].contains(key) {
            bail!(
                "{} is under two `{key}=` folders; a partition key can appear once",
                first.path.display()
            );
        }
    }
    if let Some(other) = files.iter().find(|file| keys(file) != first_keys) {
        bail!(
            "{} is under partition folders ({}) but {} is under ({}); every file \
             must sit under the same partition keys",
            other.path.display(),
            keys(other).join(", "),
            first.path.display(),
            first_keys.join(", ")
        );
    }
    Ok(listing)
}

fn walk(
    folder: &Path,
    depth: usize,
    partitions: &[(String, Option<String>)],
    listing: &mut Listing,
    entries: &mut usize,
    cancelled: &dyn Fn() -> bool,
) -> Result<()> {
    if cancelled() {
        return Err(Cancelled.into());
    }
    // Taken before listing, so an entry added while it is listed shows as a change.
    listing.folders.push(FolderStamp::of(folder));
    let mut children = Vec::new();
    for entry in
        std::fs::read_dir(folder).with_context(|| format!("list folder {}", folder.display()))?
    {
        *entries += 1;
        if *entries > MAX_FOLDER_ENTRIES {
            bail!(
                "this folder holds more than {MAX_FOLDER_ENTRIES} entries; open the \
                 folder that holds the dataset itself"
            );
        }
        let entry = entry.with_context(|| format!("list folder {}", folder.display()))?;
        children.push(entry.path());
    }
    children.sort();
    for path in children {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name.starts_with('.') || name.starts_with('_') {
            continue;
        }
        if path.is_dir() {
            if depth + 1 > MAX_FOLDER_DEPTH {
                bail!(
                    "{} is more than {MAX_FOLDER_DEPTH} folders deep",
                    path.display()
                );
            }
            let mut below = partitions.to_vec();
            below.extend(parse_partition(&name));
            walk(&path, depth + 1, &below, listing, entries, cancelled)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
        {
            if listing.files.len() >= MAX_DATASET_FILES {
                bail!("this folder holds more than {MAX_DATASET_FILES} Parquet files");
            }
            listing.files.push(FoundFile {
                path,
                partitions: partitions.to_vec(),
            });
        }
    }
    Ok(())
}

/// `key=value` → the key and value, percent-decoded; Hive's null marker and an
/// empty value are `None`. Other folder names are not partitions.
fn parse_partition(name: &str) -> Option<(String, Option<String>)> {
    let (key, value) = name.split_once('=')?;
    let key = percent_decode(key);
    if key.trim().is_empty() {
        return None;
    }
    let value = percent_decode(value);
    let value = (!value.is_empty() && value != HIVE_NULL).then_some(value);
    Some((key, value))
}

/// Decodes `%XX` escapes; anything else is kept as written.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escaped = (bytes[index] == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                decoded.push(byte);
                index += 3;
            }
            None => {
                decoded.push(bytes[index]);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// A folder's columns, merged from its files' columns (ADR 0019).
#[derive(Debug)]
pub(crate) struct MergedColumns {
    /// Each column's name, the type that holds every file's values
    /// ([`crate::types::common_type`]) and whether it can be null.
    pub(crate) fields: Vec<Field>,
    /// For each file, the file's top-level column holding each merged column;
    /// `None` where the file doesn't have it (its rows are null there).
    pub(crate) file_columns: Vec<Vec<Option<usize>>>,
}

/// Merges the files' columns by name: the first file's columns in its order,
/// then each column a later file adds, in the order it appears. Types are
/// widened to hold every file's values; a column some file lacks can be null.
/// `files` are each file's description (for messages) and its schema, with
/// every type already a [`crate::types::read_type`].
pub(crate) fn merge_columns(files: &[(String, Schema)]) -> Result<MergedColumns> {
    let mut fields: Vec<Field> = Vec::new();
    // The file each column's current type comes from, for messages.
    let mut origins: Vec<usize> = Vec::new();
    let mut present = Vec::new();
    let mut by_name = std::collections::HashMap::new();
    let mut file_columns = Vec::with_capacity(files.len());
    for (index, (name, schema)) in files.iter().enumerate() {
        let mut columns = vec![None; fields.len()];
        for (position, field) in schema.fields().iter().enumerate() {
            let merged = match by_name.get(field.name()) {
                Some(merged) => *merged,
                None => {
                    by_name.insert(field.name().clone(), fields.len());
                    fields.push(field.as_ref().clone());
                    origins.push(index);
                    present.push(0_usize);
                    columns.push(None);
                    fields.len() - 1
                }
            };
            if columns[merged].is_some() {
                bail!("{name} has two columns named {}", field.name());
            }
            columns[merged] = Some(position);
            present[merged] += 1;
            let current = &fields[merged];
            let Some(data_type) = crate::types::common_type(current.data_type(), field.data_type())
            else {
                bail!(
                    "{name} and {} can't be read as one table: column {} is {} in {name}                      but {} in {}, and no type holds both",
                    files[origins[merged]].0,
                    field.name(),
                    field.data_type(),
                    current.data_type(),
                    files[origins[merged]].0
                );
            };
            if &data_type != current.data_type() {
                origins[merged] = index;
            }
            let nullable = current.is_nullable() || field.is_nullable();
            fields[merged] = current
                .clone()
                .with_data_type(data_type)
                .with_nullable(nullable);
        }
        file_columns.push(columns);
    }
    for (field, present) in fields.iter_mut().zip(&present) {
        if *present < files.len() {
            *field = field.clone().with_nullable(true);
        }
    }
    // Columns added by later files are missing from the earlier files' maps.
    for columns in &mut file_columns {
        columns.resize(fields.len(), None);
    }
    Ok(MergedColumns {
        fields,
        file_columns,
    })
}

/// The partition columns of `files` (which share their keys), typed from all the
/// values: integers written the way Rust writes them become `Int64`.
pub(crate) fn partition_columns(files: &[FoundFile]) -> Vec<PartitionColumn> {
    let Some(first) = files.first() else {
        return Vec::new();
    };
    first
        .partitions
        .iter()
        .enumerate()
        .map(|(position, (name, _))| {
            let integers = files.iter().all(|file| {
                file.partitions
                    .get(position)
                    .and_then(|(_, value)| value.as_deref())
                    .is_none_or(is_integer)
            });
            PartitionColumn {
                name: name.clone(),
                data_type: if integers {
                    DataType::Int64
                } else {
                    DataType::Utf8
                },
            }
        })
        .collect()
}

/// `true` for text that parses as an `i64` and prints back the same (so `007`
/// and `+1` stay text).
fn is_integer(text: &str) -> bool {
    text.parse::<i64>()
        .is_ok_and(|value| value.to_string() == text)
}

/// A column of `rows` copies of a partition value.
pub(crate) fn constant_array(
    data_type: &DataType,
    value: Option<&str>,
    rows: usize,
) -> Result<ArrayRef> {
    Ok(match (data_type, value) {
        (_, None) => new_null_array(data_type, rows),
        (DataType::Int64, Some(text)) => {
            let value = text
                .parse::<i64>()
                .with_context(|| format!("partition value {text} is not an integer"))?;
            std::sync::Arc::new(Int64Array::from_value(value, rows))
        }
        (_, Some(text)) => std::sync::Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            text, rows,
        ))),
    })
}

/// Statistics for a partition column in one row group: every row holds `value`,
/// so filters on partition columns skip whole files.
pub(crate) fn partition_statistics(
    data_type: &DataType,
    value: Option<&str>,
    rows: u64,
) -> Statistics {
    match (data_type, value) {
        (DataType::Int64, Some(text)) => {
            let value = text.parse::<i64>().ok();
            Statistics::int64(value, value, Some(1), Some(0), false)
        }
        (DataType::Int64, None) => Statistics::int64(None, None, Some(0), Some(rows), false),
        (_, Some(text)) => {
            let value = Some(ByteArray::from(text));
            Statistics::byte_array(value.clone(), value, Some(1), Some(0), false)
        }
        (_, None) => Statistics::byte_array(None, None, Some(0), Some(rows), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hive_partition_folder_names() {
        assert_eq!(
            parse_partition("year=2024"),
            Some(("year".to_string(), Some("2024".to_string())))
        );
        assert_eq!(
            parse_partition("city=New%20York"),
            Some(("city".to_string(), Some("New York".to_string())))
        );
        assert_eq!(
            parse_partition("city=__HIVE_DEFAULT_PARTITION__"),
            Some(("city".to_string(), None))
        );
        assert_eq!(parse_partition("day="), Some(("day".to_string(), None)));
        assert_eq!(parse_partition("data"), None);
        assert_eq!(parse_partition("=x"), None);
        // A broken escape is kept as written.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("a%zzb"), "a%zzb");
    }

    #[test]
    fn types_partition_columns_from_every_value() {
        let file = |values: &[Option<&str>]| FoundFile {
            path: PathBuf::new(),
            partitions: ["year", "code"]
                .iter()
                .zip(values)
                .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
                .collect(),
        };
        let files = vec![
            file(&[Some("2023"), Some("1")]),
            file(&[None, Some("007")]),
            file(&[Some("-5"), Some("2")]),
        ];
        let columns = partition_columns(&files);
        assert_eq!(columns[0].data_type, DataType::Int64);
        assert_eq!(columns[1].data_type, DataType::Utf8);
        assert_eq!(
            constant_array(&DataType::Int64, Some("-5"), 3)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            constant_array(&DataType::Utf8, None, 2)
                .unwrap()
                .null_count(),
            2
        );
    }

    #[test]
    fn finds_files_in_name_order_and_checks_partition_keys() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        for path in [
            "year=2024/month=2/b.parquet",
            "year=2024/month=2/a.parquet",
            "year=2023/month=12/part.PARQUET",
            "year=2023/month=12/_SUCCESS",
            "year=2023/month=12/notes.txt",
            "_delta_log/x.parquet",
            ".hidden/y.parquet",
        ] {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"").unwrap();
        }
        let files = find_files(root, &|| false).unwrap().files;
        let names = files
            .iter()
            .map(|file| {
                file.path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "year=2023/month=12/part.PARQUET",
                "year=2024/month=2/a.parquet",
                "year=2024/month=2/b.parquet"
            ]
        );
        assert_eq!(
            files[0].partitions,
            [
                ("year".to_string(), Some("2023".to_string())),
                ("month".to_string(), Some("12".to_string()))
            ]
        );

        // A file outside the partition folders does not fit the table.
        std::fs::write(root.join("loose.parquet"), b"").unwrap();
        let error = find_files(root, &|| false).unwrap_err().to_string();
        assert!(error.contains("same partition keys"), "{error}");

        let empty = tempfile::tempdir().unwrap();
        let error = find_files(empty.path(), &|| false).unwrap_err().to_string();
        assert!(error.contains("no .parquet files"), "{error}");
        assert!(
            find_files(root, &|| true)
                .unwrap_err()
                .downcast_ref::<Cancelled>()
                .is_some()
        );
    }
}
