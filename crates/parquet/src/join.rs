//! Hash joins between sources, and the temporary tables that hold joined and
//! subquery rows (ADR 0009).
//!
//! One side of a join (the *build* side) is read into memory and hashed by its
//! key columns; the other (the *probe* side) streams past it a batch at a time,
//! and the joined rows are written to a temporary Parquet table that the query
//! engine then reads like any other source.

use std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{concat::concat_batches, filter::filter_record_batch, take::take};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::{
    FilterExpr, ParquetSource, Projection,
    source::{Scalar, scalar_at},
    spill::TempFolder,
    types::{is_comparable, is_signed_integer, is_string, is_unsigned_integer},
};

/// Rows the in-memory side of a join may hold, after its filters.
pub const MAX_JOIN_BUILD_ROWS: usize = 1_000_000;
/// Memory the in-memory side of a join may use, rows and hash keys together.
pub const MAX_JOIN_BUILD_BYTES: usize = 256 * 1024 * 1024;
/// Key columns in one join's `ON`.
pub const MAX_JOIN_KEYS: usize = 4;
/// Largest temporary table a join or subquery may write.
pub const MAX_PREPARED_BYTES: u64 = 32 * 1024 * 1024 * 1024;
/// Rows per batch read from the inputs and written to the output.
const BATCH_ROWS: usize = 8_192;
/// Rows per row group of a temporary table, so filters can skip row groups.
const ROW_GROUP_ROWS: usize = 262_144;
/// A row group is written out once this much is buffered.
const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
const PREPARED_PREFIX: &str = "pavi-query-";
/// Hash map overhead counted per in-memory row, besides its key values.
const ROW_OVERHEAD_BYTES: usize = 64;

/// Writes batches to a temporary Parquet table, read back as a [`ParquetSource`]
/// that deletes the file when dropped.
pub struct TempTableWriter {
    folder: TempFolder,
    path: PathBuf,
    writer: ArrowWriter<File>,
    schema: SchemaRef,
}

impl TempTableWriter {
    /// A table with `schema`'s columns, every one nullable.
    pub fn new(schema: &Schema) -> Result<Self> {
        let schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<Field>>(),
        ));
        let folder = TempFolder::create(PREPARED_PREFIX)?;
        let path = folder.path.join("table.parquet");
        let file = File::create(&path)
            .with_context(|| format!("create temporary table {}", path.display()))?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
        Ok(Self {
            folder,
            path,
            writer,
            schema,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Writes `batch`, whose columns must match the table's in order and type.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), batch.columns().to_vec())
            .context("a temporary table row does not match its columns")?;
        self.writer.write(&batch)?;
        if self.writer.in_progress_size() > MAX_BUFFERED_BYTES {
            self.writer.flush()?;
        }
        if self.writer.bytes_written() as u64 > MAX_PREPARED_BYTES {
            bail!(
                "this query's joined or subquery rows need more than {} GB of temporary disk \
                 space; filter the tables first",
                MAX_PREPARED_BYTES / (1024 * 1024 * 1024)
            );
        }
        Ok(())
    }

    /// Closes the file and opens it; `name` is what the table is called in messages.
    pub fn finish(self, name: &str) -> Result<ParquetSource> {
        self.writer.close().context("finish the temporary table")?;
        ParquetSource::open_temporary(self.path, self.folder, name)
    }
}

/// One input of a join: a source, the filter applied while reading it, the
/// columns it contributes to the output and its key columns.
#[derive(Clone)]
pub struct JoinSide {
    pub source: Arc<ParquetSource>,
    pub filter: Option<FilterExpr>,
    /// Source columns in the output, in order.
    pub columns: Vec<usize>,
    /// Source columns compared with the other side's keys, in `ON` order.
    pub keys: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinKind {
    /// Rows with a match on both sides.
    Inner,
    /// Every left row; right columns are null where nothing matches.
    Left,
    /// Left rows that have a match (`IN (SELECT …)`); left columns only.
    Semi,
    /// Left rows that have no match (`NOT IN (SELECT …)`); left columns only.
    Anti,
}

impl JoinSide {
    fn field(&self, column: usize) -> Result<Field> {
        self.source
            .schema()
            .fields()
            .get(column)
            .map(|field| field.as_ref().clone())
            .ok_or_else(|| anyhow!("join column {column} out of range"))
    }
}

/// How a key type is compared: integers of any width with each other, text of
/// any kind with each other, anything else only with the same type.
#[derive(Clone, Debug, PartialEq)]
enum KeyFamily {
    Integer,
    Text,
    Exact(DataType),
}

fn key_family(data_type: &DataType) -> KeyFamily {
    match data_type {
        DataType::Dictionary(_, values) => key_family(values),
        data_type if is_signed_integer(data_type) || is_unsigned_integer(data_type) => {
            KeyFamily::Integer
        }
        data_type if is_string(data_type) => KeyFamily::Text,
        data_type => KeyFamily::Exact(data_type.clone()),
    }
}

/// Checks that two sides can be joined: as many keys on each, at most
/// [`MAX_JOIN_KEYS`], each pair comparable and of matching types.
pub fn check_join_keys(left: &JoinSide, right: &JoinSide) -> Result<()> {
    if left.keys.is_empty() || left.keys.len() != right.keys.len() {
        bail!("a join needs the same number of key columns on each side");
    }
    if left.keys.len() > MAX_JOIN_KEYS {
        bail!("a join compares at most {MAX_JOIN_KEYS} pairs of columns");
    }
    for (&left_key, &right_key) in left.keys.iter().zip(&right.keys) {
        check_join_key_types(&left.field(left_key)?, &right.field(right_key)?)?;
    }
    Ok(())
}

/// Checks that two key columns can be compared in a join: both comparable,
/// and both integers, both text, or of the same type.
pub fn check_join_key_types(left: &Field, right: &Field) -> Result<()> {
    for field in [left, right] {
        if !is_comparable(field.data_type()) {
            bail!(
                "{} is {}, which can't be compared in a join",
                field.name(),
                field.data_type()
            );
        }
    }
    if key_family(left.data_type()) != key_family(right.data_type()) {
        bail!(
            "a join compares {} ({}) with {} ({}); the types must match",
            left.name(),
            left.data_type(),
            right.name(),
            right.data_type()
        );
    }
    Ok(())
}

/// Joins `left` and `right` into a temporary table. Its columns are `left`'s
/// then (for inner and left joins) `right`'s, named by `names`. `checkpoint`
/// gets the share done (0 to 1) and cancels by returning `true`.
pub fn join(
    left: &JoinSide,
    right: &JoinSide,
    kind: JoinKind,
    names: &[String],
    checkpoint: &dyn Fn(f64) -> bool,
) -> Result<ParquetSource> {
    check_join_keys(left, right)?;
    let outputs_right = matches!(kind, JoinKind::Inner | JoinKind::Left);
    let mut fields = Vec::new();
    for column in &left.columns {
        fields.push(left.field(*column)?);
    }
    if outputs_right {
        for column in &right.columns {
            fields.push(right.field(*column)?);
        }
    }
    if names.len() != fields.len() {
        bail!(
            "a join was given {} names for {} columns",
            names.len(),
            fields.len()
        );
    }
    let fields = fields
        .into_iter()
        .zip(names)
        .map(|(field, name)| field.with_name(name.clone()))
        .collect::<Vec<_>>();
    let mut writer = TempTableWriter::new(&Schema::new(fields))?;

    // An inner join keeps the side with fewer rows in memory; the others keep
    // the right side, whose rows are the optional or looked-up ones.
    let build_is_left =
        kind == JoinKind::Inner && left.source.row_count() < right.source.row_count();
    let (build, probe) = if build_is_left {
        (left, right)
    } else {
        (right, left)
    };
    let build_output = if outputs_right {
        build.columns.len()
    } else {
        0
    };
    let table = BuildTable::read(build, build_output, &|done| checkpoint(done / 2.0))?;
    if kind == JoinKind::Anti && table.has_null_key {
        // `x NOT IN (… NULL …)` is never true.
        return writer.finish("join");
    }

    let probe_rows = probe.source.row_count().max(1) as f64;
    scan(
        probe,
        &|row| checkpoint(0.5 + row as f64 / probe_rows / 2.0),
        &mut |batch| {
            let keys = row_keys(&batch, probe.columns.len(), probe.keys.len())?;
            match kind {
                JoinKind::Semi | JoinKind::Anti => {
                    let keep = keys
                        .iter()
                        .map(|key| {
                            let found =
                                key.as_ref().is_some_and(|key| table.rows.contains_key(key));
                            Some(key.is_some() && (found == (kind == JoinKind::Semi)))
                        })
                        .collect::<BooleanArray>();
                    let kept = filter_record_batch(&batch, &keep)?;
                    writer.write(&kept.project(&(0..probe.columns.len()).collect::<Vec<_>>())?)
                }
                JoinKind::Inner | JoinKind::Left => {
                    let mut probe_rows = Vec::new();
                    let mut build_rows = Vec::new();
                    for (row, key) in keys.iter().enumerate() {
                        let matches = key.as_ref().and_then(|key| table.rows.get(key));
                        match matches {
                            Some(matches) => {
                                for &build_row in matches {
                                    probe_rows.push(row as u32);
                                    build_rows.push(Some(build_row));
                                }
                            }
                            None if kind == JoinKind::Left => {
                                probe_rows.push(row as u32);
                                build_rows.push(None);
                            }
                            None => {}
                        }
                        if probe_rows.len() >= BATCH_ROWS {
                            let (probe_part, build_part) = (
                                std::mem::take(&mut probe_rows),
                                std::mem::take(&mut build_rows),
                            );
                            write_joined(
                                &mut writer,
                                &batch,
                                probe,
                                &table,
                                probe_part,
                                build_part,
                                build_is_left,
                            )?;
                        }
                    }
                    write_joined(
                        &mut writer,
                        &batch,
                        probe,
                        &table,
                        probe_rows,
                        build_rows,
                        build_is_left,
                    )
                }
            }
        },
    )?;
    writer.finish("join")
}

/// The in-memory side: its output columns as one batch, and its rows by key.
struct BuildTable {
    batch: Option<RecordBatch>,
    rows: HashMap<Vec<Scalar>, Vec<u32>>,
    has_null_key: bool,
}

impl BuildTable {
    /// Reads `side`, keeping its first `output_columns` columns and hashing its keys.
    fn read(
        side: &JoinSide,
        output_columns: usize,
        checkpoint: &dyn Fn(f64) -> bool,
    ) -> Result<Self> {
        let source_rows = side.source.row_count().max(1) as f64;
        let mut batches = Vec::new();
        let mut rows = 0_usize;
        let mut bytes = 0_usize;
        let mut table = HashMap::new();
        let mut has_null_key = false;
        let over_budget = || {
            anyhow!(
                "the side of this join kept in memory has more than {} rows or {} MB after its \
                 filters; filter it, or put the smaller table on the right",
                MAX_JOIN_BUILD_ROWS,
                MAX_JOIN_BUILD_BYTES / (1024 * 1024)
            )
        };
        scan(
            side,
            &|row| checkpoint(row as f64 / source_rows),
            &mut |batch| {
                let keys = row_keys(&batch, side.columns.len(), side.keys.len())?;
                for key in keys {
                    match key {
                        Some(key) => {
                            bytes +=
                                key.iter().map(scalar_bytes).sum::<usize>() + ROW_OVERHEAD_BYTES;
                            table.entry(key).or_insert_with(Vec::new).push(rows as u32);
                        }
                        None => {
                            has_null_key = true;
                            bytes += ROW_OVERHEAD_BYTES;
                        }
                    }
                    rows += 1;
                }
                let kept = batch.project(&(0..output_columns).collect::<Vec<_>>())?;
                bytes += kept.get_array_memory_size();
                batches.push(kept);
                if rows > MAX_JOIN_BUILD_ROWS || bytes > MAX_JOIN_BUILD_BYTES {
                    return Err(over_budget());
                }
                Ok(())
            },
        )?;
        let batch = match batches.first() {
            Some(first) if output_columns > 0 => Some(concat_batches(&first.schema(), &batches)?),
            _ => None,
        };
        Ok(Self {
            batch,
            rows: table,
            has_null_key,
        })
    }
}

fn scalar_bytes(scalar: &Scalar) -> usize {
    std::mem::size_of::<Scalar>()
        + match scalar {
            Scalar::Utf8(text) => text.len(),
            _ => 0,
        }
}

/// Writes joined rows: `probe_rows` index `batch`, `build_rows` the build
/// table (`None` gives nulls, for a left join's unmatched rows).
fn write_joined(
    writer: &mut TempTableWriter,
    batch: &RecordBatch,
    probe: &JoinSide,
    table: &BuildTable,
    probe_rows: Vec<u32>,
    build_rows: Vec<Option<u32>>,
    build_is_left: bool,
) -> Result<()> {
    if probe_rows.is_empty() {
        return Ok(());
    }
    let probe_indices = UInt32Array::from(probe_rows);
    let build_indices = UInt32Array::from(build_rows);
    let probe_columns = (0..probe.columns.len())
        .map(|column| take(batch.column(column).as_ref(), &probe_indices, None))
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
    let build_columns = match &table.batch {
        Some(build) => build
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &build_indices, None))
            .collect::<std::result::Result<Vec<ArrayRef>, _>>()?,
        None => Vec::new(),
    };
    let columns = if build_is_left {
        build_columns.into_iter().chain(probe_columns).collect()
    } else {
        probe_columns.into_iter().chain(build_columns).collect()
    };
    let schema = writer.schema();
    writer.write(&RecordBatch::try_new(schema, columns)?)
}

/// Each row's key (the last `keys` columns of `batch`, after its `columns`
/// output columns), or `None` when a key value is null. Unsigned integers that
/// fit are compared as signed ones, so any integer types can be joined.
fn row_keys(batch: &RecordBatch, columns: usize, keys: usize) -> Result<Vec<Option<Vec<Scalar>>>> {
    let key_columns = (columns..columns + keys)
        .map(|column| batch.column(column))
        .collect::<Vec<_>>();
    (0..batch.num_rows())
        .map(|row| {
            let mut key = Vec::with_capacity(keys);
            for column in &key_columns {
                let value = scalar_at(column.as_ref(), row, column.data_type())?;
                key.push(match value {
                    Scalar::Null => return Ok(None),
                    Scalar::U64(value) => match i64::try_from(value) {
                        Ok(value) => Scalar::I64(value),
                        Err(_) => Scalar::U64(value),
                    },
                    value => value,
                });
            }
            Ok(Some(key))
        })
        .collect()
}

/// Reads `side` with its filter, calling `each` with batches of its output
/// columns followed by its key columns. `checkpoint` gets the source row
/// reached and cancels by returning `true`.
fn scan(
    side: &JoinSide,
    checkpoint: &dyn Fn(u64) -> bool,
    each: &mut dyn FnMut(RecordBatch) -> Result<()>,
) -> Result<()> {
    let column_count = side.source.column_count();
    let read = side
        .columns
        .iter()
        .chain(&side.keys)
        .copied()
        .collect::<Vec<_>>();
    // A column can be both output and key; the reader wants each once.
    let mut unique = Vec::new();
    for column in &read {
        if !unique.contains(column) {
            unique.push(*column);
        }
    }
    let projection = Projection::columns(unique.clone(), column_count)?;
    let positions = read
        .iter()
        .map(|column| unique.iter().position(|kept| kept == column).unwrap_or(0))
        .collect::<Vec<_>>();
    let rows = side.source.row_count();
    match &side.filter {
        Some(filter) => {
            let mut start = 0;
            while start < rows {
                let window = side.source.read_filtered_from(
                    filter,
                    start,
                    BATCH_ROWS,
                    &projection,
                    checkpoint,
                )?;
                if window.batch.num_rows() > 0 {
                    each(window.batch.project(&positions)?)?;
                }
                if window.next_row <= start {
                    break;
                }
                start = window.next_row;
            }
        }
        None => {
            let mut start = 0;
            while start < rows {
                if checkpoint(start) {
                    return Err(crate::Cancelled.into());
                }
                let batch = side.source.read_window(start, BATCH_ROWS, &projection)?;
                if batch.num_rows() == 0 {
                    break;
                }
                start += batch.num_rows() as u64;
                each(batch.project(&positions)?)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, Int64Array, StringArray, UInt8Array};
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::value::scalar_text;

    fn table(directory: &std::path::Path, name: &str, batch: RecordBatch) -> Arc<ParquetSource> {
        let path = directory.join(name);
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Arc::new(ParquetSource::open(&path).unwrap())
    }

    fn rows(source: &ParquetSource) -> Vec<String> {
        let batch = source
            .read_window(0, 1_000, &Projection::all(source.column_count()))
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                (0..batch.num_columns())
                    .map(|column| {
                        scalar_text(batch.column(column).as_ref(), row)
                            .unwrap()
                            .unwrap_or_else(|| "null".into())
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect()
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    fn sides() -> (tempfile::TempDir, JoinSide, JoinSide) {
        let directory = tempfile::tempdir().unwrap();
        let orders = table(
            directory.path(),
            "orders.parquet",
            RecordBatch::try_from_iter(vec![
                (
                    "order",
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                ),
                (
                    "customer",
                    Arc::new(Int64Array::from(vec![
                        Some(10),
                        Some(20),
                        Some(10),
                        None,
                        Some(99),
                    ])),
                ),
            ])
            .unwrap(),
        );
        let customers = table(
            directory.path(),
            "customers.parquet",
            RecordBatch::try_from_iter(vec![
                (
                    "id",
                    Arc::new(UInt8Array::from(vec![10, 20, 30])) as ArrayRef,
                ),
                ("name", Arc::new(StringArray::from(vec!["Ann", "Bo", "Cy"]))),
            ])
            .unwrap(),
        );
        (
            directory,
            JoinSide {
                source: orders,
                filter: None,
                columns: vec![0, 1],
                keys: vec![1],
            },
            JoinSide {
                source: customers,
                filter: None,
                columns: vec![1],
                keys: vec![0],
            },
        )
    }

    #[test]
    fn joins_inner_left_semi_and_anti_with_sql_null_rules() {
        let (_directory, orders, customers) = sides();
        let both = names(&["order", "customer", "name"]);
        let inner = join(&orders, &customers, JoinKind::Inner, &both, &|_| false).unwrap();
        assert_eq!(rows(&inner), ["1,10,Ann", "2,20,Bo", "3,10,Ann"]);
        let left = join(&orders, &customers, JoinKind::Left, &both, &|_| false).unwrap();
        assert_eq!(
            rows(&left),
            [
                "1,10,Ann",
                "2,20,Bo",
                "3,10,Ann",
                "4,null,null",
                "5,99,null"
            ]
        );
        let left_only = names(&["order", "customer"]);
        let semi = join(&orders, &customers, JoinKind::Semi, &left_only, &|_| false).unwrap();
        assert_eq!(rows(&semi), ["1,10", "2,20", "3,10"]);
        // A null key is never "not in" anything.
        let anti = join(&orders, &customers, JoinKind::Anti, &left_only, &|_| false).unwrap();
        assert_eq!(rows(&anti), ["5,99"]);

        // NOT IN against a list containing null: no row passes.
        let anti = join(
            &customers.clone(),
            &JoinSide {
                columns: vec![],
                ..orders.clone()
            },
            JoinKind::Anti,
            &names(&["name"]),
            &|_| false,
        )
        .map(|source| rows(&source));
        assert_eq!(anti.unwrap(), Vec::<String>::new());
    }

    #[test]
    fn keeps_the_smaller_side_and_filters_while_reading() {
        let (_directory, mut orders, customers) = sides();
        // Customers first: the inner join still keeps the smaller table in memory
        // and writes left columns first.
        let swapped = join(
            &JoinSide {
                columns: vec![1, 0],
                ..customers.clone()
            },
            &orders,
            JoinKind::Inner,
            &names(&["name", "id", "order", "customer"]),
            &|_| false,
        )
        .unwrap();
        assert_eq!(rows(&swapped), ["Ann,10,1,10", "Bo,20,2,20", "Ann,10,3,10"]);
        orders.filter = Some(FilterExpr::parse("order >= 2").unwrap());
        let filtered = join(
            &orders,
            &customers,
            JoinKind::Inner,
            &names(&["order", "customer", "name"]),
            &|_| false,
        )
        .unwrap();
        assert_eq!(rows(&filtered), ["2,20,Bo", "3,10,Ann"]);
        assert!(
            join(
                &orders,
                &customers,
                JoinKind::Inner,
                &names(&["a", "b", "c"]),
                &|_| true
            )
            .err()
            .unwrap()
            .downcast_ref::<crate::Cancelled>()
            .is_some()
        );
    }

    #[test]
    fn rejects_mismatched_keys() {
        let (_directory, orders, customers) = sides();
        let text_key = JoinSide {
            keys: vec![1],
            ..customers.clone()
        };
        let error = check_join_keys(&orders, &text_key).unwrap_err().to_string();
        assert!(error.contains("types must match"), "{error}");
        let two_keys = JoinSide {
            keys: vec![0, 1],
            ..orders.clone()
        };
        assert!(check_join_keys(&two_keys, &customers).is_err());
    }
}
