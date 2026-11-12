//! Shared fixtures for the fuzz targets and the seed generator.

use std::{
    cell::RefCell,
    fs::{self, File},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, DictionaryArray, Float64Array,
    Int32Array, Int64Array, LargeStringArray, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray, types::Int32Type,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use parquet_reader::{ParquetSource, value};

thread_local! {
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Replaces libfuzzer-sys's panic hook, which aborts on every panic, even one PAVI catches
/// on purpose (`load_arrow_metadata`, `GuardedReader`). Call it from `fuzz_target!`'s `init:`.
/// The hook only remembers the message; [`run`] decides whether the panic was a bug.
pub fn allow_contained_panics() {
    panic::set_hook(Box::new(|info| {
        let message = info.payload_as_str().unwrap_or("non-text panic payload");
        let location = info
            .location()
            .map(|location| format!(" at {location}"))
            .unwrap_or_default();
        LAST_PANIC.with(|slot| *slot.borrow_mut() = Some(format!("{message}{location}")));
    }));
}

/// Runs one fuzz input. A panic that escapes PAVI's code is a bug: print it and abort so
/// libFuzzer saves the input.
pub fn run(check: impl FnOnce()) {
    if panic::catch_unwind(AssertUnwindSafe(check)).is_err() {
        let message = LAST_PANIC.with(|slot| slot.borrow_mut().take());
        eprintln!("panic escaped PAVI: {}", message.unwrap_or_default());
        std::process::abort();
    }
}

/// A per-process scratch directory, so parallel fuzz jobs do not share files.
pub fn scratch_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pavi-fuzz-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("create fuzz scratch dir");
    dir
}

/// A small batch covering the column types that filters, SQL and formatting accept or reject.
pub fn fixture_batch(rows: i32) -> RecordBatch {
    let ids = 0..rows;
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(
        ids.clone()
            .map(|id| (id % 3 != 0).then(|| (0..id % 4).map(Some).collect::<Vec<_>>())),
    );
    let point = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(ids.clone())) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from_iter(
                ids.clone()
                    .map(|id| (id % 2 == 0).then(|| format!("p{id}"))),
            )) as ArrayRef,
        ),
    ]);
    let category: DictionaryArray<Int32Type> = ids
        .clone()
        .map(|id| ["red", "green", "blue"][(id % 3) as usize])
        .collect();
    let timestamps = TimestampMillisecondArray::from_iter_values(
        ids.clone()
            .map(|id| 1_700_000_000_000 + i64::from(id) * 3_600_000),
    )
    .with_timezone("Europe/Berlin");
    let amounts = Decimal128Array::from_iter_values(ids.clone().map(|id| i128::from(id) * 1_234))
        .with_precision_and_scale(10, 2)
        .expect("decimal");
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(ids.clone().map(i64::from))),
        ),
        (
            "small",
            Arc::new(Int32Array::from_iter(
                ids.clone().map(|id| (id % 5 != 0).then_some(id - 3)),
            )),
        ),
        (
            "score",
            Arc::new(Float64Array::from_iter_values(
                ids.clone().map(|id| f64::from(id) * 1.5 - 4.0),
            )),
        ),
        (
            "name",
            Arc::new(StringArray::from_iter(
                ids.clone()
                    .map(|id| (id % 4 != 0).then(|| format!("name-{id}"))),
            )),
        ),
        (
            "text",
            Arc::new(LargeStringArray::from_iter_values(
                ids.clone().map(|id| "é".repeat((id % 5) as usize)),
            )),
        ),
        (
            "active",
            Arc::new(BooleanArray::from_iter(
                ids.clone().map(|id| Some(id % 2 == 0)),
            )),
        ),
        (
            "day",
            Arc::new(Date32Array::from_iter_values(
                ids.clone().map(|id| 19_700 + id),
            )),
        ),
        ("ts", Arc::new(timestamps)),
        ("amount", Arc::new(amounts)),
        (
            "at",
            Arc::new(arrow_array::Time64MicrosecondArray::from_iter(
                ids.clone()
                    .map(|id| (id % 6 != 0).then_some(i64::from(id) * 600_000_000)),
            )),
        ),
        ("category", Arc::new(category)),
        ("tags", Arc::new(tags)),
        ("point", Arc::new(point)),
    ];
    let fields: Fields = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .expect("fixture batch")
}

/// Writes `batch` as Parquet with the given compression and row-group size.
pub fn write_parquet(
    path: &Path,
    batch: &RecordBatch,
    compression: Compression,
    row_group_rows: usize,
) {
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let file = File::create(path).expect("create parquet");
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), Some(properties)).expect("parquet writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close parquet");
}

/// The source that SQL and filter inputs run against, opened once per process.
pub fn fixture_source() -> Arc<ParquetSource> {
    static SOURCE: OnceLock<Arc<ParquetSource>> = OnceLock::new();
    SOURCE
        .get_or_init(|| {
            let path = scratch_dir().join("fixture.parquet");
            write_parquet(&path, &fixture_batch(40), Compression::SNAPPY, 16);
            Arc::new(ParquetSource::open(&path).expect("open fixture"))
        })
        .clone()
}

/// Formats every cell the way the grid, CSV export and profiling would.
pub fn format_all(batch: &RecordBatch) {
    for column in batch.columns() {
        for row in 0..column.len() {
            let _ = value::format_cell_with_limit(column.as_ref(), row, 256);
            let _ = value::scalar_text(column.as_ref(), row);
        }
    }
}

/// Seed Parquet files: every fixture column type, under each codec PAVI can read.
pub fn parquet_seeds() -> Vec<(String, Vec<u8>)> {
    let codecs = [
        ("plain", Compression::UNCOMPRESSED),
        ("snappy", Compression::SNAPPY),
        ("zstd", Compression::ZSTD(ZstdLevel::default())),
        ("gzip", Compression::GZIP(GzipLevel::default())),
        ("lz4", Compression::LZ4_RAW),
        ("brotli", Compression::BROTLI(BrotliLevel::default())),
    ];
    let dir = scratch_dir();
    codecs
        .into_iter()
        .map(|(name, codec)| {
            let path = dir.join(format!("seed-{name}"));
            write_parquet(&path, &fixture_batch(12), codec, 5);
            (format!("seed-{name}"), fs::read(&path).expect("read seed"))
        })
        .collect()
}

/// File extensions the `other_formats` target picks from with its first byte.
pub const OTHER_FORMAT_EXTENSIONS: &[&str] = &["csv", "tsv", "jsonl", "arrow"];

/// Seeds for `other_formats`: the selector byte, then a small file of that format.
pub fn other_format_seeds() -> Vec<(String, Vec<u8>)> {
    let with = |selector: u8, bytes: &[u8]| {
        let mut seed = vec![selector];
        seed.extend_from_slice(bytes);
        seed
    };
    let mut arrow = Vec::new();
    let batch = fixture_batch(12);
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new(&mut arrow, &batch.schema()).expect("ipc writer");
    writer.write(&batch).expect("write ipc");
    writer.finish().expect("finish ipc");
    drop(writer);
    vec![
        (
            "seed-csv".to_string(),
            with(0, b"id,name,score,day\n1,\"a, b\",1.5,2024-01-02\n2,,x,\n"),
        ),
        ("seed-tsv".to_string(), with(1, b"a\tb\n1\ttrue\n")),
        (
            "seed-jsonl".to_string(),
            with(2, b"{\"id\": 1, \"tags\": [1, 2], \"point\": {\"x\": 0.5}}\n{\"id\": null}\n"),
        ),
        (
            "seed-json-array".to_string(),
            with(2, b"[{\"id\": 1, \"s\": \"]\\\"[\"}, {\"id\": 2}]"),
        ),
        ("seed-arrow".to_string(), with(3, &arrow)),
    ]
}

pub const FILTER_SEEDS: &[&str] = &[
    "id >= 3",
    "small != 2",
    "score < 1.5",
    "name contains \"me-1\"",
    "text contains é",
    "active == true",
    "day >= 2023-12-10",
    "ts < 2023-11-15T02:00:00",
    "ts >= 2023-11-14T23:13:20+01:00",
    "amount > 10",
    "category == red",
    "tags == 1",
    "0 > 42",
    "id >= 3 AND (name contains me OR active == true)",
    "category IN (red, \"dark blue\") OR score NOT BETWEEN 1 AND 2.5",
    "name LIKE me-_% AND text IS NOT NULL OR amount IS NULL",
    "amount > 10.5 AND at < 01:30:00",
    "amount == 12.340 OR at BETWEEN 00:10:00 AND 00:20:00.5",
];

pub const SQL_SEEDS: &[&str] = &[
    "SELECT * FROM dataset",
    "SELECT name, id FROM dataset WHERE score > 2.5 LIMIT 7",
    "SELECT id FROM dataset WHERE day >= '2023-12-10' ORDER BY id DESC LIMIT 3",
    "SELECT name, ts FROM dataset ORDER BY ts",
    "SELECT COUNT(*) FROM dataset",
    "SELECT category, COUNT(*), SUM(score), AVG(small), MIN(ts), MAX(name) FROM dataset GROUP BY category",
    "SELECT active, MAX(day) FROM dataset WHERE id < 30 GROUP BY active",
    "SELECT * FROM dataset WHERE name LIKE '%2%'",
    "SELECT id FROM other",
    "SELECT id FROM dataset WHERE (id < 5 OR id > 30) AND active = true",
    "SELECT name FROM dataset WHERE category IN ('red', 'blue') AND score NOT BETWEEN 1 AND 2",
    "SELECT COUNT(*) FROM dataset WHERE name LIKE 'me-_%' AND amount IS NOT NULL",
    "SELECT SUM(amount), AVG(amount), MIN(at), MAX(at) FROM dataset WHERE at > '00:30:00'",
    "SELECT at, COUNT(*) FROM dataset GROUP BY at",
    "SELECT id FROM dataset ORDER BY amount DESC LIMIT 5",
    "SELECT id, name FROM dataset ORDER BY active DESC, day, id DESC",
    "SELECT active, category, COUNT(*), MAX(score) FROM dataset GROUP BY active, category",
];
