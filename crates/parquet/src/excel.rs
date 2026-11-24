//! Excel and OpenDocument workbooks read into a temporary Parquet file, one
//! sheet at a time (ADR 0021).
//!
//! The sheet is read twice: once to choose each column's type from every
//! cell, once to write the rows. `.xlsx` and `.xlsm` sheets are streamed cell
//! by cell; `.xls`, `.xlsb` and `.ods` sheets are read whole by `calamine`, so
//! their size is checked first.

use std::{
    fs::File,
    io::{BufReader, Read},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Date32Builder, DurationMillisecondBuilder, Float64Builder, Int64Builder,
        StringBuilder, TimestampMillisecondBuilder,
    },
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use calamine::{Data, Reader, SheetType};

use crate::{
    convert::{BATCH_ROWS, FileFormat, ImportOptions, ImportReport, ParquetSink},
    source::{Cancelled, panic_message},
    spill::TempFolder,
};

/// Largest uncompressed size of what an `.xlsx` or `.xlsm` workbook keeps in
/// memory while its sheet is streamed: shared strings, styles and the
/// workbook part (every part but the worksheets).
pub const MAX_EXCEL_SHARED_BYTES: u64 = 256 * 1024 * 1024;
/// Largest uncompressed size of an `.xlsb` or `.ods` workbook, or file size of
/// an `.xls`, whose sheet is read whole into memory.
pub const MAX_EXCEL_WHOLE_BYTES: u64 = 256 * 1024 * 1024;
/// Columns a sheet may have (Excel's own limit is 16,384).
pub const MAX_EXCEL_COLUMNS: u32 = 16_384;

/// How a workbook is stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkbookKind {
    Xlsx,
    Xlsb,
    Xls,
    Ods,
}

/// The kind of workbook at `path`: by extension, else by its first bytes and
/// (for a zip) the parts it holds.
fn kind(path: &Path) -> Result<WorkbookKind> {
    let extension = path
        .extension()
        .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "xlsx" | "xlsm" => return Ok(WorkbookKind::Xlsx),
        "xlsb" => return Ok(WorkbookKind::Xlsb),
        "xls" => return Ok(WorkbookKind::Xls),
        "ods" => return Ok(WorkbookKind::Ods),
        _ => {}
    }
    let mut magic = [0_u8; 4];
    File::open(path)
        .and_then(|mut file| file.read_exact(&mut magic))
        .with_context(|| format!("read {}", path.display()))?;
    if magic == [0xD0, 0xCF, 0x11, 0xE0] {
        return Ok(WorkbookKind::Xls);
    }
    if magic.starts_with(b"PK") {
        let archive = zip::ZipArchive::new(BufReader::new(File::open(path)?))
            .context("read the workbook as a zip archive")?;
        let has = |name: &str| {
            archive
                .file_names()
                .any(|entry| entry.is_ok_and(|entry| entry == name))
        };
        if has("xl/workbook.xml") {
            return Ok(WorkbookKind::Xlsx);
        }
        if has("xl/workbook.bin") {
            return Ok(WorkbookKind::Xlsb);
        }
        if has("content.xml") {
            return Ok(WorkbookKind::Ods);
        }
    }
    bail!(
        "{} is not an Excel or OpenDocument workbook",
        path.display()
    )
}

/// Refuses a workbook whose parts read into memory would be too large, before
/// `calamine` reads them. Sizes are the uncompressed sizes the zip records.
fn check_size(path: &Path, kind: WorkbookKind) -> Result<()> {
    if kind == WorkbookKind::Xls {
        let length = std::fs::metadata(path)?.len();
        if length > MAX_EXCEL_WHOLE_BYTES {
            bail!(
                "this .xls workbook is {} MB; PAVI reads .xls files of at most {} MB \
                 (save it as .xlsx to read larger sheets)",
                length / (1024 * 1024),
                MAX_EXCEL_WHOLE_BYTES / (1024 * 1024)
            );
        }
        return Ok(());
    }
    let mut archive = zip::ZipArchive::new(BufReader::new(File::open(path)?))
        .context("read the workbook as a zip archive")?;
    let mut held = 0_u64;
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .context("read the workbook's list of parts")?;
        // A streamed worksheet is never held whole.
        if kind == WorkbookKind::Xlsx
            && entry
                .name()
                .is_ok_and(|name| name.starts_with("xl/worksheets/"))
        {
            continue;
        }
        held = held.saturating_add(entry.size());
    }
    let limit = if kind == WorkbookKind::Xlsx {
        MAX_EXCEL_SHARED_BYTES
    } else {
        MAX_EXCEL_WHOLE_BYTES
    };
    if held > limit {
        bail!(
            "this workbook needs {} MB of memory to read; PAVI reads at most {} MB{}",
            held / (1024 * 1024),
            limit / (1024 * 1024),
            if kind == WorkbookKind::Xlsx {
                ""
            } else {
                " (save it as .xlsx to read larger sheets)"
            }
        );
    }
    Ok(())
}

/// An open workbook: `.xlsx` streams cells, the others hold the sheet's range.
enum Workbook {
    Xlsx(calamine::Xlsx<BufReader<File>>),
    Whole(calamine::Sheets<BufReader<File>>),
}

impl Workbook {
    fn open(path: &Path, kind: WorkbookKind) -> Result<Self> {
        let file = BufReader::new(File::open(path)?);
        Ok(match kind {
            WorkbookKind::Xlsx => {
                Self::Xlsx(calamine::Xlsx::new(file).map_err(|error| anyhow!("{error}"))?)
            }
            WorkbookKind::Xlsb => Self::Whole(calamine::Sheets::Xlsb(
                calamine::Xlsb::new(file).map_err(|error| anyhow!("{error}"))?,
            )),
            WorkbookKind::Xls => Self::Whole(calamine::Sheets::Xls(
                calamine::Xls::new(file).map_err(|error| anyhow!("{error}"))?,
            )),
            WorkbookKind::Ods => Self::Whole(calamine::Sheets::Ods(
                calamine::Ods::new(file).map_err(|error| anyhow!("{error}"))?,
            )),
        })
    }

    /// The workbook's worksheets (not chart or macro sheets), in order.
    fn worksheets(&self) -> Vec<String> {
        let sheets = match self {
            Self::Xlsx(workbook) => workbook.sheets_metadata(),
            Self::Whole(workbook) => workbook.sheets_metadata(),
        };
        sheets
            .iter()
            .filter(|sheet| sheet.typ == SheetType::WorkSheet)
            .map(|sheet| sheet.name.clone())
            .collect()
    }

    /// Calls `visit` with each used cell of `sheet` in row order, and
    /// `progress` with the share of rows done; stops when it returns `true`.
    fn scan(
        &mut self,
        sheet: &str,
        range: &mut Option<calamine::Range<Data>>,
        visit: &mut dyn FnMut(u32, u32, &Data) -> Result<()>,
        stop: &dyn Fn(f64) -> bool,
    ) -> Result<()> {
        match self {
            Self::Xlsx(workbook) => {
                let mut cells = workbook
                    .worksheet_cells_reader(sheet)
                    .map_err(|error| anyhow!("{error}"))?;
                let last = cells.dimensions().end.0.max(1);
                let mut checked = 0_u32;
                while let Some(cell) = cells.next_cell().map_err(|error| anyhow!("{error}"))? {
                    let (row, column) = cell.get_position();
                    if row >= checked.saturating_add(PROGRESS_ROWS) {
                        checked = row;
                        if stop(f64::from(row) / f64::from(last)) {
                            return Err(Cancelled.into());
                        }
                    }
                    let value = Data::from(cell.get_value().clone());
                    visit(row, column, &value)?;
                }
            }
            Self::Whole(workbook) => {
                if range.is_none() {
                    *range = Some(
                        workbook
                            .worksheet_range(sheet)
                            .map_err(|error| anyhow!("{error}"))?,
                    );
                }
                let Some(range) = range.as_ref() else {
                    return Ok(());
                };
                let last = range.end().map_or(1, |(row, _)| row.max(1));
                let mut checked = 0_u32;
                let (top, left) = range.start().unwrap_or((0, 0));
                for (row, column, value) in range.used_cells() {
                    let (row, column) = (top + row as u32, left + column as u32);
                    if row >= checked.saturating_add(PROGRESS_ROWS) {
                        checked = row;
                        if stop(f64::from(row) / f64::from(last)) {
                            return Err(Cancelled.into());
                        }
                    }
                    visit(row, column, value)?;
                }
            }
        }
        Ok(())
    }
}

/// Rows between two progress and cancellation checks.
const PROGRESS_ROWS: u32 = 4_096;

/// What a column holds, from its cells; widened as more are seen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CellKind {
    Empty,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    Duration,
    Text,
}

impl CellKind {
    fn of(value: &Data) -> Self {
        match value {
            Data::Empty | Data::Error(_) => Self::Empty,
            Data::Int(_) => Self::Int,
            // Excel keeps every number as a float; whole ones are integers.
            Data::Float(number) => {
                if number.fract() == 0.0 && number.abs() < 9_007_199_254_740_992.0 {
                    Self::Int
                } else {
                    Self::Float
                }
            }
            Data::Bool(_) => Self::Bool,
            Data::DateTime(date) if date.is_duration() => Self::Duration,
            Data::DateTime(date) => {
                let (_, _, _, hour, minute, second, milli) = date.to_ymd_hms_milli();
                if (hour, minute, second, milli) == (0, 0, 0, 0) {
                    Self::Date
                } else {
                    Self::DateTime
                }
            }
            Data::DateTimeIso(text) => match parse_iso(text) {
                Some((_, 0)) if !text.contains('T') => Self::Date,
                Some(_) => Self::DateTime,
                None => Self::Text,
            },
            Data::String(_) | Data::DurationIso(_) => Self::Text,
        }
    }

    fn merge(self, other: Self) -> Self {
        use CellKind::*;
        match (self, other) {
            (Empty, kind) | (kind, Empty) => kind,
            (left, right) if left == right => left,
            (Int, Float) | (Float, Int) => Float,
            (Date, DateTime) | (DateTime, Date) => DateTime,
            _ => Text,
        }
    }

    fn data_type(self, all_text: bool) -> DataType {
        if all_text {
            return DataType::Utf8;
        }
        match self {
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Date => DataType::Date32,
            Self::DateTime => DataType::Timestamp(TimeUnit::Millisecond, None),
            Self::Duration => DataType::Duration(TimeUnit::Millisecond),
            Self::Empty | Self::Text => DataType::Utf8,
        }
    }
}

/// Days from 1970-01-01 to a calendar date (proleptic Gregorian).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// An Excel date as days and milliseconds into the day since 1970-01-01.
fn excel_date(date: &calamine::ExcelDateTime) -> (i64, i64) {
    let (year, month, day, hour, minute, second, milli) = date.to_ymd_hms_milli();
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day));
    let millis = ((i64::from(hour) * 60 + i64::from(minute)) * 60 + i64::from(second)) * 1000
        + i64::from(milli);
    (days, millis)
}

/// `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS[.fff]` (as OpenDocument stores dates)
/// as days and milliseconds into the day since 1970-01-01.
fn parse_iso(text: &str) -> Option<(i64, i64)> {
    let (date, time) = match text.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (text, None),
    };
    let mut parts = date.splitn(3, '-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: i64 = parts.next()?.parse().ok()?;
    let day: i64 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut millis = 0;
    if let Some(time) = time {
        let mut parts = time.splitn(3, ':');
        let hour: i64 = parts.next()?.parse().ok()?;
        let minute: i64 = parts.next()?.parse().ok()?;
        let second: f64 = parts.next().unwrap_or("0").parse().ok()?;
        if hour > 23 || minute > 59 || !(0.0..61.0).contains(&second) {
            return None;
        }
        millis = (hour * 60 + minute) * 60_000 + (second * 1000.0).round() as i64;
    }
    Some((days_from_civil(year, month, day), millis))
}

/// A cell as text, for text columns and column names.
fn cell_text(value: &Data) -> Option<String> {
    Some(match value {
        Data::Empty => return None,
        Data::Int(number) => number.to_string(),
        Data::Float(number) => number.to_string(),
        Data::String(text) | Data::DateTimeIso(text) | Data::DurationIso(text) => text.clone(),
        Data::Bool(value) => value.to_string(),
        Data::Error(error) => error.to_string(),
        Data::DateTime(date) => {
            let (year, month, day, hour, minute, second, milli) = date.to_ymd_hms_milli();
            if date.is_duration() {
                return Some(date.as_f64().to_string());
            }
            if (hour, minute, second, milli) == (0, 0, 0, 0) {
                format!("{year:04}-{month:02}-{day:02}")
            } else if milli == 0 {
                format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}")
            } else {
                format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{milli:03}"
                )
            }
        }
    })
}

/// One column's values for the batch being built.
enum Column {
    Int(Int64Builder),
    Float(Float64Builder),
    Bool(BooleanBuilder),
    Date(Date32Builder),
    DateTime(TimestampMillisecondBuilder),
    Duration(DurationMillisecondBuilder),
    Text(StringBuilder),
}

impl Column {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Int64 => Self::Int(Int64Builder::new()),
            DataType::Float64 => Self::Float(Float64Builder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Date32 => Self::Date(Date32Builder::new()),
            DataType::Timestamp(..) => Self::DateTime(TimestampMillisecondBuilder::new()),
            DataType::Duration(_) => Self::Duration(DurationMillisecondBuilder::new()),
            _ => Self::Text(StringBuilder::new()),
        }
    }

    /// Appends a cell; one that doesn't hold the column's type is null.
    fn append(&mut self, value: Option<&Data>) {
        let value = value.unwrap_or(&Data::Empty);
        let date = || match value {
            Data::DateTime(date) if !date.is_duration() => Some(excel_date(date)),
            Data::DateTimeIso(text) => parse_iso(text),
            _ => None,
        };
        match self {
            Self::Int(builder) => builder.append_option(match value {
                Data::Int(number) => Some(*number),
                Data::Float(number) => Some(*number as i64),
                _ => None,
            }),
            Self::Float(builder) => builder.append_option(match value {
                Data::Int(number) => Some(*number as f64),
                Data::Float(number) => Some(*number),
                _ => None,
            }),
            Self::Bool(builder) => builder.append_option(match value {
                Data::Bool(value) => Some(*value),
                _ => None,
            }),
            Self::Date(builder) => {
                builder.append_option(date().and_then(|(days, _)| i32::try_from(days).ok()));
            }
            Self::DateTime(builder) => builder.append_option(
                date().and_then(|(days, millis)| days.checked_mul(86_400_000)?.checked_add(millis)),
            ),
            Self::Duration(builder) => builder.append_option(match value {
                Data::DateTime(date) if date.is_duration() => {
                    Some((date.as_f64() * 86_400_000.0).round() as i64)
                }
                _ => None,
            }),
            Self::Text(builder) => builder.append_option(cell_text(value)),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int(builder) => Arc::new(builder.finish()),
            Self::Float(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::DateTime(builder) => Arc::new(builder.finish()),
            Self::Duration(builder) => Arc::new(builder.finish()),
            Self::Text(builder) => Arc::new(builder.finish()),
        }
    }
}

/// What the first pass learns about a sheet.
struct Shape {
    /// The row holding the column names, when the sheet has a header row.
    header_row: Option<u32>,
    names: Vec<(u32, String)>,
    kinds: Vec<CellKind>,
    /// The first and last rows that hold data.
    first_row: Option<u32>,
    last_row: u32,
}

/// The column names: from the header row, made unique; `column_N` where a
/// name is missing.
fn column_names(shape: &Shape) -> Vec<String> {
    let mut names: Vec<String> = Vec::with_capacity(shape.kinds.len());
    let mut used = std::collections::HashSet::new();
    for column in 0..shape.kinds.len() {
        let base = shape
            .names
            .iter()
            .find(|(at, _)| *at as usize == column)
            .map(|(_, name)| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| format!("column_{}", column + 1));
        let mut name = base.clone();
        let mut suffix = 2;
        while !used.insert(name.clone()) {
            name = format!("{base}_{suffix}");
            suffix += 1;
        }
        names.push(name);
    }
    names
}

/// Reads one sheet of the workbook at `source` into a temporary Parquet file.
pub(crate) fn convert(
    source: &Path,
    options: &ImportOptions,
    checkpoint: &dyn Fn(f64) -> bool,
) -> Result<(PathBuf, TempFolder, ImportReport)> {
    let kind = kind(source)?;
    check_size(source, kind)?;
    // calamine panics on some malformed workbooks instead of returning an
    // error; as for the other readers, a panic becomes an error.
    match catch_unwind(AssertUnwindSafe(|| read(source, kind, options, checkpoint))) {
        Ok(result) => result,
        Err(payload) => bail!("malformed Excel data: {}", panic_message(&*payload)),
    }
    .with_context(|| format!("read the workbook {}", source.display()))
}

fn read(
    source: &Path,
    kind: WorkbookKind,
    options: &ImportOptions,
    checkpoint: &dyn Fn(f64) -> bool,
) -> Result<(PathBuf, TempFolder, ImportReport)> {
    let mut workbook = Workbook::open(source, kind)?;
    let sheets = workbook.worksheets();
    let sheet = match &options.sheet {
        Some(name) => sheets
            .iter()
            .find(|sheet| *sheet == name)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "the workbook has no sheet named {name:?}; its sheets are {}",
                    sheets.join(", ")
                )
            })?,
        None => sheets
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("the workbook has no worksheet"))?,
    };
    let mut range = None;

    // First pass: the header row, the columns and their kinds.
    let mut shape = Shape {
        header_row: None,
        names: Vec::new(),
        kinds: Vec::new(),
        first_row: None,
        last_row: 0,
    };
    let header = options.header;
    workbook.scan(
        &sheet,
        &mut range,
        &mut |row, column, value| {
            if matches!(value, Data::Empty) {
                return Ok(());
            }
            if column >= MAX_EXCEL_COLUMNS {
                bail!("the sheet has more than {MAX_EXCEL_COLUMNS} columns");
            }
            let column_index = column as usize;
            if shape.kinds.len() <= column_index {
                shape.kinds.resize(column_index + 1, CellKind::Empty);
            }
            if header
                && shape.header_row.is_none_or(|header| header == row)
                && shape.first_row.is_none()
            {
                shape.header_row = Some(row);
                if let Some(name) = cell_text(value) {
                    shape.names.push((column, name));
                }
                return Ok(());
            }
            shape.first_row.get_or_insert(row);
            shape.last_row = row;
            shape.kinds[column_index] = shape.kinds[column_index].merge(CellKind::of(value));
            Ok(())
        },
        &|done| checkpoint(done / 2.0),
    )?;
    let names = column_names(&shape);
    let schema: SchemaRef = Arc::new(Schema::new(
        names
            .iter()
            .zip(&shape.kinds)
            .map(|(name, kind)| Field::new(name, kind.data_type(options.all_text), true))
            .collect::<Vec<_>>(),
    ));

    // Second pass: the rows, a batch at a time. Empty rows between data rows
    // are kept, as rows of nulls.
    let mut sink = ParquetSink::new(source, Arc::clone(&schema))?;
    let mut columns: Vec<Column> = schema
        .fields()
        .iter()
        .map(|field| Column::new(field.data_type()))
        .collect();
    let mut row_cells: Vec<Option<Data>> = vec![None; columns.len()];
    let mut rows_in_batch = 0_usize;
    let flush =
        |columns: &mut Vec<Column>, sink: &mut ParquetSink, rows: &mut usize| -> Result<()> {
            if *rows == 0 {
                return Ok(());
            }
            let arrays = columns.iter_mut().map(Column::finish).collect::<Vec<_>>();
            sink.write(&RecordBatch::try_new(Arc::clone(&schema), arrays)?)?;
            *rows = 0;
            Ok(())
        };
    let end_row = |columns: &mut Vec<Column>, cells: &mut Vec<Option<Data>>| {
        for (column, cell) in columns.iter_mut().zip(cells.iter_mut()) {
            column.append(cell.take().as_ref());
        }
    };
    if let Some(first) = shape.first_row {
        let mut current = first;
        workbook.scan(
            &sheet,
            &mut range,
            &mut |row, column, value| {
                if Some(row) == shape.header_row || row < first || row > shape.last_row {
                    return Ok(());
                }
                // Finish the rows before this cell's row, empty ones included.
                while current < row {
                    end_row(&mut columns, &mut row_cells);
                    rows_in_batch += 1;
                    current += 1;
                    if rows_in_batch >= BATCH_ROWS {
                        flush(&mut columns, &mut sink, &mut rows_in_batch)?;
                    }
                }
                if let Some(cell) = row_cells.get_mut(column as usize) {
                    *cell = Some(value.clone());
                }
                Ok(())
            },
            &|done| checkpoint(0.5 + done / 2.0),
        )?;
        // The last data row.
        end_row(&mut columns, &mut row_cells);
        rows_in_batch += 1;
        flush(&mut columns, &mut sink, &mut rows_in_batch)?;
    }
    let (path, folder) = sink.finish()?;
    Ok((
        path,
        folder,
        ImportReport {
            format: FileFormat::Excel,
            delimiter: None,
            header: options.header,
            encoding: None,
            all_text: options.all_text,
            retyped: Vec::new(),
            sheet: Some(sheet),
            sheets,
        },
    ))
}

#[cfg(test)]
mod tests {
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

    use super::*;
    use crate::{ParquetSource, Projection, value::scalar_text};

    fn texts(source: &ParquetSource, column: usize) -> Vec<String> {
        let batch = source
            .read_window(
                0,
                source.row_count() as usize,
                &Projection::all(source.column_count()),
            )
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                scalar_text(batch.column(column).as_ref(), row)
                    .unwrap()
                    .unwrap_or_else(|| "null".to_string())
            })
            .collect()
    }

    /// A workbook with a "Sales" sheet of every kind of cell, and a "Notes" sheet.
    fn workbook(path: &Path) {
        let mut workbook = Workbook::new();
        let date = Format::new().set_num_format("yyyy-mm-dd");
        let sheet = workbook.add_worksheet().set_name("Sales").unwrap();
        for (column, name) in ["id", "price", "paid", "day", "when", "note", "", "id"]
            .iter()
            .enumerate()
        {
            if !name.is_empty() {
                sheet.write_string(0, column as u16, *name).unwrap();
            }
        }
        sheet.write_number(1, 0, 1).unwrap();
        sheet.write_number(1, 1, 2.5).unwrap();
        sheet.write_boolean(1, 2, true).unwrap();
        sheet
            .write_datetime_with_format(1, 3, ExcelDateTime::from_ymd(2024, 2, 29).unwrap(), &date)
            .unwrap();
        sheet
            .write_datetime_with_format(
                1,
                4,
                ExcelDateTime::from_ymd(2024, 1, 2)
                    .unwrap()
                    .and_hms(10, 30, 0)
                    .unwrap(),
                &Format::new().set_num_format("yyyy-mm-dd hh:mm"),
            )
            .unwrap();
        sheet.write_string(1, 5, "first").unwrap();
        sheet.write_number(1, 6, 7).unwrap();
        // Row 3 is empty; row 4 has a text id and a whole price.
        sheet.write_string(3, 0, "A-9").unwrap();
        sheet.write_number(3, 1, 4).unwrap();
        sheet.write_boolean(3, 2, false).unwrap();
        sheet.write_number(3, 5, 12).unwrap();
        workbook
            .add_worksheet()
            .set_name("Notes")
            .unwrap()
            .write_string(0, 0, "only")
            .unwrap();
        workbook.save(path).unwrap();
    }

    #[test]
    fn reads_a_sheet_with_a_type_per_column() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sales.xlsx");
        workbook(&path);
        let source = ParquetSource::open(&path).unwrap();
        assert_eq!(source.format(), FileFormat::Excel);
        let shape = source
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            shape,
            [
                ("id".to_string(), DataType::Utf8),
                ("price".to_string(), DataType::Float64),
                ("paid".to_string(), DataType::Boolean),
                ("day".to_string(), DataType::Date32),
                (
                    "when".to_string(),
                    DataType::Timestamp(TimeUnit::Millisecond, None)
                ),
                ("note".to_string(), DataType::Utf8),
                ("column_7".to_string(), DataType::Int64),
                ("id_2".to_string(), DataType::Utf8),
            ]
        );
        // The empty row between the data rows is kept.
        assert_eq!(source.row_count(), 3);
        assert_eq!(texts(&source, 0), ["1", "null", "A-9"]);
        assert_eq!(texts(&source, 1), ["2.5", "null", "4"]);
        assert_eq!(texts(&source, 2), ["true", "null", "false"]);
        assert_eq!(texts(&source, 3), ["2024-02-29", "null", "null"]);
        assert_eq!(texts(&source, 4), ["2024-01-02T10:30:00", "null", "null"]);
        assert_eq!(texts(&source, 5), ["first", "null", "12"]);
        let report = source.import_report().unwrap();
        assert_eq!(report.sheets, ["Sales", "Notes"]);
        assert_eq!(report.summary(), "Excel, sheet Sales (1 of 2), header row");
        assert_eq!(report.encoding, None);
    }

    #[test]
    fn reads_the_sheet_and_header_asked_for() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sales.xlsx");
        workbook(&path);
        let open = |options: ImportOptions| {
            ParquetSource::open_with_options(&path, None, &options, &|_| false)
        };
        let notes = open(ImportOptions {
            sheet: Some("Notes".to_string()),
            header: false,
            ..ImportOptions::default()
        })
        .unwrap();
        assert_eq!(notes.schema().field(0).name(), "column_1");
        assert_eq!(texts(&notes, 0), ["only"]);

        let error = format!(
            "{:#}",
            open(ImportOptions {
                sheet: Some("Missing".to_string()),
                ..ImportOptions::default()
            })
            .err()
            .unwrap()
        );
        assert!(error.contains("its sheets are Sales, Notes"), "{error}");

        let text = open(ImportOptions {
            all_text: true,
            ..ImportOptions::default()
        })
        .unwrap();
        assert!(
            text.schema()
                .fields()
                .iter()
                .all(|field| field.data_type() == &DataType::Utf8)
        );
        assert_eq!(texts(&text, 3), ["2024-02-29", "null", "null"]);

        // Found by its contents when the name doesn't say, and cancellable.
        let renamed = directory.path().join("sales.bin");
        std::fs::copy(&path, &renamed).unwrap();
        let source = ParquetSource::open_with_options(
            &renamed,
            Some(FileFormat::Excel),
            &ImportOptions::default(),
            &|_| false,
        )
        .unwrap();
        assert_eq!(source.row_count(), 3);
        let not_a_workbook = directory.path().join("plain.txt");
        std::fs::write(&not_a_workbook, "a,b\n").unwrap();
        assert!(
            ParquetSource::open_with_options(
                &not_a_workbook,
                Some(FileFormat::Excel),
                &ImportOptions::default(),
                &|_| false
            )
            .is_err()
        );
    }

    #[test]
    fn converts_dates_and_text() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(1900, 1, 1), -25_567);
        assert_eq!(parse_iso("2024-01-02"), Some((19_724, 0)));
        assert_eq!(
            parse_iso("2024-01-02T01:02:03.5"),
            Some((19_724, 3_723_500))
        );
        assert_eq!(parse_iso("2024-13-02"), None);
        assert_eq!(parse_iso("noon"), None);
        assert_eq!(cell_text(&Data::Float(0.1)), Some("0.1".to_string()));
        assert_eq!(cell_text(&Data::Float(3.0)), Some("3".to_string()));
        assert_eq!(
            cell_text(&Data::Error(calamine::CellErrorType::NA)),
            Some("#N/A".to_string())
        );
        assert_eq!(CellKind::Int.merge(CellKind::Float), CellKind::Float);
        assert_eq!(CellKind::Date.merge(CellKind::DateTime), CellKind::DateTime);
        assert_eq!(CellKind::Bool.merge(CellKind::Int), CellKind::Text);
        assert_eq!(CellKind::Empty.merge(CellKind::Bool), CellKind::Bool);
    }
}
