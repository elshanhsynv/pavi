use anyhow::{Result, bail};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;

pub const DEFAULT_POINT_LIMIT: usize = 2_000;
pub const MAX_INPUT_ROWS: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChartKind {
    Line,
    Bar,
    Scatter,
    Histogram,
}

impl ChartKind {
    pub const ALL: [Self; 4] = [Self::Line, Self::Bar, Self::Scatter, Self::Histogram];

    pub fn label(self) -> &'static str {
        match self {
            Self::Line => "Line",
            Self::Bar => "Bar",
            Self::Scatter => "Scatter",
            Self::Histogram => "Histogram",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChartConfig {
    pub kind: ChartKind,
    pub x_column: String,
    pub y_column: String,
    pub point_limit: usize,
    pub bins: usize,
    pub title: String,
}

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            kind: ChartKind::Line,
            x_column: String::new(),
            y_column: String::new(),
            point_limit: DEFAULT_POINT_LIMIT,
            bins: 20,
            title: String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChartPoint {
    pub x: f64,
    pub y: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChartBar {
    pub label: String,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBin {
    pub start: f64,
    pub end: f64,
    pub count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChartValues {
    Points(Vec<ChartPoint>),
    Bars(Vec<ChartBar>),
    Histogram(Vec<HistogramBin>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChartModel {
    pub kind: ChartKind,
    pub values: ChartValues,
    pub input_rows: usize,
    pub skipped_rows: usize,
    pub input_capped: bool,
    pub reduced: bool,
}

impl ChartModel {
    pub fn is_empty(&self) -> bool {
        match &self.values {
            ChartValues::Points(values) => values.is_empty(),
            ChartValues::Bars(values) => values.is_empty(),
            ChartValues::Histogram(values) => values.iter().all(|bin| bin.count == 0),
        }
    }

    pub fn output_len(&self) -> usize {
        match &self.values {
            ChartValues::Points(values) => values.len(),
            ChartValues::Bars(values) => values.len(),
            ChartValues::Histogram(values) => values.len(),
        }
    }
}

enum RawValue {
    Point(ChartPoint),
    Bar(ChartBar),
    Histogram(f64),
}

/// Retains only compact, chart-ready values while query batches are released immediately.
pub struct ChartAccumulator {
    config: ChartConfig,
    values: Vec<RawValue>,
    skipped_rows: usize,
    input_capped: bool,
}

impl ChartAccumulator {
    pub fn new(config: ChartConfig) -> Result<Self> {
        if config.x_column.trim().is_empty() {
            bail!("choose an X/value column");
        }
        if config.kind != ChartKind::Histogram && config.y_column.trim().is_empty() {
            bail!("choose a Y column");
        }
        if !(2..=MAX_INPUT_ROWS).contains(&config.point_limit) {
            bail!("point limit must be between 2 and {MAX_INPUT_ROWS}");
        }
        if !(1..=100).contains(&config.bins) {
            bail!("histogram bins must be between 1 and 100");
        }
        Ok(Self {
            config,
            values: Vec::with_capacity(DEFAULT_POINT_LIMIT),
            skipped_rows: 0,
            input_capped: false,
        })
    }

    /// Returns false when the bounded chart-input sample is full.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<bool> {
        let x = column(batch, &self.config.x_column)?;
        let y = if self.config.kind == ChartKind::Histogram {
            None
        } else {
            Some(column(batch, &self.config.y_column)?)
        };
        validate_types(self.config.kind, x.data_type(), y.map(Array::data_type))?;

        for row in 0..batch.num_rows() {
            if self.values.len() == MAX_INPUT_ROWS {
                self.input_capped = true;
                return Ok(false);
            }
            let value = match self.config.kind {
                ChartKind::Line | ChartKind::Scatter => {
                    let y = y.ok_or_else(|| anyhow::anyhow!("missing chart Y column"))?;
                    match (numeric(x, row)?, numeric(y, row)?) {
                        (Some(x), Some(y)) if x.is_finite() && y.is_finite() => {
                            RawValue::Point(ChartPoint { x, y })
                        }
                        _ => {
                            self.skipped_rows += 1;
                            continue;
                        }
                    }
                }
                ChartKind::Bar => {
                    let y = y.ok_or_else(|| anyhow::anyhow!("missing chart Y column"))?;
                    match (label(x, row)?, numeric(y, row)?) {
                        (Some(label), Some(value)) if value.is_finite() => {
                            RawValue::Bar(ChartBar { label, value })
                        }
                        _ => {
                            self.skipped_rows += 1;
                            continue;
                        }
                    }
                }
                ChartKind::Histogram => match numeric(x, row)? {
                    Some(value) if value.is_finite() => RawValue::Histogram(value),
                    _ => {
                        self.skipped_rows += 1;
                        continue;
                    }
                },
            };
            self.values.push(value);
        }
        Ok(true)
    }

    pub fn finish(self) -> ChartModel {
        let input_rows = self.values.len();
        let kind = self.config.kind;
        let values = match kind {
            ChartKind::Line | ChartKind::Scatter => {
                let points = self
                    .values
                    .into_iter()
                    .filter_map(|value| match value {
                        RawValue::Point(point) => Some(point),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                ChartValues::Points(reduce(points, self.config.point_limit))
            }
            ChartKind::Bar => {
                let bars = self
                    .values
                    .into_iter()
                    .filter_map(|value| match value {
                        RawValue::Bar(bar) => Some(bar),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                ChartValues::Bars(reduce(bars, self.config.point_limit))
            }
            ChartKind::Histogram => {
                let values = self
                    .values
                    .into_iter()
                    .filter_map(|value| match value {
                        RawValue::Histogram(value) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                ChartValues::Histogram(histogram(&values, self.config.bins))
            }
        };
        let output_len = match &values {
            ChartValues::Points(values) => values.len(),
            ChartValues::Bars(values) => values.len(),
            ChartValues::Histogram(values) => values.len(),
        };
        ChartModel {
            kind,
            values,
            input_rows,
            skipped_rows: self.skipped_rows,
            input_capped: self.input_capped,
            reduced: matches!(kind, ChartKind::Line | ChartKind::Scatter | ChartKind::Bar)
                && input_rows > output_len,
        }
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let index = batch
        .schema()
        .index_of(name.trim())
        .map_err(|_| anyhow::anyhow!("query result has no column `{}`", name.trim()))?;
    Ok(batch.column(index))
}

fn validate_types(kind: ChartKind, x: &DataType, y: Option<&DataType>) -> Result<()> {
    match kind {
        ChartKind::Line | ChartKind::Scatter => {
            if !numeric_type(x) || !y.is_some_and(numeric_type) {
                bail!(
                    "line and scatter charts require numeric, date, or timestamp X and Y columns"
                );
            }
        }
        ChartKind::Bar => {
            if !label_type(x) || !y.is_some_and(numeric_type) {
                bail!("bar charts require a string or numeric X column and a numeric Y column");
            }
        }
        ChartKind::Histogram if !numeric_type(x) => {
            bail!("histograms require a numeric, date, or timestamp value column");
        }
        ChartKind::Histogram => {}
    }
    Ok(())
}

fn numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

fn label_type(data_type: &DataType) -> bool {
    numeric_type(data_type) || matches!(data_type, DataType::Utf8)
}

fn numeric(array: &ArrayRef, row: usize) -> Result<Option<f64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .map(|values| values.value(row) as f64)
        };
    }
    let value = match array.data_type() {
        DataType::Int8 => value!(Int8Array),
        DataType::Int16 => value!(Int16Array),
        DataType::Int32 => value!(Int32Array),
        DataType::Int64 => value!(Int64Array),
        DataType::UInt8 => value!(UInt8Array),
        DataType::UInt16 => value!(UInt16Array),
        DataType::UInt32 => value!(UInt32Array),
        DataType::UInt64 => value!(UInt64Array),
        DataType::Float32 => value!(Float32Array),
        DataType::Float64 => value!(Float64Array),
        DataType::Date32 => value!(Date32Array),
        DataType::Date64 => value!(Date64Array),
        DataType::Timestamp(unit, _) => match unit {
            arrow_schema::TimeUnit::Second => value!(TimestampSecondArray),
            arrow_schema::TimeUnit::Millisecond => value!(TimestampMillisecondArray),
            arrow_schema::TimeUnit::Microsecond => value!(TimestampMicrosecondArray),
            arrow_schema::TimeUnit::Nanosecond => value!(TimestampNanosecondArray),
        },
        other => bail!("unsupported chart numeric type {other:?}"),
    };
    value.map_or_else(
        || bail!("invalid Arrow array for chart column"),
        |value| Ok(Some(value)),
    )
}

fn label(array: &ArrayRef, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if matches!(array.data_type(), DataType::Utf8) {
        return array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|values| Some(values.value(row).to_owned()))
            .map_or_else(|| bail!("invalid Arrow string array for chart column"), Ok);
    }
    Ok(numeric(array, row)?.map(|value| value.to_string()))
}

fn reduce<T>(values: Vec<T>, limit: usize) -> Vec<T> {
    if values.len() <= limit {
        return values;
    }
    let last = values.len() - 1;
    let mut values = values.into_iter().map(Some).collect::<Vec<_>>();
    (0..limit)
        .map(|index| index * last / (limit - 1))
        .filter_map(|index| values[index].take())
        .collect()
}

fn histogram(values: &[f64], bins: usize) -> Vec<HistogramBin> {
    let Some((&min, &max)) = values
        .iter()
        .min_by(|left, right| left.total_cmp(right))
        .zip(values.iter().max_by(|left, right| left.total_cmp(right)))
    else {
        return Vec::new();
    };
    if min == max {
        return vec![HistogramBin {
            start: min,
            end: max,
            count: values.len(),
        }];
    }
    let width = (max - min) / bins as f64;
    let mut counts = vec![0; bins];
    for value in values {
        let index = (((value - min) / width) as usize).min(bins - 1);
        counts[index] += 1;
    }
    counts
        .into_iter()
        .enumerate()
        .map(|(index, count)| HistogramBin {
            start: min + width * index as f64,
            end: min + width * (index + 1) as f64,
            count,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Float64, true),
            Field::new("category", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    None,
                    Some(30.0),
                    Some(40.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    fn config(kind: ChartKind) -> ChartConfig {
        ChartConfig {
            kind,
            x_column: if kind == ChartKind::Bar {
                "category".to_string()
            } else {
                "x".to_string()
            },
            y_column: "y".to_string(),
            point_limit: 2,
            bins: 2,
            title: String::new(),
        }
    }

    #[test]
    fn builds_line_scatter_and_bar_charts_while_skipping_nulls() {
        for kind in [ChartKind::Line, ChartKind::Scatter, ChartKind::Bar] {
            let mut accumulator = ChartAccumulator::new(config(kind)).unwrap();
            assert!(accumulator.push_batch(&batch()).unwrap());
            let model = accumulator.finish();
            assert_eq!(model.input_rows, 2);
            assert_eq!(model.skipped_rows, 2);
            assert!(!model.is_empty());
        }
    }

    #[test]
    fn calculates_histograms_and_handles_empty_values() {
        let mut accumulator = ChartAccumulator::new(config(ChartKind::Histogram)).unwrap();
        accumulator.push_batch(&batch()).unwrap();
        let model = accumulator.finish();
        let ChartValues::Histogram(bins) = model.values else {
            panic!("expected histogram");
        };
        assert_eq!(bins.iter().map(|bin| bin.count).sum::<usize>(), 3);

        let empty = ChartAccumulator::new(config(ChartKind::Histogram))
            .unwrap()
            .finish();
        assert!(empty.is_empty());
    }

    #[test]
    fn reduces_deterministically_at_the_point_limit() {
        let values = (0..10)
            .map(|value| ChartPoint {
                x: value as f64,
                y: value as f64,
            })
            .collect();
        let reduced = reduce(values, 3);
        assert_eq!(
            reduced,
            vec![
                ChartPoint { x: 0.0, y: 0.0 },
                ChartPoint { x: 4.0, y: 4.0 },
                ChartPoint { x: 9.0, y: 9.0 }
            ]
        );
    }

    #[test]
    fn rejects_incompatible_columns_and_caps_input() {
        let mut accumulator = ChartAccumulator::new(config(ChartKind::Line)).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Utf8, false),
            Field::new("y", DataType::Int32, false),
        ]));
        let invalid = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        assert!(accumulator.push_batch(&invalid).is_err());

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ]));
        let rows = MAX_INPUT_ROWS + 1;
        let large = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..rows as i32)),
                Arc::new(Int32Array::from_iter_values(0..rows as i32)),
            ],
        )
        .unwrap();
        let mut accumulator = ChartAccumulator::new(config(ChartKind::Line)).unwrap();
        assert!(!accumulator.push_batch(&large).unwrap());
        let model = accumulator.finish();
        assert!(model.input_capped);
        assert_eq!(model.input_rows, MAX_INPUT_ROWS);
        assert!(model.output_len() <= 2);
    }
}
