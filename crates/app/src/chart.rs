use anyhow::{Result, bail};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use pavi_query::QueryExecution;
use pavi_runtime::GenerationId;

pub const DEFAULT_POINT_LIMIT: usize = 2_000;
pub const MAX_INPUT_ROWS: usize = 16_384;
/// Y columns one chart draws, each as its own series.
pub const MAX_SERIES: usize = 6;

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
    /// One series per column (at most [`MAX_SERIES`]); empty names are skipped.
    pub y_columns: Vec<String>,
    pub point_limit: usize,
    pub bins: usize,
    pub title: String,
}

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            kind: ChartKind::Line,
            x_column: String::new(),
            y_columns: vec![String::new()],
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

/// One Y column's points.
#[derive(Clone, Debug, PartialEq)]
pub struct ChartSeries {
    pub name: String,
    pub points: Vec<ChartPoint>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBin {
    pub start: f64,
    pub end: f64,
    pub count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChartValues {
    Points(Vec<ChartSeries>),
    /// One label per bar group; each series has one value per label (`None`
    /// when null).
    Bars {
        labels: Vec<String>,
        series: Vec<(String, Vec<Option<f64>>)>,
    },
    Histogram(Vec<HistogramBin>),
}

/// What the X axis (or a histogram's value axis) holds, for its tick labels.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum AxisFormat {
    #[default]
    Number,
    /// The stored value of a date, timestamp or time column (days, or the
    /// type's unit), labelled as that type.
    Temporal(DataType),
}

impl AxisFormat {
    fn of(data_type: &DataType) -> Self {
        match data_type {
            DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time32(_)
            | DataType::Time64(_) => Self::Temporal(data_type.clone()),
            _ => Self::Number,
        }
    }

    /// A tick label for `value`; `step` is the distance between ticks.
    pub fn label(&self, value: f64, step: f64) -> String {
        match self {
            Self::Number => parquet_reader::value::axis_number(value, step),
            Self::Temporal(data_type) => parquet_reader::value::temporal_text(data_type, value)
                .unwrap_or_else(|| parquet_reader::value::axis_number(value, step)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChartModel {
    pub kind: ChartKind,
    pub values: ChartValues,
    pub x_axis: AxisFormat,
    pub input_rows: usize,
    pub skipped_rows: usize,
    pub input_capped: bool,
    pub reduced: bool,
}

impl ChartModel {
    pub fn is_empty(&self) -> bool {
        match &self.values {
            ChartValues::Points(series) => series.iter().all(|series| series.points.is_empty()),
            ChartValues::Bars { labels, .. } => labels.is_empty(),
            ChartValues::Histogram(values) => values.iter().all(|bin| bin.count == 0),
        }
    }

    /// Points, bar groups or bins drawn.
    pub fn output_len(&self) -> usize {
        match &self.values {
            ChartValues::Points(series) => series
                .iter()
                .map(|series| series.points.len())
                .max()
                .unwrap_or(0),
            ChartValues::Bars { labels, .. } => labels.len(),
            ChartValues::Histogram(values) => values.len(),
        }
    }

    /// The series names, for a legend.
    pub fn series_names(&self) -> Vec<&str> {
        match &self.values {
            ChartValues::Points(series) => {
                series.iter().map(|series| series.name.as_str()).collect()
            }
            ChartValues::Bars { series, .. } => {
                series.iter().map(|(name, _)| name.as_str()).collect()
            }
            ChartValues::Histogram(_) => Vec::new(),
        }
    }
}

/// One input row's chart values.
enum RawValue {
    /// An X value and each series' Y value.
    Point(f64, Vec<Option<f64>>),
    /// A label and each series' value.
    Bar(String, Vec<Option<f64>>),
    Histogram(f64),
}

/// Retains only compact, chart-ready values while query batches are released immediately.
pub struct ChartAccumulator {
    config: ChartConfig,
    /// The Y columns in use: the config's, without empty names.
    series: Vec<String>,
    values: Vec<RawValue>,
    x_axis: AxisFormat,
    skipped_rows: usize,
    input_capped: bool,
}

impl ChartAccumulator {
    pub fn new(config: ChartConfig) -> Result<Self> {
        if config.x_column.trim().is_empty() {
            bail!("choose an X/value column");
        }
        let series = config
            .y_columns
            .iter()
            .map(|column| column.trim().to_owned())
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>();
        if config.kind != ChartKind::Histogram && series.is_empty() {
            bail!("choose a Y column");
        }
        if series.len() > MAX_SERIES {
            bail!("a chart draws at most {MAX_SERIES} Y columns");
        }
        if !(2..=MAX_INPUT_ROWS).contains(&config.point_limit) {
            bail!("point limit must be between 2 and {MAX_INPUT_ROWS}");
        }
        if !(1..=100).contains(&config.bins) {
            bail!("histogram bins must be between 1 and 100");
        }
        Ok(Self {
            config,
            series,
            values: Vec::with_capacity(DEFAULT_POINT_LIMIT),
            x_axis: AxisFormat::Number,
            skipped_rows: 0,
            input_capped: false,
        })
    }

    /// Returns false when the bounded chart-input sample is full.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<bool> {
        let x = column(batch, &self.config.x_column)?;
        let ys = if self.config.kind == ChartKind::Histogram {
            Vec::new()
        } else {
            self.series
                .iter()
                .map(|name| column(batch, name))
                .collect::<Result<Vec<_>>>()?
        };
        for y in &ys {
            validate_types(self.config.kind, x.data_type(), Some(y.data_type()))?;
        }
        if ys.is_empty() {
            validate_types(self.config.kind, x.data_type(), None)?;
        }
        self.x_axis = AxisFormat::of(x.data_type());

        for row in 0..batch.num_rows() {
            if self.values.len() == MAX_INPUT_ROWS {
                self.input_capped = true;
                return Ok(false);
            }
            let finite = |value: Option<f64>| value.filter(|value| value.is_finite());
            let value = match self.config.kind {
                ChartKind::Line | ChartKind::Scatter => {
                    let values = ys
                        .iter()
                        .map(|y| numeric(y, row).map(finite))
                        .collect::<Result<Vec<_>>>()?;
                    match finite(numeric(x, row)?) {
                        Some(x) if values.iter().any(Option::is_some) => RawValue::Point(x, values),
                        _ => {
                            self.skipped_rows += 1;
                            continue;
                        }
                    }
                }
                ChartKind::Bar => {
                    let values = ys
                        .iter()
                        .map(|y| numeric(y, row).map(finite))
                        .collect::<Result<Vec<_>>>()?;
                    match label(x, row)? {
                        Some(label) if values.iter().any(Option::is_some) => {
                            RawValue::Bar(label, values)
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
        let names = self.series;
        let values = match kind {
            ChartKind::Line | ChartKind::Scatter => {
                let mut rows = reduce(
                    self.values
                        .into_iter()
                        .filter_map(|value| match value {
                            RawValue::Point(x, values) => Some((x, values)),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                    self.config.point_limit,
                );
                // A line runs left to right, whatever order the rows came in
                // (at most `point_limit` of them; ties keep their row order).
                if kind == ChartKind::Line {
                    rows.sort_by(|left, right| left.0.total_cmp(&right.0));
                }
                ChartValues::Points(
                    names
                        .iter()
                        .enumerate()
                        .map(|(index, name)| ChartSeries {
                            name: name.clone(),
                            points: rows
                                .iter()
                                .filter_map(|(x, values)| {
                                    values
                                        .get(index)
                                        .copied()
                                        .flatten()
                                        .map(|y| ChartPoint { x: *x, y })
                                })
                                .collect(),
                        })
                        .collect(),
                )
            }
            ChartKind::Bar => {
                let rows = reduce(
                    self.values
                        .into_iter()
                        .filter_map(|value| match value {
                            RawValue::Bar(label, values) => Some((label, values)),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                    self.config.point_limit,
                );
                ChartValues::Bars {
                    labels: rows.iter().map(|(label, _)| label.clone()).collect(),
                    series: names
                        .iter()
                        .enumerate()
                        .map(|(index, name)| {
                            (
                                name.clone(),
                                rows.iter()
                                    .map(|(_, values)| values.get(index).copied().flatten())
                                    .collect(),
                            )
                        })
                        .collect(),
                }
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
            ChartValues::Points(series) => series
                .iter()
                .map(|series| series.points.len())
                .max()
                .unwrap_or(0),
            ChartValues::Bars { labels, .. } => labels.len(),
            ChartValues::Histogram(values) => values.len(),
        };
        ChartModel {
            kind,
            values,
            x_axis: self.x_axis,
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
            | DataType::Time32(_)
            | DataType::Time64(_)
    ) || parquet_reader::types::is_decimal(data_type)
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
        // Times of day are plotted in their stored unit (seconds, ms, µs or ns).
        DataType::Time32(arrow_schema::TimeUnit::Second) => {
            value!(arrow_array::Time32SecondArray)
        }
        DataType::Time32(_) => value!(arrow_array::Time32MillisecondArray),
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            value!(arrow_array::Time64MicrosecondArray)
        }
        DataType::Time64(_) => value!(arrow_array::Time64NanosecondArray),
        data_type if parquet_reader::types::is_decimal(data_type) => {
            parquet_reader::value::decimal_f64(array.as_ref(), row)
        }
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

/// Chart window state: source, settings, the running query and its result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChartSource {
    Sql,
    CurrentResult,
}

impl ChartSource {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Sql => "SQL query",
            Self::CurrentResult => "Current result",
        }
    }
}

pub(crate) struct ChartState {
    /// Where the chart was last drawn on screen, for saving it as PNG.
    pub(crate) drawn_at: Option<eframe::egui::Rect>,
    /// A PNG waiting for the window screenshot it is cut from.
    pub(crate) pending_png: Option<std::path::PathBuf>,
    /// An image being written on another thread: the path or an error.
    pub(crate) saving:
        Option<std::sync::mpsc::Receiver<std::result::Result<std::path::PathBuf, String>>>,
    pub(crate) visible: bool,
    pub(crate) source: ChartSource,
    pub(crate) sql_input: String,
    pub(crate) config: ChartConfig,
    pub(crate) generation: GenerationId,
    pub(crate) execution: Option<QueryExecution>,
    pub(crate) accumulator: Option<ChartAccumulator>,
    pub(crate) model: Option<ChartModel>,
    pub(crate) error: Option<String>,
    pub(crate) status: String,
}

impl Default for ChartState {
    fn default() -> Self {
        Self {
            drawn_at: None,
            pending_png: None,
            saving: None,
            visible: false,
            source: ChartSource::Sql,
            sql_input: "SELECT * FROM dataset".to_string(),
            config: ChartConfig::default(),
            generation: GenerationId(0),
            execution: None,
            accumulator: None,
            model: None,
            error: None,
            status: "Choose a chart source and columns".to_string(),
        }
    }
}

impl ChartState {
    pub(crate) fn loading(&self) -> bool {
        self.execution.is_some()
    }

    pub(crate) fn cancel(&mut self) {
        if let Some(execution) = &mut self.execution {
            execution.cancel();
        }
        self.execution = None;
        self.accumulator = None;
    }

    pub(crate) fn reset(&mut self) {
        // Only say results were cleared when there were some.
        let had_results = self.model.is_some() || self.execution.is_some() || self.error.is_some();
        self.cancel();
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.model = None;
        self.error = None;
        self.status = if had_results {
            "Dataset changed; chart results were cleared".to_string()
        } else {
            Self::default().status
        };
    }

    /// Encodes and writes an image on another thread, through a temporary file
    /// that is renamed when complete.
    pub(crate) fn save_image(
        &mut self,
        path: std::path::PathBuf,
        encode: impl FnOnce() -> Result<Vec<u8>> + Send + 'static,
    ) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let target = path.clone();
        let spawned = std::thread::Builder::new()
            .name("pavi-chart-image".to_string())
            .spawn(move || {
                let result = encode().and_then(|bytes| {
                    let temporary = target.with_extension("pavi-image.tmp");
                    std::fs::write(&temporary, bytes)?;
                    std::fs::rename(&temporary, &target).inspect_err(|_| {
                        let _ = std::fs::remove_file(&temporary);
                    })?;
                    Ok(target)
                });
                let _ = sender.send(result.map_err(|error| format!("{error:#}")));
            });
        match spawned {
            Ok(_) => {
                self.saving = Some(receiver);
                self.status = format!("Saving the chart to {}…", path.display());
            }
            Err(error) => self.status = format!("Save chart: {error}"),
        }
    }

    /// The status text once an image save has finished.
    pub(crate) fn poll_save(&mut self) -> Option<String> {
        let receiver = self.saving.as_ref()?;
        let result = match receiver.try_recv() {
            Ok(result) => result,
            Err(std::sync::mpsc::TryRecvError::Empty) => return None,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                Err("the image writer stopped".to_string())
            }
        };
        self.saving = None;
        Some(match result {
            Ok(path) => format!("Saved the chart to {}", path.display()),
            Err(error) => format!("Save chart: {error}"),
        })
    }

    pub(crate) fn next_generation(&mut self) -> GenerationId {
        self.generation = GenerationId(self.generation.0.saturating_add(1));
        self.generation
    }
}

/// A screenshot region as PNG bytes.
pub(crate) fn encode_png(image: &eframe::egui::ColorImage) -> Result<Vec<u8>> {
    let [width, height] = image.size;
    let mut bytes = Vec::new();
    {
        let mut encoder =
            png::Encoder::new(&mut bytes, u32::try_from(width)?, u32::try_from(height)?);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header()?;
        let pixels = image
            .pixels
            .iter()
            .flat_map(|pixel| pixel.to_srgba_unmultiplied())
            .collect::<Vec<u8>>();
        writer.write_image_data(&pixels)?;
    }
    Ok(bytes)
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
            y_columns: vec!["y".to_string()],
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

    #[test]
    fn draws_several_y_columns_as_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("day", DataType::Date32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
                Arc::new(Float64Array::from(vec![None, Some(20.0), Some(30.0)])),
                Arc::new(arrow_array::Date32Array::from(vec![0, 1, 2])),
            ],
        )
        .unwrap();
        let mut config = config(ChartKind::Line);
        config.y_columns = vec!["a".into(), " ".into(), "b".into()];
        config.point_limit = 100;
        let mut accumulator = ChartAccumulator::new(config.clone()).unwrap();
        accumulator.push_batch(&batch).unwrap();
        let model = accumulator.finish();
        let ChartValues::Points(series) = &model.values else {
            panic!("expected points");
        };
        assert_eq!(model.series_names(), ["a", "b"]);
        assert_eq!(series[0].points.len(), 2);
        assert_eq!(series[1].points.len(), 2);
        assert_eq!(model.x_axis, AxisFormat::Number);

        // Bars keep one value per series per label, null where missing.
        config.kind = ChartKind::Bar;
        config.x_column = "x".into();
        let mut accumulator = ChartAccumulator::new(config.clone()).unwrap();
        accumulator.push_batch(&batch).unwrap();
        let ChartValues::Bars { labels, series } = accumulator.finish().values else {
            panic!("expected bars");
        };
        assert_eq!(labels, ["1", "2", "3"]);
        assert_eq!(series[0].1, [Some(1.0), None, Some(3.0)]);

        // A date X axis labels its ticks as dates.
        config.kind = ChartKind::Scatter;
        config.x_column = "day".into();
        let mut accumulator = ChartAccumulator::new(config.clone()).unwrap();
        accumulator.push_batch(&batch).unwrap();
        let model = accumulator.finish();
        assert_eq!(model.x_axis.label(1.0, 1.0), "1970-01-02");

        // A line runs left to right even when the rows don't.
        let reversed = RecordBatch::try_new(
            batch.schema(),
            vec![
                Arc::new(Int32Array::from(vec![3, 2, 1])),
                Arc::new(Float64Array::from(vec![Some(3.0), None, Some(1.0)])),
                Arc::new(Float64Array::from(vec![Some(30.0), Some(20.0), None])),
                Arc::new(arrow_array::Date32Array::from(vec![2, 1, 0])),
            ],
        )
        .unwrap();
        config.kind = ChartKind::Line;
        let mut accumulator = ChartAccumulator::new(config.clone()).unwrap();
        accumulator.push_batch(&reversed).unwrap();
        let ChartValues::Points(series) = accumulator.finish().values else {
            panic!("expected points");
        };
        let xs = series[0]
            .points
            .iter()
            .map(|point| point.x)
            .collect::<Vec<_>>();
        assert_eq!(xs, [0.0, 2.0]);

        config.y_columns = (0..=MAX_SERIES).map(|index| format!("y{index}")).collect();
        assert!(ChartAccumulator::new(config).is_err());
    }
}
