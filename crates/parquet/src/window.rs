//! Window functions over rows sorted by their window (ADR 0024).
//!
//! The query engine sorts the rows by `PARTITION BY` then `ORDER BY` (spilling
//! to disk like any sort) and [`compute_windows`] streams them once, adding a
//! column per window function, to a temporary table. Nothing is held per
//! partition, so a partition can be any size:
//!
//! - `ROW_NUMBER`, `RANK`, `DENSE_RANK` and `ROWS` frames are counted as the rows
//!   go by, noting where each partition and each set of tied rows (peers) starts.
//! - `LAG(x, n)` and `LEAD(x, n)` read the same rows through a second cursor `n`
//!   rows behind or ahead.
//! - An aggregate over a whole partition, or up to the last of the current
//!   row's peers (the default with `ORDER BY`), needs rows that come later: a
//!   first pass writes one total per partition or per set of peers to a small
//!   temporary table, which the second pass reads alongside the rows.

use std::{collections::VecDeque, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    UInt32Array, new_null_array,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{concat::concat, take::take, zip::zip};

use crate::{
    AggregateExpr, AggregateFunction, AggregateSpec, FilterExpr, ParquetSource, Projection,
    ScanCursor, TempTableWriter,
    source::{
        Accumulator, DECIMAL128_LIMIT, Scalar, aggregate_output_type, build_array, check_aggregate,
        decimal_divisor, scalar_at, scalar_compare,
    },
    types::is_comparable,
};

/// Largest `LAG`/`LEAD` offset, and largest `n` in `ROWS BETWEEN n PRECEDING
/// AND CURRENT ROW`: the moving frame keeps `n + 1` values.
pub const MAX_WINDOW_OFFSET: usize = 1_000;
/// Rows read and written per step; tiny in tests, so windows cross batches.
const BATCH_ROWS: usize = if cfg!(test) { 3 } else { 8_192 };

/// What a window function computes for each row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFunction {
    /// 1, 2, 3 … within the partition.
    RowNumber,
    /// 1 plus the rows before the row's peers: ties share a rank, then a gap.
    Rank,
    /// 1 plus the sets of peers before the row's: ties share a rank, no gap.
    DenseRank,
    /// The column's value `n` rows earlier in the partition.
    Lag(usize),
    /// The column's value `n` rows later in the partition.
    Lead(usize),
    /// COUNT, SUM, AVG, MIN or MAX over the frame.
    Aggregate(AggregateFunction),
}

impl WindowFunction {
    pub fn sql_name(self) -> &'static str {
        match self {
            Self::RowNumber => "ROW_NUMBER",
            Self::Rank => "RANK",
            Self::DenseRank => "DENSE_RANK",
            Self::Lag(_) => "LAG",
            Self::Lead(_) => "LEAD",
            Self::Aggregate(function) => function.sql_name(),
        }
    }
}

/// The rows an aggregate window sees for each row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrame {
    /// The whole partition (no `ORDER BY`, or `UNBOUNDED FOLLOWING`).
    Partition,
    /// The partition up to the last of the row's peers: the default with
    /// `ORDER BY`, `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
    Peers,
    /// The partition up to the row itself: `ROWS BETWEEN UNBOUNDED PRECEDING
    /// AND CURRENT ROW`.
    Rows,
    /// The row and the `n` before it in the partition: `ROWS BETWEEN n
    /// PRECEDING AND CURRENT ROW`.
    Preceding(usize),
}

/// One window function; `column` indexes the input's columns.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowExpr {
    pub function: WindowFunction,
    /// The column it reads; `None` for the rankings and `COUNT(*)`.
    pub column: Option<usize>,
    /// `LAG`/`LEAD`'s value where there is no row that far away, as text;
    /// `None` gives NULL.
    pub default: Option<String>,
    /// Used by aggregates; the others ignore it.
    pub frame: WindowFrame,
}

/// The window functions that share one `OVER (PARTITION BY … ORDER BY …)`.
/// The rows must come sorted by `partition_by`, then by `order_by`.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<usize>,
    /// The `ORDER BY` columns: rows equal in all of them are peers.
    pub order_by: Vec<usize>,
    pub windows: Vec<WindowExpr>,
}

impl WindowSpec {
    fn peer_keys(&self) -> Vec<usize> {
        let mut keys = self.partition_by.clone();
        for key in &self.order_by {
            if !keys.contains(key) {
                keys.push(*key);
            }
        }
        keys
    }

    fn needs_totals(&self) -> bool {
        self.windows.iter().any(|window| {
            matches!(window.function, WindowFunction::Aggregate(_))
                && matches!(window.frame, WindowFrame::Partition | WindowFrame::Peers)
        })
    }
}

impl WindowExpr {
    fn aggregate(&self) -> Option<AggregateExpr> {
        match (self.function, self.column) {
            (WindowFunction::Aggregate(AggregateFunction::Count), None) => {
                Some(AggregateExpr::count_all())
            }
            (WindowFunction::Aggregate(function), Some(column)) => {
                Some(AggregateExpr::of(function, column))
            }
            _ => None,
        }
    }
}

/// Checks `spec` against the input's `schema`, and gives the type of each
/// window function's column.
pub fn check_windows(schema: &Schema, spec: &WindowSpec) -> Result<Vec<DataType>> {
    for &key in spec.partition_by.iter().chain(&spec.order_by) {
        let field = schema
            .fields()
            .get(key)
            .ok_or_else(|| anyhow!("window key column {key} out of range"))?;
        if !is_comparable(field.data_type()) {
            bail!(
                "PARTITION BY and ORDER BY in OVER (…) are not supported for {} columns",
                field.data_type()
            );
        }
    }
    spec.windows
        .iter()
        .map(|window| window_output_type(schema, window))
        .collect()
}

fn window_output_type(schema: &Schema, window: &WindowExpr) -> Result<DataType> {
    let name = window.function.sql_name();
    let field = window
        .column
        .map(|column| {
            schema
                .fields()
                .get(column)
                .ok_or_else(|| anyhow!("window column {column} out of range"))
        })
        .transpose()?;
    if let WindowFrame::Preceding(rows) = window.frame
        && rows > MAX_WINDOW_OFFSET
    {
        bail!("a window frame reaches at most {MAX_WINDOW_OFFSET} rows back, not {rows}");
    }
    match window.function {
        WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
            if field.is_some() {
                bail!("{name}() takes no column");
            }
            Ok(DataType::Int64)
        }
        WindowFunction::Lag(offset) | WindowFunction::Lead(offset) => {
            if offset > MAX_WINDOW_OFFSET {
                bail!("{name} reaches at most {MAX_WINDOW_OFFSET} rows away, not {offset}");
            }
            let field = field.ok_or_else(|| anyhow!("{name} needs a column"))?;
            default_value(window.default.as_deref(), field.data_type(), name)?;
            Ok(field.data_type().clone())
        }
        WindowFunction::Aggregate(function) => {
            if !matches!(
                function,
                AggregateFunction::Count
                    | AggregateFunction::Sum
                    | AggregateFunction::Avg
                    | AggregateFunction::Min
                    | AggregateFunction::Max
            ) {
                bail!(
                    "{} … OVER (…) is not supported; window aggregates are COUNT, SUM, AVG, MIN \
                     and MAX",
                    function.sql_name()
                );
            }
            let expression = window
                .aggregate()
                .ok_or_else(|| anyhow!("only COUNT may use *"))?;
            check_aggregate(schema, &AggregateSpec::new(vec![expression])?)?;
            aggregate_output_type(expression, field.map(|field| field.data_type()))
        }
    }
}

/// `LAG`/`LEAD`'s default as one value of `data_type`.
fn default_value(text: Option<&str>, data_type: &DataType, name: &str) -> Result<ArrayRef> {
    let Some(text) = text else {
        return Ok(new_null_array(data_type, 1));
    };
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    cast_with_options(&StringArray::from(vec![text]), data_type, &options)
        .map_err(|_| anyhow!("{name}'s default {text} is not a {data_type} value"))
}

/// Reads `source`'s rows that pass `filter`, keeping `columns`, and writes
/// them with a column per window function after them, named `names`. The
/// rows must come sorted by the spec's `PARTITION BY` then `ORDER BY`
/// columns; the spec's indexes refer to `columns`. `checkpoint` gets the
/// share done, from 0 to 1, and stops the work when it returns `true`.
pub fn compute_windows(
    source: &ParquetSource,
    filter: Option<FilterExpr>,
    columns: &[usize],
    spec: &WindowSpec,
    names: &[String],
    checkpoint: &dyn Fn(f64) -> bool,
) -> Result<ParquetSource> {
    if columns.is_empty() {
        bail!("window functions need at least one input column");
    }
    if names.len() != spec.windows.len() {
        bail!(
            "{} names for {} window functions",
            names.len(),
            spec.windows.len()
        );
    }
    let source_schema = source.schema();
    let input = Schema::new(
        columns
            .iter()
            .map(|column| {
                source_schema
                    .fields()
                    .get(*column)
                    .map(|field| field.as_ref().clone())
                    .ok_or_else(|| anyhow!("column {column} out of range"))
            })
            .collect::<Result<Vec<Field>>>()?,
    );
    let types = check_windows(&input, spec)?;
    let mut fields = input.fields().iter().cloned().collect::<Vec<_>>();
    for (name, data_type) in names.iter().zip(&types) {
        fields.push(Arc::new(Field::new(name, data_type.clone(), true)));
    }
    let output: SchemaRef = Arc::new(Schema::new(fields));
    let reader = Input {
        source,
        filter,
        columns,
        schema: &input,
    };
    let total = source.row_count().max(1) as f64;

    // First pass: totals per partition and per set of peers.
    let totals = if spec.needs_totals() {
        Some(Totals::write(&reader, spec, &types, &|row| {
            checkpoint(row as f64 / total / 2.0)
        })?)
    } else {
        None
    };
    let start = if totals.is_some() { 0.5 } else { 0.0 };
    let scale = 1.0 - start;
    let step = |row: u64| checkpoint(start + row as f64 / total * scale);

    let mut main = reader.rows(&(0..columns.len()).collect::<Vec<_>>())?;
    let mut partitions = Boundaries::new(spec.partition_by.clone());
    let mut peers = Boundaries::new(spec.peer_keys());
    let mut states = spec
        .windows
        .iter()
        .zip(&types)
        .map(|(window, data_type)| WindowState::new(window, data_type, spec, &reader))
        .collect::<Result<Vec<_>>>()?;
    let (mut partition_totals, mut peer_totals) = match totals {
        Some(totals) => (
            totals.partitions.map(TotalsLane::new).transpose()?,
            totals.peers.map(TotalsLane::new).transpose()?,
        ),
        None => (None, None),
    };
    let mut writer = TempTableWriter::new(&output)?;
    let mut ordinal = 0_u64;
    while let Some(batch) = main.next(BATCH_ROWS, &step)? {
        let rows = batch.num_rows();
        let partition_starts = partitions.starts(&batch)?;
        let peer_starts = peers.starts(&batch)?;
        let ordinals = partition_starts
            .iter()
            .map(|start| {
                ordinal += u64::from(*start);
                ordinal
            })
            .collect::<Vec<_>>();
        let partition_values = partition_totals
            .as_mut()
            .map(|lane| lane.values(&partition_starts))
            .transpose()?;
        let peer_values = peer_totals
            .as_mut()
            .map(|lane| lane.values(&peer_starts))
            .transpose()?;
        let mut arrays = batch.columns().to_vec();
        for state in &mut states {
            arrays.push(state.compute(
                &batch,
                &partition_starts,
                &peer_starts,
                &ordinals,
                partition_values.as_deref(),
                peer_values.as_deref(),
                &step,
            )?);
        }
        writer.write(&RecordBatch::try_new_with_options(
            Arc::clone(&output),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)?;
    }
    writer.finish("window")
}

/// The rows windows read: `source` filtered, as `columns`.
struct Input<'a> {
    source: &'a ParquetSource,
    filter: Option<FilterExpr>,
    columns: &'a [usize],
    /// The input's columns.
    schema: &'a Schema,
}

impl Input<'_> {
    /// A stream of the input's rows from the first, with the input columns
    /// at `wanted` (positions in `columns`), in that order.
    fn rows(&self, wanted: &[usize]) -> Result<Rows<'_>> {
        // Each source column once; an empty list still reads one, for the row count.
        let mut read = Vec::new();
        for position in wanted {
            let column = *self
                .columns
                .get(*position)
                .ok_or_else(|| anyhow!("window input column {position} out of range"))?;
            if !read.contains(&column) {
                read.push(column);
            }
        }
        let count_only = read.is_empty();
        if count_only {
            read.push(self.columns[0]);
        }
        let positions = wanted
            .iter()
            .map(|position| {
                let column = self.columns[*position];
                read.iter()
                    .position(|kept| *kept == column)
                    .ok_or_else(|| anyhow!("window input column {column} was not read"))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(
            wanted
                .iter()
                .map(|position| self.schema.field(*position).clone())
                .collect::<Vec<_>>(),
        ));
        let projection = Projection::columns(read, self.source.column_count())?;
        Ok(Rows {
            source: self.source,
            cursor: self
                .source
                .scan_cursor(self.filter.clone(), 0, &projection)?,
            positions,
            schema,
        })
    }
}

/// Rows read in order, a batch at a time.
struct Rows<'a> {
    source: &'a ParquetSource,
    cursor: ScanCursor,
    /// Where each wanted column is in a read batch.
    positions: Vec<usize>,
    schema: SchemaRef,
}

impl Rows<'_> {
    /// The next up to `count` rows; fewer only at the end, `None` after it.
    fn next(
        &mut self,
        count: usize,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<Option<RecordBatch>> {
        if self.cursor.is_finished() {
            return Ok(None);
        }
        let window = self.cursor.next_window(self.source, count, checkpoint)?;
        let rows = window.batch.num_rows();
        if rows == 0 {
            return Ok(None);
        }
        let columns = self
            .positions
            .iter()
            .map(|position| Arc::clone(window.batch.column(*position)))
            .collect();
        Ok(Some(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?))
    }
}

/// Finds where runs of equal key values start, across batches.
struct Boundaries {
    keys: Vec<usize>,
    /// The last row's keys, as one-row arrays.
    previous: Option<Vec<ArrayRef>>,
}

impl Boundaries {
    fn new(keys: Vec<usize>) -> Self {
        Self {
            keys,
            previous: None,
        }
    }

    /// For each row of `batch`, whether its keys differ from the row before
    /// (the very first row starts a run).
    fn starts(&mut self, batch: &RecordBatch) -> Result<Vec<bool>> {
        let rows = batch.num_rows();
        let mut starts = vec![false; rows];
        if rows == 0 {
            return Ok(starts);
        }
        let first = self.previous.is_none();
        if self.keys.is_empty() {
            starts[0] = first;
            self.previous = Some(Vec::new());
            return Ok(starts);
        }
        let columns = self
            .keys
            .iter()
            .map(|key| Arc::clone(batch.column(*key)))
            .collect::<Vec<_>>();
        let (combined, offset) = match &self.previous {
            Some(previous) => (
                previous
                    .iter()
                    .zip(&columns)
                    .map(|(last, column)| Ok(concat(&[last.as_ref(), column.as_ref()])?))
                    .collect::<Result<Vec<_>>>()?,
                1,
            ),
            None => (columns.clone(), 0),
        };
        for range in arrow_ord::partition::partition(&combined)?.ranges() {
            if range.start >= offset {
                starts[range.start - offset] = true;
            }
        }
        if first {
            starts[0] = true;
        }
        self.previous = Some(
            columns
                .iter()
                .map(|column| column.slice(rows - 1, 1))
                .collect(),
        );
        Ok(starts)
    }
}

/// The totals tables the first pass writes: one row per partition and one
/// per set of peers, a column per window that needs them.
struct Totals {
    partitions: Option<ParquetSource>,
    peers: Option<ParquetSource>,
}

impl Totals {
    fn write(
        input: &Input<'_>,
        spec: &WindowSpec,
        types: &[DataType],
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<Self> {
        let mut partition_side =
            TotalsWriter::new(spec, types, input.schema, WindowFrame::Partition)?;
        let mut peer_side = TotalsWriter::new(spec, types, input.schema, WindowFrame::Peers)?;
        let mut wanted = spec.peer_keys();
        for window in &spec.windows {
            if let Some(column) = window.column
                && !wanted.contains(&column)
            {
                wanted.push(column);
            }
        }
        let position = |column: usize| wanted.iter().position(|kept| *kept == column);
        let mut rows = input.rows(&wanted)?;
        let mut partitions = Boundaries::new(
            spec.partition_by
                .iter()
                .filter_map(|key| position(*key))
                .collect(),
        );
        let mut peers = Boundaries::new(
            spec.peer_keys()
                .iter()
                .filter_map(|key| position(*key))
                .collect(),
        );
        let mut started = false;
        while let Some(batch) = rows.next(BATCH_ROWS, checkpoint)? {
            let partition_starts = partitions.starts(&batch)?;
            let peer_starts = peers.starts(&batch)?;
            for side in [&mut partition_side, &mut peer_side].into_iter().flatten() {
                side.update(&batch, &position, &partition_starts, &peer_starts, started)?;
            }
            started = true;
        }
        Ok(Self {
            partitions: partition_side
                .map(|side| side.finish(started))
                .transpose()?,
            peers: peer_side.map(|side| side.finish(started)).transpose()?,
        })
    }
}

/// Accumulates the windows of one frame (whole partition, or up to the
/// peers) and writes a row each time a partition or set of peers ends.
struct TotalsWriter {
    frame: WindowFrame,
    windows: Vec<(AggregateExpr, Option<DataType>)>,
    accumulators: Vec<Accumulator>,
    /// Values not yet written, a list per window.
    pending: Vec<Vec<Option<Scalar>>>,
    schema: SchemaRef,
    writer: TempTableWriter,
}

impl TotalsWriter {
    fn new(
        spec: &WindowSpec,
        types: &[DataType],
        input: &Schema,
        frame: WindowFrame,
    ) -> Result<Option<Self>> {
        let mut windows = Vec::new();
        let mut fields = Vec::new();
        for (index, (window, data_type)) in spec.windows.iter().zip(types).enumerate() {
            if window.frame != frame {
                continue;
            }
            let Some(expression) = window.aggregate() else {
                continue;
            };
            let input_type = window
                .column
                .map(|column| input.field(column).data_type().clone());
            windows.push((expression, input_type));
            fields.push(Field::new(format!("total{index}"), data_type.clone(), true));
        }
        if windows.is_empty() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new(fields));
        Ok(Some(Self {
            frame,
            accumulators: new_accumulators(&windows)?,
            pending: vec![Vec::new(); windows.len()],
            windows,
            writer: TempTableWriter::new(&schema)?,
            schema,
        }))
    }

    fn update(
        &mut self,
        batch: &RecordBatch,
        position: &dyn Fn(usize) -> Option<usize>,
        partition_starts: &[bool],
        peer_starts: &[bool],
        started: bool,
    ) -> Result<()> {
        let starts = match self.frame {
            WindowFrame::Peers => peer_starts,
            _ => partition_starts,
        };
        let columns = self
            .windows
            .iter()
            .map(|(expression, _)| {
                expression
                    .column()
                    .map(|column| {
                        position(column)
                            .map(|index| batch.column(index))
                            .ok_or_else(|| anyhow!("window column {column} was not read"))
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            if starts[row] && (started || row > 0) {
                self.emit()?;
            }
            if partition_starts[row] {
                self.accumulators = new_accumulators(&self.windows)?;
            }
            for ((accumulator, (expression, data_type)), column) in self
                .accumulators
                .iter_mut()
                .zip(&self.windows)
                .zip(&columns)
            {
                let value = match (column, data_type) {
                    (Some(column), Some(data_type)) => {
                        Some(scalar_at(column.as_ref(), row, data_type)?)
                    }
                    _ => None,
                };
                accumulator.update(*expression, value)?;
            }
        }
        Ok(())
    }

    /// Notes each window's value for the partition or peers just ended.
    fn emit(&mut self) -> Result<()> {
        for ((pending, accumulator), (expression, _)) in self
            .pending
            .iter_mut()
            .zip(&self.accumulators)
            .zip(&self.windows)
        {
            pending.push(accumulator.value(expression.function()));
        }
        if self
            .pending
            .first()
            .is_some_and(|values| values.len() >= BATCH_ROWS)
        {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let rows = self.pending.first().map_or(0, Vec::len);
        if rows == 0 {
            return Ok(());
        }
        let arrays = self
            .pending
            .iter_mut()
            .zip(self.schema.fields())
            .map(|(values, field)| {
                let array = build_array(field.data_type(), values);
                values.clear();
                array
            })
            .collect::<Result<Vec<_>>>()?;
        self.writer
            .write(&RecordBatch::try_new(Arc::clone(&self.schema), arrays)?)
    }

    fn finish(mut self, started: bool) -> Result<ParquetSource> {
        if started {
            self.emit()?;
        }
        self.flush()?;
        self.writer.finish("window totals")
    }
}

fn new_accumulators(windows: &[(AggregateExpr, Option<DataType>)]) -> Result<Vec<Accumulator>> {
    windows
        .iter()
        .map(|(expression, data_type)| Accumulator::new(*expression, data_type.as_ref()))
        .collect()
}

/// Reads a totals table alongside the rows: one totals row per run.
struct TotalsLane {
    source: ParquetSource,
    cursor: ScanCursor,
    /// The totals row of the run the last batch ended in.
    current: Option<Vec<ArrayRef>>,
}

impl TotalsLane {
    fn new(source: ParquetSource) -> Result<Self> {
        let projection = Projection::all(source.column_count());
        let cursor = source.scan_cursor(None, 0, &projection)?;
        Ok(Self {
            source,
            cursor,
            current: None,
        })
    }

    /// Each totals column, a value per row of a batch whose runs start at `starts`.
    fn values(&mut self, starts: &[bool]) -> Result<Vec<ArrayRef>> {
        let new = starts.iter().filter(|start| **start).count();
        let read = if new > 0 {
            self.cursor
                .next_window(&self.source, new, &|_| false)?
                .batch
        } else {
            RecordBatch::new_empty(self.source.schema())
        };
        if read.num_rows() != new {
            bail!("the window totals are out of step with the rows");
        }
        let combined = (0..read.num_columns())
            .map(|column| match &self.current {
                Some(current) => Ok(concat(&[
                    current[column].as_ref(),
                    read.column(column).as_ref(),
                ])?),
                None => Ok(Arc::clone(read.column(column))),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut index = if self.current.is_some() { 0_i64 } else { -1 };
        let indices = starts
            .iter()
            .map(|start| {
                index += i64::from(*start);
                u32::try_from(index).context("window totals start before the rows")
            })
            .collect::<Result<Vec<_>>>()?;
        let indices = UInt32Array::from(indices);
        let values = combined
            .iter()
            .map(|column| Ok(take(column.as_ref(), &indices, None)?))
            .collect::<Result<Vec<_>>>()?;
        if let Some(rows) = combined.first().map(|column| column.len())
            && rows > 0
        {
            self.current = Some(
                combined
                    .iter()
                    .map(|column| column.slice(rows - 1, 1))
                    .collect(),
            );
        }
        Ok(values)
    }
}

/// What one window function keeps between rows.
enum WindowState<'a> {
    Rank {
        function: WindowFunction,
        row: i64,
        rank: i64,
    },
    Shifted {
        lane: Box<ShiftedLane<'a>>,
        default: ArrayRef,
    },
    Running {
        expression: AggregateExpr,
        input: Option<DataType>,
        output: DataType,
        accumulator: Option<Accumulator>,
    },
    Moving {
        expression: AggregateExpr,
        input: Option<DataType>,
        output: DataType,
        rows: usize,
        moving: Option<Moving>,
    },
    /// The nth column of the partition or peers totals.
    Total { peers: bool, column: usize },
}

impl<'a> WindowState<'a> {
    fn new(
        window: &WindowExpr,
        output: &DataType,
        spec: &WindowSpec,
        input: &'a Input<'a>,
    ) -> Result<Self> {
        let input_type = window
            .column
            .map(|column| input.schema.field(column).data_type().clone());
        Ok(match window.function {
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                Self::Rank {
                    function: window.function,
                    row: 0,
                    rank: 0,
                }
            }
            WindowFunction::Lag(offset) | WindowFunction::Lead(offset) => {
                let column = window
                    .column
                    .ok_or_else(|| anyhow!("{} needs a column", window.function.sql_name()))?;
                let mut wanted = spec.partition_by.clone();
                wanted.push(column);
                let shift = match window.function {
                    WindowFunction::Lag(_) => -(offset as i64),
                    _ => offset as i64,
                };
                Self::Shifted {
                    lane: Box::new(ShiftedLane::new(
                        input.rows(&wanted)?,
                        spec.partition_by.len(),
                        shift,
                    )?),
                    default: default_value(
                        window.default.as_deref(),
                        output,
                        window.function.sql_name(),
                    )?,
                }
            }
            WindowFunction::Aggregate(_) => {
                let expression = window
                    .aggregate()
                    .ok_or_else(|| anyhow!("only COUNT may use *"))?;
                match window.frame {
                    WindowFrame::Rows => Self::Running {
                        expression,
                        input: input_type,
                        output: output.clone(),
                        accumulator: None,
                    },
                    WindowFrame::Preceding(rows) => Self::Moving {
                        expression,
                        input: input_type,
                        output: output.clone(),
                        rows,
                        moving: None,
                    },
                    frame @ (WindowFrame::Partition | WindowFrame::Peers) => {
                        let peers = frame == WindowFrame::Peers;
                        // Its column among the totals of its frame.
                        let column = spec
                            .windows
                            .iter()
                            .take_while(|other| !std::ptr::eq(*other, window))
                            .filter(|other| other.frame == frame && other.aggregate().is_some())
                            .count();
                        Self::Total { peers, column }
                    }
                }
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn compute(
        &mut self,
        batch: &RecordBatch,
        partition_starts: &[bool],
        peer_starts: &[bool],
        ordinals: &[u64],
        partition_totals: Option<&[ArrayRef]>,
        peer_totals: Option<&[ArrayRef]>,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<ArrayRef> {
        let rows = batch.num_rows();
        match self {
            Self::Rank {
                function,
                row,
                rank,
            } => {
                let mut values = Vec::with_capacity(rows);
                for index in 0..rows {
                    if partition_starts[index] {
                        *row = 0;
                        *rank = 0;
                    }
                    *row += 1;
                    match function {
                        WindowFunction::Rank if peer_starts[index] => *rank = *row,
                        WindowFunction::DenseRank if peer_starts[index] => *rank += 1,
                        _ => {}
                    }
                    values.push(match function {
                        WindowFunction::RowNumber => *row,
                        _ => *rank,
                    });
                }
                Ok(Arc::new(Int64Array::from(values)))
            }
            Self::Shifted { lane, default } => {
                let (values, lane_ordinals) = lane.next(rows, checkpoint)?;
                let same = BooleanArray::from(
                    ordinals
                        .iter()
                        .zip(&lane_ordinals)
                        .map(|(main, lane)| main == lane)
                        .collect::<Vec<_>>(),
                );
                Ok(zip(
                    &same,
                    &values,
                    &arrow_array::Scalar::new(Arc::clone(default)),
                )?)
            }
            Self::Running {
                expression,
                input,
                output,
                accumulator,
            } => {
                let column = expression.column().map(|column| batch.column(column));
                let mut values = Vec::with_capacity(rows);
                for (index, start) in partition_starts.iter().enumerate() {
                    if *start || accumulator.is_none() {
                        *accumulator = Some(Accumulator::new(*expression, input.as_ref())?);
                    }
                    let accumulator = accumulator
                        .as_mut()
                        .ok_or_else(|| anyhow!("running window has no accumulator"))?;
                    let value = match (column, input.as_ref()) {
                        (Some(column), Some(data_type)) => {
                            Some(scalar_at(column.as_ref(), index, data_type)?)
                        }
                        _ => None,
                    };
                    accumulator.update(*expression, value)?;
                    values.push(accumulator.value(expression.function()));
                }
                build_array(output, &values)
            }
            Self::Moving {
                expression,
                input,
                output,
                rows: frame_rows,
                moving,
            } => {
                let column = expression.column().map(|column| batch.column(column));
                let mut values = Vec::with_capacity(rows);
                for (index, start) in partition_starts.iter().enumerate() {
                    if *start || moving.is_none() {
                        *moving = Some(Moving::new(
                            *expression,
                            input.as_ref(),
                            output,
                            *frame_rows,
                        ));
                    }
                    let moving = moving
                        .as_mut()
                        .ok_or_else(|| anyhow!("moving window has no state"))?;
                    let value = match (column, input.as_ref()) {
                        (Some(column), Some(data_type)) => {
                            scalar_at(column.as_ref(), index, data_type)?
                        }
                        // COUNT(*): every row counts.
                        _ => Scalar::Bool(true),
                    };
                    moving.push(value);
                    values.push(moving.value()?);
                }
                build_array(output, &values)
            }
            Self::Total { peers, column } => {
                let totals = if *peers {
                    peer_totals
                } else {
                    partition_totals
                };
                totals
                    .and_then(|totals| totals.get(*column))
                    .cloned()
                    .ok_or_else(|| anyhow!("window totals are missing"))
            }
        }
    }
}

/// A column read `shift` rows away from the main rows (behind when
/// negative), with the partition each of those rows is in.
struct ShiftedLane<'a> {
    rows: Rows<'a>,
    partitions: Boundaries,
    ordinal: u64,
    /// Rows of NULL still to give before the first row (`LAG`).
    leading_nulls: usize,
    /// Rows still to skip before the first given row (`LEAD`).
    skip: usize,
    /// The value column's type.
    data_type: DataType,
}

impl<'a> ShiftedLane<'a> {
    /// `rows` holds the partition keys, then the value column.
    fn new(rows: Rows<'a>, keys: usize, shift: i64) -> Result<Self> {
        let data_type = rows
            .schema
            .fields()
            .get(keys)
            .map(|field| field.data_type().clone())
            .ok_or_else(|| anyhow!("shifted window column is missing"))?;
        let distance = usize::try_from(shift.unsigned_abs()).context("window offset too large")?;
        Ok(Self {
            rows,
            partitions: Boundaries::new((0..keys).collect()),
            ordinal: 0,
            leading_nulls: if shift < 0 { distance } else { 0 },
            skip: if shift > 0 { distance } else { 0 },
            data_type,
        })
    }

    /// The value and the partition ordinal of the next `count` shifted rows;
    /// ordinal 0 (no partition) where the shift falls outside the rows.
    fn next(
        &mut self,
        count: usize,
        checkpoint: &dyn Fn(u64) -> bool,
    ) -> Result<(ArrayRef, Vec<u64>)> {
        while self.skip > 0 {
            let Some(batch) = self.rows.next(self.skip.min(BATCH_ROWS), checkpoint)? else {
                self.skip = 0;
                break;
            };
            self.skip -= batch.num_rows();
            self.ordinals(&batch)?;
        }
        let nulls = self.leading_nulls.min(count);
        self.leading_nulls -= nulls;
        let mut values = Vec::new();
        let mut ordinals = vec![0_u64; nulls];
        if nulls > 0 {
            values.push(new_null_array(&self.data_type, nulls));
        }
        let wanted = count - nulls;
        if wanted > 0
            && let Some(batch) = self.rows.next(wanted, checkpoint)?
        {
            ordinals.extend(self.ordinals(&batch)?);
            values.push(Arc::clone(batch.column(batch.num_columns() - 1)));
        }
        // Past the last row: NULL.
        let missing = count - ordinals.len();
        if missing > 0 {
            values.push(new_null_array(&self.data_type, missing));
            ordinals.extend(std::iter::repeat_n(0, missing));
        }
        let values = match values.as_slice() {
            [single] => Arc::clone(single),
            _ => concat(&values.iter().map(AsRef::as_ref).collect::<Vec<_>>())?,
        };
        Ok((values, ordinals))
    }

    fn ordinals(&mut self, batch: &RecordBatch) -> Result<Vec<u64>> {
        Ok(self
            .partitions
            .starts(batch)?
            .iter()
            .map(|start| {
                self.ordinal += u64::from(*start);
                self.ordinal
            })
            .collect())
    }
}

/// An aggregate over the last `rows + 1` rows of a partition: sums and counts
/// add the value that enters and take away the one that leaves; MIN and MAX
/// keep the values that can still become the extreme.
struct Moving {
    function: AggregateFunction,
    counts_rows: bool,
    output: DataType,
    size: usize,
    values: VecDeque<Scalar>,
    count: u64,
    integer: i128,
    float: f64,
    divisor: f64,
    /// Row number and value of the candidates for MIN or MAX, best first.
    extremes: VecDeque<(u64, Scalar)>,
    seen: u64,
}

impl Moving {
    fn new(
        expression: AggregateExpr,
        input: Option<&DataType>,
        output: &DataType,
        rows: usize,
    ) -> Self {
        Self {
            function: expression.function(),
            counts_rows: expression.column().is_none(),
            output: output.clone(),
            size: rows + 1,
            values: VecDeque::new(),
            count: 0,
            integer: 0,
            float: 0.0,
            divisor: input.map_or(1.0, decimal_divisor),
            extremes: VecDeque::new(),
            seen: 0,
        }
    }

    fn push(&mut self, value: Scalar) {
        self.seen += 1;
        self.add(&value, true);
        if matches!(
            self.function,
            AggregateFunction::Min | AggregateFunction::Max
        ) && !value.is_null()
        {
            let keep = |candidate: &Scalar| {
                let order = scalar_compare(candidate, &value);
                match self.function {
                    AggregateFunction::Min => order.is_lt(),
                    _ => order.is_gt(),
                }
            };
            while self.extremes.back().is_some_and(|(_, last)| !keep(last)) {
                self.extremes.pop_back();
            }
            self.extremes.push_back((self.seen, value.clone()));
        }
        self.values.push_back(value);
        if self.values.len() > self.size
            && let Some(left) = self.values.pop_front()
        {
            self.add(&left, false);
        }
        let oldest = self.seen.saturating_sub(self.size as u64);
        while self.extremes.front().is_some_and(|(row, _)| *row <= oldest) {
            self.extremes.pop_front();
        }
    }

    /// Adds `value` to the sums and count, or takes it away.
    fn add(&mut self, value: &Scalar, entering: bool) {
        if value.is_null() && !self.counts_rows {
            return;
        }
        let sign = if entering { 1 } else { -1 };
        if entering {
            self.count += 1;
        } else {
            self.count = self.count.saturating_sub(1);
        }
        match value {
            Scalar::I64(value) => self.integer += sign * i128::from(*value),
            Scalar::U64(value) => self.integer += sign * i128::from(*value),
            Scalar::Decimal(value) => self.integer += sign * *value,
            _ => {}
        }
        if let Ok(number) = value.f64() {
            if entering {
                self.float += number;
            } else {
                self.float -= number;
            }
        }
    }

    fn value(&self) -> Result<Option<Scalar>> {
        Ok(match self.function {
            AggregateFunction::Count => Some(Scalar::U64(self.count)),
            _ if self.count == 0 => None,
            AggregateFunction::Sum => Some(match self.output {
                DataType::Int64 => Scalar::I64(
                    i64::try_from(self.integer).context("SUM overflow for signed integer")?,
                ),
                DataType::UInt64 => Scalar::U64(
                    u64::try_from(self.integer).context("SUM overflow for unsigned integer")?,
                ),
                DataType::Decimal128(..) => {
                    if self.integer.unsigned_abs() >= DECIMAL128_LIMIT {
                        bail!("SUM overflow for decimal (more than 38 digits)");
                    }
                    Scalar::Decimal(self.integer)
                }
                _ => Scalar::F64(self.float.to_bits()),
            }),
            AggregateFunction::Avg => Some(Scalar::F64(
                (self.float / self.count as f64 / self.divisor).to_bits(),
            )),
            _ => self.extremes.front().map(|(_, value)| value.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array};

    /// A file with `group`, `day` and `amount` columns, sorted by group then day.
    fn source(groups: &[&str], days: &[i32], amounts: &[Option<f64>]) -> ParquetSource {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, true),
            Field::new("day", DataType::Int32, true),
            Field::new("amount", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(groups.to_vec())),
                Arc::new(Int32Array::from(days.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap();
        let mut writer = TempTableWriter::new(&schema).unwrap();
        // Small batches, so the windows cross batch boundaries.
        for start in (0..batch.num_rows()).step_by(2) {
            writer
                .write(&batch.slice(start, 2.min(batch.num_rows() - start)))
                .unwrap();
        }
        writer.finish("test").unwrap()
    }

    fn texts(source: &ParquetSource, column: usize) -> Vec<String> {
        let mut cursor = source
            .scan_cursor(None, 0, &Projection::all(source.column_count()))
            .unwrap();
        let mut out = Vec::new();
        while !cursor.is_finished() {
            let batch = cursor.next_window(source, 1024, &|_| false).unwrap().batch;
            for row in 0..batch.num_rows() {
                out.push(
                    crate::value::scalar_text(batch.column(column).as_ref(), row)
                        .unwrap()
                        .unwrap_or_else(|| "null".to_owned()),
                );
            }
        }
        out
    }

    fn window(function: WindowFunction, column: Option<usize>, frame: WindowFrame) -> WindowExpr {
        WindowExpr {
            function,
            column,
            default: None,
            frame,
        }
    }

    fn run(source: &ParquetSource, spec: &WindowSpec) -> Vec<Vec<String>> {
        let names = (0..spec.windows.len())
            .map(|index| format!("w{index}"))
            .collect::<Vec<_>>();
        let output = compute_windows(source, None, &[0, 1, 2], spec, &names, &|_| false).unwrap();
        (3..output.column_count())
            .map(|column| texts(&output, column))
            .collect()
    }

    #[test]
    fn ranks_and_shifts_within_partitions() {
        let source = source(
            &["a", "a", "a", "a", "b", "b", "b"],
            &[1, 2, 2, 3, 1, 1, 5],
            &[
                Some(1.0),
                Some(2.0),
                Some(3.0),
                None,
                Some(5.0),
                Some(6.0),
                Some(7.0),
            ],
        );
        let mut lag = window(WindowFunction::Lag(1), Some(2), WindowFrame::Peers);
        lag.default = Some("0".to_owned());
        let spec = WindowSpec {
            partition_by: vec![0],
            order_by: vec![1],
            windows: vec![
                window(WindowFunction::RowNumber, None, WindowFrame::Peers),
                window(WindowFunction::Rank, None, WindowFrame::Peers),
                window(WindowFunction::DenseRank, None, WindowFrame::Peers),
                lag,
                window(WindowFunction::Lead(2), Some(1), WindowFrame::Peers),
            ],
        };
        assert_eq!(
            run(&source, &spec),
            vec![
                vec!["1", "2", "3", "4", "1", "2", "3"],
                vec!["1", "2", "2", "4", "1", "1", "3"],
                vec!["1", "2", "2", "3", "1", "1", "2"],
                vec!["0", "1", "2", "3", "0", "5", "6"],
                vec!["2", "3", "null", "null", "5", "null", "null"],
            ]
        );
    }

    #[test]
    fn aggregates_over_partitions_peers_and_rows() {
        let source = source(
            &["a", "a", "a", "a", "b", "b", "b"],
            &[1, 2, 2, 3, 1, 1, 5],
            &[
                Some(1.0),
                Some(2.0),
                Some(3.0),
                None,
                Some(5.0),
                Some(6.0),
                Some(7.0),
            ],
        );
        let sum = WindowFunction::Aggregate(AggregateFunction::Sum);
        let spec = WindowSpec {
            partition_by: vec![0],
            order_by: vec![1],
            windows: vec![
                window(sum, Some(2), WindowFrame::Partition),
                window(sum, Some(2), WindowFrame::Peers),
                window(sum, Some(2), WindowFrame::Rows),
                window(sum, Some(2), WindowFrame::Preceding(1)),
                window(
                    WindowFunction::Aggregate(AggregateFunction::Count),
                    None,
                    WindowFrame::Peers,
                ),
                window(
                    WindowFunction::Aggregate(AggregateFunction::Max),
                    Some(1),
                    WindowFrame::Preceding(1),
                ),
                window(
                    WindowFunction::Aggregate(AggregateFunction::Avg),
                    Some(2),
                    WindowFrame::Preceding(2),
                ),
            ],
        };
        assert_eq!(
            run(&source, &spec),
            vec![
                vec!["6", "6", "6", "6", "18", "18", "18"],
                vec!["1", "6", "6", "6", "11", "11", "18"],
                vec!["1", "3", "6", "6", "5", "11", "18"],
                vec!["1", "3", "5", "3", "5", "11", "13"],
                vec!["1", "3", "3", "4", "2", "2", "3"],
                vec!["1", "2", "2", "3", "1", "1", "5"],
                vec!["1", "1.5", "2", "2.5", "5", "5.5", "6"],
            ]
        );
    }

    #[test]
    fn windows_without_partitions_cover_every_row() {
        let source = source(
            &["a", "b", "c", "d", "e"],
            &[5, 4, 3, 2, 1],
            &[Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)],
        );
        let spec = WindowSpec {
            partition_by: Vec::new(),
            order_by: Vec::new(),
            windows: vec![
                window(WindowFunction::RowNumber, None, WindowFrame::Partition),
                window(
                    WindowFunction::Aggregate(AggregateFunction::Sum),
                    Some(2),
                    WindowFrame::Partition,
                ),
                window(WindowFunction::Lead(1), Some(1), WindowFrame::Partition),
                window(WindowFunction::Rank, None, WindowFrame::Partition),
            ],
        };
        assert_eq!(
            run(&source, &spec),
            vec![
                vec!["1", "2", "3", "4", "5"],
                vec!["15", "15", "15", "15", "15"],
                vec!["4", "3", "2", "1", "null"],
                vec!["1", "1", "1", "1", "1"],
            ]
        );
    }

    #[test]
    fn explains_what_windows_cannot_do() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]);
        let spec = |window: WindowExpr| WindowSpec {
            partition_by: Vec::new(),
            order_by: Vec::new(),
            windows: vec![window],
        };
        let message = |window: WindowExpr| {
            check_windows(&schema, &spec(window))
                .unwrap_err()
                .to_string()
        };
        assert!(
            message(window(
                WindowFunction::Aggregate(AggregateFunction::Sum),
                Some(0),
                WindowFrame::Partition
            ))
            .contains("SUM needs a number column")
        );
        assert!(
            message(window(
                WindowFunction::Aggregate(AggregateFunction::Median),
                Some(1),
                WindowFrame::Partition
            ))
            .contains("window aggregates are COUNT, SUM, AVG, MIN and MAX")
        );
        assert!(
            message(window(
                WindowFunction::Lag(5000),
                Some(1),
                WindowFrame::Partition
            ))
            .contains("at most 1000 rows")
        );
        let mut lag = window(WindowFunction::Lag(1), Some(1), WindowFrame::Partition);
        lag.default = Some("soon".to_owned());
        assert!(message(lag).contains("LAG's default soon is not a Float64 value"));
        assert_eq!(
            check_windows(
                &schema,
                &spec(window(
                    WindowFunction::Lead(1),
                    Some(0),
                    WindowFrame::Partition
                ))
            )
            .unwrap(),
            vec![DataType::Utf8]
        );
    }

    #[test]
    fn stops_when_cancelled() {
        let source = source(&["a", "b"], &[1, 2], &[Some(1.0), Some(2.0)]);
        let spec = WindowSpec {
            partition_by: vec![0],
            order_by: Vec::new(),
            windows: vec![window(
                WindowFunction::Aggregate(AggregateFunction::Sum),
                Some(2),
                WindowFrame::Partition,
            )],
        };
        let error = compute_windows(&source, None, &[0, 1, 2], &spec, &["w".to_owned()], &|_| {
            true
        })
        .err()
        .unwrap();
        assert!(error.is::<crate::Cancelled>());
    }
}
