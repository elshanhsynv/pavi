//! Calendar arithmetic for SQL calculations (ADR 0023): `DATE_TRUNC`,
//! `DATE_DIFF`, `± INTERVAL` and `NOW()`. Dates and timestamps are worked on
//! as wall-clock times in the column's time zone (UTC for `NOW()`), so a day
//! or month starts where people expect it to.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{Array, ArrayRef, Int64Array, TimestampMicrosecondArray, timezone::Tz};
use arrow_schema::{DataType, TimeUnit};
use chrono::{
    DateTime, Datelike, Duration, Months, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
};

/// A calendar unit for `DATE_TRUNC` and `DATE_DIFF`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DateUnit {
    Year,
    Quarter,
    Month,
    /// Weeks start on Monday, as ISO 8601 counts.
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

impl DateUnit {
    /// `'month'`, `MONTHS`, `mon`… as people write them, any case.
    pub fn parse(text: &str) -> Result<Self> {
        let word = text.trim().to_ascii_lowercase();
        let word = word.strip_suffix('s').unwrap_or(&word);
        Ok(match word {
            "year" | "yr" | "y" => Self::Year,
            "quarter" | "q" => Self::Quarter,
            "month" | "mon" => Self::Month,
            "week" | "w" => Self::Week,
            "day" | "d" => Self::Day,
            "hour" | "hr" | "h" => Self::Hour,
            "minute" | "min" | "m" => Self::Minute,
            "second" | "sec" => Self::Second,
            _ => bail!(
                "{text:?} is not a date unit; use year, quarter, month, week, day, hour, \
                 minute or second"
            ),
        })
    }
}

/// A span of calendar time for `± INTERVAL`: months and days are calendar
/// steps (a month from January 31 is the end of February), the rest exact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub millis: i64,
}

impl Interval {
    /// Parses `'7 days'`, `'1 month 2 days'`, `'90 minutes'`, or a number with
    /// `unit` given apart (`INTERVAL 7 DAY`).
    pub fn parse(text: &str, unit: Option<&str>) -> Result<Self> {
        let mut interval = Self {
            months: 0,
            days: 0,
            millis: 0,
        };
        let words = text.split_whitespace().collect::<Vec<_>>();
        let pairs: Vec<(&str, &str)> = match (unit, words.as_slice()) {
            (Some(unit), [amount]) => vec![(amount, unit)],
            (None, words) if !words.is_empty() && words.len() % 2 == 0 => {
                words.chunks(2).map(|pair| (pair[0], pair[1])).collect()
            }
            _ => bail!("an interval is written like INTERVAL '7 days' or INTERVAL 7 DAY"),
        };
        for (amount, unit) in pairs {
            let amount: i64 = amount
                .parse()
                .map_err(|_| anyhow!("{amount:?} in an interval is not a whole number"))?;
            let small = |value: i64| i32::try_from(value).context("the interval is too long");
            match DateUnit::parse(unit)? {
                DateUnit::Year => interval.months += small(amount.saturating_mul(12))?,
                DateUnit::Quarter => interval.months += small(amount.saturating_mul(3))?,
                DateUnit::Month => interval.months += small(amount)?,
                DateUnit::Week => interval.days += small(amount.saturating_mul(7))?,
                DateUnit::Day => interval.days += small(amount)?,
                DateUnit::Hour => interval.millis += amount.saturating_mul(3_600_000),
                DateUnit::Minute => interval.millis += amount.saturating_mul(60_000),
                DateUnit::Second => interval.millis += amount.saturating_mul(1_000),
            }
        }
        Ok(interval)
    }

    pub fn negated(self) -> Self {
        Self {
            months: -self.months,
            days: -self.days,
            millis: -self.millis,
        }
    }

    /// Whether it moves the time of day, not only the date.
    pub fn has_time(self) -> bool {
        self.millis != 0
    }
}

/// Whether a value of this type is a date or timestamp these functions take.
pub fn is_date_or_time(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// The type `DATE_TRUNC` gives: a date stays a date, a timestamp keeps its
/// unit and zone. A `Date64` is read as a timestamp (it holds milliseconds).
pub fn trunc_type(input: &DataType) -> DataType {
    match input {
        DataType::Date64 => DataType::Timestamp(TimeUnit::Millisecond, None),
        other => other.clone(),
    }
}

/// The type `± INTERVAL` gives: a date stays a date unless the interval has
/// hours, minutes or seconds, which make it a timestamp.
pub fn shift_type(input: &DataType, interval: Interval) -> DataType {
    match input {
        DataType::Date32 if interval.has_time() => DataType::Timestamp(TimeUnit::Millisecond, None),
        other => trunc_type(other),
    }
}

/// Wall-clock times of a date or timestamp array, and how to go back.
struct Clock {
    zone: Option<Tz>,
}

impl Clock {
    fn for_type(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            zone: match data_type {
                DataType::Timestamp(_, Some(zone)) => Some(
                    zone.parse::<Tz>()
                        .map_err(|error| anyhow!("time zone {zone}: {error}"))?,
                ),
                _ => None,
            },
        })
    }

    /// Each value as a wall-clock time.
    fn local(&self, array: &ArrayRef) -> Result<Vec<Option<NaiveDateTime>>> {
        let target = DataType::Timestamp(TimeUnit::Microsecond, None);
        let micros = arrow_cast::cast(array, &target)?;
        let micros = micros
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .context("expected microsecond timestamps")?;
        Ok(micros
            .iter()
            .map(|value| {
                let instant = DateTime::from_timestamp_micros(value?)?;
                Some(match &self.zone {
                    Some(zone) => instant.with_timezone(zone).naive_local(),
                    None => instant.naive_utc(),
                })
            })
            .collect())
    }

    /// Wall-clock times back as an array of `target`.
    fn array(&self, values: Vec<Option<NaiveDateTime>>, target: &DataType) -> Result<ArrayRef> {
        let micros = values
            .into_iter()
            .map(|value| {
                let value = value?;
                Some(match &self.zone {
                    // A time skipped by a clock change moves to the next valid one.
                    Some(zone) => zone
                        .from_local_datetime(&value)
                        .earliest()
                        .unwrap_or_else(|| Utc.from_utc_datetime(&value).with_timezone(zone))
                        .timestamp_micros(),
                    None => value.and_utc().timestamp_micros(),
                })
            })
            .collect::<TimestampMicrosecondArray>();
        let zone = match target {
            DataType::Timestamp(_, zone) => zone.clone(),
            _ => None,
        };
        let micros: ArrayRef = Arc::new(micros.with_timezone_opt(zone));
        Ok(arrow_cast::cast(&micros, target)?)
    }
}

/// The start of the `unit` a wall-clock time is in.
fn truncate(value: NaiveDateTime, unit: DateUnit) -> Option<NaiveDateTime> {
    let date = value.date();
    let day = |date: NaiveDate| date.and_hms_opt(0, 0, 0);
    match unit {
        DateUnit::Year => day(NaiveDate::from_ymd_opt(date.year(), 1, 1)?),
        DateUnit::Quarter => day(NaiveDate::from_ymd_opt(
            date.year(),
            (date.month() - 1) / 3 * 3 + 1,
            1,
        )?),
        DateUnit::Month => day(date.with_day(1)?),
        DateUnit::Week => {
            day(date - Duration::days(i64::from(date.weekday().num_days_from_monday())))
        }
        DateUnit::Day => day(date),
        DateUnit::Hour => date.and_hms_opt(value.hour(), 0, 0),
        DateUnit::Minute => date.and_hms_opt(value.hour(), value.minute(), 0),
        DateUnit::Second => date.and_hms_opt(value.hour(), value.minute(), value.second()),
    }
}

/// `DATE_TRUNC(unit, values)`, as `result`.
pub fn date_trunc(unit: DateUnit, values: &ArrayRef, result: &DataType) -> Result<ArrayRef> {
    let clock = Clock::for_type(values.data_type())?;
    let local = clock
        .local(values)?
        .into_iter()
        .map(|value| value.and_then(|value| truncate(value, unit)))
        .collect();
    clock.array(local, result)
}

/// `DATE_DIFF(unit, start, end)`: how many `unit` boundaries lie from `start`
/// to `end` (negative when `end` is earlier), counted on wall-clock times.
pub fn date_diff(unit: DateUnit, start: &ArrayRef, end: &ArrayRef) -> Result<ArrayRef> {
    let starts = Clock::for_type(start.data_type())?.local(start)?;
    let ends = Clock::for_type(end.data_type())?.local(end)?;
    let months = |value: NaiveDateTime| i64::from(value.year()) * 12 + i64::from(value.month0());
    Ok(Arc::new(
        starts
            .into_iter()
            .zip(ends)
            .map(|(start, end)| {
                let (start, end) = (start?, end?);
                let (from, to) = (truncate(start, unit)?, truncate(end, unit)?);
                Some(match unit {
                    DateUnit::Year => i64::from(to.year() - from.year()),
                    DateUnit::Quarter => months(to) / 3 - months(from) / 3,
                    DateUnit::Month => months(to) - months(from),
                    DateUnit::Week => (to - from).num_days() / 7,
                    DateUnit::Day => (to - from).num_days(),
                    DateUnit::Hour => (to - from).num_hours(),
                    DateUnit::Minute => (to - from).num_minutes(),
                    DateUnit::Second => (to - from).num_seconds(),
                })
            })
            .collect::<Int64Array>(),
    ))
}

/// `values + interval`, as `result`. Months first (clamped to the month's
/// last day), then days, then the time.
pub fn shift(interval: Interval, values: &ArrayRef, result: &DataType) -> Result<ArrayRef> {
    let clock = Clock::for_type(values.data_type())?;
    let moved = clock
        .local(values)?
        .into_iter()
        .map(|value| {
            let value = value?;
            let value = match interval.months {
                0 => value,
                months if months > 0 => {
                    value.checked_add_months(Months::new(months.unsigned_abs()))?
                }
                months => value.checked_sub_months(Months::new(months.unsigned_abs()))?,
            };
            value
                .checked_add_signed(Duration::days(i64::from(interval.days)))?
                .checked_add_signed(Duration::milliseconds(interval.millis))
        })
        .collect();
    clock.array(moved, result)
}

/// The current time, in microseconds since 1970 (UTC), for `NOW()`.
pub fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
        })
}

/// Today's date in UTC, in days since 1970, for `CURRENT_DATE`.
pub fn today_days() -> i32 {
    i32::try_from(now_micros().div_euclid(86_400_000_000)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use arrow_array::{Date32Array, TimestampMillisecondArray};

    use super::*;

    fn texts(array: &ArrayRef) -> Vec<String> {
        (0..array.len())
            .map(|row| parquet_reader::value::format_cell(array.as_ref(), row))
            .collect()
    }

    #[test]
    fn reads_units_and_intervals_as_people_write_them() {
        assert_eq!(
            DateUnit::parse("'MONTHS'".trim_matches('\'')).unwrap(),
            DateUnit::Month
        );
        assert_eq!(DateUnit::parse(" Week ").unwrap(), DateUnit::Week);
        assert!(DateUnit::parse("fortnight").is_err());
        assert_eq!(
            Interval::parse("1 month 2 days", None).unwrap(),
            Interval {
                months: 1,
                days: 2,
                millis: 0
            }
        );
        assert_eq!(
            Interval::parse("90", Some("MINUTE")).unwrap().millis,
            5_400_000
        );
        assert_eq!(Interval::parse("2 years", None).unwrap().months, 24);
        assert!(Interval::parse("a while", None).is_err());
    }

    #[test]
    fn truncates_diffs_and_shifts_dates_and_timestamps() {
        // 2024-02-29 and 2024-05-15.
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(19_782), Some(19_858), None]));
        let month = date_trunc(DateUnit::Month, &dates, &DataType::Date32).unwrap();
        assert_eq!(texts(&month), ["2024-02-01", "2024-05-01", "null"]);
        let quarter = date_trunc(DateUnit::Quarter, &dates, &DataType::Date32).unwrap();
        assert_eq!(texts(&quarter), ["2024-01-01", "2024-04-01", "null"]);
        // 2024-02-29 was a Thursday; its week starts on Monday the 26th.
        let week = date_trunc(DateUnit::Week, &dates, &DataType::Date32).unwrap();
        assert_eq!(texts(&week)[0], "2024-02-26");

        // A month after January 31 is the last day of February.
        let january: ArrayRef = Arc::new(Date32Array::from(vec![19_753]));
        let next = shift(
            Interval::parse("1 month", None).unwrap(),
            &january,
            &DataType::Date32,
        )
        .unwrap();
        assert_eq!(texts(&next), ["2024-02-29"]);
        let hours = Interval::parse("36", Some("hour")).unwrap();
        let later = shift(hours, &january, &shift_type(&DataType::Date32, hours)).unwrap();
        assert_eq!(texts(&later), ["2024-02-01T12:00:00"]);

        let januaries: ArrayRef = Arc::new(Date32Array::from(vec![19_753; 3]));
        let diff = date_diff(DateUnit::Month, &januaries, &dates).unwrap();
        assert_eq!(texts(&diff), ["1", "4", "null"]);
        let days = date_diff(DateUnit::Day, &dates, &januaries).unwrap();
        assert_eq!(texts(&days)[0], "-29");

        // In a time zone, a day starts at local midnight: 2024-03-10 23:30
        // in New York is 03:30 UTC on the 11th.
        let zone = DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into()));
        let evening: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(vec![1_710_127_800_000])
                .with_timezone("America/New_York"),
        );
        let day = date_trunc(DateUnit::Day, &evening, &zone).unwrap();
        assert_eq!(texts(&day), ["2024-03-10T00:00:00-05:00"]);
    }
}
