//! Comparing two open tables: their schemas side by side, and SQL that finds
//! the rows whose key is in one table only. The rows themselves are found by
//! the SQL workspace (an anti join, ADR 0009), so nothing here reads data.

use arrow_schema::DataType;
use parquet_reader::types::is_comparable;

use crate::sql_editor::quote_column;

/// The Compare window: which other tab, and which column identifies a row.
#[derive(Default)]
pub(crate) struct CompareState {
    pub(crate) visible: bool,
    /// The other tab's SQL table name.
    pub(crate) other: Option<String>,
    /// A column both tables have, with the same type.
    pub(crate) key: Option<String>,
}

/// How two schemas differ. Columns are matched by name.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct SchemaDiff {
    /// Columns only the open table has.
    pub(crate) only_left: Vec<(String, DataType)>,
    /// Columns only the other table has.
    pub(crate) only_right: Vec<(String, DataType)>,
    /// Columns both have, with different types: name, left type, right type.
    pub(crate) type_differs: Vec<(String, DataType, DataType)>,
    /// Columns both have with the same type, in the open table's order.
    pub(crate) same: Vec<(String, DataType)>,
}

impl SchemaDiff {
    pub(crate) fn new(left: &[(String, DataType)], right: &[(String, DataType)]) -> Self {
        let mut diff = Self::default();
        for (name, data_type) in left {
            match right.iter().find(|(other, _)| other == name) {
                None => diff.only_left.push((name.clone(), data_type.clone())),
                Some((_, other)) if other == data_type => {
                    diff.same.push((name.clone(), data_type.clone()));
                }
                Some((_, other)) => {
                    diff.type_differs
                        .push((name.clone(), data_type.clone(), other.clone()));
                }
            }
        }
        for (name, data_type) in right {
            if !left.iter().any(|(other, _)| other == name) {
                diff.only_right.push((name.clone(), data_type.clone()));
            }
        }
        diff
    }

    /// Whether the two schemas are the same, column order aside.
    pub(crate) fn identical(&self) -> bool {
        self.only_left.is_empty() && self.only_right.is_empty() && self.type_differs.is_empty()
    }

    /// Columns that can identify rows in both tables: same name, same type,
    /// and a type SQL can compare.
    pub(crate) fn key_candidates(&self) -> Vec<&str> {
        self.same
            .iter()
            .filter(|(_, data_type)| is_comparable(data_type))
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

/// Which rows a comparison query finds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum KeyMatch {
    /// Rows of the open table whose key the other table lacks.
    OnlyLeft,
    /// Rows of the other table whose key the open table lacks.
    OnlyRight,
    /// Rows of the open table whose key the other table has too.
    Both,
}

/// SQL for the rows `matching` names, keyed by `key`. `other` is the other
/// tab's table name; the open table is `dataset`.
pub(crate) fn key_query(key: &str, other: &str, matching: KeyMatch) -> String {
    let key = quote_column(key);
    let (from, to, operator) = match matching {
        KeyMatch::OnlyLeft => ("dataset", other, "NOT IN"),
        KeyMatch::OnlyRight => (other, "dataset", "NOT IN"),
        KeyMatch::Both => ("dataset", other, "IN"),
    };
    format!("SELECT * FROM {from} WHERE {key} {operator} (SELECT {key} FROM {to})")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(list: &[(&str, DataType)]) -> Vec<(String, DataType)> {
        list.iter()
            .map(|(name, data_type)| ((*name).to_owned(), data_type.clone()))
            .collect()
    }

    #[test]
    fn diffs_schemas_by_name() {
        let left = columns(&[
            ("id", DataType::Int64),
            ("price", DataType::Float64),
            ("old", DataType::Utf8),
            ("tags", DataType::new_list(DataType::Utf8, true)),
        ]);
        let right = columns(&[
            ("price", DataType::Decimal128(10, 2)),
            ("id", DataType::Int64),
            ("new", DataType::Boolean),
            ("tags", DataType::new_list(DataType::Utf8, true)),
        ]);
        let diff = SchemaDiff::new(&left, &right);
        assert_eq!(diff.only_left, columns(&[("old", DataType::Utf8)]));
        assert_eq!(diff.only_right, columns(&[("new", DataType::Boolean)]));
        assert_eq!(
            diff.type_differs,
            vec![(
                "price".to_owned(),
                DataType::Float64,
                DataType::Decimal128(10, 2)
            )]
        );
        assert_eq!(diff.same.len(), 2);
        assert!(!diff.identical());
        // A list column is the same in both, but it can't be a key.
        assert_eq!(diff.key_candidates(), vec!["id"]);

        let same = SchemaDiff::new(&left, &left);
        assert!(same.identical());
    }

    #[test]
    fn writes_key_queries() {
        assert_eq!(
            key_query("id", "flights_2", KeyMatch::OnlyLeft),
            "SELECT * FROM dataset WHERE id NOT IN (SELECT id FROM flights_2)"
        );
        assert_eq!(
            key_query("trip id", "trips", KeyMatch::OnlyRight),
            "SELECT * FROM trips WHERE \"trip id\" NOT IN (SELECT \"trip id\" FROM dataset)"
        );
        assert_eq!(
            key_query("select", "t", KeyMatch::Both),
            "SELECT * FROM dataset WHERE \"select\" IN (SELECT \"select\" FROM t)"
        );
    }
}
