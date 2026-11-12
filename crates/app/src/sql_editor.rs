//! What the SQL editor needs to know about the text: tokens for highlighting and
//! completions for the word at the cursor. Pure functions, so they are tested
//! without a window; `ui/sql.rs` draws them.

use std::ops::Range;

/// Most completions listed at once.
pub(crate) const MAX_SUGGESTIONS: usize = 8;

/// Words the SQL subset uses, offered as completions and highlighted.
const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "AND", "OR", "NOT", "IN",
    "BETWEEN", "LIKE", "IS", "NULL", "ASC", "DESC", "NULLS", "FIRST", "LAST", "TRUE", "FALSE",
    "JOIN", "INNER", "LEFT", "OUTER", "ON", "AS",
];
/// Aggregate functions, highlighted apart from keywords.
const FUNCTIONS: &[&str] = &["COUNT", "SUM", "AVG", "MIN", "MAX"];
/// The tab running the query, always a table name.
const TABLE: &str = "dataset";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TokenKind {
    Keyword,
    Function,
    /// A name of the open file's columns, plain or quoted.
    Column,
    /// `dataset` or another open tab's table name.
    Table,
    /// `'text'`.
    String,
    Number,
    /// `-- to the end of the line`.
    Comment,
    /// Anything else: spaces, operators, unknown names.
    Plain,
}

/// Splits `text` into byte ranges and their kinds, covering all of it in order.
pub(crate) fn tokens(
    text: &str,
    columns: &[String],
    tables: &[String],
) -> Vec<(Range<usize>, TokenKind)> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let start = at;
        let kind = match bytes[at] {
            b'-' if bytes.get(at + 1) == Some(&b'-') => {
                at = text[at..].find('\n').map_or(bytes.len(), |end| at + end);
                TokenKind::Comment
            }
            quote @ (b'\'' | b'"') => {
                at += 1;
                // A doubled quote inside is an escaped quote.
                loop {
                    match bytes.get(at) {
                        None => break,
                        Some(byte) if *byte == quote => {
                            at += 1;
                            if bytes.get(at) != Some(&quote) {
                                break;
                            }
                            at += 1;
                        }
                        Some(_) => at += 1,
                    }
                }
                if quote == b'\'' {
                    TokenKind::String
                } else {
                    let name = text[start + 1..at]
                        .trim_end_matches('"')
                        .replace("\"\"", "\"");
                    if columns.contains(&name) {
                        TokenKind::Column
                    } else {
                        TokenKind::Plain
                    }
                }
            }
            byte if byte.is_ascii_digit() => {
                at += 1;
                while bytes
                    .get(at)
                    .is_some_and(|byte| byte.is_ascii_digit() || *byte == b'.')
                {
                    at += 1;
                }
                TokenKind::Number
            }
            byte if is_word_byte(byte) => {
                while bytes.get(at).copied().is_some_and(is_word_byte) {
                    at += 1;
                }
                word_kind(&text[start..at], columns, tables)
            }
            _ => {
                // Up to the next character that could start another token.
                at += text[at..].chars().next().map_or(1, char::len_utf8);
                while at < bytes.len()
                    && !matches!(bytes[at], b'\'' | b'"' | b'-')
                    && !is_word_byte(bytes[at])
                {
                    at += text[at..].chars().next().map_or(1, char::len_utf8);
                }
                TokenKind::Plain
            }
        };
        tokens.push((start..at, kind));
    }
    tokens
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
}

fn word_kind(word: &str, columns: &[String], tables: &[String]) -> TokenKind {
    // A column wins: a file may well have a column called `count` or `name`.
    if columns
        .iter()
        .any(|column| column.eq_ignore_ascii_case(word))
    {
        TokenKind::Column
    } else if KEYWORDS
        .iter()
        .any(|keyword| keyword.eq_ignore_ascii_case(word))
    {
        TokenKind::Keyword
    } else if FUNCTIONS
        .iter()
        .any(|function| function.eq_ignore_ascii_case(word))
    {
        TokenKind::Function
    } else if word.eq_ignore_ascii_case(TABLE)
        || tables.iter().any(|table| table.eq_ignore_ascii_case(word))
    {
        TokenKind::Table
    } else {
        TokenKind::Plain
    }
}

/// One completion: the text that replaces the word being typed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Suggestion {
    pub(crate) insert: String,
    pub(crate) kind: TokenKind,
}

/// The word being typed: where it starts (in characters) and what it is so far.
/// `None` inside a string or comment, or when the cursor is not right after a word.
pub(crate) fn word_before(text: &str, cursor: usize) -> Option<(usize, String)> {
    let end = text
        .char_indices()
        .nth(cursor)
        .map_or(text.len(), |(at, _)| at);
    let in_literal = tokens(&text[..end], &[], &[])
        .last()
        .is_some_and(|(range, kind)| {
            range.end == end && matches!(kind, TokenKind::String | TokenKind::Comment)
        });
    if in_literal {
        return None;
    }
    let start = text[..end]
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_alphanumeric() || *c == '_')
        .last()
        .map(|(at, _)| at)?;
    let word = &text[start..end];
    if word.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    Some((text[..start].chars().count(), word.to_owned()))
}

/// Columns, keywords, functions and table names that start with `prefix`
/// (ignoring case), columns first. Then columns that only contain it. A word
/// already typed out in full is not offered.
pub(crate) fn suggestions(prefix: &str, columns: &[String], tables: &[String]) -> Vec<Suggestion> {
    if prefix.is_empty() {
        return Vec::new();
    }
    let lower = prefix.to_lowercase();
    let starts = |name: &str| name.to_lowercase().starts_with(&lower) && name.len() > prefix.len();
    let mut found = Vec::new();
    for column in columns.iter().filter(|column| starts(column)) {
        found.push(Suggestion {
            insert: quote_column(column),
            kind: TokenKind::Column,
        });
    }
    for (words, kind) in [
        (KEYWORDS, TokenKind::Keyword),
        (FUNCTIONS, TokenKind::Function),
        (&[TABLE][..], TokenKind::Table),
    ] {
        for word in words.iter().filter(|word| starts(word)) {
            found.push(Suggestion {
                insert: (*word).to_owned(),
                kind,
            });
        }
    }
    for table in tables.iter().filter(|table| starts(table)) {
        found.push(Suggestion {
            insert: table.clone(),
            kind: TokenKind::Table,
        });
    }
    for column in columns.iter().filter(|column| {
        !starts(column)
            && !column.eq_ignore_ascii_case(prefix)
            && column.to_lowercase().contains(&lower)
    }) {
        found.push(Suggestion {
            insert: quote_column(column),
            kind: TokenKind::Column,
        });
    }
    found.truncate(MAX_SUGGESTIONS);
    found
}

/// A column name as SQL needs it: in double quotes when it is not a plain word
/// or could be read as a keyword.
pub(crate) fn quote_column(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !KEYWORDS
            .iter()
            .chain(FUNCTIONS)
            .any(|word| word.eq_ignore_ascii_case(name));
    if plain {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// `text` with the characters `start..cursor` replaced by `insert`, and the
/// cursor (in characters) right after it.
pub(crate) fn complete(text: &str, start: usize, cursor: usize, insert: &str) -> (String, usize) {
    let byte = |chars: usize| {
        text.char_indices()
            .nth(chars)
            .map_or(text.len(), |(at, _)| at)
    };
    let mut result = String::with_capacity(text.len() + insert.len());
    result.push_str(&text[..byte(start)]);
    result.push_str(insert);
    result.push_str(&text[byte(cursor)..]);
    (result, start + insert.chars().count())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Vec<String> {
        ["DEP_DELAY", "DEP_TIME", "dest city", "count", "id"]
            .map(String::from)
            .to_vec()
    }

    #[test]
    fn tokenizes_the_sql_subset() {
        let text = "SELECT DEP_DELAY, COUNT(*) FROM dataset -- busy\nWHERE \"dest city\" = 'it''s' AND id >= 12.5";
        let kinds = tokens(text, &columns(), &[])
            .into_iter()
            .filter(|(_, kind)| *kind != TokenKind::Plain)
            .map(|(range, kind)| (&text[range], kind))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                ("SELECT", TokenKind::Keyword),
                ("DEP_DELAY", TokenKind::Column),
                // A column named `count` wins over the function.
                ("COUNT", TokenKind::Column),
                ("FROM", TokenKind::Keyword),
                ("dataset", TokenKind::Table),
                ("-- busy", TokenKind::Comment),
                ("WHERE", TokenKind::Keyword),
                ("\"dest city\"", TokenKind::Column),
                ("'it''s'", TokenKind::String),
                ("AND", TokenKind::Keyword),
                ("id", TokenKind::Column),
                ("12.5", TokenKind::Number),
            ]
        );
        // Tokens cover the text exactly, also with unfinished quotes and non-ASCII text.
        for text in ["SELECT 'open", "x = \"unclosed", "é -- ü", ""] {
            let ranges = tokens(text, &columns(), &[]);
            let mut at = 0;
            for (range, _) in ranges {
                assert_eq!(range.start, at, "{text}");
                at = range.end;
            }
            assert_eq!(at, text.len(), "{text}");
        }
    }

    #[test]
    fn finds_the_word_at_the_cursor() {
        assert_eq!(word_before("SELECT DEP_D", 12), Some((7, "DEP_D".into())));
        assert_eq!(
            word_before("SELECT DEP_D FROM", 12),
            Some((7, "DEP_D".into()))
        );
        assert_eq!(word_before("SELECT ", 7), None, "nothing typed yet");
        assert_eq!(word_before("x = 'DEP", 8), None, "inside a string");
        assert_eq!(word_before("-- DEP", 6), None, "inside a comment");
        assert_eq!(word_before("x > 12", 6), None, "a number");
        // Positions are in characters.
        assert_eq!(word_before("é é_x", 5), Some((2, "é_x".into())));
    }

    #[test]
    fn suggests_columns_first_then_words_then_partial_matches() {
        let names = |prefix: &str| {
            suggestions(prefix, &columns(), &[])
                .into_iter()
                .map(|suggestion| suggestion.insert)
                .collect::<Vec<_>>()
        };
        assert_eq!(names("dep"), ["DEP_DELAY", "DEP_TIME"]);
        assert_eq!(
            names("de"),
            ["DEP_DELAY", "DEP_TIME", "\"dest city\"", "DESC"]
        );
        assert_eq!(names("dat"), ["dataset"]);
        // `count` is a column too, so it is quoted; COUNT the function is offered as well.
        assert_eq!(names("cou"), ["\"count\"", "COUNT"]);
        assert_eq!(names("time"), ["DEP_TIME"], "a column containing the text");
        assert_eq!(names("id"), Vec::<String>::new(), "already complete");
        assert!(names("").is_empty());
        let many = (0..20)
            .map(|index| format!("col{index}"))
            .collect::<Vec<_>>();
        assert_eq!(suggestions("col", &many, &[]).len(), MAX_SUGGESTIONS);
    }

    #[test]
    fn quotes_column_names_that_need_it_and_replaces_the_word() {
        assert_eq!(quote_column("DEP_DELAY"), "DEP_DELAY");
        assert_eq!(quote_column("dest city"), "\"dest city\"");
        assert_eq!(quote_column("2020"), "\"2020\"");
        assert_eq!(quote_column("order"), "\"order\"");
        assert_eq!(quote_column("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(
            complete("SELECT dep FROM x", 7, 10, "DEP_DELAY"),
            ("SELECT DEP_DELAY FROM x".to_string(), 16)
        );
        assert_eq!(complete("é de", 2, 4, "DESC"), ("é DESC".to_string(), 6));
    }
}
