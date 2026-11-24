//! Import options for CSV and JSON files chosen in the app (ADR 0020): what
//! the user chose, how it is kept in the session, and the dialog's state.

use std::path::{Path, PathBuf};

use parquet_reader::{FileFormat, ImportOptions, TextEncoding};

use crate::session::SavedImport;

/// How the user chose to read a file: a format other than its extension's,
/// and options for CSV and JSON.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ImportChoice {
    pub(crate) format: Option<FileFormat>,
    pub(crate) options: ImportOptions,
}

impl ImportChoice {
    /// Nothing chosen: every guess is made as usual.
    pub(crate) fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// As the session keeps it.
    pub(crate) fn to_saved(&self, path: &Path) -> SavedImport {
        SavedImport {
            path: path.to_path_buf(),
            format: match self.format {
                Some(FileFormat::Csv) => "csv",
                Some(FileFormat::Json) => "json",
                Some(FileFormat::Excel) => "excel",
                _ => "",
            }
            .to_string(),
            delimiter: self
                .options
                .delimiter
                .map(|byte| char::from(byte).to_string())
                .unwrap_or_default(),
            no_header: !self.options.header,
            encoding: self
                .options
                .encoding
                .map(|encoding| encoding.name().to_string())
                .unwrap_or_default(),
            all_text: self.options.all_text,
            sheet: self.options.sheet.clone().unwrap_or_default(),
        }
    }

    /// From the session; anything it can't read is left to be guessed.
    pub(crate) fn from_saved(saved: &SavedImport) -> Self {
        let delimiter = match saved.delimiter.as_bytes() {
            [byte] if byte.is_ascii() && *byte != b'\n' && *byte != b'"' => Some(*byte),
            _ => None,
        };
        Self {
            format: match saved.format.as_str() {
                "csv" => Some(FileFormat::Csv),
                "json" => Some(FileFormat::Json),
                "excel" => Some(FileFormat::Excel),
                _ => None,
            },
            options: ImportOptions {
                delimiter,
                header: !saved.no_header,
                encoding: TextEncoding::parse(&saved.encoding),
                all_text: saved.all_text,
                sheet: (!saved.sheet.is_empty()).then(|| saved.sheet.clone()),
            },
        }
    }
}

/// The delimiter picked in the dialog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DelimiterChoice {
    Guess,
    Comma,
    Semicolon,
    Tab,
    Pipe,
    /// The character typed in the box next to it.
    Other,
}

impl DelimiterChoice {
    pub(crate) const ALL: [Self; 6] = [
        Self::Guess,
        Self::Comma,
        Self::Semicolon,
        Self::Tab,
        Self::Pipe,
        Self::Other,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Guess => "Guess",
            Self::Comma => "Comma ,",
            Self::Semicolon => "Semicolon ;",
            Self::Tab => "Tab",
            Self::Pipe => "Pipe |",
            Self::Other => "Other",
        }
    }
}

/// The Import options window: which file, and the choices being edited.
#[derive(Clone, Debug)]
pub(crate) struct ImportDialog {
    pub(crate) path: PathBuf,
    /// `None` goes by the extension.
    pub(crate) format: Option<FileFormat>,
    pub(crate) delimiter: DelimiterChoice,
    pub(crate) other: String,
    pub(crate) header: bool,
    /// `None` detects it.
    pub(crate) encoding: Option<TextEncoding>,
    pub(crate) all_text: bool,
    /// Excel: the sheet's name; empty for the first.
    pub(crate) sheet: String,
    /// Excel: the workbook's sheets, when it has been read.
    pub(crate) sheets: Vec<String>,
    /// How the file was read last, when it is open (`ImportReport::summary`).
    pub(crate) read_as: Option<String>,
    /// Columns retyped after a late value, last time it was read.
    pub(crate) retyped: Vec<String>,
    pub(crate) error: Option<String>,
}

impl ImportDialog {
    pub(crate) fn new(path: PathBuf, choice: ImportChoice) -> Self {
        let (delimiter, other) = match choice.options.delimiter {
            None => (DelimiterChoice::Guess, String::new()),
            Some(b',') => (DelimiterChoice::Comma, String::new()),
            Some(b';') => (DelimiterChoice::Semicolon, String::new()),
            Some(b'\t') => (DelimiterChoice::Tab, String::new()),
            Some(b'|') => (DelimiterChoice::Pipe, String::new()),
            Some(byte) => (DelimiterChoice::Other, char::from(byte).to_string()),
        };
        Self {
            path,
            format: choice.format,
            delimiter,
            other,
            header: choice.options.header,
            encoding: choice.options.encoding,
            all_text: choice.options.all_text,
            sheet: choice.options.sheet.clone().unwrap_or_default(),
            sheets: Vec::new(),
            read_as: None,
            retyped: Vec::new(),
            error: None,
        }
    }

    /// The format the file will be read as.
    pub(crate) fn effective_format(&self) -> FileFormat {
        self.format
            .unwrap_or_else(|| FileFormat::from_path(&self.path))
    }

    /// The choice the dialog shows, or why it can't be used.
    pub(crate) fn choice(&self) -> Result<ImportChoice, String> {
        let delimiter = match self.delimiter {
            DelimiterChoice::Guess => None,
            DelimiterChoice::Comma => Some(b','),
            DelimiterChoice::Semicolon => Some(b';'),
            DelimiterChoice::Tab => Some(b'\t'),
            DelimiterChoice::Pipe => Some(b'|'),
            DelimiterChoice::Other => match self.other.as_bytes() {
                [byte]
                    if byte.is_ascii()
                        && !byte.is_ascii_alphanumeric()
                        && *byte != b'"'
                        && *byte != b'\n' =>
                {
                    Some(*byte)
                }
                _ => {
                    return Err(
                        "Type one character between fields, such as : or ~ (not a letter, \
                         digit or quote)"
                            .to_string(),
                    );
                }
            },
        };
        Ok(ImportChoice {
            format: self.format,
            options: ImportOptions {
                delimiter,
                header: self.header,
                encoding: self.encoding,
                all_text: self.all_text,
                sheet: (!self.sheet.trim().is_empty()).then(|| self.sheet.trim().to_string()),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_a_choice_in_the_session_and_reads_it_back() {
        let choice = ImportChoice {
            format: Some(FileFormat::Csv),
            options: ImportOptions {
                delimiter: Some(b'\t'),
                header: false,
                encoding: Some(TextEncoding::Utf16Le),
                all_text: true,
                sheet: Some("Q3".to_string()),
            },
        };
        let saved = choice.to_saved(Path::new("a.txt"));
        assert_eq!(saved.encoding, "UTF-16 LE");
        assert_eq!(ImportChoice::from_saved(&saved), choice);
        assert!(ImportChoice::default().is_default());
        // Values from another version read as guesses.
        let odd = SavedImport {
            format: "xlsx".to_string(),
            delimiter: "ab".to_string(),
            encoding: "ebcdic".to_string(),
            ..SavedImport::default()
        };
        assert!(ImportChoice::from_saved(&odd).is_default());
    }

    #[test]
    fn the_dialog_checks_the_delimiter_typed() {
        let mut dialog = ImportDialog::new(
            PathBuf::from("a.csv"),
            ImportChoice {
                format: None,
                options: ImportOptions {
                    delimiter: Some(b':'),
                    ..ImportOptions::default()
                },
            },
        );
        assert_eq!(dialog.delimiter, DelimiterChoice::Other);
        assert_eq!(dialog.choice().unwrap().options.delimiter, Some(b':'));
        dialog.other = "ab".to_string();
        assert!(dialog.choice().is_err());
        dialog.delimiter = DelimiterChoice::Tab;
        assert_eq!(dialog.choice().unwrap().options.delimiter, Some(b'\t'));
        assert_eq!(dialog.effective_format(), FileFormat::Csv);
    }
}
