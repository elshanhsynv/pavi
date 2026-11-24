use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

pub const SESSION_VERSION: u32 = 1;
pub const MAX_HISTORY_ENTRIES: usize = 50;
pub const MAX_RECENT_FILES: usize = 12;
/// Tabs reopened at start: the files and folders open when the app closed.
pub const MAX_OPEN_FILES: usize = 16;
pub const MAX_QUERY_BYTES: usize = 16 * 1024;
pub const MAX_HISTORY_BYTES: usize = 256 * 1024;
pub const MAX_GRID_LAYOUTS: usize = 8;
pub const MAX_LAYOUT_COLUMNS: usize = 2_048;
/// Saved queries kept in the session file.
pub const MAX_SAVED_QUERIES: usize = 100;
/// Characters of a saved query's name.
pub const MAX_SAVED_NAME_CHARS: usize = 80;
/// Bytes of a tab's filter kept to restore it at start.
pub const MAX_SAVED_FILTER_BYTES: usize = 16 * 1024;
/// Sort keys kept per tab (the app's own sort limit).
pub const MAX_SAVED_SORT_KEYS: usize = 8;
/// Files whose import options the user chose, kept to read them the same way.
pub const MAX_SAVED_IMPORTS: usize = 32;
/// The interface zoom the app allows, as a factor of the normal size.
pub const MIN_ZOOM: f32 = 0.5;
pub const MAX_ZOOM: f32 = 2.0;

/// The colour theme the user picked (ADR 0010).
#[derive(Clone, Copy, Debug, Default, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ThemeChoice {
    #[default]
    Dark,
    Light,
    /// Light or dark as the operating system is set.
    System,
}

impl ThemeChoice {
    /// Whether to draw with the light palette, given the system's theme
    /// (`None` when the platform doesn't say).
    pub fn is_light(self, system_light: Option<bool>) -> bool {
        match self {
            Self::Dark => false,
            Self::Light => true,
            Self::System => system_light.unwrap_or(false),
        }
    }
}

/// An unknown name (from a newer version, or a hand edit) reads as the
/// default instead of discarding the whole session.
impl<'de> Deserialize<'de> for ThemeChoice {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(match name.as_str() {
            "light" => Self::Light,
            "system" => Self::System,
            _ => Self::Dark,
        })
    }
}

/// A SQL query the user saved under a name.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct SavedQuery {
    pub name: String,
    pub sql: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct GridLayoutPreference {
    pub schema_key: String,
    pub order: Vec<String>,
    pub hidden: Vec<String>,
    pub widths: BTreeMap<String, u16>,
    /// Shown columns pinned to the left.
    pub pinned: usize,
    /// Number formats of number columns, by column name.
    pub number_formats: BTreeMap<String, SavedNumberFormat>,
}

/// A number column's format (`parquet_reader::NumberFormat`) as saved.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct SavedNumberFormat {
    pub decimals: Option<u8>,
    pub thousands: bool,
}

/// One sort key of a tab, by column name.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct SavedSortKey {
    pub column: String,
    pub descending: bool,
}

/// The filter and sort a tab showed, restored when the tab reopens at start.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct TabView {
    pub path: PathBuf,
    /// Filter text as the filter box takes it; empty for none.
    pub filter: String,
    pub sort: Vec<SavedSortKey>,
}

impl TabView {
    pub fn is_empty(&self) -> bool {
        self.filter.trim().is_empty() && self.sort.is_empty()
    }
}

/// How the user chose to read a CSV or JSON file (ADR 0020), kept by path so
/// every later open of it (Reload, the recent list, tabs at start) reads it
/// the same way. Text fields, so an unknown value from another version reads
/// as "guess" instead of discarding the session.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct SavedImport {
    pub path: PathBuf,
    /// `csv` or `json`; empty to go by the extension.
    pub format: String,
    /// The delimiter character; empty to guess it.
    pub delimiter: String,
    pub no_header: bool,
    /// An encoding name `TextEncoding::parse` reads; empty to detect it.
    pub encoding: String,
    pub all_text: bool,
    /// Excel: the sheet's name; empty for the first.
    pub sheet: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Preferences {
    pub inspector_visible: bool,
    pub chart_visible: bool,
    pub grid_layouts: Vec<GridLayoutPreference>,
    pub theme: ThemeChoice,
    /// Interface zoom, 1 for the normal size.
    pub zoom: f32,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            inspector_visible: false,
            chart_visible: false,
            grid_layouts: Vec::new(),
            theme: ThemeChoice::default(),
            zoom: 1.0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct QueryHistoryEntry {
    pub sql: String,
    pub timestamp_unix_secs: u64,
    pub duration_ms: u64,
    pub success: bool,
    pub row_count: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SessionState {
    pub version: u32,
    pub recent_files: Vec<PathBuf>,
    pub last_opened_file: Option<PathBuf>,
    /// Files and folders open in tabs, in tab order.
    pub open_files: Vec<PathBuf>,
    /// Position in `open_files` of the tab that was shown.
    pub active_file: usize,
    pub sql_input: String,
    pub query_history: Vec<QueryHistoryEntry>,
    /// Sorted by name, ignoring case.
    pub saved_queries: Vec<SavedQuery>,
    pub preferences: Preferences,
    /// The filter and sort of open tabs that had one, by path.
    pub tab_views: Vec<TabView>,
    /// Import options the user chose, newest first (≤ `MAX_SAVED_IMPORTS`).
    pub imports: Vec<SavedImport>,
    /// When the newest crash report already told about was written (seconds
    /// since 1970), so each crash is reported once.
    pub last_crash_report: u64,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            version: SESSION_VERSION,
            recent_files: Vec::new(),
            last_opened_file: None,
            open_files: Vec::new(),
            active_file: 0,
            sql_input: String::new(),
            query_history: Vec::new(),
            saved_queries: Vec::new(),
            preferences: Preferences {
                inspector_visible: true,
                ..Preferences::default()
            },
            tab_views: Vec::new(),
            imports: Vec::new(),
            last_crash_report: 0,
        }
    }
}

impl SessionState {
    pub fn grid_layout(&self, schema_key: &str) -> Option<&GridLayoutPreference> {
        self.preferences
            .grid_layouts
            .iter()
            .find(|layout| layout.schema_key == schema_key)
    }

    pub fn save_grid_layout(&mut self, layout: GridLayoutPreference) {
        if layout.order.len() > MAX_LAYOUT_COLUMNS {
            return;
        }
        self.preferences
            .grid_layouts
            .retain(|existing| existing.schema_key != layout.schema_key);
        self.preferences.grid_layouts.insert(0, layout);
        self.preferences.grid_layouts.truncate(MAX_GRID_LAYOUTS);
    }

    /// The import options chosen for `path`, if any.
    pub fn saved_import(&self, path: &Path) -> Option<&SavedImport> {
        self.imports.iter().find(|saved| saved.path == path)
    }

    /// Keeps `saved` as the import options of its path, or forgets the
    /// options of `path` when `saved` is `None`. A URL with a query string
    /// (which may hold a token) is never kept.
    pub fn set_saved_import(&mut self, path: &Path, saved: Option<SavedImport>) {
        self.imports.retain(|existing| existing.path != path);
        if let Some(saved) = saved
            && !is_secret_url(path)
        {
            self.imports.insert(0, saved);
            self.imports.truncate(MAX_SAVED_IMPORTS);
        }
    }

    pub fn record_recent_file(&mut self, path: PathBuf) {
        if is_secret_url(&path) {
            return;
        }
        self.recent_files.retain(|existing| existing != &path);
        self.recent_files.insert(0, path.clone());
        self.recent_files.truncate(MAX_RECENT_FILES);
        self.last_opened_file = Some(path);
    }

    pub fn record_query(
        &mut self,
        sql: &str,
        duration_ms: u64,
        success: bool,
        row_count: Option<u64>,
    ) {
        let sql = bounded_text(sql, MAX_QUERY_BYTES);
        if sql.trim().is_empty() {
            return;
        }
        self.query_history.retain(|entry| entry.sql != sql);
        self.query_history.insert(
            0,
            QueryHistoryEntry {
                sql,
                timestamp_unix_secs: now_unix_secs(),
                duration_ms,
                success,
                row_count,
            },
        );
        self.trim_history();
    }

    /// Saves `sql` as `name`, replacing a saved query of the same name (ignoring
    /// case). Returns the name it was saved under, or why it was not saved.
    pub fn save_query(&mut self, name: &str, sql: &str) -> Result<String, String> {
        let name = name
            .trim()
            .chars()
            .take(MAX_SAVED_NAME_CHARS)
            .collect::<String>();
        if name.is_empty() {
            return Err("Give the query a name first".to_owned());
        }
        if sql.trim().is_empty() {
            return Err("There is no query to save".to_owned());
        }
        if sql.len() > MAX_QUERY_BYTES {
            return Err(format!(
                "Queries longer than {} KB cannot be saved",
                MAX_QUERY_BYTES / 1024
            ));
        }
        let existing = self
            .saved_queries
            .iter()
            .position(|saved| saved.name.eq_ignore_ascii_case(&name));
        match existing {
            Some(index) => self.saved_queries[index].sql = sql.to_owned(),
            None if self.saved_queries.len() >= MAX_SAVED_QUERIES => {
                return Err(format!(
                    "At most {MAX_SAVED_QUERIES} queries can be saved; delete one first"
                ));
            }
            None => self.saved_queries.push(SavedQuery {
                name: name.clone(),
                sql: sql.to_owned(),
            }),
        }
        self.sort_saved_queries();
        Ok(name)
    }

    pub fn remove_saved_query(&mut self, index: usize) {
        if index < self.saved_queries.len() {
            self.saved_queries.remove(index);
        }
    }

    fn sort_saved_queries(&mut self) {
        self.saved_queries
            .sort_by_key(|saved| saved.name.to_lowercase());
    }

    pub fn clear_history(&mut self) {
        self.query_history.clear();
    }

    pub fn remove_history(&mut self, index: usize) {
        if index < self.query_history.len() {
            self.query_history.remove(index);
        }
    }

    /// Keeps only paths that may be saved: a URL with a query string may carry
    /// a signature or token, so it is never written to the session file.
    pub fn set_open_files(&mut self, files: Vec<PathBuf>, active: usize) {
        let active_path = files.get(active).cloned();
        self.open_files = files
            .into_iter()
            .filter(|path| !is_secret_url(path))
            .collect();
        self.active_file = active_path
            .and_then(|active| self.open_files.iter().position(|path| *path == active))
            .unwrap_or(0);
    }

    /// Forgets every recent file. Open tabs stay open.
    pub fn clear_recent_files(&mut self) {
        self.recent_files.clear();
        self.last_opened_file = None;
    }

    /// The saved filter and sort of the tab showing `path`.
    pub fn tab_view(&self, path: &Path) -> Option<&TabView> {
        self.tab_views.iter().find(|view| view.path == path)
    }

    /// Keeps `views` (one per open tab that has a filter or sort) for the next
    /// start. Like open files, a URL with a query string is never saved.
    pub fn set_tab_views(&mut self, views: Vec<TabView>) {
        self.tab_views = views
            .into_iter()
            .filter(|view| !view.is_empty() && !is_secret_url(&view.path))
            .collect();
    }

    pub fn remove_recent_file(&mut self, path: &Path) {
        self.recent_files.retain(|existing| existing != path);
        if self.last_opened_file.as_deref() == Some(path) {
            self.last_opened_file = self.recent_files.first().cloned();
        }
    }

    pub fn normalize(&mut self) {
        if self.version != SESSION_VERSION {
            *self = Self::default();
            return;
        }
        // Files and folders (a folder opens as one dataset) that still exist,
        // and URLs (ADR 0008).
        let mut seen = Vec::new();
        self.recent_files.retain(|path| {
            still_there(path) && !seen.iter().any(|existing| existing == path) && {
                seen.push(path.clone());
                true
            }
        });
        self.recent_files.truncate(MAX_RECENT_FILES);
        if self
            .last_opened_file
            .as_ref()
            .is_some_and(|path| !still_there(path))
        {
            self.last_opened_file = None;
        }
        let active = self.open_files.get(self.active_file).cloned();
        let mut seen = Vec::new();
        self.open_files.retain(|path| {
            still_there(path) && !seen.iter().any(|existing| existing == path) && {
                seen.push(path.clone());
                true
            }
        });
        self.open_files.truncate(MAX_OPEN_FILES);
        self.active_file = active
            .and_then(|active| self.open_files.iter().position(|path| *path == active))
            .unwrap_or(0);
        self.sql_input = bounded_text(&self.sql_input, MAX_QUERY_BYTES);
        self.query_history
            .retain(|entry| !entry.sql.trim().is_empty());
        for entry in &mut self.query_history {
            entry.sql = bounded_text(&entry.sql, MAX_QUERY_BYTES);
        }
        self.trim_history();
        self.saved_queries.retain(|saved| {
            !saved.name.trim().is_empty()
                && !saved.sql.trim().is_empty()
                && saved.sql.len() <= MAX_QUERY_BYTES
        });
        for saved in &mut self.saved_queries {
            saved.name = saved.name.chars().take(MAX_SAVED_NAME_CHARS).collect();
        }
        self.saved_queries.truncate(MAX_SAVED_QUERIES);
        self.sort_saved_queries();
        self.preferences.grid_layouts.retain(|layout| {
            !layout.schema_key.is_empty() && layout.order.len() <= MAX_LAYOUT_COLUMNS
        });
        self.preferences.grid_layouts.truncate(MAX_GRID_LAYOUTS);
        for layout in &mut self.preferences.grid_layouts {
            layout
                .number_formats
                .retain(|name, _| layout.order.contains(name));
            for format in layout.number_formats.values_mut() {
                format.decimals = format
                    .decimals
                    .map(|decimals| decimals.min(parquet_reader::MAX_FORMAT_DECIMALS));
            }
        }
        self.preferences.zoom = if self.preferences.zoom.is_finite() {
            self.preferences.zoom.clamp(MIN_ZOOM, MAX_ZOOM)
        } else {
            1.0
        };
        // Only tabs that reopen keep a view.
        let open_files = &self.open_files;
        self.tab_views
            .retain(|view| open_files.contains(&view.path));
        for view in &mut self.tab_views {
            if view.filter.len() > MAX_SAVED_FILTER_BYTES {
                view.filter.clear();
            }
            view.sort.truncate(MAX_SAVED_SORT_KEYS);
        }
        self.tab_views.retain(|view| !view.is_empty());
        self.tab_views.truncate(MAX_OPEN_FILES);
        self.imports.retain(|saved| !is_secret_url(&saved.path));
        self.imports.truncate(MAX_SAVED_IMPORTS);
    }

    fn trim_history(&mut self) {
        self.query_history.truncate(MAX_HISTORY_ENTRIES);
        while self
            .query_history
            .iter()
            .map(|entry| entry.sql.len())
            .sum::<usize>()
            > MAX_HISTORY_BYTES
        {
            self.query_history.pop();
        }
    }
}

/// The folder for the session file on `os` (as `std::env::consts::OS` names
/// it), from the environment `env` reads: `%LOCALAPPDATA%\\PAVI` on Windows,
/// `~/Library/Application Support/PAVI` on macOS, and `$XDG_STATE_HOME/pavi`
/// (by default `~/.local/state/pavi`) elsewhere. `None` when the variables it
/// needs are missing or not absolute paths.
pub fn session_dir(os: &str, env: &dyn Fn(&str) -> Option<std::ffi::OsString>) -> Option<PathBuf> {
    // Absolute as `os` spells it (`C:\…` or `\\server\…` on Windows, `/…`
    // elsewhere), whatever this machine is, so every branch is testable here.
    let absolute = |name: &str| {
        env(name).map(PathBuf::from).filter(|path| {
            let text = path.to_string_lossy();
            if os == "windows" {
                let bytes = text.as_bytes();
                text.starts_with("\\\\")
                    || (bytes.len() >= 3
                        && bytes[0].is_ascii_alphabetic()
                        && bytes[1] == b':'
                        && (bytes[2] == b'\\' || bytes[2] == b'/'))
            } else {
                text.starts_with('/')
            }
        })
    };
    match os {
        "windows" => absolute("LOCALAPPDATA").map(|base| base.join("PAVI")),
        "macos" => absolute("HOME").map(|home| {
            home.join("Library")
                .join("Application Support")
                .join("PAVI")
        }),
        _ => absolute("XDG_STATE_HOME")
            .or_else(|| absolute("HOME").map(|home| home.join(".local").join("state")))
            .map(|base| base.join("pavi")),
    }
}

#[derive(Clone, Debug)]
pub struct SessionStore {
    path: PathBuf,
}

impl SessionStore {
    /// Where the session is kept on this machine.
    pub fn default_path() -> PathBuf {
        let env = |name: &str| std::env::var_os(name);
        session_dir(std::env::consts::OS, &env)
            .unwrap_or_else(|| std::env::temp_dir().join("PAVI"))
            .join("session-v1.json")
    }

    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn load(&self) -> SessionState {
        let mut state = self
            .read_state(&self.path)
            .or_else(|| self.read_state(&self.temporary_path()))
            .unwrap_or_default();
        state.normalize();
        state
    }

    pub fn save(&self, state: &SessionState) -> std::io::Result<()> {
        let Some(parent) = self.path.parent() else {
            return Ok(());
        };
        fs::create_dir_all(parent)?;
        let mut state = state.clone();
        state.normalize();
        let contents = serde_json::to_vec_pretty(&state).map_err(std::io::Error::other)?;
        let temporary = self.temporary_path();
        fs::write(&temporary, contents)?;
        if fs::rename(&temporary, &self.path).is_err() {
            let _ = fs::remove_file(&self.path);
            fs::rename(temporary, &self.path)?;
        }
        Ok(())
    }

    fn temporary_path(&self) -> PathBuf {
        self.path.with_extension("tmp")
    }

    fn read_state(&self, path: &Path) -> Option<SessionState> {
        let contents = fs::read(path).ok()?;
        serde_json::from_slice(&contents).ok()
    }
}

fn bounded_text(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    value
        .char_indices()
        .take_while(|(index, _)| *index < limit)
        .map(|(_, character)| character)
        .collect()
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// A URL with a query string, which may carry a signature or token
/// (pre-signed S3 URLs, Azure SAS URLs). Never saved in the session.
pub fn is_secret_url(path: &Path) -> bool {
    parquet_reader::is_remote(path) && path.to_string_lossy().contains('?')
}

/// A local path that still exists, or a URL that may be saved (whether it is
/// still there is only known by opening it).
fn still_there(path: &Path) -> bool {
    if parquet_reader::is_remote(path) {
        !is_secret_url(path)
    } else {
        path.exists()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn store() -> (TempDir, SessionStore) {
        let directory = TempDir::new().unwrap();
        let store = SessionStore::new(directory.path().join("session.json"));
        (directory, store)
    }

    #[test]
    fn saves_loads_and_round_trips_restart_state() {
        let (directory, store) = store();
        let file = directory.path().join("data.parquet");
        fs::write(&file, []).unwrap();
        let mut state = SessionState::default();
        state.record_recent_file(file.clone());
        state.sql_input = "SELECT id FROM dataset".to_string();
        state.record_query(&state.sql_input.clone(), 12, true, Some(3));
        state.preferences.chart_visible = true;
        store.save(&state).unwrap();

        assert_eq!(store.load(), state);
    }

    #[test]
    fn keeps_the_theme_choice_and_reads_unknown_ones_as_dark() {
        let (_directory, store) = store();
        let mut state = SessionState::default();
        assert_eq!(state.preferences.theme, ThemeChoice::Dark);
        state.preferences.theme = ThemeChoice::System;
        store.save(&state).unwrap();
        assert_eq!(store.load().preferences.theme, ThemeChoice::System);

        let read = |json: &str| {
            serde_json::from_str::<SessionState>(json)
                .unwrap()
                .preferences
                .theme
        };
        assert_eq!(
            read(r#"{"preferences":{"theme":"light"}}"#),
            ThemeChoice::Light
        );
        assert_eq!(
            read(r#"{"preferences":{"theme":"sepia"}}"#),
            ThemeChoice::Dark
        );
        // Sessions written before themes existed.
        assert_eq!(
            read(r#"{"preferences":{"chart_visible":true}}"#),
            ThemeChoice::Dark
        );

        assert!(!ThemeChoice::Dark.is_light(Some(true)));
        assert!(ThemeChoice::Light.is_light(Some(false)));
        assert!(ThemeChoice::System.is_light(Some(true)));
        assert!(!ThemeChoice::System.is_light(Some(false)));
        assert!(!ThemeChoice::System.is_light(None));
    }

    #[test]
    fn keeps_the_session_in_each_platforms_own_folder() {
        use std::ffi::OsString;
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| OsString::from(value))
            }
        };
        let windows = env(&[
            ("LOCALAPPDATA", "C:\\Users\\a\\AppData\\Local"),
            ("HOME", "/home/a"),
        ]);
        assert_eq!(
            session_dir("windows", &windows),
            Some(PathBuf::from("C:\\Users\\a\\AppData\\Local").join("PAVI"))
        );
        let home = env(&[("HOME", "/Users/a")]);
        assert_eq!(
            session_dir("macos", &home),
            Some(PathBuf::from("/Users/a/Library/Application Support/PAVI"))
        );
        assert_eq!(
            session_dir("linux", &env(&[("HOME", "/home/a")])),
            Some(PathBuf::from("/home/a/.local/state/pavi"))
        );
        assert_eq!(
            session_dir(
                "linux",
                &env(&[("HOME", "/home/a"), ("XDG_STATE_HOME", "/state")])
            ),
            Some(PathBuf::from("/state/pavi"))
        );
        // A relative XDG_STATE_HOME is ignored, as the XDG spec says.
        assert_eq!(
            session_dir(
                "freebsd",
                &env(&[("HOME", "/home/a"), ("XDG_STATE_HOME", "state")])
            ),
            Some(PathBuf::from("/home/a/.local/state/pavi"))
        );
        // Nothing to go on: the caller falls back to the temp folder.
        assert_eq!(session_dir("linux", &env(&[])), None);
        assert_eq!(session_dir("windows", &env(&[("HOME", "/home/a")])), None);
        assert_eq!(
            session_dir("windows", &env(&[("LOCALAPPDATA", "Local")])),
            None
        );
        assert!(session_dir("windows", &env(&[("LOCALAPPDATA", "\\\\server\\a")])).is_some());
        // A Windows path means nothing on Linux, and the other way round.
        assert_eq!(
            session_dir("linux", &env(&[("HOME", "C:\\Users\\a")])),
            None
        );
        assert!(SessionStore::default_path().ends_with("session-v1.json"));
    }

    #[test]
    fn keeps_history_order_deduplicated_and_bounded() {
        let mut state = SessionState::default();
        for index in 0..=MAX_HISTORY_ENTRIES {
            state.record_query(&format!("SELECT {index}"), 0, true, None);
        }
        assert_eq!(state.query_history.len(), MAX_HISTORY_ENTRIES);
        assert_eq!(
            state.query_history[0].sql,
            format!("SELECT {MAX_HISTORY_ENTRIES}")
        );
        state.record_query("SELECT 5", 1, false, None);
        assert_eq!(state.query_history[0].sql, "SELECT 5");
        assert_eq!(state.query_history.len(), MAX_HISTORY_ENTRIES);
    }

    #[test]
    fn clears_history_and_deduplicates_recent_files() {
        let (directory, _) = store();
        let file = directory.path().join("data.parquet");
        fs::write(&file, []).unwrap();
        let mut state = SessionState::default();
        state.record_recent_file(file.clone());
        state.record_recent_file(file);
        state.record_query("SELECT 1", 0, true, Some(1));
        assert_eq!(state.recent_files.len(), 1);
        state.clear_history();
        assert!(state.query_history.is_empty());
    }

    #[test]
    fn ignores_missing_corrupt_and_wrong_version_state() {
        let (directory, store) = store();
        assert_eq!(store.load(), SessionState::default());
        fs::write(directory.path().join("session.json"), b"not json").unwrap();
        assert_eq!(store.load(), SessionState::default());
        let wrong_version = r#"{"version":99,"recent_files":[],"last_opened_file":null,"sql_input":"x","query_history":[],"preferences":{"inspector_visible":false,"chart_visible":false}}"#;
        fs::write(directory.path().join("session.json"), wrong_version).unwrap();
        assert_eq!(store.load(), SessionState::default());
    }

    #[test]
    fn accepts_partial_current_version_state() {
        let (directory, store) = store();
        fs::write(
            directory.path().join("session.json"),
            br#"{"version":1,"sql_input":"SELECT 1"}"#,
        )
        .unwrap();

        let loaded = store.load();
        assert_eq!(loaded.sql_input, "SELECT 1");
        assert!(loaded.recent_files.is_empty());
        assert!(loaded.preferences.inspector_visible);
    }

    #[test]
    fn recovers_a_complete_temporary_file_after_an_interrupted_replace() {
        let (directory, store) = store();
        let state = SessionState {
            sql_input: "SELECT recovered FROM dataset".to_string(),
            ..SessionState::default()
        };
        fs::write(
            directory.path().join("session.tmp"),
            serde_json::to_vec(&state).unwrap(),
        )
        .unwrap();

        assert_eq!(store.load(), state);
    }

    #[test]
    fn keeps_urls_but_never_saves_ones_with_a_query_string() {
        let plain = PathBuf::from("s3://bucket/data.parquet");
        let signed = PathBuf::from("https://bucket.s3.amazonaws.com/d.parquet?X-Amz-Signature=abc");
        let mut state = SessionState::default();
        state.record_recent_file(plain.clone());
        state.record_recent_file(signed.clone());
        assert_eq!(state.recent_files, std::slice::from_ref(&plain));

        state.set_open_files(vec![signed.clone(), plain.clone()], 1);
        assert_eq!(state.open_files, std::slice::from_ref(&plain));
        assert_eq!(state.active_file, 0);

        // A URL can't be checked for existence, so loading keeps it; a signed
        // one that got into the file anyway is dropped.
        state.recent_files.push(signed);
        state.normalize();
        assert_eq!(state.recent_files, std::slice::from_ref(&plain));
        assert_eq!(state.open_files, [plain]);
    }

    #[test]
    fn removes_unavailable_recent_files_and_never_serializes_runtime_data() {
        let (directory, store) = store();
        let missing = directory.path().join("missing.parquet");
        let mut state = SessionState::default();
        state.recent_files.push(missing);
        state.normalize();
        assert!(state.recent_files.is_empty());
        state.record_query("SELECT id FROM dataset", 0, true, Some(2));
        store.save(&state).unwrap();
        let saved = fs::read_to_string(directory.path().join("session.json")).unwrap();
        assert!(!saved.contains("task_id"));
        assert!(!saved.contains("generation"));
        assert!(!saved.contains("RecordBatch"));
    }

    #[test]
    fn saves_named_queries_sorted_bounded_and_replaced_by_name() {
        let mut state = SessionState::default();
        assert_eq!(
            state.save_query("  b busy  ", "SELECT 1"),
            Ok("b busy".to_string())
        );
        assert_eq!(
            state.save_query("Alpha", "SELECT 2"),
            Ok("Alpha".to_string())
        );
        // The same name (ignoring case) replaces the query.
        assert_eq!(
            state.save_query("B BUSY", "SELECT 3"),
            Ok("B BUSY".to_string())
        );
        assert_eq!(
            state
                .saved_queries
                .iter()
                .map(|saved| (saved.name.as_str(), saved.sql.as_str()))
                .collect::<Vec<_>>(),
            [("Alpha", "SELECT 2"), ("b busy", "SELECT 3")]
        );
        assert!(state.save_query(" ", "SELECT 1").is_err());
        assert!(state.save_query("x", "  ").is_err());
        assert!(
            state
                .save_query("x", &"x".repeat(MAX_QUERY_BYTES + 1))
                .is_err()
        );
        let long = "n".repeat(MAX_SAVED_NAME_CHARS + 10);
        assert_eq!(
            state.save_query(&long, "SELECT 1").unwrap().chars().count(),
            MAX_SAVED_NAME_CHARS
        );
        for index in state.saved_queries.len()..MAX_SAVED_QUERIES {
            state
                .save_query(&format!("q{index:03}"), "SELECT 1")
                .unwrap();
        }
        assert!(state.save_query("one too many", "SELECT 1").is_err());
        state.remove_saved_query(0);
        assert_eq!(state.saved_queries.len(), MAX_SAVED_QUERIES - 1);

        // Bad entries in a session file are dropped when it loads.
        let mut loaded = SessionState {
            saved_queries: vec![
                SavedQuery {
                    name: "z".into(),
                    sql: "SELECT 1".into(),
                },
                SavedQuery {
                    name: String::new(),
                    sql: "SELECT 2".into(),
                },
                SavedQuery {
                    name: "a".into(),
                    sql: String::new(),
                },
            ],
            ..SessionState::default()
        };
        loaded.normalize();
        assert_eq!(loaded.saved_queries.len(), 1);
    }

    #[test]
    fn keeps_zoom_tab_views_and_number_formats_within_bounds() {
        // A session saved before zoom existed opens at the normal size.
        let old: SessionState =
            serde_json::from_str(r#"{"version":1,"preferences":{"inspector_visible":true}}"#)
                .unwrap();
        assert_eq!(old.preferences.zoom, 1.0);
        let mut state = SessionState::default();
        state.preferences.zoom = 9.0;
        state.normalize();
        assert_eq!(state.preferences.zoom, MAX_ZOOM);
        state.preferences.zoom = f32::NAN;
        state.normalize();
        assert_eq!(state.preferences.zoom, 1.0);

        // Views are kept only for tabs that reopen, and never for signed URLs.
        let directory = tempfile::tempdir().unwrap();
        let open = directory.path().join("open.parquet");
        let closed = directory.path().join("closed.parquet");
        fs::write(&open, b"").unwrap();
        fs::write(&closed, b"").unwrap();
        let signed = PathBuf::from("https://example.com/a.parquet?sig=secret");
        state.set_open_files(vec![open.clone(), closed.clone()], 0);
        let view = |path: &Path, filter: &str| TabView {
            path: path.to_path_buf(),
            filter: filter.to_string(),
            sort: vec![SavedSortKey {
                column: "id".to_string(),
                descending: true,
            }],
        };
        state.set_tab_views(vec![
            view(&open, "id > 3"),
            view(&closed, "id > 4"),
            view(&signed, "id > 5"),
            TabView {
                path: open.clone(),
                ..TabView::default()
            },
        ]);
        assert_eq!(
            state.tab_views.len(),
            2,
            "the signed URL and the empty view are dropped"
        );
        state.set_open_files(vec![open.clone()], 0);
        state.normalize();
        assert_eq!(state.tab_views, [view(&open, "id > 3")]);
        assert_eq!(state.tab_view(&open).unwrap().sort[0].column, "id");

        // A huge filter is dropped; the sort is kept, within bounds.
        state.tab_views[0].filter = "x".repeat(MAX_SAVED_FILTER_BYTES + 1);
        state.tab_views[0].sort = vec![SavedSortKey::default(); MAX_SAVED_SORT_KEYS + 3];
        state.normalize();
        assert!(state.tab_views[0].filter.is_empty());
        assert_eq!(state.tab_views[0].sort.len(), MAX_SAVED_SORT_KEYS);

        // Number formats of columns the layout no longer has are dropped.
        state.save_grid_layout(GridLayoutPreference {
            schema_key: "a|b".to_string(),
            order: vec!["a".to_string(), "b".to_string()],
            number_formats: BTreeMap::from([
                (
                    "a".to_string(),
                    SavedNumberFormat {
                        decimals: Some(200),
                        thousands: true,
                    },
                ),
                ("gone".to_string(), SavedNumberFormat::default()),
            ]),
            ..GridLayoutPreference::default()
        });
        state.normalize();
        let formats = &state.grid_layout("a|b").unwrap().number_formats;
        assert_eq!(formats.len(), 1);
        assert_eq!(
            formats["a"].decimals,
            Some(parquet_reader::MAX_FORMAT_DECIMALS)
        );

        state.record_recent_file(open.clone());
        state.clear_recent_files();
        assert!(state.recent_files.is_empty());
        assert_eq!(state.last_opened_file, None);
    }

    #[test]
    fn keeps_a_bounded_schema_keyed_grid_layout() {
        let mut state = SessionState::default();
        state.save_grid_layout(GridLayoutPreference {
            schema_key: "id|name".to_string(),
            order: vec!["name".to_string(), "id".to_string()],
            hidden: vec!["id".to_string()],
            widths: BTreeMap::from([("name".to_string(), 240)]),
            pinned: 1,
            ..GridLayoutPreference::default()
        });
        assert_eq!(state.grid_layout("id|name").unwrap().order[0], "name");
        assert_eq!(state.grid_layout("id|name").unwrap().pinned, 1);
        // Layouts saved before pinning existed load with nothing pinned.
        let old: GridLayoutPreference =
            serde_json::from_str(r#"{"schema_key":"a","order":[],"hidden":[],"widths":{}}"#)
                .unwrap();
        assert_eq!(old.pinned, 0);
        for index in 0..=MAX_GRID_LAYOUTS {
            state.save_grid_layout(GridLayoutPreference {
                schema_key: format!("schema-{index}"),
                ..GridLayoutPreference::default()
            });
        }
        assert_eq!(state.preferences.grid_layouts.len(), MAX_GRID_LAYOUTS);
    }
}
