use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

pub const SESSION_VERSION: u32 = 1;
pub const MAX_HISTORY_ENTRIES: usize = 50;
pub const MAX_RECENT_FILES: usize = 12;
pub const MAX_QUERY_BYTES: usize = 16 * 1024;
pub const MAX_HISTORY_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct Preferences {
    pub inspector_visible: bool,
    pub chart_visible: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct QueryHistoryEntry {
    pub sql: String,
    pub timestamp_unix_secs: u64,
    pub duration_ms: u64,
    pub success: bool,
    pub row_count: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct SessionState {
    pub version: u32,
    pub recent_files: Vec<PathBuf>,
    pub last_opened_file: Option<PathBuf>,
    pub sql_input: String,
    pub query_history: Vec<QueryHistoryEntry>,
    pub preferences: Preferences,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            version: SESSION_VERSION,
            recent_files: Vec::new(),
            last_opened_file: None,
            sql_input: String::new(),
            query_history: Vec::new(),
            preferences: Preferences {
                inspector_visible: true,
                ..Preferences::default()
            },
        }
    }
}

impl SessionState {
    pub fn record_recent_file(&mut self, path: PathBuf) {
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

    pub fn clear_history(&mut self) {
        self.query_history.clear();
    }

    pub fn remove_history(&mut self, index: usize) {
        if index < self.query_history.len() {
            self.query_history.remove(index);
        }
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
        let mut seen = Vec::new();
        self.recent_files.retain(|path| {
            path.is_file() && !seen.iter().any(|existing| existing == path) && {
                seen.push(path.clone());
                true
            }
        });
        self.recent_files.truncate(MAX_RECENT_FILES);
        if self
            .last_opened_file
            .as_ref()
            .is_some_and(|path| !path.is_file())
        {
            self.last_opened_file = None;
        }
        self.sql_input = bounded_text(&self.sql_input, MAX_QUERY_BYTES);
        self.query_history
            .retain(|entry| !entry.sql.trim().is_empty());
        for entry in &mut self.query_history {
            entry.sql = bounded_text(&entry.sql, MAX_QUERY_BYTES);
        }
        self.trim_history();
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

#[derive(Clone, Debug)]
pub struct SessionStore {
    path: PathBuf,
}

impl SessionStore {
    pub fn default_path() -> PathBuf {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join("PAVI")
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
}
