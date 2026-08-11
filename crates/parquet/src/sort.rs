#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortRequest {
    pub column: usize,
    pub descending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SortStatus {
    Unsupported(String),
}

pub fn unsupported_sort_message() -> String {
    "sorting requires external sort; not implemented in this MVP".to_string()
}
