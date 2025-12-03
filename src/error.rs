use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("Query parse error: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),

    #[error("Crate not found: {0}")]
    CrateNotFound(String),

    #[error("Failed to load crate from {path}: {reason}")]
    LoadError { path: String, reason: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid crate path: {0}")]
    InvalidPath(PathBuf),

    #[error("Opendirectory error: {0}")]
    OpenDirectory(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error("Invalid crate format: {0}")]
    InvalidCrateFormat(String),
}
