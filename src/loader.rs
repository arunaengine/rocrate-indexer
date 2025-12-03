use rocraters::ro_crate::read::read_crate;
use std::path::PathBuf;

use crate::error::IndexError;

/// Source from which to load an RO-Crate
#[derive(Debug, Clone)]
pub enum CrateSource {
    /// Local directory containing ro-crate-metadata.json
    Directory(std::path::PathBuf),
    /// Local zip file
    ZipFile(std::path::PathBuf),
    /// Remote URL (zip or metadata file)
    Url(String),
}

impl CrateSource {
    /// Derive a crate identifier from the source
    pub fn to_crate_id(&self) -> String {
        match self {
            CrateSource::Directory(p) => p.display().to_string(),
            CrateSource::ZipFile(p) => p.display().to_string(),
            CrateSource::Url(u) => u.clone(),
        }
    }
}

/// Load an RO-Crate from a local path (directory or zip)
pub fn load_from_path(path: &PathBuf) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    if !path.exists() {
        return Err(IndexError::InvalidPath(path.to_path_buf()));
    }

    // ro-crate-rs read_crate handles both directories and zips
    read_crate(path, 0).map_err(|e| IndexError::LoadError {
        path: path.display().to_string(),
        reason: format!("{:#?}", e),
    })
}

/// Load from a URL
///
/// Note: This is a placeholder. Full implementation would:
/// 1. Fetch the URL content
/// 2. If zip, extract to temp dir and read
/// 3. If JSON, parse directly as metadata
pub fn load_from_url(url: &str) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    let content = reqwest::blocking::get(url)
        .map_err(|e| IndexError::LoadError {
            path: url.to_string(),
            reason: format!("{:#?}", e),
        })?
        .text()
        .map_err(|e| IndexError::LoadError {
            path: url.to_string(),
            reason: format!("{:#?}", e),
        })?;

    rocraters::ro_crate::read::read_crate_obj(&content, 0).map_err(|e| IndexError::LoadError {
        path: url.to_string(),
        reason: format!("{:#?}", e),
    })
}

/// Load from any source
pub fn load(source: &CrateSource) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    match source {
        CrateSource::Directory(p) | CrateSource::ZipFile(p) => load_from_path(p),
        CrateSource::Url(u) => load_from_url(u),
    }
}
