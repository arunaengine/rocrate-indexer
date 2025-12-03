use std::path::PathBuf;

use rocraters::ro_crate::read::read_crate;
use uuid::Uuid;

use crate::error::IndexError;

/// Source from which to load an RO-Crate
#[derive(Debug, Clone)]
pub enum CrateSource {
    /// Local directory containing ro-crate-metadata.json
    Directory(PathBuf),
    /// Local zip file
    ZipFile(PathBuf),
    /// Remote URL (zip or metadata file)
    Url(String),
}

impl CrateSource {
    /// Derive a crate identifier from the source
    /// - URLs: use the URL as-is
    /// - Local paths: UUID with folder/file name as suffix
    pub fn to_crate_id(&self) -> String {
        match self {
            CrateSource::Url(u) => u.clone(),
            CrateSource::Directory(p) => {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
                format!("{}-{}", Uuid::new_v4(), name)
            }
            CrateSource::ZipFile(p) => {
                let name = p.file_stem().and_then(|n| n.to_str()).unwrap_or("unknown");
                format!("{}-{}", Uuid::new_v4(), name)
            }
        }
    }

    /// Get the base URL for resolving relative paths in subcrates
    pub fn base_url(&self) -> Option<String> {
        match self {
            CrateSource::Url(u) => {
                // Remove the filename part to get the base directory URL
                if let Some(pos) = u.rfind('/') {
                    Some(u[..=pos].to_string())
                } else {
                    Some(format!("{}/", u))
                }
            }
            _ => None,
        }
    }
}

/// Load an RO-Crate from a local path (directory or zip)
pub fn load_from_path(path: &PathBuf) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    if !path.exists() {
        return Err(IndexError::InvalidPath(path.to_path_buf()));
    }

    read_crate(path, 0).map_err(|e| IndexError::LoadError {
        path: path.display().to_string(),
        reason: format!("{:#?}", e),
    })
}

/// Load from a URL and return both the crate and raw JSON
pub fn load_from_url(
    url: &str,
) -> Result<(rocraters::ro_crate::rocrate::RoCrate, String), IndexError> {
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

    let crate_data = rocraters::ro_crate::read::read_crate_obj(&content, 0).map_err(|e| {
        IndexError::LoadError {
            path: url.to_string(),
            reason: format!("{:#?}", e),
        }
    })?;

    Ok((crate_data, content))
}

/// Load from a path and return both the crate and raw JSON
pub fn load_from_path_with_json(
    path: &PathBuf,
) -> Result<(rocraters::ro_crate::rocrate::RoCrate, String), IndexError> {
    let crate_data = load_from_path(path)?;

    // Read the metadata JSON
    let metadata_path = if path.is_dir() {
        path.join("ro-crate-metadata.json")
    } else {
        // For zip files, serialize the loaded crate back to JSON
        let json = serde_json::to_string_pretty(&crate_data)?;
        return Ok((crate_data, json));
    };

    let content = std::fs::read_to_string(&metadata_path).map_err(|e| IndexError::LoadError {
        path: metadata_path.display().to_string(),
        reason: e.to_string(),
    })?;

    Ok((crate_data, content))
}

/// Load from any source, returning both crate and raw JSON
pub fn load_with_json(
    source: &CrateSource,
) -> Result<(rocraters::ro_crate::rocrate::RoCrate, String), IndexError> {
    match source {
        CrateSource::Directory(p) | CrateSource::ZipFile(p) => load_from_path_with_json(p),
        CrateSource::Url(u) => load_from_url(u),
    }
}

/// Load from any source (backward compatibility)
pub fn load(source: &CrateSource) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    match source {
        CrateSource::Directory(p) | CrateSource::ZipFile(p) => load_from_path(p),
        CrateSource::Url(u) => load_from_url(u).map(|(crate_data, _)| crate_data),
    }
}
