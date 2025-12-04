use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use rocraters::ro_crate::read::read_crate;
use uuid::Uuid;
use zip::ZipArchive;

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

/// Load an RO-Crate from a local directory
pub fn load_from_directory(
    path: &PathBuf,
) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    if !path.exists() {
        return Err(IndexError::InvalidPath(path.to_path_buf()));
    }

    read_crate(path, 0).map_err(|e| IndexError::LoadError {
        path: path.display().to_string(),
        reason: format!("{:#?}", e),
    })
}

/// Load an RO-Crate from a zip file by extracting ro-crate-metadata.json
pub fn load_from_zip(
    path: &PathBuf,
) -> Result<(rocraters::ro_crate::rocrate::RoCrate, String), IndexError> {
    if !path.exists() {
        return Err(IndexError::InvalidPath(path.to_path_buf()));
    }

    let file = File::open(path).map_err(|e| IndexError::LoadError {
        path: path.display().to_string(),
        reason: format!("Failed to open zip file: {}", e),
    })?;

    let mut archive = ZipArchive::new(file).map_err(|e| IndexError::LoadError {
        path: path.display().to_string(),
        reason: format!("Failed to read zip archive: {}", e),
    })?;

    // Look for ro-crate-metadata.json (could be at root or in a subdirectory)
    let metadata_filename = find_metadata_in_zip(&mut archive)?;

    let mut metadata_file =
        archive
            .by_name(&metadata_filename)
            .map_err(|e| IndexError::LoadError {
                path: path.display().to_string(),
                reason: format!("Failed to extract {}: {}", metadata_filename, e),
            })?;

    let mut content = String::new();
    metadata_file
        .read_to_string(&mut content)
        .map_err(|e| IndexError::LoadError {
            path: path.display().to_string(),
            reason: format!("Failed to read metadata file: {}", e),
        })?;

    let crate_data = rocraters::ro_crate::read::read_crate_obj(&content, 0).map_err(|e| {
        IndexError::LoadError {
            path: path.display().to_string(),
            reason: format!("Failed to parse RO-Crate metadata: {:#?}", e),
        }
    })?;

    Ok((crate_data, content))
}

/// Find ro-crate-metadata.json in a zip archive
fn find_metadata_in_zip<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
) -> Result<String, IndexError> {
    // First try exact match at root
    for i in 0..archive.len() {
        if let Ok(file) = archive.by_index(i) {
            let name = file.name();
            if name == "ro-crate-metadata.json" {
                return Ok(name.to_string());
            }
        }
    }

    // Then try to find it anywhere in the archive
    for i in 0..archive.len() {
        if let Ok(file) = archive.by_index(i) {
            let name = file.name();
            if name.ends_with("ro-crate-metadata.json") {
                return Ok(name.to_string());
            }
        }
    }

    Err(IndexError::LoadError {
        path: "zip".to_string(),
        reason: "No ro-crate-metadata.json found in archive".to_string(),
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

/// Load from a directory and return both the crate and raw JSON
pub fn load_from_directory_with_json(
    path: &PathBuf,
) -> Result<(rocraters::ro_crate::rocrate::RoCrate, String), IndexError> {
    let crate_data = load_from_directory(path)?;

    let metadata_path = path.join("ro-crate-metadata.json");
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
        CrateSource::Directory(p) => load_from_directory_with_json(p),
        CrateSource::ZipFile(p) => load_from_zip(p),
        CrateSource::Url(u) => load_from_url(u),
    }
}

/// Load from any source (backward compatibility)
pub fn load(source: &CrateSource) -> Result<rocraters::ro_crate::rocrate::RoCrate, IndexError> {
    match source {
        CrateSource::Directory(p) => load_from_directory(p),
        CrateSource::ZipFile(p) => load_from_zip(p).map(|(crate_data, _)| crate_data),
        CrateSource::Url(u) => load_from_url(u).map(|(crate_data, _)| crate_data),
    }
}
