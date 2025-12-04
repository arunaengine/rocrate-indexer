use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use rocraters::ro_crate::read::read_crate_obj;
use rocraters::ro_crate::rocrate::RoCrate;
use ulid::Ulid;
use zip::ZipArchive;

use crate::error::IndexError;

/// Source from which to load an RO-Crate
#[derive(Debug, Clone)]
pub enum CrateSource {
    /// Local directory containing ro-crate-metadata.json
    Directory(PathBuf),
    /// Local zip file
    ZipFile(PathBuf),
    /// Remote URL (may or may not end with ro-crate-metadata.json)
    Url(String),
    /// Subcrate within a zip archive (parent_id, zip_path, subpath within zip)
    ZipSubcrate {
        parent_id: String,
        zip_path: PathBuf,
        subpath: String,
    },
    /// Subcrate from a URL (parent keeps URL, subcrate gets resolved URL)
    UrlSubcrate {
        parent_id: String,
        metadata_url: String,
    },
}

impl CrateSource {
    /// Derive a crate identifier from the source
    /// - URLs: use the URL as-is
    /// - Local paths: <ULID>/name
    /// - Subcrates: inherit parent ID with subpath appended
    pub fn to_crate_id(&self) -> String {
        match self {
            CrateSource::Url(u) => normalize_url_for_id(u),
            CrateSource::Directory(p) => {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
                format!("{}/{}", Ulid::new(), name)
            }
            CrateSource::ZipFile(p) => {
                let name = p.file_stem().and_then(|n| n.to_str()).unwrap_or("unknown");
                format!("{}/{}", Ulid::new(), name)
            }
            CrateSource::ZipSubcrate {
                parent_id, subpath, ..
            } => {
                // Append subpath to parent ID
                let clean_subpath = subpath
                    .trim_matches('/')
                    .trim_end_matches("/ro-crate-metadata.json");
                // Remove any *-ro-crate-metadata.json suffix
                let clean_subpath = clean_subpath
                    .rsplit_once('/')
                    .map(|(path, _)| path)
                    .unwrap_or(clean_subpath);
                format!("{}/{}", parent_id, clean_subpath)
            }
            CrateSource::UrlSubcrate { metadata_url, .. } => {
                // For URL subcrates, use the metadata URL as the ID
                normalize_url_for_id(metadata_url)
            }
        }
    }

    /// Get the base URL for resolving relative paths in subcrates
    pub fn base_url(&self) -> Option<String> {
        match self {
            CrateSource::Url(u) => {
                let normalized = normalize_url_for_id(u);
                if let Some(pos) = normalized.rfind('/') {
                    Some(normalized[..=pos].to_string())
                } else {
                    Some(format!("{}/", normalized))
                }
            }
            CrateSource::UrlSubcrate { metadata_url, .. } => {
                if let Some(pos) = metadata_url.rfind('/') {
                    Some(metadata_url[..=pos].to_string())
                } else {
                    Some(format!("{}/", metadata_url))
                }
            }
            _ => None,
        }
    }

    /// Check if this is a local source (directory or zip)
    pub fn is_local(&self) -> bool {
        matches!(
            self,
            CrateSource::Directory(_) | CrateSource::ZipFile(_) | CrateSource::ZipSubcrate { .. }
        )
    }

    /// Get the zip path if this is a zip-based source
    pub fn zip_path(&self) -> Option<&PathBuf> {
        match self {
            CrateSource::ZipFile(p) => Some(p),
            CrateSource::ZipSubcrate { zip_path, .. } => Some(zip_path),
            _ => None,
        }
    }
}

/// Normalize URL for use as crate ID
/// Removes trailing ro-crate-metadata.json if present
fn normalize_url_for_id(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if url.ends_with("ro-crate-metadata.json") {
        // Find the last path segment
        if let Some(pos) = url.rfind('/') {
            return url[..pos].to_string();
        }
    }
    url.to_string()
}

/// Load an RO-Crate from a local directory
pub fn load_from_directory(path: &PathBuf) -> Result<RoCrate, IndexError> {
    if !path.exists() {
        return Err(IndexError::InvalidPath(path.to_path_buf()));
    }

    rocraters::ro_crate::read::read_crate(path, 0).map_err(|e| IndexError::LoadError {
        path: path.display().to_string(),
        reason: format!("{:#?}", e),
    })
}

/// Load an RO-Crate from a zip file by extracting ro-crate-metadata.json
pub fn load_from_zip(path: &PathBuf) -> Result<(RoCrate, String), IndexError> {
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

    // Look for ro-crate-metadata.json at root
    let metadata_filename = find_root_metadata_in_zip(&mut archive)?;
    load_metadata_from_zip(&mut archive, &metadata_filename, path)
}

/// Load a subcrate from within a zip archive
pub fn load_from_zip_subpath(
    zip_path: &PathBuf,
    subpath: &str,
) -> Result<(RoCrate, String), IndexError> {
    let file = File::open(zip_path).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to open zip file: {}", e),
    })?;

    let mut archive = ZipArchive::new(file).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to read zip archive: {}", e),
    })?;

    load_metadata_from_zip(&mut archive, subpath, zip_path)
}

/// Load metadata content from a zip archive entry
fn load_metadata_from_zip(
    archive: &mut ZipArchive<File>,
    entry_path: &str,
    zip_path: &PathBuf,
) -> Result<(RoCrate, String), IndexError> {
    let mut metadata_file = archive
        .by_name(entry_path)
        .map_err(|e| IndexError::LoadError {
            path: zip_path.display().to_string(),
            reason: format!("Failed to extract {}: {}", entry_path, e),
        })?;

    let mut content = String::new();
    metadata_file
        .read_to_string(&mut content)
        .map_err(|e| IndexError::LoadError {
            path: zip_path.display().to_string(),
            reason: format!("Failed to read metadata file: {}", e),
        })?;

    let crate_data = read_crate_obj(&content, 0).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to parse RO-Crate metadata: {:#?}", e),
    })?;

    Ok((crate_data, content))
}

/// Find ro-crate-metadata.json at the root of a zip archive
fn find_root_metadata_in_zip<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
) -> Result<String, IndexError> {
    for i in 0..archive.len() {
        if let Ok(file) = archive.by_index(i) {
            let name = file.name();
            // Root level only: no directory separators before the filename
            if !name.contains('/') && name.ends_with("ro-crate-metadata.json") {
                return Ok(name.to_string());
            }
            // Or single top-level directory (common when zipping a folder)
            let parts: Vec<&str> = name.split('/').collect();
            if parts.len() == 2 && parts[1].ends_with("ro-crate-metadata.json") {
                return Ok(name.to_string());
            }
        }
    }

    Err(IndexError::LoadError {
        path: "zip".to_string(),
        reason: "No ro-crate-metadata.json found at archive root".to_string(),
    })
}

/// Scan a zip archive for all potential subcrate metadata files
/// Returns a list of (entity_id, metadata_path) pairs for entities that have metadata in the archive
pub fn find_subcrate_metadata_in_zip(
    zip_path: &PathBuf,
    entity_ids: &[String],
) -> Result<Vec<(String, String)>, IndexError> {
    let file = File::open(zip_path).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to open zip file: {}", e),
    })?;

    let mut archive = ZipArchive::new(file).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to read zip archive: {}", e),
    })?;

    // Collect all entries ending with ro-crate-metadata.json
    let mut metadata_entries: Vec<String> = Vec::new();
    for i in 0..archive.len() {
        if let Ok(file) = archive.by_index(i) {
            let name = file.name();
            if name.ends_with("ro-crate-metadata.json") {
                metadata_entries.push(name.to_string());
            }
        }
    }

    // Match entity IDs to metadata files
    let mut matches = Vec::new();
    for entity_id in entity_ids {
        // Normalize entity ID: remove leading ./ and trailing /
        let normalized = entity_id.trim_start_matches("./").trim_end_matches('/');

        // Look for metadata file in this directory
        for entry in &metadata_entries {
            let entry_dir = entry.trim_end_matches(|c| c != '/').trim_end_matches('/');

            // Check if entry is in the entity's directory
            // Entry could be "subdir/ro-crate-metadata.json" or "subdir/prefix-ro-crate-metadata.json"
            if entry_dir == normalized || entry_dir.ends_with(&format!("/{}", normalized)) {
                matches.push((entity_id.clone(), entry.clone()));
                break; // Take first match for this entity
            }
        }
    }

    Ok(matches)
}

/// Load from a URL, handling both direct metadata URLs and directory URLs
pub fn load_from_url(url: &str) -> Result<(RoCrate, String), IndexError> {
    let (final_url, content) = fetch_metadata_from_url(url)?;

    let crate_data = read_crate_obj(&content, 0).map_err(|e| IndexError::LoadError {
        path: final_url,
        reason: format!("Failed to parse RO-Crate metadata: {:#?}", e),
    })?;

    Ok((crate_data, content))
}

/// Fetch metadata from URL, trying /ro-crate-metadata.json if URL doesn't point to metadata
fn fetch_metadata_from_url(url: &str) -> Result<(String, String), IndexError> {
    // If URL already ends with ro-crate-metadata.json, fetch directly
    if url.ends_with("ro-crate-metadata.json") {
        let content = fetch_url(url)?;
        return Ok((url.to_string(), content));
    }

    // Try appending /ro-crate-metadata.json first
    let metadata_url = format!("{}/ro-crate-metadata.json", url.trim_end_matches('/'));
    match fetch_url(&metadata_url) {
        Ok(content) => {
            // Verify it looks like JSON
            if content.trim().starts_with('{') {
                return Ok((metadata_url, content));
            }
        }
        Err(_) => {}
    }

    // Fall back to fetching URL directly (maybe it IS the metadata)
    let content = fetch_url(url)?;
    if content.trim().starts_with('{') {
        Ok((url.to_string(), content))
    } else {
        Err(IndexError::LoadError {
            path: url.to_string(),
            reason: "URL does not contain valid RO-Crate metadata".to_string(),
        })
    }
}

/// Simple URL fetch
fn fetch_url(url: &str) -> Result<String, IndexError> {
    reqwest::blocking::get(url)
        .map_err(|e| IndexError::LoadError {
            path: url.to_string(),
            reason: format!("HTTP request failed: {}", e),
        })?
        .text()
        .map_err(|e| IndexError::LoadError {
            path: url.to_string(),
            reason: format!("Failed to read response: {}", e),
        })
}

/// Load from a directory and return both the crate and raw JSON
pub fn load_from_directory_with_json(path: &PathBuf) -> Result<(RoCrate, String), IndexError> {
    let crate_data = load_from_directory(path)?;

    // Find metadata file (could have prefix)
    let metadata_path = find_metadata_in_directory(path)?;
    let content = std::fs::read_to_string(&metadata_path).map_err(|e| IndexError::LoadError {
        path: metadata_path.display().to_string(),
        reason: e.to_string(),
    })?;

    Ok((crate_data, content))
}

/// Find ro-crate-metadata.json (with optional prefix) in a directory
fn find_metadata_in_directory(path: &PathBuf) -> Result<PathBuf, IndexError> {
    // Try standard name first
    let standard = path.join("ro-crate-metadata.json");
    if standard.exists() {
        return Ok(standard);
    }

    // Look for *-ro-crate-metadata.json
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if let Some(name_str) = name.to_str() {
                if name_str.ends_with("-ro-crate-metadata.json") {
                    return Ok(entry.path());
                }
            }
        }
    }

    Err(IndexError::LoadError {
        path: path.display().to_string(),
        reason: "No ro-crate-metadata.json found".to_string(),
    })
}

/// Load from any source, returning both crate and raw JSON
pub fn load_with_json(source: &CrateSource) -> Result<(RoCrate, String), IndexError> {
    match source {
        CrateSource::Directory(p) => load_from_directory_with_json(p),
        CrateSource::ZipFile(p) => load_from_zip(p),
        CrateSource::Url(u) => load_from_url(u),
        CrateSource::ZipSubcrate {
            zip_path, subpath, ..
        } => load_from_zip_subpath(zip_path, subpath),
        CrateSource::UrlSubcrate { metadata_url, .. } => load_from_url(metadata_url),
    }
}

/// Load from any source (backward compatibility)
pub fn load(source: &CrateSource) -> Result<RoCrate, IndexError> {
    load_with_json(source).map(|(crate_data, _)| crate_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url_for_id() {
        assert_eq!(
            normalize_url_for_id("https://example.org/crate/ro-crate-metadata.json"),
            "https://example.org/crate"
        );
        assert_eq!(
            normalize_url_for_id("https://example.org/crate/"),
            "https://example.org/crate"
        );
        assert_eq!(
            normalize_url_for_id("https://example.org/crate"),
            "https://example.org/crate"
        );
    }

    #[test]
    fn test_crate_id_generation() {
        let url_source = CrateSource::Url("https://example.org/data/".to_string());
        assert_eq!(url_source.to_crate_id(), "https://example.org/data");

        let url_meta_source =
            CrateSource::Url("https://example.org/data/ro-crate-metadata.json".to_string());
        assert_eq!(url_meta_source.to_crate_id(), "https://example.org/data");
    }

    #[test]
    fn test_subcrate_id_inheritance() {
        let subcrate = CrateSource::ZipSubcrate {
            parent_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV/mydata".to_string(),
            zip_path: PathBuf::from("/tmp/test.zip"),
            subpath: "experiments/ro-crate-metadata.json".to_string(),
        };
        assert_eq!(
            subcrate.to_crate_id(),
            "01ARZ3NDEKTSV4RRFFQ69G5FAV/mydata/experiments"
        );
    }
}
