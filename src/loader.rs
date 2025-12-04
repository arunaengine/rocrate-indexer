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
    /// Local zip file with optional name hint for ID generation
    ZipFile {
        path: PathBuf,
        name_hint: Option<String>,
    },
    /// Remote URL (may or may not end with ro-crate-metadata.json)
    Url(String),
    /// Subcrate within a zip archive
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
    /// Create a ZipFile source from a path (no name hint)
    pub fn zip(path: PathBuf) -> Self {
        CrateSource::ZipFile {
            path,
            name_hint: None,
        }
    }

    /// Create a ZipFile source with a name hint
    pub fn zip_with_name(path: PathBuf, name: impl Into<String>) -> Self {
        CrateSource::ZipFile {
            path,
            name_hint: Some(name.into()),
        }
    }

    /// Derive a crate identifier from the source
    /// - URLs: use the URL as-is
    /// - Local paths: <ULID> or <ULID>/name if name available
    /// - Subcrates: inherit parent ID with subpath appended
    pub fn to_crate_id(&self) -> String {
        match self {
            CrateSource::Url(u) => normalize_url_for_id(u),
            CrateSource::Directory(p) => {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
                format!("{}/{}", Ulid::new(), name)
            }
            CrateSource::ZipFile { path, name_hint } => {
                let ulid = Ulid::new();
                match name_hint {
                    Some(name) => {
                        // Clean up the name - remove .zip extension if present
                        let clean_name = name.trim_end_matches(".zip").trim_end_matches(".ZIP");
                        format!("{}/{}", ulid, clean_name)
                    }
                    None => {
                        // Try to get name from path, fall back to just ULID
                        match path.file_stem().and_then(|n| n.to_str()) {
                            Some(name) if !name.starts_with("rocrate_") && !is_uuid_like(name) => {
                                format!("{}/{}", ulid, name)
                            }
                            _ => ulid.to_string(),
                        }
                    }
                }
            }
            CrateSource::ZipSubcrate {
                parent_id, subpath, ..
            } => {
                // Extract directory path from subpath, removing the metadata filename
                let clean_subpath = extract_directory_from_metadata_path(subpath);
                if clean_subpath.is_empty() {
                    parent_id.clone()
                } else {
                    format!("{}/{}", parent_id, clean_subpath)
                }
            }
            CrateSource::UrlSubcrate { metadata_url, .. } => normalize_url_for_id(metadata_url),
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
            CrateSource::Directory(_)
                | CrateSource::ZipFile { .. }
                | CrateSource::ZipSubcrate { .. }
        )
    }

    /// Get the zip path if this is a zip-based source
    pub fn zip_path(&self) -> Option<&PathBuf> {
        match self {
            CrateSource::ZipFile { path, .. } => Some(path),
            CrateSource::ZipSubcrate { zip_path, .. } => Some(zip_path),
            _ => None,
        }
    }
}

/// Check if a string looks like a UUID (for filtering temp filenames)
fn is_uuid_like(s: &str) -> bool {
    // UUIDs are 36 chars with hyphens, or 32 without
    let without_hyphens: String = s.chars().filter(|c| *c != '-').collect();
    without_hyphens.len() == 32 && without_hyphens.chars().all(|c| c.is_ascii_hexdigit())
}

/// Extract the directory path from a metadata file path
/// e.g., "subdir/ro-crate-metadata.json" -> "subdir"
/// e.g., "subdir/prefix-ro-crate-metadata.json" -> "subdir"
/// e.g., "ro-crate-metadata.json" -> ""
fn extract_directory_from_metadata_path(path: &str) -> String {
    if let Some(pos) = path.rfind('/') {
        path[..pos].trim_matches('/').to_string()
    } else {
        String::new()
    }
}

/// Normalize URL for use as crate ID
/// Removes trailing ro-crate-metadata.json if present
fn normalize_url_for_id(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if url.ends_with("ro-crate-metadata.json") {
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

/// Load an RO-Crate from a zip file by extracting the root ro-crate-metadata.json
/// Returns (crate_data, json_content, root_prefix)
pub fn load_from_zip(path: &PathBuf) -> Result<(RoCrate, String, String), IndexError> {
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

    // Find the root metadata file (must be at top level)
    let (metadata_filename, root_prefix) = find_root_metadata_in_zip(&mut archive)?;
    let (crate_data, content) =
        load_metadata_from_zip_archive(&mut archive, &metadata_filename, path)?;

    Ok((crate_data, content, root_prefix))
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

    load_metadata_from_zip_archive(&mut archive, subpath, zip_path)
}

/// Load metadata content from a zip archive entry
fn load_metadata_from_zip_archive(
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

/// Find the root ro-crate-metadata.json in a zip archive
/// Returns (full_path, root_prefix) where root_prefix is the top-level directory if any
fn find_root_metadata_in_zip<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
) -> Result<(String, String), IndexError> {
    // Collect all entries
    let mut entries: Vec<String> = Vec::new();
    for i in 0..archive.len() {
        if let Ok(file) = archive.by_index(i) {
            entries.push(file.name().to_string());
        }
    }

    // First, check for metadata directly at root (no directory)
    for entry in &entries {
        if !entry.contains('/') && entry.ends_with("ro-crate-metadata.json") {
            return Ok((entry.clone(), String::new()));
        }
    }

    // Find the common top-level directory (if archive was created by zipping a folder)
    // This is the case when ALL entries start with the same directory prefix
    let top_level_dirs: std::collections::HashSet<_> = entries
        .iter()
        .filter_map(|e| e.split('/').next())
        .filter(|s| !s.is_empty())
        .collect();

    if top_level_dirs.len() == 1 {
        let prefix = top_level_dirs.into_iter().next().unwrap();
        // Look for metadata in this single top-level directory
        let expected_root = format!("{}/", prefix);
        for entry in &entries {
            if entry.starts_with(&expected_root) {
                let remainder = &entry[expected_root.len()..];
                // Must be directly in the top-level dir, not a subdirectory
                if !remainder.contains('/') && remainder.ends_with("ro-crate-metadata.json") {
                    return Ok((entry.clone(), prefix.to_string()));
                }
            }
        }
    }

    // If we have multiple top-level items, the root metadata must be at the actual root
    Err(IndexError::LoadError {
        path: "zip".to_string(),
        reason: "No root ro-crate-metadata.json found at archive root".to_string(),
    })
}

/// Find metadata files for specific subcrate entity IDs in a zip archive
/// Only returns matches for the given entity IDs (based on the parent's @graph)
pub fn find_subcrate_metadata_in_zip(
    zip_path: &PathBuf,
    entity_ids: &[String],
    root_prefix: &str,
) -> Result<Vec<(String, String)>, IndexError> {
    let file = File::open(zip_path).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to open zip file: {}", e),
    })?;

    let mut archive = ZipArchive::new(file).map_err(|e| IndexError::LoadError {
        path: zip_path.display().to_string(),
        reason: format!("Failed to read zip archive: {}", e),
    })?;

    // Collect all metadata entries (excluding root)
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

        // Build expected path prefix (accounting for root_prefix)
        let expected_dir = if root_prefix.is_empty() {
            normalized.to_string()
        } else {
            format!("{}/{}", root_prefix, normalized)
        };

        // Look for metadata file in this directory
        for entry in &metadata_entries {
            let entry_dir = extract_directory_from_metadata_path(entry);

            if entry_dir == expected_dir {
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

/// Load from any source, returning crate, JSON, and optional root prefix (for zips)
pub fn load_with_json(source: &CrateSource) -> Result<(RoCrate, String, String), IndexError> {
    match source {
        CrateSource::Directory(p) => {
            let (crate_data, json) = load_from_directory_with_json(p)?;
            Ok((crate_data, json, String::new()))
        }
        CrateSource::ZipFile { path, .. } => load_from_zip(path),
        CrateSource::Url(u) => {
            let (crate_data, json) = load_from_url(u)?;
            Ok((crate_data, json, String::new()))
        }
        CrateSource::ZipSubcrate {
            zip_path, subpath, ..
        } => {
            let (crate_data, json) = load_from_zip_subpath(zip_path, subpath)?;
            Ok((crate_data, json, String::new()))
        }
        CrateSource::UrlSubcrate { metadata_url, .. } => {
            let (crate_data, json) = load_from_url(metadata_url)?;
            Ok((crate_data, json, String::new()))
        }
    }
}

/// Load from any source (backward compatibility)
pub fn load(source: &CrateSource) -> Result<RoCrate, IndexError> {
    load_with_json(source).map(|(crate_data, _, _)| crate_data)
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

    #[test]
    fn test_is_uuid_like() {
        assert!(is_uuid_like("626a838e-398d-4010-8c57-92c5cea1798c"));
        assert!(is_uuid_like("626a838e398d401c8c5792c5cea1798c"));
        assert!(!is_uuid_like("mydata"));
        assert!(!is_uuid_like("rocrate_test"));
    }

    #[test]
    fn test_extract_directory_from_metadata_path() {
        assert_eq!(
            extract_directory_from_metadata_path("subdir/ro-crate-metadata.json"),
            "subdir"
        );
        assert_eq!(
            extract_directory_from_metadata_path("a/b/ro-crate-metadata.json"),
            "a/b"
        );
        assert_eq!(
            extract_directory_from_metadata_path("ro-crate-metadata.json"),
            ""
        );
    }

    #[test]
    fn test_zip_with_name_hint() {
        let source = CrateSource::zip_with_name(PathBuf::from("/tmp/test.zip"), "mydata.zip");
        let id = source.to_crate_id();
        // Should be ULID/mydata (without .zip)
        assert!(id.ends_with("/mydata"));
        assert!(!id.ends_with(".zip"));
    }

    #[test]
    fn test_zip_without_name_hint_uuid_path() {
        let source = CrateSource::ZipFile {
            path: PathBuf::from("/tmp/rocrate_626a838e-398d-4010-8c57-92c5cea1798c.zip"),
            name_hint: None,
        };
        let id = source.to_crate_id();
        // Should be just ULID (no /rocrate_uuid suffix)
        assert!(!id.contains('/'));
        assert!(!id.contains("rocrate_"));
    }
}
