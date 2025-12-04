pub mod config;
pub mod error;
pub mod extract;
pub mod index;
pub mod loader;
pub mod query;
pub mod store;

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

use rocraters::ro_crate::rocrate::RoCrate;

use crate::config::Config;
use crate::error::IndexError;
use crate::extract::{detect_subcrates_from_url, get_subcrate_entity_ids};
use crate::index::SearchIndex;
use crate::loader::find_subcrate_metadata_in_zip;
use crate::query::QueryEngine;
use crate::store::CrateStore;

// Re-export key types for convenience
pub use crate::config::{Config as IndexConfig, Manifest};
pub use crate::extract::SubcrateInfo;
pub use crate::loader::CrateSource;
pub use crate::query::SearchHit;

/// Thread-safe RO-Crate index
pub type SharedCrateIndex = Arc<RwLock<CrateIndex>>;

/// Result of adding a crate (includes subcrate info)
#[derive(Debug)]
pub struct AddResult {
    /// The crate ID that was added
    pub crate_id: String,
    /// Number of entities indexed
    pub entity_count: usize,
    /// Subcrates that were discovered and added
    pub subcrates: Vec<AddResult>,
}

/// Main interface for indexing and searching RO-Crates
pub struct CrateIndex {
    config: Config,
    manifest: Manifest,
    store: CrateStore,
    search_index: SearchIndex,
}

impl CrateIndex {
    /// Create a new in-memory index (for testing)
    pub fn new_in_memory() -> Result<Self, IndexError> {
        Ok(Self {
            config: Config::from_current_dir()?,
            manifest: Manifest::default(),
            store: CrateStore::new(),
            search_index: SearchIndex::new_in_memory()?,
        })
    }

    /// Open or create a persistent index in the current directory
    pub fn open_or_create() -> Result<Self, IndexError> {
        let config = Config::from_current_dir()?;
        config.ensure_directories()?;

        let manifest = config.load_manifest()?;
        let search_index = SearchIndex::open_or_create(&config.index_dir())?;

        let mut idx = Self {
            config,
            manifest,
            store: CrateStore::new(),
            search_index,
        };

        // Load all crate metadata into memory
        idx.load_all_metadata()?;

        Ok(idx)
    }

    /// Open or create at a specific base path
    pub fn open_or_create_at(base_path: &Path) -> Result<Self, IndexError> {
        let config = Config::new(base_path.to_path_buf());
        config.ensure_directories()?;

        let manifest = config.load_manifest()?;
        let search_index = SearchIndex::open_or_create(&config.index_dir())?;

        let mut idx = Self {
            config,
            manifest,
            store: CrateStore::new(),
            search_index,
        };

        idx.load_all_metadata()?;

        Ok(idx)
    }

    /// Load all metadata files listed in manifest into memory
    fn load_all_metadata(&mut self) -> Result<(), IndexError> {
        for crate_id in &self.manifest.crates.clone() {
            let metadata_path = self.config.metadata_path_for_crate(crate_id);
            if metadata_path.exists() {
                let content = std::fs::read_to_string(&metadata_path)?;
                let crate_data: RoCrate = rocraters::ro_crate::read::read_crate_obj(&content, 0)
                    .map_err(|e| IndexError::LoadError {
                        path: metadata_path.display().to_string(),
                        reason: format!("{:#?}", e),
                    })?;
                self.store.insert(crate_id.clone(), crate_data);
            }
        }
        Ok(())
    }

    /// Wrap in Arc<RwLock<>> for shared access
    pub fn into_shared(self) -> SharedCrateIndex {
        Arc::new(RwLock::new(self))
    }

    /// Check if a crate ID is already indexed (cycle detection)
    pub fn is_indexed(&self, crate_id: &str) -> bool {
        self.manifest.contains(crate_id)
    }

    /// Add a crate from a source (path, zip, url) with automatic subcrate discovery
    pub fn add_from_source(&mut self, source: &CrateSource) -> Result<AddResult, IndexError> {
        let crate_id = source.to_crate_id();

        // Cycle detection: skip if already indexed
        if self.is_indexed(&crate_id) {
            return Ok(AddResult {
                crate_id,
                entity_count: 0,
                subcrates: Vec::new(),
            });
        }

        let (crate_data, raw_json) = loader::load_with_json(source)?;

        // Save metadata to disk
        let metadata_path = self.config.metadata_path_for_crate(&crate_id);
        std::fs::write(&metadata_path, &raw_json)?;

        // Convert to JSON for indexing and subcrate detection
        let entities = self.graph_to_json(&crate_data)?;

        // Index the parent crate
        let entity_count = self.index_crate(&crate_id, &entities)?;

        // Store in memory
        self.store.insert(crate_id.clone(), crate_data);

        // Update manifest
        self.manifest.add_crate(crate_id.clone());
        self.config.save_manifest(&self.manifest)?;

        // Discover and add subcrates based on source type
        let subcrates = self.discover_and_add_subcrates(source, &crate_id, &entities)?;

        Ok(AddResult {
            crate_id,
            entity_count,
            subcrates,
        })
    }

    /// Discover and add subcrates based on the source type
    fn discover_and_add_subcrates(
        &mut self,
        source: &CrateSource,
        parent_id: &str,
        entities: &[serde_json::Value],
    ) -> Result<Vec<AddResult>, IndexError> {
        match source {
            CrateSource::Url(_) | CrateSource::UrlSubcrate { .. } => {
                // For URL sources, use subjectOf or default metadata path
                let base_url = source.base_url();
                let subcrate_infos = detect_subcrates_from_url(entities, base_url.as_deref());
                self.add_url_subcrates(parent_id, subcrate_infos)
            }
            CrateSource::ZipFile(zip_path) | CrateSource::ZipSubcrate { zip_path, .. } => {
                // For zip sources, scan the archive for metadata files
                self.add_zip_subcrates(parent_id, zip_path, entities)
            }
            CrateSource::Directory(dir_path) => {
                // For directory sources, look for subdirectories with metadata
                self.add_directory_subcrates(parent_id, dir_path, entities)
            }
        }
    }

    /// Add subcrates discovered from URL-based sources
    fn add_url_subcrates(
        &mut self,
        parent_id: &str,
        subcrates: Vec<SubcrateInfo>,
    ) -> Result<Vec<AddResult>, IndexError> {
        let mut results = Vec::new();

        for info in subcrates {
            // Check if it's a relative path that should be an absolute URL
            let source = if info.metadata_url.starts_with("http://")
                || info.metadata_url.starts_with("https://")
            {
                // Absolute URL - could be external or same-origin subcrate
                CrateSource::UrlSubcrate {
                    parent_id: parent_id.to_string(),
                    metadata_url: info.metadata_url,
                }
            } else {
                // Relative path - this shouldn't happen for URL sources after resolution
                // but handle it just in case
                continue;
            };

            // Skip if already indexed (cycle detection)
            let subcrate_id = source.to_crate_id();
            if self.is_indexed(&subcrate_id) {
                continue;
            }

            match self.add_from_source(&source) {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Log but don't fail - subcrate might not be accessible
                    eprintln!("Warning: Failed to add subcrate {}: {}", info.entity_id, e);
                }
            }
        }

        Ok(results)
    }

    /// Add subcrates from within a zip archive
    fn add_zip_subcrates(
        &mut self,
        parent_id: &str,
        zip_path: &std::path::PathBuf,
        entities: &[serde_json::Value],
    ) -> Result<Vec<AddResult>, IndexError> {
        // Get potential subcrate entity IDs
        let entity_ids = get_subcrate_entity_ids(entities);
        if entity_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Check for URL subcrates first (external references from zip)
        let base_url = None; // Zips don't have base URLs
        let url_subcrates: Vec<_> = detect_subcrates_from_url(entities, base_url)
            .into_iter()
            .filter(|s| !s.is_relative)
            .collect();

        // Add URL subcrates
        let mut results = self.add_url_subcrates(parent_id, url_subcrates)?;

        // Find matching metadata files in the zip
        let zip_matches = find_subcrate_metadata_in_zip(zip_path, &entity_ids)?;

        for (entity_id, metadata_path) in zip_matches {
            let source = CrateSource::ZipSubcrate {
                parent_id: parent_id.to_string(),
                zip_path: zip_path.clone(),
                subpath: metadata_path,
            };

            // Skip if already indexed
            let subcrate_id = source.to_crate_id();
            if self.is_indexed(&subcrate_id) {
                continue;
            }

            match self.add_from_source(&source) {
                Ok(result) => results.push(result),
                Err(e) => {
                    eprintln!("Warning: Failed to add zip subcrate {}: {}", entity_id, e);
                }
            }
        }

        Ok(results)
    }

    /// Add subcrates from subdirectories
    fn add_directory_subcrates(
        &mut self,
        parent_id: &str,
        dir_path: &std::path::PathBuf,
        entities: &[serde_json::Value],
    ) -> Result<Vec<AddResult>, IndexError> {
        let entity_ids = get_subcrate_entity_ids(entities);
        if entity_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        for entity_id in entity_ids {
            // Normalize path
            let subpath = entity_id.trim_start_matches("./").trim_end_matches('/');
            let subdir = dir_path.join(subpath);

            if !subdir.is_dir() {
                continue;
            }

            // Look for metadata file in subdirectory
            let has_metadata = find_metadata_in_dir(&subdir);
            if !has_metadata {
                continue;
            }

            // Create a unique ID inheriting from parent
            let subcrate_id = format!("{}/{}", parent_id, subpath);

            // Skip if already indexed
            if self.is_indexed(&subcrate_id) {
                continue;
            }

            // We need to load and index manually to preserve the ID
            match self.add_directory_subcrate(&subcrate_id, &subdir) {
                Ok(result) => results.push(result),
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to add directory subcrate {}: {}",
                        entity_id, e
                    );
                }
            }
        }

        Ok(results)
    }

    /// Add a directory subcrate with a specific ID
    fn add_directory_subcrate(
        &mut self,
        crate_id: &str,
        dir_path: &std::path::PathBuf,
    ) -> Result<AddResult, IndexError> {
        let (crate_data, raw_json) = loader::load_from_directory_with_json(dir_path)?;

        // Save metadata
        let metadata_path = self.config.metadata_path_for_crate(crate_id);
        std::fs::write(&metadata_path, &raw_json)?;

        // Convert and index
        let entities = self.graph_to_json(&crate_data)?;
        let entity_count = self.index_crate(crate_id, &entities)?;

        // Store
        self.store.insert(crate_id.to_string(), crate_data);
        self.manifest.add_crate(crate_id.to_string());
        self.config.save_manifest(&self.manifest)?;

        // Recursive subcrate discovery
        let subcrates = self.add_directory_subcrates(crate_id, dir_path, &entities)?;

        Ok(AddResult {
            crate_id: crate_id.to_string(),
            entity_count,
            subcrates,
        })
    }

    /// Index entities for a crate
    fn index_crate(
        &mut self,
        crate_id: &str,
        entities: &[serde_json::Value],
    ) -> Result<usize, IndexError> {
        // Remove existing if present (update semantics)
        if self.store.contains(crate_id) {
            self.remove_from_index(crate_id)?;
        }

        let mut writer = self.search_index.writer()?;
        let count = self
            .search_index
            .index_entities(&mut writer, crate_id, entities)?;
        writer.commit()?;
        self.search_index.reload_reader()?;

        Ok(count)
    }

    /// Remove crate from search index only (not from store/manifest)
    fn remove_from_index(&mut self, crate_id: &str) -> Result<(), IndexError> {
        let mut writer = self.search_index.writer()?;
        self.search_index.remove_crate(&mut writer, crate_id);
        writer.commit()?;
        self.search_index.reload_reader()?;
        Ok(())
    }

    /// Add a crate from a path (convenience method)
    pub fn add_from_path(&mut self, path: &Path) -> Result<AddResult, IndexError> {
        let source = if path.is_dir() {
            CrateSource::Directory(path.to_path_buf())
        } else {
            CrateSource::ZipFile(path.to_path_buf())
        };
        self.add_from_source(&source)
    }

    /// Add a crate from a URL (convenience method)
    pub fn add_from_url(&mut self, url: &str) -> Result<AddResult, IndexError> {
        self.add_from_source(&CrateSource::Url(url.to_string()))
    }

    /// Remove a crate from the index
    pub fn remove(&mut self, crate_id: &str) -> Result<(), IndexError> {
        // Remove from search index
        self.remove_from_index(crate_id)?;

        // Remove from memory store
        self.store.remove(crate_id);

        // Remove metadata file
        let metadata_path = self.config.metadata_path_for_crate(crate_id);
        if metadata_path.exists() {
            std::fs::remove_file(metadata_path)?;
        }

        // Update manifest
        self.manifest.remove_crate(crate_id);
        self.config.save_manifest(&self.manifest)?;

        Ok(())
    }

    /// Full-text search
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, IndexError> {
        QueryEngine::new(&self.search_index).search(query, limit)
    }

    /// Search by entity type
    pub fn search_by_type(
        &self,
        type_name: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, IndexError> {
        QueryEngine::new(&self.search_index).search_by_type(type_name, limit)
    }

    /// Search by entity ID (exact match)
    pub fn search_by_id(&self, entity_id: &str) -> Result<Vec<SearchHit>, IndexError> {
        QueryEngine::new(&self.search_index).search_by_id(entity_id)
    }

    /// Combined type + content search
    pub fn search_typed(
        &self,
        type_name: &str,
        content: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, IndexError> {
        QueryEngine::new(&self.search_index).search_typed_content(type_name, content, limit)
    }

    /// Find all crate IDs containing a keyword
    pub fn find_crates(&self, query: &str) -> Result<HashSet<String>, IndexError> {
        QueryEngine::new(&self.search_index).find_crates(query)
    }

    /// Find all crates referencing an entity ID
    pub fn find_crates_by_entity(&self, entity_id: &str) -> Result<HashSet<String>, IndexError> {
        QueryEngine::new(&self.search_index).find_crates_by_entity(entity_id)
    }

    /// Get raw crate data from memory
    pub fn get_crate(&self, crate_id: &str) -> Option<&RoCrate> {
        self.store.get(crate_id)
    }

    /// Get raw crate metadata JSON from disk
    pub fn get_crate_json(&self, crate_id: &str) -> Result<Option<String>, IndexError> {
        if !self.manifest.contains(crate_id) {
            return Ok(None);
        }
        let metadata_path = self.config.metadata_path_for_crate(crate_id);
        if metadata_path.exists() {
            Ok(Some(std::fs::read_to_string(metadata_path)?))
        } else {
            Ok(None)
        }
    }

    /// List all indexed crate IDs
    pub fn list_crates(&self) -> Vec<String> {
        self.manifest.crates.clone()
    }

    /// Number of indexed crates
    pub fn crate_count(&self) -> usize {
        self.manifest.crates.len()
    }

    /// Convert RoCrate graph to JSON values for indexing
    fn graph_to_json(&self, crate_data: &RoCrate) -> Result<Vec<serde_json::Value>, IndexError> {
        let json = serde_json::to_value(crate_data.graph.clone())?;

        match json {
            serde_json::Value::Array(arr) => Ok(arr),
            _ => Err(IndexError::InvalidCrateFormat(
                "Expected @graph to be an array".to_string(),
            )),
        }
    }

    /// Add a crate directly from JSON string (for file uploads)
    pub fn add_from_json(
        &mut self,
        json_str: &str,
        name_hint: &str,
    ) -> Result<AddResult, IndexError> {
        // Parse the JSON
        let crate_data = rocraters::ro_crate::read::read_crate_obj(json_str, 0).map_err(|e| {
            IndexError::LoadError {
                path: name_hint.to_string(),
                reason: format!("{:#?}", e),
            }
        })?;

        // Generate a crate ID using ULID
        let crate_id = format!("{}/{}", ulid::Ulid::new(), name_hint);

        // Cycle detection
        if self.is_indexed(&crate_id) {
            return Ok(AddResult {
                crate_id,
                entity_count: 0,
                subcrates: Vec::new(),
            });
        }

        // Save metadata to disk
        let metadata_path = self.config.metadata_path_for_crate(&crate_id);
        std::fs::write(&metadata_path, json_str)?;

        // Convert to JSON for indexing
        let entities = self.graph_to_json(&crate_data)?;

        // Index the crate
        let entity_count = self.index_crate(&crate_id, &entities)?;

        // Store in memory
        self.store.insert(crate_id.clone(), crate_data);

        // Update manifest
        self.manifest.add_crate(crate_id.clone());
        self.config.save_manifest(&self.manifest)?;

        // For JSON uploads, check for URL subcrates only (no local file access)
        let url_subcrates: Vec<_> = detect_subcrates_from_url(&entities, None)
            .into_iter()
            .filter(|s| !s.is_relative)
            .collect();
        let subcrates = self.add_url_subcrates(&crate_id, url_subcrates)?;

        Ok(AddResult {
            crate_id,
            entity_count,
            subcrates,
        })
    }
}

/// Check if a directory contains a metadata file
fn find_metadata_in_dir(dir: &std::path::Path) -> bool {
    if dir.join("ro-crate-metadata.json").exists() {
        return true;
    }

    // Look for *-ro-crate-metadata.json
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with("-ro-crate-metadata.json") {
                    return true;
                }
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_index() {
        let index = CrateIndex::new_in_memory().unwrap();
        assert_eq!(index.crate_count(), 0);
    }

    #[test]
    fn test_shared_index() {
        let index = CrateIndex::new_in_memory().unwrap().into_shared();

        {
            let guard = index.read().unwrap();
            assert_eq!(guard.crate_count(), 0);
        }
    }

    #[test]
    fn test_cycle_detection() {
        let index = CrateIndex::new_in_memory().unwrap();
        assert!(!index.is_indexed("test-crate"));
    }
}
