use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::IndexError;

const INDEX_DIR_NAME: &str = ".rocrate-index";
const METADATA_DIR_NAME: &str = "metadata";
const INDEX_SUBDIR_NAME: &str = "index";
const MANIFEST_FILE_NAME: &str = "manifest.json";

/// Configuration for index paths and directories
#[derive(Debug, Clone)]
pub struct Config {
    base_dir: PathBuf,
}

/// Manifest tracking all indexed crate IDs
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub crates: Vec<String>,
}

impl Config {
    /// Create config with a specific base directory
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Create config using the current working directory
    pub fn from_current_dir() -> Result<Self, IndexError> {
        let cwd = std::env::current_dir()?;
        Ok(Self::new(cwd.join(INDEX_DIR_NAME)))
    }

    /// Get the base directory path
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Get the metadata storage directory path
    pub fn metadata_dir(&self) -> PathBuf {
        self.base_dir.join(METADATA_DIR_NAME)
    }

    /// Get the Tantivy index directory path
    pub fn index_dir(&self) -> PathBuf {
        self.base_dir.join(INDEX_SUBDIR_NAME)
    }

    /// Get the manifest file path
    pub fn manifest_path(&self) -> PathBuf {
        self.base_dir.join(MANIFEST_FILE_NAME)
    }

    /// Create all necessary directories if they don't exist
    pub fn ensure_directories(&self) -> Result<(), IndexError> {
        fs::create_dir_all(&self.base_dir)?;
        fs::create_dir_all(self.metadata_dir())?;
        Ok(())
    }

    /// Check if the index has been initialized
    pub fn is_initialized(&self) -> bool {
        self.base_dir.exists() && self.manifest_path().exists()
    }

    /// Load manifest from disk, or return empty manifest if not exists
    pub fn load_manifest(&self) -> Result<Manifest, IndexError> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(Manifest::default());
        }
        let content = fs::read_to_string(&path)?;
        let manifest: Manifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    /// Save manifest to disk
    pub fn save_manifest(&self, manifest: &Manifest) -> Result<(), IndexError> {
        let path = self.manifest_path();
        let content = serde_json::to_string_pretty(manifest)?;
        fs::write(path, content)?;
        Ok(())
    }

    /// Get the path where a crate's metadata should be stored
    /// Uses a hash of the crate_id for the filename to handle URLs safely
    pub fn metadata_path_for_crate(&self, crate_id: &str) -> PathBuf {
        let hash = hash_crate_id(crate_id);
        self.metadata_dir().join(format!("{}.json", hash))
    }
}

impl Manifest {
    /// Add a crate ID to the manifest (no duplicates)
    pub fn add_crate(&mut self, crate_id: String) {
        if !self.crates.contains(&crate_id) {
            self.crates.push(crate_id);
        }
    }

    /// Remove a crate ID from the manifest
    pub fn remove_crate(&mut self, crate_id: &str) {
        self.crates.retain(|id| id != crate_id);
    }

    /// Check if a crate ID exists in the manifest
    pub fn contains(&self, crate_id: &str) -> bool {
        self.crates.iter().any(|id| id == crate_id)
    }
}

/// Generate a stable hash string for a crate ID (used for filenames)
fn hash_crate_id(s: &str) -> String {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_stability() {
        let id = "https://example.org/crate";
        let hash1 = hash_crate_id(id);
        let hash2 = hash_crate_id(id);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_manifest_operations() {
        let mut manifest = Manifest::default();
        assert!(!manifest.contains("test"));

        manifest.add_crate("test".to_string());
        assert!(manifest.contains("test"));

        // No duplicates
        manifest.add_crate("test".to_string());
        assert_eq!(manifest.crates.len(), 1);

        manifest.remove_crate("test");
        assert!(!manifest.contains("test"));
    }
}
