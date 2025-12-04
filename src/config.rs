use std::collections::HashMap;
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

/// Information about an indexed crate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateEntry {
    /// The unique identifier for this crate
    pub crate_id: String,
    /// Full ancestry path: list of crate IDs from root to this crate (inclusive)
    /// Single element for root-level crates, [parent_id, crate_id] for first-level subcrates, etc.
    pub full_path: Vec<String>,
    /// Human-readable name extracted from the crate metadata
    pub name: Option<String>,
    /// Description extracted from the crate metadata
    pub description: Option<String>,
}

impl CrateEntry {
    /// Create a new crate entry (root-level crate)
    pub fn new(crate_id: String) -> Self {
        let full_path = vec![crate_id.clone()];
        Self {
            crate_id,
            full_path,
            name: None,
            description: None,
        }
    }

    /// Create a crate entry with a parent path (subcrate)
    pub fn with_parent(crate_id: String, parent_path: Vec<String>) -> Self {
        let mut full_path = parent_path;
        full_path.push(crate_id.clone());
        Self {
            crate_id,
            full_path,
            name: None,
            description: None,
        }
    }

    /// Set the name
    pub fn with_name(mut self, name: Option<String>) -> Self {
        self.name = name;
        self
    }

    /// Set the description
    pub fn with_description(mut self, description: Option<String>) -> Self {
        self.description = description;
        self
    }

    /// Check if this is a root-level crate (no parents)
    pub fn is_root(&self) -> bool {
        self.full_path.len() <= 1
    }

    /// Get the direct parent crate ID, if any
    pub fn parent_id(&self) -> Option<&str> {
        if self.full_path.len() > 1 {
            self.full_path
                .get(self.full_path.len() - 2)
                .map(|s| s.as_str())
        } else {
            None
        }
    }
}

/// Manifest tracking all indexed crates with their metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    /// Map of crate_id to CrateEntry
    pub crates: HashMap<String, CrateEntry>,
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
    /// Add a crate entry to the manifest
    pub fn add_crate(&mut self, entry: CrateEntry) {
        self.crates.insert(entry.crate_id.clone(), entry);
    }

    /// Remove a crate from the manifest
    pub fn remove_crate(&mut self, crate_id: &str) {
        self.crates.remove(crate_id);
    }

    /// Check if a crate ID exists in the manifest
    pub fn contains(&self, crate_id: &str) -> bool {
        self.crates.contains_key(crate_id)
    }

    /// Get a crate entry by ID
    pub fn get(&self, crate_id: &str) -> Option<&CrateEntry> {
        self.crates.get(crate_id)
    }

    /// Get all crate IDs
    pub fn crate_ids(&self) -> Vec<String> {
        self.crates.keys().cloned().collect()
    }

    /// Get the number of indexed crates
    pub fn len(&self) -> usize {
        self.crates.len()
    }

    /// Check if the manifest is empty
    pub fn is_empty(&self) -> bool {
        self.crates.is_empty()
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

        let entry = CrateEntry::new("test".to_string())
            .with_name(Some("Test Crate".to_string()))
            .with_description(Some("A test crate".to_string()));

        manifest.add_crate(entry);
        assert!(manifest.contains("test"));

        // Get entry back
        let retrieved = manifest.get("test").unwrap();
        assert_eq!(retrieved.name, Some("Test Crate".to_string()));
        assert_eq!(retrieved.description, Some("A test crate".to_string()));

        // No duplicates - adding same ID replaces
        let entry2 =
            CrateEntry::new("test".to_string()).with_name(Some("Updated Name".to_string()));
        manifest.add_crate(entry2);
        assert_eq!(manifest.len(), 1);
        assert_eq!(
            manifest.get("test").unwrap().name,
            Some("Updated Name".to_string())
        );

        manifest.remove_crate("test");
        assert!(!manifest.contains("test"));
    }

    #[test]
    fn test_crate_entry_path() {
        let root = CrateEntry::new("root-crate".to_string());
        assert!(root.is_root());
        assert_eq!(root.parent_id(), None);
        assert_eq!(root.full_path, vec!["root-crate".to_string()]);

        // with_parent takes the parent's full_path (not including subcrate yet)
        let subcrate =
            CrateEntry::with_parent("subcrate".to_string(), vec!["root-crate".to_string()]);
        assert!(!subcrate.is_root());
        assert_eq!(subcrate.parent_id(), Some("root-crate"));
        assert_eq!(
            subcrate.full_path,
            vec!["root-crate".to_string(), "subcrate".to_string()]
        );

        let deep = CrateEntry::with_parent(
            "deep".to_string(),
            vec!["root-crate".to_string(), "subcrate".to_string()],
        );
        assert_eq!(deep.parent_id(), Some("subcrate"));
        assert_eq!(
            deep.full_path,
            vec![
                "root-crate".to_string(),
                "subcrate".to_string(),
                "deep".to_string(),
            ]
        );
    }
}
