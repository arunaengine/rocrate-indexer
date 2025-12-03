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

use crate::error::IndexError;
use crate::index::SearchIndex;
use crate::loader::CrateSource;
use crate::query::{QueryEngine, SearchHit};
use crate::store::CrateStore;

/// Thread-safe RO-Crate index
pub type SharedCrateIndex = Arc<RwLock<CrateIndex>>;

/// Main interface for indexing and searching RO-Crates
pub struct CrateIndex {
    store: CrateStore,
    search_index: SearchIndex,
}

impl CrateIndex {
    /// Create a new in-memory index
    pub fn new_in_memory() -> Result<Self, IndexError> {
        Ok(Self {
            store: CrateStore::new(),
            search_index: SearchIndex::new_in_memory()?,
        })
    }

    /// Create or open a persistent index
    pub fn open_or_create(index_path: &Path) -> Result<Self, IndexError> {
        Ok(Self {
            store: CrateStore::new(),
            search_index: SearchIndex::open_or_create(index_path)?,
        })
    }

    /// Wrap in Arc<RwLock<>> for shared access
    pub fn into_shared(self) -> SharedCrateIndex {
        Arc::new(RwLock::new(self))
    }

    /// Add a pre-loaded RoCrate
    pub fn add(&mut self, crate_id: String, crate_data: RoCrate) -> Result<usize, IndexError> {
        // Remove existing if present (update semantics)
        if self.store.contains(&crate_id) {
            self.remove(&crate_id)?;
        }

        // Convert graph to JSON values
        let entities = self.graph_to_json(&crate_data)?;

        // Index entities
        let mut writer = self.search_index.writer()?;
        let count = self
            .search_index
            .index_entities(&mut writer, &crate_id, &entities)?;
        writer.commit()?;

        // Store raw crate
        self.store.insert(crate_id, crate_data);
        self.search_index.reload_reader()?;

        Ok(count)
    }

    /// Load and add from a source (path, zip, url)
    pub fn add_from_source(&mut self, source: &CrateSource) -> Result<usize, IndexError> {
        let crate_id = source.to_crate_id();
        let crate_data = loader::load(source)?;
        self.add(crate_id, crate_data)
    }

    /// Load and add from a path (convenience method)
    pub fn add_from_path(&mut self, path: &Path) -> Result<usize, IndexError> {
        let source = if path.is_dir() {
            CrateSource::Directory(path.to_path_buf())
        } else {
            CrateSource::ZipFile(path.to_path_buf())
        };
        self.add_from_source(&source)
    }

    /// Remove a crate from the index
    pub fn remove(&mut self, crate_id: &str) -> Result<(), IndexError> {
        let mut writer = self.search_index.writer()?;
        self.search_index.remove_crate(&mut writer, crate_id);
        writer.commit()?;

        self.store.remove(crate_id);
        self.search_index.reload_reader()?;

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

    /// Get raw crate data
    pub fn get_crate(&self, crate_id: &str) -> Option<&RoCrate> {
        self.store.get(crate_id)
    }

    /// List all indexed crate IDs
    pub fn list_crates(&self) -> Vec<String> {
        self.store.list_crates().map(String::from).collect()
    }

    /// Number of indexed crates
    pub fn crate_count(&self) -> usize {
        self.store.len()
    }

    /// Convert RoCrate graph to JSON values for indexing
    fn graph_to_json(&self, crate_data: &RoCrate) -> Result<Vec<serde_json::Value>, IndexError> {
        // Serialize the whole crate to get the @graph array
        let json = serde_json::to_value(crate_data)?;

        match json.get("@graph") {
            Some(serde_json::Value::Array(arr)) => Ok(arr.clone()),
            _ => Ok(vec![]),
        }
    }
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
}
