use rocraters::ro_crate::rocrate::RoCrate;
use std::collections::HashMap;

/// In-memory storage for raw RO-Crate data
pub struct CrateStore {
    crates: HashMap<String, RoCrate>,
}

impl CrateStore {
    pub fn new() -> Self {
        Self {
            crates: HashMap::new(),
        }
    }

    pub fn insert(&mut self, crate_id: String, crate_data: RoCrate) -> Option<RoCrate> {
        self.crates.insert(crate_id, crate_data)
    }

    pub fn get(&self, crate_id: &str) -> Option<&RoCrate> {
        self.crates.get(crate_id)
    }

    pub fn remove(&mut self, crate_id: &str) -> Option<RoCrate> {
        self.crates.remove(crate_id)
    }

    pub fn contains(&self, crate_id: &str) -> bool {
        self.crates.contains_key(crate_id)
    }

    pub fn list_crates(&self) -> impl Iterator<Item = &str> {
        self.crates.keys().map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.crates.len()
    }

    pub fn is_empty(&self) -> bool {
        self.crates.is_empty()
    }
}

impl Default for CrateStore {
    fn default() -> Self {
        Self::new()
    }
}
