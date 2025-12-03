use std::path::Path;
use tantivy::{
    Index, IndexReader, IndexSettings, IndexWriter, TantivyDocument, Term,
    directory::MmapDirectory,
    schema::{FAST, Field, STORED, STRING, Schema, TEXT},
};

use crate::error::IndexError;
use crate::extract::{extract_id, extract_text, extract_types, resolve_id};

const HEAP_SIZE: usize = 50_000_000; // 50MB

pub struct SearchIndex {
    pub index: Index,
    reader: IndexReader,
    pub(crate) id_field: Field,
    pub(crate) occurs_in_field: Field,
    pub(crate) entity_type_field: Field,
    pub(crate) content_field: Field,
    pub(crate) properties_field: Field,
}

impl SearchIndex {
    /// Create an in-memory index
    pub fn new_in_memory() -> Result<Self, IndexError> {
        let schema = Self::build_schema();
        let index = Index::create_in_ram(schema);
        Self::from_index(index)
    }

    /// Create or open a persistent index at the given path
    pub fn open_or_create(path: &Path) -> Result<Self, IndexError> {
        let schema = Self::build_schema();

        let index = if path.exists() {
            Index::open_in_dir(path)?
        } else {
            std::fs::create_dir_all(path)?;
            let dir = MmapDirectory::open(path)?;
            Index::create(dir, schema, IndexSettings::default())?
        };

        Self::from_index(index)
    }

    fn from_index(index: Index) -> Result<Self, IndexError> {
        let schema = index.schema();
        let reader = index.reader()?;

        Ok(Self {
            id_field: schema.get_field("id").unwrap(),
            occurs_in_field: schema.get_field("occurs_in").unwrap(),
            entity_type_field: schema.get_field("entity_type").unwrap(),
            content_field: schema.get_field("content").unwrap(),
            properties_field: schema.get_field("properties").unwrap(),
            index,
            reader,
        })
    }

    fn build_schema() -> Schema {
        let mut builder = Schema::builder();

        // Resolved entity @id
        builder.add_text_field("id", STRING | STORED | FAST);

        // Crate identifier this entity belongs to
        builder.add_text_field("occurs_in", STRING | STORED | FAST);

        // @type values (multi-valued)
        builder.add_text_field("entity_type", STRING | FAST);

        // Full-text content (not stored, just indexed)
        builder.add_text_field("content", TEXT);

        builder.add_json_field("properties", TEXT);

        builder.build()
    }

    pub fn writer(&self) -> Result<IndexWriter, IndexError> {
        Ok(self.index.writer(HEAP_SIZE)?)
    }

    pub fn reload_reader(&mut self) -> Result<(), IndexError> {
        self.reader.reload()?;
        Ok(())
    }

    pub fn searcher(&self) -> tantivy::Searcher {
        self.reader.searcher()
    }

    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    /// Index all entities from a crate's JSON-LD graph
    pub fn index_entities(
        &self,
        writer: &mut IndexWriter,
        crate_id: &str,
        entities: &[serde_json::Value],
    ) -> Result<usize, IndexError> {
        let mut count = 0;

        for entity in entities {
            let entity_id = match extract_id(entity) {
                Some(id) => id,
                None => continue, // Skip entities without @id
            };

            let resolved_id = resolve_id(entity_id, crate_id);
            let types = extract_types(entity);
            let content = extract_text(entity);

            let mut doc = TantivyDocument::new();
            doc.add_text(self.id_field, &resolved_id);
            doc.add_text(self.occurs_in_field, crate_id);
            doc.add_field_value(self.properties_field, entity);

            for t in &types {
                doc.add_text(self.entity_type_field, t);
            }

            if !content.is_empty() {
                doc.add_text(self.content_field, &content);
            }

            writer.add_document(doc)?;
            count += 1;
        }

        Ok(count)
    }

    /// Remove all indexed documents for a crate
    pub fn remove_crate(&self, writer: &mut IndexWriter, crate_id: &str) {
        let term = Term::from_field_text(self.occurs_in_field, crate_id);
        writer.delete_term(term);
    }
}
