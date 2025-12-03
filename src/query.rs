use std::collections::HashSet;
use tantivy::{
    Term,
    collector::TopDocs,
    query::{BooleanQuery, Occur, QueryParser, TermQuery},
    schema::{IndexRecordOption, Value},
};

use crate::error::IndexError;
use crate::index::SearchIndex;

/// A single search result
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub entity_id: String,
    pub crate_id: String,
    pub score: f32,
}

/// Query builder and executor
pub struct QueryEngine<'a> {
    index: &'a SearchIndex,
}

impl<'a> QueryEngine<'a> {
    pub fn new(index: &'a SearchIndex) -> Self {
        Self { index }
    }

    /// Preprocess a query - currently just passes through
    ///
    /// Since "properties" is a default field in the QueryParser, Tantivy will
    /// automatically search JSON paths in that field when an unknown field name
    /// is specified. For example, searching "author.name:Smith" will look for
    /// the path "author.name" in the default JSON fields.
    ///
    /// We do NOT prefix unknown fields with "properties." because:
    /// 1. Tantivy's QueryParser handles this automatically for default fields
    /// 2. Prefixing causes issues with how QueryParser parses field.path:value
    ///
    /// Examples:
    /// - "Smith" -> searches content and properties (all text values)
    /// - "content:Smith" -> searches content field
    /// - "author.name:Smith" -> searches properties JSON field at path author.name
    /// - "name:Test" -> searches properties JSON field at path name
    fn preprocess_query(&self, query_str: &str) -> String {
        // Pass through - QueryParser handles JSON paths in default fields automatically
        query_str.to_string()
    }

    /// Full-text search across content
    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<SearchHit>, IndexError> {
        let processed_query = self.preprocess_query(query_str);

        let parser = QueryParser::for_index(
            &self.index.index,
            vec![self.index.content_field, self.index.properties_field],
        );
        let query = parser.parse_query(&processed_query)?;

        let searcher = self.index.searcher();
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        self.collect_hits(&searcher, top_docs)
    }

    /// Search entities by @type
    pub fn search_by_type(
        &self,
        type_name: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, IndexError> {
        let term = Term::from_field_text(self.index.entity_type_field, type_name);
        let query = TermQuery::new(term, IndexRecordOption::Basic);

        let searcher = self.index.searcher();
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        self.collect_hits(&searcher, top_docs)
    }

    /// Find all occurrences of an entity by @id
    pub fn search_by_id(&self, entity_id: &str) -> Result<Vec<SearchHit>, IndexError> {
        let term = Term::from_field_text(self.index.id_field, entity_id);
        let query = TermQuery::new(term, IndexRecordOption::Basic);

        let searcher = self.index.searcher();
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1000))?;

        self.collect_hits(&searcher, top_docs)
    }

    /// Combined search: type + content
    /// E.g., find all Person entities matching "Smith"
    pub fn search_typed_content(
        &self,
        type_name: &str,
        content_query: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>, IndexError> {
        let type_term = Term::from_field_text(self.index.entity_type_field, type_name);
        let type_query = TermQuery::new(type_term, IndexRecordOption::Basic);

        let processed_query = self.preprocess_query(content_query);
        let content_parser = QueryParser::for_index(
            &self.index.index,
            vec![self.index.content_field, self.index.properties_field],
        );
        let content_query = content_parser.parse_query(&processed_query)?;

        let combined = BooleanQuery::new(vec![
            (Occur::Must, Box::new(type_query)),
            (Occur::Must, content_query),
        ]);

        let searcher = self.index.searcher();
        let top_docs = searcher.search(&combined, &TopDocs::with_limit(limit))?;

        self.collect_hits(&searcher, top_docs)
    }

    /// Get unique crate IDs matching a content query
    pub fn find_crates(&self, query_str: &str) -> Result<HashSet<String>, IndexError> {
        let hits = self.search(query_str, 10_000)?;
        Ok(hits.into_iter().map(|h| h.crate_id).collect())
    }

    /// Get all crates containing entities of a specific type
    pub fn find_crates_by_type(&self, type_name: &str) -> Result<HashSet<String>, IndexError> {
        let hits = self.search_by_type(type_name, 10_000)?;
        Ok(hits.into_iter().map(|h| h.crate_id).collect())
    }

    /// Get all crates referencing a specific entity ID (e.g., an ORCID)
    pub fn find_crates_by_entity(&self, entity_id: &str) -> Result<HashSet<String>, IndexError> {
        let hits = self.search_by_id(entity_id)?;
        Ok(hits.into_iter().map(|h| h.crate_id).collect())
    }

    fn collect_hits(
        &self,
        searcher: &tantivy::Searcher,
        top_docs: Vec<(f32, tantivy::DocAddress)>,
    ) -> Result<Vec<SearchHit>, IndexError> {
        let mut hits = Vec::with_capacity(top_docs.len());

        for (score, doc_addr) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_addr)?;

            let entity_id = doc
                .get_first(self.index.id_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let crate_id = doc
                .get_first(self.index.occurs_in_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            hits.push(SearchHit {
                entity_id,
                crate_id,
                score,
            });
        }

        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_query_no_field() {
        let index = SearchIndex::new_in_memory().unwrap();
        let engine = QueryEngine::new(&index);

        assert_eq!(engine.preprocess_query("Smith"), "Smith");
        assert_eq!(engine.preprocess_query("hello world"), "hello world");
    }

    #[test]
    fn test_preprocess_query_known_field() {
        let index = SearchIndex::new_in_memory().unwrap();
        let engine = QueryEngine::new(&index);

        assert_eq!(engine.preprocess_query("content:Smith"), "content:Smith");
        assert_eq!(
            engine.preprocess_query("entity_type:Person"),
            "entity_type:Person"
        );
    }

    #[test]
    fn test_preprocess_query_passthrough() {
        let index = SearchIndex::new_in_memory().unwrap();
        let engine = QueryEngine::new(&index);

        // Queries pass through unchanged - QueryParser handles JSON paths automatically
        assert_eq!(
            engine.preprocess_query("author.name:Smith"),
            "author.name:Smith"
        );
        assert_eq!(engine.preprocess_query("name:Test"), "name:Test");
    }

    #[test]
    fn test_preprocess_query_mixed() {
        let index = SearchIndex::new_in_memory().unwrap();
        let engine = QueryEngine::new(&index);

        assert_eq!(
            engine.preprocess_query("entity_type:Person AND author.name:Smith"),
            "entity_type:Person AND author.name:Smith"
        );
    }
}
