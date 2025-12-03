use std::collections::HashSet;
use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, Occur, QueryParser, TermQuery},
    schema::{IndexRecordOption, Value},
    Term,
};

use crate::error::IndexError;
use crate::index::SearchIndex;

/// Known top-level fields that should not be prefixed with "properties."
const KNOWN_FIELDS: &[&str] = &["id", "occurs_in", "entity_type", "content", "properties"];

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

    /// Preprocess a query to add "properties." prefix to unknown field paths
    ///
    /// Examples:
    /// - "author.name:Smith" -> "properties.author.name:Smith"
    /// - "content:Smith" -> "content:Smith" (known field, unchanged)
    /// - "Smith" -> "Smith" (no field, unchanged)
    fn preprocess_query(&self, query_str: &str) -> String {
        // Simple preprocessing: find field:value patterns and prefix unknown fields
        let mut result = String::new();
        let mut chars = query_str.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '"' {
                // Inside quoted string, copy as-is until closing quote
                result.push(c);
                while let Some(inner) = chars.next() {
                    result.push(inner);
                    if inner == '"' {
                        break;
                    }
                    if inner == '\\' {
                        if let Some(escaped) = chars.next() {
                            result.push(escaped);
                        }
                    }
                }
            } else if c.is_alphabetic() || c == '_' || c == '@' {
                // Potential field name
                let mut word = String::new();
                word.push(c);

                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' || next == '.' || next == '@' {
                        word.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // Check if followed by colon (field query)
                if chars.peek() == Some(&':') {
                    chars.next(); // consume colon

                    // Check if this is a known field
                    let field_root = word.split('.').next().unwrap_or(&word);
                    if KNOWN_FIELDS.contains(&field_root) {
                        result.push_str(&word);
                        result.push(':');
                    } else {
                        // Prefix with properties.
                        result.push_str("properties.");
                        result.push_str(&word);
                        result.push(':');
                    }
                } else {
                    result.push_str(&word);
                }
            } else {
                result.push(c);
            }
        }

        result
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
    fn test_preprocess_query_unknown_field() {
        let index = SearchIndex::new_in_memory().unwrap();
        let engine = QueryEngine::new(&index);

        assert_eq!(
            engine.preprocess_query("author.name:Smith"),
            "properties.author.name:Smith"
        );
        assert_eq!(engine.preprocess_query("name:Test"), "properties.name:Test");
    }

    #[test]
    fn test_preprocess_query_mixed() {
        let index = SearchIndex::new_in_memory().unwrap();
        let engine = QueryEngine::new(&index);

        assert_eq!(
            engine.preprocess_query("entity_type:Person AND author.name:Smith"),
            "entity_type:Person AND properties.author.name:Smith"
        );
    }
}
