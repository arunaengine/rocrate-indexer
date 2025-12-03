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

    /// Full-text search across content
    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<SearchHit>, IndexError> {
        let parser = QueryParser::for_index(&self.index.index, vec![self.index.content_field]);
        let query = parser.parse_query(query_str)?;

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

        let content_parser =
            QueryParser::for_index(&self.index.index, vec![self.index.content_field]);
        let content_query = content_parser.parse_query(content_query)?;

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
