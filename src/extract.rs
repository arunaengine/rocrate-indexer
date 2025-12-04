use serde_json::Value;

const TEXT_FIELDS: &[&str] = &[
    "name",
    "description",
    "alternateName",
    "keywords",
    "abstract",
    "text",
    "headline",
    "about",
];

const PERSON_FIELDS: &[&str] = &["author", "creator", "contributor", "publisher"];

/// RO-Crate conformsTo URL prefix
const ROCRATE_PROFILE_PREFIX: &str = "https://w3id.org/ro/crate";

/// Information about a discovered subcrate
#[derive(Debug, Clone)]
pub struct SubcrateInfo {
    /// The @id of the subcrate entity
    pub entity_id: String,
    /// The resolved URL/path to the subcrate's metadata file
    pub metadata_url: String,
    /// Whether this is a relative path (for zip extraction) or absolute URL
    pub is_relative: bool,
}

/// Metadata extracted from the root entity of an RO-Crate
#[derive(Debug, Clone, Default)]
pub struct RootMetadata {
    /// Human-readable name of the crate
    pub name: Option<String>,
    /// Description of the crate
    pub description: Option<String>,
}

/// Extract full-text searchable content from a JSON-LD entity
pub fn extract_text(entity: &Value) -> String {
    let mut parts = Vec::new();

    // Direct text fields
    for field in TEXT_FIELDS {
        if let Some(value) = entity.get(field) {
            collect_strings(&mut parts, value);
        }
    }

    // Nested person/org name fields
    for field in PERSON_FIELDS {
        if let Some(value) = entity.get(field) {
            collect_names(&mut parts, value);
        }
    }

    parts.join(" ")
}

fn collect_strings(parts: &mut Vec<String>, value: &Value) {
    match value {
        Value::String(s) => parts.push(s.clone()),
        Value::Array(arr) => {
            for v in arr {
                collect_strings(parts, v);
            }
        }
        _ => {}
    }
}

fn collect_names(parts: &mut Vec<String>, value: &Value) {
    match value {
        Value::Object(obj) => {
            if let Some(Value::String(name)) = obj.get("name") {
                parts.push(name.clone());
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_names(parts, v);
            }
        }
        _ => {}
    }
}

/// Extract @type as a list of type names
pub fn extract_types(entity: &Value) -> Vec<String> {
    match entity.get("@type") {
        Some(Value::String(t)) => vec![t.clone()],
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => vec![],
    }
}

/// Extract @id from entity
pub fn extract_id(entity: &Value) -> Option<&str> {
    entity.get("@id").and_then(|v| v.as_str())
}

/// Check if an entity conforms to the RO-Crate specification
pub fn conforms_to_rocrate(entity: &Value) -> bool {
    let conforms_to = match entity.get("conformsTo") {
        Some(v) => v,
        None => return false,
    };

    // Can be a single object or array
    let check_id = |v: &Value| -> bool {
        v.get("@id")
            .and_then(|id| id.as_str())
            .map(|id| id.starts_with(ROCRATE_PROFILE_PREFIX))
            .unwrap_or(false)
    };

    match conforms_to {
        Value::Object(_) => check_id(conforms_to),
        Value::Array(arr) => arr.iter().any(check_id),
        Value::String(s) => s.starts_with(ROCRATE_PROFILE_PREFIX),
        _ => false,
    }
}

/// Extract the metadata URL from subjectOf property
fn extract_subject_of_url(entity: &Value) -> Option<String> {
    let subject_of = entity.get("subjectOf")?;

    // Can be object with @id, or array of objects
    let extract_id =
        |v: &Value| -> Option<String> { v.get("@id").and_then(|id| id.as_str()).map(String::from) };

    match subject_of {
        Value::Object(_) => extract_id(subject_of),
        Value::Array(arr) => {
            // Find first entry that looks like a metadata file
            for item in arr {
                if let Some(id) = extract_id(item) {
                    if id.contains("ro-crate-metadata") || id.ends_with(".json") {
                        return Some(id);
                    }
                }
            }
            // Fallback to first entry
            arr.first().and_then(extract_id)
        }
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Find the root Dataset entity in an RO-Crate graph
/// The root is typically the entity with @id "./" that conforms to RO-Crate
pub fn find_root_entity(entities: &[Value]) -> Option<&Value> {
    // First, try to find entity with @id "./" that is a Dataset
    for entity in entities {
        let id = extract_id(entity);
        let types = extract_types(entity);

        if id == Some("./") && types.iter().any(|t| t == "Dataset") {
            return Some(entity);
        }
    }

    // Fallback: find any Dataset that conforms to RO-Crate spec (but not a subcrate reference)
    for entity in entities {
        let types = extract_types(entity);
        if !types.iter().any(|t| t == "Dataset") {
            continue;
        }

        // Must conform to RO-Crate
        if !conforms_to_rocrate(entity) {
            continue;
        }

        // Skip if it looks like a subcrate reference (has subjectOf pointing elsewhere)
        // The root entity typically doesn't have subjectOf, or it points to the metadata descriptor
        let id = extract_id(entity).unwrap_or("");
        if id != "./" && !id.is_empty() {
            // This might be a subcrate reference, not the root
            // Check if there's a subjectOf that's not the local metadata
            if let Some(subject_of) = extract_subject_of_url(entity) {
                if !subject_of.starts_with("ro-crate-metadata")
                    && !subject_of.ends_with("ro-crate-metadata.json")
                {
                    continue;
                }
            }
        }

        return Some(entity);
    }

    None
}

/// Extract metadata (name, description) from the root entity of an RO-Crate
pub fn extract_root_metadata(entities: &[Value]) -> RootMetadata {
    let root = match find_root_entity(entities) {
        Some(entity) => entity,
        None => return RootMetadata::default(),
    };

    let name = root
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| clean_name(s));

    let description = root
        .get("description")
        .and_then(|v| v.as_str())
        .map(String::from);

    RootMetadata { name, description }
}

/// Clean up a name value (remove "./" prefix if present, trim whitespace)
fn clean_name(name: &str) -> String {
    let cleaned = name.trim();
    let cleaned = cleaned.strip_prefix("./").unwrap_or(cleaned);
    cleaned.to_string()
}

/// Find all Dataset entities that conform to RO-Crate (potential subcrates)
/// Returns entity IDs for further processing
pub fn find_potential_subcrates(entities: &[Value]) -> Vec<String> {
    let mut subcrate_ids = Vec::new();

    for entity in entities {
        // Must be a Dataset
        let types = extract_types(entity);
        if !types.iter().any(|t| t == "Dataset") {
            continue;
        }

        // Must conform to RO-Crate spec
        if !conforms_to_rocrate(entity) {
            continue;
        }

        if let Some(id) = extract_id(entity) {
            // Skip the root entity
            if id == "./" {
                continue;
            }
            subcrate_ids.push(id.to_string());
        }
    }

    subcrate_ids
}

/// Detect subcrates from URL-based crates
/// Uses subjectOf if available, otherwise defaults to <id>/ro-crate-metadata.json
pub fn detect_subcrates_from_url(entities: &[Value], base_url: Option<&str>) -> Vec<SubcrateInfo> {
    let mut subcrates = Vec::new();

    for entity in entities {
        // Must be a Dataset
        let types = extract_types(entity);
        if !types.iter().any(|t| t == "Dataset") {
            continue;
        }

        // Must conform to RO-Crate spec
        if !conforms_to_rocrate(entity) {
            continue;
        }

        let entity_id = match extract_id(entity) {
            Some(id) => id.to_string(),
            None => continue,
        };

        // Skip the root entity
        if entity_id == "./" {
            continue;
        }

        // Try to get metadata URL from subjectOf
        let metadata_url = if let Some(subject_of) = extract_subject_of_url(entity) {
            subject_of
        } else {
            // Default to <id>/ro-crate-metadata.json
            let id_normalized = entity_id.trim_end_matches('/');
            format!("{}/ro-crate-metadata.json", id_normalized)
        };

        // Check if it's already absolute or needs resolution
        let is_relative =
            !metadata_url.starts_with("http://") && !metadata_url.starts_with("https://");

        let resolved_url = if is_relative {
            resolve_url(&metadata_url, base_url)
        } else {
            metadata_url.clone()
        };

        subcrates.push(SubcrateInfo {
            entity_id,
            metadata_url: resolved_url,
            is_relative,
        });
    }

    subcrates
}

/// Detect subcrates for local/zip sources
/// Returns entity IDs that should be checked against the zip contents
pub fn get_subcrate_entity_ids(entities: &[Value]) -> Vec<String> {
    find_potential_subcrates(entities)
}

/// Resolve a potentially relative URL against a base URL
pub fn resolve_url(url: &str, base: Option<&str>) -> String {
    // Already absolute
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_string();
    }

    let base = match base {
        Some(b) => b,
        None => return url.to_string(),
    };

    let base = base.trim_end_matches('/');

    if url.starts_with("./") {
        format!("{}/{}", base, &url[2..])
    } else if url.starts_with('/') {
        // Absolute path - need to extract origin from base
        if let Some(idx) = base.find("://") {
            if let Some(end) = base[idx + 3..].find('/') {
                let origin = &base[..idx + 3 + end];
                return format!("{}{}", origin, url);
            }
        }
        format!("{}{}", base, url)
    } else {
        format!("{}/{}", base, url)
    }
}

/// Resolve a relative @id against the crate's base identifier
pub fn resolve_id(entity_id: &str, crate_base: &str) -> String {
    // Already absolute
    if entity_id.starts_with("http://") || entity_id.starts_with("https://") {
        return entity_id.to_string();
    }

    let base = crate_base.trim_end_matches('/');

    match entity_id {
        "./" => base.to_string(),
        id if id.starts_with("./") => format!("{}/{}", base, &id[2..]),
        id if id.starts_with('#') => format!("{}{}", crate_base, id),
        id => format!("{}/{}", base, id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_relative_ids() {
        let base = "https://example.org/crate/123";

        assert_eq!(resolve_id("./", base), "https://example.org/crate/123");
        assert_eq!(
            resolve_id("./data.csv", base),
            "https://example.org/crate/123/data.csv"
        );
        assert_eq!(
            resolve_id("#person1", base),
            "https://example.org/crate/123#person1"
        );
        assert_eq!(
            resolve_id("https://orcid.org/0000-0001", base),
            "https://orcid.org/0000-0001"
        );
    }

    #[test]
    fn test_extract_types_single() {
        let entity = serde_json::json!({"@type": "Person"});
        assert_eq!(extract_types(&entity), vec!["Person"]);
    }

    #[test]
    fn test_extract_types_multiple() {
        let entity = serde_json::json!({"@type": ["Dataset", "SoftwareSourceCode"]});
        assert_eq!(
            extract_types(&entity),
            vec!["Dataset", "SoftwareSourceCode"]
        );
    }

    #[test]
    fn test_conforms_to_rocrate() {
        let entity = serde_json::json!({
            "@type": "Dataset",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
        });
        assert!(conforms_to_rocrate(&entity));

        let entity_array = serde_json::json!({
            "@type": "Dataset",
            "conformsTo": [
                {"@id": "https://w3id.org/ro/crate/1.1"},
                {"@id": "https://example.org/other"}
            ]
        });
        assert!(conforms_to_rocrate(&entity_array));

        let not_rocrate = serde_json::json!({
            "@type": "Dataset",
            "conformsTo": {"@id": "https://example.org/other"}
        });
        assert!(!conforms_to_rocrate(&not_rocrate));
    }

    #[test]
    fn test_detect_subcrates_with_subject_of() {
        let entities = vec![
            serde_json::json!({
                "@id": "https://example.org/subcrate/",
                "@type": "Dataset",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "subjectOf": {"@id": "https://example.org/subcrate/ro-crate-metadata.json"}
            }),
            serde_json::json!({
                "@id": "./data.csv",
                "@type": "File"
            }),
        ];

        let subcrates = detect_subcrates_from_url(&entities, None);
        assert_eq!(subcrates.len(), 1);
        assert_eq!(subcrates[0].entity_id, "https://example.org/subcrate/");
        assert_eq!(
            subcrates[0].metadata_url,
            "https://example.org/subcrate/ro-crate-metadata.json"
        );
    }

    #[test]
    fn test_detect_subcrates_without_subject_of() {
        let entities = vec![serde_json::json!({
            "@id": "./experiments/",
            "@type": "Dataset",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
        })];

        let base_url = Some("https://example.org/parent");
        let subcrates = detect_subcrates_from_url(&entities, base_url);

        assert_eq!(subcrates.len(), 1);
        assert_eq!(subcrates[0].entity_id, "./experiments/");
        assert_eq!(
            subcrates[0].metadata_url,
            "https://example.org/parent/experiments/ro-crate-metadata.json"
        );
    }

    #[test]
    fn test_find_potential_subcrates() {
        let entities = vec![
            serde_json::json!({
                "@id": "./",
                "@type": "Dataset",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "name": "Root Crate"
            }),
            serde_json::json!({
                "@id": "./experiments/",
                "@type": "Dataset",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
            }),
            serde_json::json!({
                "@id": "./data/",
                "@type": "Dataset",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}
            }),
            serde_json::json!({
                "@id": "./regular/",
                "@type": "Dataset"
                // No conformsTo - not a subcrate
            }),
        ];

        let ids = find_potential_subcrates(&entities);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"./experiments/".to_string()));
        assert!(ids.contains(&"./data/".to_string()));
        // Should NOT contain "./" (root) or "./regular/" (no conformsTo)
        assert!(!ids.contains(&"./".to_string()));
    }

    #[test]
    fn test_resolve_url() {
        let base = Some("https://example.org/crates/parent");

        assert_eq!(
            resolve_url("./subcrate/metadata.json", base),
            "https://example.org/crates/parent/subcrate/metadata.json"
        );
        assert_eq!(
            resolve_url("subcrate/metadata.json", base),
            "https://example.org/crates/parent/subcrate/metadata.json"
        );
        assert_eq!(
            resolve_url("https://other.org/crate.json", base),
            "https://other.org/crate.json"
        );
    }

    #[test]
    fn test_find_root_entity() {
        let entities = vec![
            serde_json::json!({
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "about": {"@id": "./"}
            }),
            serde_json::json!({
                "@id": "./",
                "@type": "Dataset",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "name": "My Research Data",
                "description": "A dataset about interesting things"
            }),
            serde_json::json!({
                "@id": "./subcrate/",
                "@type": "Dataset",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "name": "Subcrate"
            }),
        ];

        let root = find_root_entity(&entities);
        assert!(root.is_some());
        let root = root.unwrap();
        assert_eq!(extract_id(root), Some("./"));
        assert_eq!(
            root.get("name").and_then(|v| v.as_str()),
            Some("My Research Data")
        );
    }

    #[test]
    fn test_extract_root_metadata() {
        let entities = vec![serde_json::json!({
            "@id": "./",
            "@type": "Dataset",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
            "name": "My Research Data",
            "description": "A dataset containing experimental results"
        })];

        let metadata = extract_root_metadata(&entities);
        assert_eq!(metadata.name, Some("My Research Data".to_string()));
        assert_eq!(
            metadata.description,
            Some("A dataset containing experimental results".to_string())
        );
    }

    #[test]
    fn test_extract_root_metadata_cleans_name() {
        let entities = vec![serde_json::json!({
            "@id": "./",
            "@type": "Dataset",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
            "name": "./my-crate",
            "description": "Test"
        })];

        let metadata = extract_root_metadata(&entities);
        assert_eq!(metadata.name, Some("my-crate".to_string()));
    }

    #[test]
    fn test_extract_root_metadata_missing() {
        let entities = vec![serde_json::json!({
            "@id": "./data.csv",
            "@type": "File"
        })];

        let metadata = extract_root_metadata(&entities);
        assert!(metadata.name.is_none());
        assert!(metadata.description.is_none());
    }
}
