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
const ROCRATE_PROFILE_PREFIX: &str = "https://w3id.org/ro/crate/";

/// Information about a discovered subcrate
#[derive(Debug, Clone)]
pub struct SubcrateInfo {
    /// The @id of the subcrate entity
    pub entity_id: String,
    /// The URL to the subcrate's metadata file (from subjectOf)
    pub metadata_url: String,
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
fn conforms_to_rocrate(entity: &Value) -> bool {
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

/// Detect subcrates in a list of entities
/// Returns information needed to fetch each subcrate
pub fn detect_subcrates(entities: &[Value], base_url: Option<&str>) -> Vec<SubcrateInfo> {
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

        // Must have subjectOf pointing to metadata
        let metadata_url = match extract_subject_of_url(entity) {
            Some(url) => url,
            None => continue,
        };

        let entity_id = match extract_id(entity) {
            Some(id) => id.to_string(),
            None => continue,
        };

        // Resolve relative URL against base
        let resolved_url = resolve_url(&metadata_url, base_url);

        subcrates.push(SubcrateInfo {
            entity_id,
            metadata_url: resolved_url,
        });
    }

    subcrates
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
    fn test_detect_subcrates() {
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

        let subcrates = detect_subcrates(&entities, None);
        assert_eq!(subcrates.len(), 1);
        assert_eq!(subcrates[0].entity_id, "https://example.org/subcrate/");
        assert_eq!(
            subcrates[0].metadata_url,
            "https://example.org/subcrate/ro-crate-metadata.json"
        );
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
}
