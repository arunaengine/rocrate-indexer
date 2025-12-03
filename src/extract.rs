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
}
