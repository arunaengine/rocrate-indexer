use axum::{
    Json, Router,
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use rocrate_indexer::{CrateIndex, CrateSource, SharedCrateIndex};

// === API Documentation ===

#[derive(OpenApi)]
#[openapi(
    paths(
        add_crate_by_url,
        add_crate_by_upload,
        list_crates,
        get_crate,
        get_crate_info,
        remove_crate,
        search,
    ),
    components(
        schemas(
            AddCrateByUrlRequest,
            AddCrateResponse,
            CrateAddedInfo,
            ListCratesResponse,
            CrateInfoResponse,
            SearchParams,
            SearchResponse,
            SearchHitResponse,
            ErrorResponse,
        )
    ),
    tags(
        (name = "crates", description = "RO-Crate management endpoints"),
        (name = "search", description = "Search endpoints")
    )
)]
struct ApiDoc;

// === Request/Response Types ===

#[derive(Debug, Deserialize, ToSchema)]
struct AddCrateByUrlRequest {
    /// URL to ro-crate-metadata.json or directory containing it
    url: String,
}

/// Information about a single crate that was added
#[derive(Debug, Serialize, ToSchema)]
struct CrateAddedInfo {
    /// The unique identifier for the crate
    crate_id: String,
    /// Number of entities indexed from this crate
    entity_count: usize,
    /// Whether this was a subcrate discovered during indexing
    is_subcrate: bool,
}

#[derive(Debug, Serialize, ToSchema)]
struct AddCrateResponse {
    /// The primary crate that was added
    primary_crate: CrateAddedInfo,
    /// All subcrates that were discovered and indexed (flattened)
    subcrates: Vec<CrateAddedInfo>,
    /// Total number of crates added (primary + subcrates)
    total_crates_added: usize,
}

#[derive(Debug, Serialize, ToSchema)]
struct ListCratesResponse {
    /// List of all indexed crate IDs (when full=false)
    #[serde(skip_serializing_if = "Option::is_none")]
    crates: Option<Vec<String>>,
    /// List of full crate info (when full=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    entries: Option<Vec<CrateInfoResponse>>,
    /// Total count of indexed crates
    count: usize,
}

/// Short info about an indexed crate
#[derive(Debug, Serialize, ToSchema)]
struct CrateInfoResponse {
    /// The unique identifier for this crate
    crate_id: String,
    /// Full path including this crate (from root to this crate)
    full_path: Vec<String>,
    /// Human-readable name extracted from the crate metadata
    name: Option<String>,
    /// Description extracted from the crate metadata  
    description: Option<String>,
    /// Whether this is a root-level crate (no parents)
    is_root: bool,
    /// The direct parent crate ID, if any
    parent_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
struct ListCratesParams {
    /// Return full info for each crate (default: false, returns only IDs)
    #[serde(default)]
    full: bool,
}

#[derive(Debug, Deserialize, ToSchema, utoipa::IntoParams)]
struct SearchParams {
    /// Tantivy query string
    ///
    /// Examples:
    /// - "e.coli" - full text search
    /// - "entity_type:Person" - search by type  
    /// - "author.name:Smith" - search by nested property
    /// - "name:Test AND entity_type:Dataset" - boolean query
    q: String,
    /// Maximum number of results (default: 10)
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    10
}

#[derive(Debug, Serialize, ToSchema)]
struct SearchResponse {
    /// Search results
    hits: Vec<SearchHitResponse>,
    /// Total number of hits returned
    count: usize,
}

#[derive(Debug, Serialize, ToSchema)]
struct SearchHitResponse {
    /// The @id of the matching entity
    entity_id: String,
    /// The crate ID containing this entity
    crate_id: String,
    /// Relevance score
    score: f32,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorResponse {
    /// Error message
    error: String,
}

// === Helper Functions ===

/// Flatten the recursive AddResult into a flat response
fn convert_add_result(result: rocrate_indexer::AddResult) -> AddCrateResponse {
    let mut subcrates = Vec::new();
    collect_subcrates(&result.subcrates, &mut subcrates);

    let total = 1 + subcrates.len();

    AddCrateResponse {
        primary_crate: CrateAddedInfo {
            crate_id: result.crate_id,
            entity_count: result.entity_count,
            is_subcrate: false,
        },
        subcrates,
        total_crates_added: total,
    }
}

/// Recursively collect all subcrates into a flat list
fn collect_subcrates(results: &[rocrate_indexer::AddResult], out: &mut Vec<CrateAddedInfo>) {
    for result in results {
        out.push(CrateAddedInfo {
            crate_id: result.crate_id.clone(),
            entity_count: result.entity_count,
            is_subcrate: true,
        });
        collect_subcrates(&result.subcrates, out);
    }
}

// === Handlers ===

/// Add an RO-Crate from a URL
#[utoipa::path(
    post,
    path = "/crates/url",
    tag = "crates",
    request_body = AddCrateByUrlRequest,
    responses(
        (status = 201, description = "Crate added successfully", body = AddCrateResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn add_crate_by_url(
    State(index): State<SharedCrateIndex>,
    Json(req): Json<AddCrateByUrlRequest>,
) -> impl IntoResponse {
    let source = CrateSource::Url(req.url);

    let result = tokio::task::spawn_blocking(move || {
        let mut idx = index.write().map_err(|e| format!("Lock error: {}", e))?;
        idx.add_from_source(&source)
            .map_err(|e| format!("Failed to add crate: {}", e))
    })
    .await;

    match result {
        Ok(Ok(add_result)) => {
            (StatusCode::CREATED, Json(convert_add_result(add_result))).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

/// Add an RO-Crate by uploading a file (zip archive or ro-crate-metadata.json)
///
/// For zip files, the original filename is used as a hint for the crate ID.
/// If no filename is provided, a ULID-only ID will be generated.
#[utoipa::path(
    post,
    path = "/crates/upload",
    tag = "crates",
    request_body(
        content_type = "multipart/form-data",
        content = Vec<u8>,
        description = "Upload a zip archive or ro-crate-metadata.json file. Field name must be 'file'."
    ),
    responses(
        (status = 201, description = "Crate added successfully", body = AddCrateResponse),
        (status = 400, description = "Invalid request or file", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn add_crate_by_upload(
    State(index): State<SharedCrateIndex>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    // Extract file from multipart
    let (filename, data) = match extract_file_from_multipart(&mut multipart).await {
        Ok(result) => result,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Determine file type and process
    let is_zip = filename.ends_with(".zip")
        || filename.ends_with(".ZIP")
        || data.starts_with(&[0x50, 0x4B, 0x03, 0x04]); // ZIP magic bytes

    // Extract a clean name hint from the original filename
    let name_hint = extract_name_hint(&filename);

    let result = tokio::task::spawn_blocking(move || {
        let mut idx = index.write().map_err(|e| format!("Lock error: {}", e))?;

        if is_zip {
            // Write to temp file and load
            let temp_path =
                std::env::temp_dir().join(format!("rocrate_{}.zip", uuid::Uuid::new_v4()));
            std::fs::write(&temp_path, &data)
                .map_err(|e| format!("Failed to write temp file: {}", e))?;

            let result = idx.add_from_zip_with_name(&temp_path, name_hint.as_deref());

            // Clean up temp file
            let _ = std::fs::remove_file(&temp_path);

            result.map_err(|e| format!("Failed to add crate: {}", e))
        } else {
            // Assume JSON metadata
            let json_str =
                String::from_utf8(data).map_err(|e| format!("Invalid UTF-8 in file: {}", e))?;

            idx.add_from_json(&json_str, name_hint.as_deref())
                .map_err(|e| format!("Failed to add crate: {}", e))
        }
    })
    .await;

    match result {
        Ok(Ok(add_result)) => {
            (StatusCode::CREATED, Json(convert_add_result(add_result))).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

/// Extract a clean name hint from a filename
fn extract_name_hint(filename: &str) -> Option<String> {
    // Get the filename without path
    let name = filename
        .rsplit('/')
        .next()
        .or_else(|| filename.rsplit('\\').next())
        .unwrap_or(filename);

    // Skip generic/temp-looking names
    if name.starts_with("rocrate_") || name == "upload.json" || name == "upload.zip" {
        return None;
    }

    // Remove extension
    let clean = name
        .trim_end_matches(".zip")
        .trim_end_matches(".ZIP")
        .trim_end_matches(".json")
        .trim_end_matches("-ro-crate-metadata");

    if clean.is_empty() {
        None
    } else {
        Some(clean.to_string())
    }
}

/// Extract filename and data from multipart upload
async fn extract_file_from_multipart(
    multipart: &mut Multipart,
) -> Result<(String, Vec<u8>), String> {
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| format!("Failed to read multipart field: {}", e))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            let filename = field.file_name().unwrap_or("upload.json").to_string();

            let data = field
                .bytes()
                .await
                .map_err(|e| format!("Failed to read file data: {}", e))?;

            return Ok((filename, data.to_vec()));
        }
    }

    Err("No file field found in multipart request".to_string())
}

/// List all indexed crate IDs
#[utoipa::path(
    get,
    path = "/crates",
    tag = "crates",
    params(ListCratesParams),
    responses(
        (status = 200, description = "List of crate IDs or full info", body = ListCratesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn list_crates(
    State(index): State<SharedCrateIndex>,
    Query(params): Query<ListCratesParams>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let idx = index.read().map_err(|e| format!("Lock error: {}", e))?;
        if params.full {
            let entries: Vec<CrateInfoResponse> = idx
                .list_crate_entries()
                .into_iter()
                .map(|entry| CrateInfoResponse {
                    crate_id: entry.crate_id.clone(),
                    full_path: entry.full_path.clone(),
                    name: entry.name.clone(),
                    description: entry.description.clone(),
                    is_root: entry.is_root(),
                    parent_id: entry.parent_id().map(String::from),
                })
                .collect();
            let count = entries.len();
            Ok::<_, String>(ListCratesResponse {
                crates: None,
                entries: Some(entries),
                count,
            })
        } else {
            let crates = idx.list_crates();
            let count = crates.len();
            Ok(ListCratesResponse {
                crates: Some(crates),
                entries: None,
                count,
            })
        }
    })
    .await;

    match result {
        Ok(Ok(response)) => (StatusCode::OK, Json(response)).into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

/// Get full metadata JSON for a crate
#[utoipa::path(
    get,
    path = "/crates/{crate_id}",
    tag = "crates",
    params(
        ("crate_id" = String, Path, description = "The crate ID (URL-encoded if necessary)")
    ),
    responses(
        (status = 200, description = "Crate metadata JSON", content_type = "application/json"),
        (status = 404, description = "Crate not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn get_crate(
    State(index): State<SharedCrateIndex>,
    Path(crate_id): Path<String>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let idx = index.read().map_err(|e| format!("Lock error: {}", e))?;
        idx.get_crate_json(&crate_id)
            .map_err(|e| format!("Failed to get crate: {}", e))
    })
    .await;

    match result {
        Ok(Ok(Some(json))) => {
            (StatusCode::OK, [("content-type", "application/json")], json).into_response()
        }
        Ok(Ok(None)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Crate not found".to_string(),
            }),
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

/// Get short info (name, description, ancestry path) for a crate
#[utoipa::path(
    get,
    path = "/crates/{crate_id}/info",
    tag = "crates",
    params(
        ("crate_id" = String, Path, description = "The crate ID (URL-encoded if necessary)")
    ),
    responses(
        (status = 200, description = "Crate info", body = CrateInfoResponse),
        (status = 404, description = "Crate not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn get_crate_info(
    State(index): State<SharedCrateIndex>,
    Path(crate_id): Path<String>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let idx = index.read().map_err(|e| format!("Lock error: {}", e))?;

        match idx.get_crate_info(&crate_id) {
            Some(entry) => Ok(Some(CrateInfoResponse {
                crate_id: entry.crate_id.clone(),
                full_path: entry.full_path.clone(),
                name: entry.name.clone(),
                description: entry.description.clone(),
                is_root: entry.is_root(),
                parent_id: entry.parent_id().map(String::from),
            })),
            None => Ok(None),
        }
    })
    .await;

    match result {
        Ok(Ok(Some(info))) => (StatusCode::OK, Json(info)).into_response(),
        Ok(Ok(None)) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Crate not found".to_string(),
            }),
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

/// Remove a crate from the index
#[utoipa::path(
    delete,
    path = "/crates/{crate_id}",
    tag = "crates",
    params(
        ("crate_id" = String, Path, description = "The crate ID to remove (URL-encoded if necessary)")
    ),
    responses(
        (status = 204, description = "Crate removed successfully"),
        (status = 404, description = "Crate not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn remove_crate(
    State(index): State<SharedCrateIndex>,
    Path(crate_id): Path<String>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let mut idx = index.write().map_err(|e| format!("Lock error: {}", e))?;

        if !idx.list_crates().contains(&crate_id) {
            return Err("Crate not found".to_string());
        }

        idx.remove(&crate_id)
            .map_err(|e| format!("Failed to remove crate: {}", e))
    })
    .await;

    match result {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(e)) if e == "Crate not found" => {
            (StatusCode::NOT_FOUND, Json(ErrorResponse { error: e })).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

/// Search for entities matching a query
#[utoipa::path(
    get,
    path = "/search",
    tag = "search",
    params(
        SearchParams
    ),
    responses(
        (status = 200, description = "Search results", body = SearchResponse),
        (status = 400, description = "Invalid query", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn search(
    State(index): State<SharedCrateIndex>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let idx = index.read().map_err(|e| format!("Lock error: {}", e))?;
        idx.search(&params.q, params.limit)
            .map_err(|e| format!("Search failed: {}", e))
    })
    .await;

    match result {
        Ok(Ok(hits)) => {
            let response = SearchResponse {
                count: hits.len(),
                hits: hits
                    .into_iter()
                    .map(|h| SearchHitResponse {
                        entity_id: h.entity_id,
                        crate_id: h.crate_id,
                        score: h.score,
                    })
                    .collect(),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(Err(e)) => {
            if e.contains("parse") || e.contains("Parse") {
                (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: e }),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task join error: {}", e),
            }),
        )
            .into_response(),
    }
}

// === Main ===

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber with env filter
    // Use RUST_LOG env var to control log level (e.g., RUST_LOG=debug)
    // Default: only show logs from rocrate_indexer and rocrate_server, skip tantivy/tower
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("warn,rocrate_indexer=info,rocrate_server=info")
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000u16);

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());

    info!("Initializing RO-Crate index...");
    let index = CrateIndex::open_or_create()?;
    let crate_count = index.crate_count();
    info!(crate_count, "Loaded crates from index");

    let shared_index: SharedCrateIndex = index.into_shared();

    let swagger = SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi());

    let app = Router::new()
        .merge(swagger)
        .route("/", get(|| async { Redirect::permanent("/swagger-ui") }))
        .route("/crates", get(list_crates))
        .route("/crates/url", post(add_crate_by_url))
        .route("/crates/upload", post(add_crate_by_upload))
        .route("/crates/{crate_id}", get(get_crate))
        .route("/crates/{crate_id}", delete(remove_crate))
        .route("/crates/{crate_id}/info", get(get_crate_info))
        .route("/search", get(search))
        .with_state(shared_index)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    tracing::info_span!(
                        "http_request",
                        method = %request.method(),
                        uri = %request.uri(),
                    )
                })
                .on_response(
                    |response: &axum::http::Response<_>,
                     latency: std::time::Duration,
                     _span: &tracing::Span| {
                        tracing::info!(
                            status = %response.status(),
                            latency = ?latency,
                            "response"
                        );
                    },
                ),
        );

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", bind_addr, port)).await?;
    info!(bind_addr, port, "Server running");
    info!(
        swagger_url = format!("http://{}:{}/swagger-ui/", bind_addr, port),
        "Swagger UI available"
    );

    axum::serve(listener, app).await?;

    Ok(())
}
