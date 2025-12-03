use axum::{
    Json, Router,
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
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
        remove_crate,
        search,
    ),
    components(
        schemas(
            AddCrateByUrlRequest,
            AddCrateResponse,
            CrateAddedInfo,
            ListCratesResponse,
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
    /// URL to ro-crate-metadata.json
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
    /// List of all indexed crate IDs
    crates: Vec<String>,
    /// Total count of indexed crates
    count: usize,
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
#[utoipa::path(
    post,
    path = "/crates/upload",
    tag = "crates",
    request_body(
        content_type = "multipart/form-data",
        content = Vec<u8>,
        description = "Upload a zip archive or ro-crate-metadata.json file"
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
    let is_zip = filename.ends_with(".zip") || data.starts_with(&[0x50, 0x4B, 0x03, 0x04]); // ZIP magic bytes

    let result = tokio::task::spawn_blocking(move || {
        let mut idx = index.write().map_err(|e| format!("Lock error: {}", e))?;

        if is_zip {
            // Write to temp file and load
            let temp_path =
                std::env::temp_dir().join(format!("rocrate_{}.zip", uuid::Uuid::new_v4()));
            std::fs::write(&temp_path, &data)
                .map_err(|e| format!("Failed to write temp file: {}", e))?;

            let result = idx.add_from_source(&CrateSource::ZipFile(temp_path.clone()));

            // Clean up temp file
            let _ = std::fs::remove_file(&temp_path);

            result.map_err(|e| format!("Failed to add crate: {}", e))
        } else {
            // Assume JSON metadata
            let json_str =
                String::from_utf8(data).map_err(|e| format!("Invalid UTF-8 in file: {}", e))?;

            idx.add_from_json(&json_str, &filename)
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
    responses(
        (status = 200, description = "List of crate IDs", body = ListCratesResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn list_crates(State(index): State<SharedCrateIndex>) -> impl IntoResponse {
    let result = tokio::task::spawn_blocking(move || {
        let idx = index.read().map_err(|e| format!("Lock error: {}", e))?;
        Ok::<_, String>(idx.list_crates())
    })
    .await;

    match result {
        Ok(Ok(crates)) => {
            let count = crates.len();
            (StatusCode::OK, Json(ListCratesResponse { crates, count })).into_response()
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
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000u16);

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());

    println!("Initializing RO-Crate index...");
    let index = CrateIndex::open_or_create()?;
    let crate_count = index.crate_count();
    println!("Loaded {} crates from index", crate_count);

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
        .route("/search", get(search))
        .with_state(shared_index)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", bind_addr, port)).await?;
    println!("Server running at http://{}:{}", bind_addr, port);
    println!(
        "Swagger UI available at http://{}:{}/swagger-ui/",
        bind_addr, port
    );

    axum::serve(listener, app).await?;

    Ok(())
}
