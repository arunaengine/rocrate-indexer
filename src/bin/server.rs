use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
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
        add_crate,
        list_crates,
        get_crate,
        remove_crate,
        search,
    ),
    components(
        schemas(
            AddCrateRequest,
            AddCrateResponse,
            SubcrateResult,
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
struct AddCrateRequest {
    /// Path to directory/zip or URL to ro-crate-metadata.json
    source: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct AddCrateResponse {
    /// The unique identifier for the added crate
    crate_id: String,
    /// Number of entities indexed from this crate
    entity_count: usize,
    /// Subcrates that were discovered and indexed
    subcrates: Vec<SubcrateResult>,
}

#[derive(Debug, Serialize, ToSchema)]
struct SubcrateResult {
    /// The unique identifier for the subcrate
    crate_id: String,
    /// Number of entities indexed from this subcrate
    entity_count: usize,
    /// Nested subcrates
    subcrates: Vec<SubcrateResult>,
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

fn convert_add_result(result: rocrate_indexer::AddResult) -> AddCrateResponse {
    AddCrateResponse {
        crate_id: result.crate_id,
        entity_count: result.entity_count,
        subcrates: result.subcrates.into_iter().map(convert_subcrate).collect(),
    }
}

fn convert_subcrate(result: rocrate_indexer::AddResult) -> SubcrateResult {
    SubcrateResult {
        crate_id: result.crate_id,
        entity_count: result.entity_count,
        subcrates: result.subcrates.into_iter().map(convert_subcrate).collect(),
    }
}

fn parse_source(source: &str) -> CrateSource {
    if source.starts_with("http://") || source.starts_with("https://") {
        CrateSource::Url(source.to_string())
    } else {
        let path = std::path::PathBuf::from(source);
        if path.is_dir() {
            CrateSource::Directory(path)
        } else {
            CrateSource::ZipFile(path)
        }
    }
}

// === Handlers ===

/// Add an RO-Crate from a path or URL
#[utoipa::path(
    post,
    path = "/crates",
    tag = "crates",
    request_body = AddCrateRequest,
    responses(
        (status = 201, description = "Crate added successfully", body = AddCrateResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
async fn add_crate(
    State(index): State<SharedCrateIndex>,
    Json(req): Json<AddCrateRequest>,
) -> impl IntoResponse {
    let source = parse_source(&req.source);

    // Run blocking index operation in a separate thread
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
            // Return raw JSON string with proper content type
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
    //let crate_id_check = crate_id.clone();

    let result = tokio::task::spawn_blocking(move || {
        let mut idx = index.write().map_err(|e| format!("Lock error: {}", e))?;

        // Check if crate exists first
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
            // Check if it's a query parse error
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
    // Parse command line args for port
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000u16);

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());

    // Initialize the index
    println!("Initializing RO-Crate index...");
    let index = CrateIndex::open_or_create()?;
    let crate_count = index.crate_count();
    println!("Loaded {} crates from index", crate_count);

    let shared_index: SharedCrateIndex = index.into_shared();

    let swagger = SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi());

    // Build router
    let app = Router::new()
        .merge(swagger)
        // API routes
        .route("/crates", post(add_crate))
        .route("/crates", get(list_crates))
        .route("/crates/{crate_id}", get(get_crate))
        .route("/crates/{crate_id}", delete(remove_crate))
        .route("/search", get(search))
        // Swagger UI
        // State
        .with_state(shared_index)
        // CORS
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
