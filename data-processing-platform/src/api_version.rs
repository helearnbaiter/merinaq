//! API Version Management
//! 
//! This module handles API versioning and routing to appropriate version handlers

use axum::{
    extract::Path,
    http::Request,
    middleware::{from_fn},
    response::Response,
    routing::{get, Router},
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    services::auth_service::AuthService,
    services::casbin_service::CasbinService,
    services::query_service::QueryService,
    database::DatabasePool,
    routes::v1,
};

/// API Version enum to track available versions
#[derive(Debug, Clone, PartialEq)]
pub enum ApiVersion {
    V1,
    // Future versions can be added here
    // V2,
    // VNext,
}

impl ApiVersion {
    /// Get the string representation of the version
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiVersion::V1 => "v1",
            // ApiVersion::V2 => "v2",
            // ApiVersion::VNext => "vnext",
        }
    }

    /// Check if a version string matches this enum variant
    pub fn matches(&self, version_str: &str) -> bool {
        self.as_str() == version_str
    }
}

/// Create the main API router with version management
pub fn create_api_router(
    db_pool: DatabasePool,
    casbin_enforcer: CasbinService,
    auth_service: Arc<RwLock<AuthService>>,
    query_service: Arc<QueryService>,
) -> Router {
    Router::new()
        // Version 1 API routes
        .nest("/v1", v1::create_v1_router(
            db_pool,
            casbin_enforcer,
            auth_service,
            query_service,
        ))
        // Redirect root to health check or API documentation
        .route("/", get(root_handler))
        // Add version info endpoint
        .route("/version", get(version_handler))
}

/// Handler for the root path
async fn root_handler() -> Response {
    use axum::Json;
    use crate::models::ApiResponse;
    
    let response = ApiResponse::success("Welcome to Data Processing Platform API");
    (axum::http::StatusCode::OK, Json(response)).into_response()
}

/// Handler for version info
async fn version_handler() -> Response {
    use axum::Json;
    use crate::models::ApiResponse;
    
    let version_info = serde_json::json!({
        "current_version": "v1",
        "supported_versions": ["v1"],
        "api_title": "Data Processing Platform API",
        "description": "A high-performance enterprise data processing platform",
        "documentation_url": "/docs" // This would point to actual documentation
    });
    
    let response = ApiResponse::success(version_info);
    (axum::http::StatusCode::OK, Json(response)).into_response()
}

/// Middleware to handle version-specific operations
pub async fn version_middleware<B>(
    request: Request<B>,
    next: axum::middleware::Next<B>,
) -> Result<Response, axum::http::StatusCode> {
    // Add version-specific headers or perform version-specific operations
    let mut response = next.run(request).await;
    
    // Add API version header to response
    response.headers_mut().insert(
        "X-API-Version",
        axum::http::HeaderValue::from_static("v1")
    );
    
    Ok(response)
}

/// Utility function to parse version from path
pub fn parse_version_from_path(path: &str) -> Option<ApiVersion> {
    let parts: Vec<&str> = path.split('/').collect();
    
    if parts.len() >= 2 && parts[1].starts_with('v') {
        match parts[1] {
            "v1" => Some(ApiVersion::V1),
            // "v2" => Some(ApiVersion::V2),
            // "vnext" => Some(ApiVersion::VNext),
            _ => None,
        }
    } else {
        None
    }
}

/// Check if the API version is deprecated
pub fn is_version_deprecated(version: &ApiVersion) -> bool {
    match version {
        // Currently no deprecated versions
        _ => false,
    }
}

/// Get deprecation warning for a version
pub fn get_deprecation_warning(version: &ApiVersion) -> Option<&'static str> {
    if is_version_deprecated(version) {
        match version {
            // ApiVersion::V1 => Some("Version 1 will be deprecated on 2024-12-31. Please upgrade to version 2."),
            _ => None,
        }
    } else {
        None
    }
}