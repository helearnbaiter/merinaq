//! Query execution handlers
//! 
//! Handles SQL query execution and result retrieval

use axum::{
    extract::{Extension, Json},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    database::DatabasePool,
    models::{ApiResponse, ExecuteQueryRequest, ExecuteQueryResponse},
    services::{auth_service::AuthService, query_service::QueryService},
};

pub async fn execute_query(
    Extension(db_pool): Extension<DatabasePool>,
    Extension(query_service): Extension<Arc<QueryService>>,
    Json(request): Json<ExecuteQueryRequest>,
) -> impl IntoResponse {
    match query_service.execute_query(&request).await {
        Ok(response) => {
            if response.success {
                (StatusCode::OK, Json(ApiResponse::success(response)))
            } else {
                (StatusCode::BAD_REQUEST, Json(ApiResponse::error("QUERY_001", &response.error.unwrap_or("Query execution failed".to_string()))))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("QUERY_002", &e.to_string())))
        }
    }
}

pub async fn execute_sql(
    Extension(db_pool): Extension<DatabasePool>,
    Extension(query_service): Extension<Arc<QueryService>>,
    Json(request): Json<ExecuteQueryRequest>,
) -> impl IntoResponse {
    // This is an alias for execute_query
    execute_query(Extension(db_pool), Extension(query_service), Json(request)).await
}

pub async fn get_schema(
    Extension(db_pool): Extension<DatabasePool>,
    Extension(query_service): Extension<Arc<QueryService>>,
) -> impl IntoResponse {
    // In a real implementation, we'd extract the data source ID from the request
    // For now, we'll return a not implemented response
    (StatusCode::NOT_IMPLEMENTED, Json(ApiResponse::<serde_json::Value>::error("QUERY_003", "Schema retrieval not implemented")))
}