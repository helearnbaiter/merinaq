//! Health check handler
//! 
//! Provides health check endpoint for monitoring and load balancers

use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::models::ApiResponse;

pub async fn health_check() -> impl IntoResponse {
    let response = ApiResponse::success(json!({
        "status": "healthy",
        "service": "Data Processing Platform",
        "version": "0.1.0",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }));
    
    (StatusCode::OK, Json(response))
}