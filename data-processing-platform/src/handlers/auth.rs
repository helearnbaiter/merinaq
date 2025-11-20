//! Authentication handlers
//! 
//! Handles login, logout, and token management endpoints

use axum::{
    extract::{Extension, Json, Path},
    http::StatusCode,
    response::IntoResponse,
    extract::State,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    database::DatabasePool,
    models::{AuthRequest, AuthResponse, ApiResponse},
    services::auth_service::AuthService,
};

pub async fn login(
    Extension(db_pool): Extension<DatabasePool>,
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
    Json(request): Json<AuthRequest>,
) -> impl IntoResponse {
    let auth_service_read = auth_service.read().await;
    match auth_service_read.authenticate_user(&db_pool, &request).await {
        Ok(response) => {
            if response.success {
                (StatusCode::OK, Json(ApiResponse::success(response)))
            } else {
                (StatusCode::UNAUTHORIZED, Json(ApiResponse::error("AUTH_001", &response.error.unwrap_or("Authentication failed".to_string()))))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("AUTH_002", &e.to_string())))
        }
    }
}

pub async fn refresh_token() -> impl IntoResponse {
    // TODO: Implement token refresh logic
    (StatusCode::NOT_IMPLEMENTED, Json(ApiResponse::<()>::error("AUTH_003", "Token refresh not implemented")))
}

pub async fn logout(
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
) -> impl IntoResponse {
    // In a real implementation, you would extract the user ID from the token
    // For now, we'll return a success response
    (StatusCode::OK, Json(ApiResponse::success("Logged out successfully")))
}