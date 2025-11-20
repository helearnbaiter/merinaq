//! Authentication handlers
//! 
//! Handles login, logout, and token management endpoints

use axum::{
    extract::{Extension, Json, Path, Query},
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

pub async fn oauth2_authorize(
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
    Path(provider_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    use crate::auth::{OAuth2Manager, OAuth2Provider};
    
    let state = params.get("state").unwrap_or(&"default_state".to_string());
    let scopes_param = params.get("scopes").map(|s| s.split(',').collect::<Vec<_>>());
    
    let auth_service_read = auth_service.read().await;
    let oauth2_manager = auth_service_read.oauth2_manager.read().await;
    
    if let Some(provider) = oauth2_manager.get_provider(&provider_name) {
        let auth_url = if let Some(ref scopes) = scopes_param {
            provider.get_authorization_url(state, Some(scopes))
        } else {
            provider.get_authorization_url(state, None)
        };
        
        (StatusCode::OK, Json(ApiResponse::success(auth_url)))
    } else {
        (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error("AUTH_004", &format!("OAuth2 provider {} not found", provider_name))))
    }
}

pub async fn oauth2_callback(
    Extension(db_pool): Extension<DatabasePool>,
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
    Path(provider_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if let Some(code) = params.get("code") {
        let auth_service_read = auth_service.read().await;
        match auth_service_read.authenticate_oauth2_user(&db_pool, &provider_name, code).await {
            Ok(response) => {
                if response.success {
                    (StatusCode::OK, Json(ApiResponse::success(response)))
                } else {
                    (StatusCode::UNAUTHORIZED, Json(ApiResponse::error("AUTH_005", &response.error.unwrap_or("OAuth2 authentication failed".to_string()))))
                }
            }
            Err(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("AUTH_006", &e.to_string())))
            }
        }
    } else {
        (StatusCode::BAD_REQUEST, Json(ApiResponse::<()>::error("AUTH_007", "No authorization code provided")))
    }
}

pub async fn refresh_token(
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
    Json(refresh_request): Json<RefreshTokenRequest>,
) -> impl IntoResponse {
    let auth_service_read = auth_service.read().await;
    match auth_service_read.refresh_token(&refresh_request.refresh_token).await {
        Ok(response) => {
            if response.success {
                (StatusCode::OK, Json(ApiResponse::success(response)))
            } else {
                (StatusCode::UNAUTHORIZED, Json(ApiResponse::error("AUTH_008", "Invalid refresh token")))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("AUTH_009", &e.to_string())))
        }
    }
}

pub async fn logout(
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
) -> impl IntoResponse {
    // In a real implementation, you would extract the user ID from the token
    // For now, we'll return a success response
    (StatusCode::OK, Json(ApiResponse::success("Logged out successfully")))
}

#[derive(serde::Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}