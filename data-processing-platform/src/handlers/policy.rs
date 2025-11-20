//! Policy management handlers
//! 
//! Handles Casbin policy CRUD operations and permission checks

use axum::{
    extract::{Extension, Json},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    models::{ApiResponse, CreatePolicyRequest, CheckPermissionRequest, CheckPermissionResponse},
    services::{auth_service::AuthService, casbin_service::CasbinService},
};

pub async fn get_policies(
    Extension(casbin_service): Extension<CasbinService>,
) -> impl IntoResponse {
    match casbin_service.get_policies().await {
        Ok(policies) => {
            (StatusCode::OK, Json(ApiResponse::success(policies)))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_001", &e.to_string())))
        }
    }
}

pub async fn create_policy(
    Extension(casbin_service): Extension<CasbinService>,
    Json(request): Json<CreatePolicyRequest>,
) -> impl IntoResponse {
    match casbin_service.add_policy(&request).await {
        Ok(success) => {
            if success {
                (StatusCode::OK, Json(ApiResponse::success("Policy created successfully")))
            } else {
                (StatusCode::BAD_REQUEST, Json(ApiResponse::error("POLICY_002", "Failed to create policy")))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_003", &e.to_string())))
        }
    }
}

pub async fn delete_policy(
    Extension(casbin_service): Extension<CasbinService>,
    // In a real implementation, we'd extract the policy ID from the path
    // For now, we'll return not implemented
) -> impl IntoResponse {
    (StatusCode::NOT_IMPLEMENTED, Json(ApiResponse::<()>::error("POLICY_004", "Policy deletion not implemented")))
}

pub async fn check_permission(
    Extension(casbin_service): Extension<CasbinService>,
    Json(request): Json<CheckPermissionRequest>,
) -> impl IntoResponse {
    match casbin_service.enforce(&request.subject, &request.resource, &request.action).await {
        Ok(allowed) => {
            let response = CheckPermissionResponse { allowed };
            (StatusCode::OK, Json(ApiResponse::success(response)))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_005", &e.to_string())))
        }
    }
}