//! Policy management handlers
//! 
//! Handles Casbin policy CRUD operations and permission checks

use axum::{
    extract::{Extension, Json, Path},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    models::{ApiResponse, CreatePolicyRequest, CheckPermissionRequest, CheckPermissionResponse, Policy},
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

pub async fn get_policy_by_type(
    Extension(casbin_service): Extension<CasbinService>,
    Path(policy_type): Path<String>,
) -> impl IntoResponse {
    match casbin_service.get_policy_by_type(&policy_type).await {
        Ok(policies) => {
            (StatusCode::OK, Json(ApiResponse::success(policies)))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_008", &e.to_string())))
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
    Json(request): Json<CreatePolicyRequest>,
) -> impl IntoResponse {
    match casbin_service.remove_policy(&request).await {
        Ok(success) => {
            if success {
                (StatusCode::OK, Json(ApiResponse::success("Policy deleted successfully")))
            } else {
                (StatusCode::BAD_REQUEST, Json(ApiResponse::error("POLICY_006", "Failed to delete policy")))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_007", &e.to_string())))
        }
    }
}

pub async fn get_permissions_for_user(
    Extension(casbin_service): Extension<CasbinService>,
    Path(user_id): Path<String>,
) -> impl IntoResponse {
    match casbin_service.get_permissions_for_user(&user_id).await {
        Ok(permissions) => {
            (StatusCode::OK, Json(ApiResponse::success(permissions)))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_009", &e.to_string())))
        }
    }
}

pub async fn get_permissions_for_resource(
    Extension(casbin_service): Extension<CasbinService>,
    Path(resource): Path<String>,
) -> impl IntoResponse {
    match casbin_service.get_permissions_for_resource(&resource).await {
        Ok(permissions) => {
            (StatusCode::OK, Json(ApiResponse::success(permissions)))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_010", &e.to_string())))
        }
    }
}

pub async fn bulk_add_policies(
    Extension(casbin_service): Extension<CasbinService>,
    Json(requests): Json<Vec<CreatePolicyRequest>>,
) -> impl IntoResponse {
    match casbin_service.add_policies(&requests).await {
        Ok(success) => {
            if success {
                (StatusCode::OK, Json(ApiResponse::success("Policies added successfully")))
            } else {
                (StatusCode::BAD_REQUEST, Json(ApiResponse::error("POLICY_011", "Failed to add some or all policies")))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::error("POLICY_012", &e.to_string())))
        }
    }
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