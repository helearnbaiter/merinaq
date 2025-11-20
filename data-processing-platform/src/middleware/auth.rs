//! Authentication and authorization middleware
//! 
//! Provides middleware functions for request authentication and authorization

use axum::{
    extract::Extension,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    models::{ApiResponse, TokenClaims},
    services::auth_service::AuthService,
};

pub async fn auth_middleware<B>(
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
    request: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    // Skip authentication for public endpoints
    let path = request.uri().path();
    if is_public_endpoint(path) {
        return Ok(next.run(request).await);
    }

    // Extract token from Authorization header
    if let Some(auth_header) = request.headers().get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = auth_str.trim_start_matches("Bearer ").trim();
                
                // Validate token
                let auth_service_read = auth_service.read().await;
                match auth_service_read.validate_token(token).await {
                    Ok(_claims) => {
                        // Token is valid, continue with request
                        drop(auth_service_read); // Release the read lock
                        return Ok(next.run(request).await);
                    }
                    Err(_) => {
                        // Invalid token
                        let unauthorized_response = Json(ApiResponse::<()>::error(
                            "AUTH_001",
                            "Invalid or expired token"
                        ));
                        let response = Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .header("content-type", "application/json")
                            .body(axum::body::Body::from(
                                serde_json::to_vec(&unauthorized_response.0).unwrap_or_default()
                            ))
                            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                        
                        return Err(StatusCode::UNAUTHORIZED);
                    }
                }
            }
        }
    }

    // No valid token found
    let unauthorized_response = Json(ApiResponse::<()>::error(
        "AUTH_002",
        "Authorization token required"
    ));
    let response = Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            serde_json::to_vec(&unauthorized_response.0).unwrap_or_default()
        ))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Err(StatusCode::UNAUTHORIZED)
}

fn is_public_endpoint(path: &str) -> bool {
    // List of public endpoints that don't require authentication
    let public_endpoints = [
        "/health",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh",
        "/api/v1/auth/logout",
    ];
    
    public_endpoints.iter().any(|&ep| path.starts_with(ep))
}

// RBAC (Role-Based Access Control) middleware
pub async fn rbac_middleware<B>(
    Extension(auth_service): Extension<Arc<RwLock<AuthService>>>,
    Extension(casbin_service): Extension<crate::services::casbin_service::CasbinService>,
    request: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    // Extract token and claims
    if let Some(auth_header) = request.headers().get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = auth_str.trim_start_matches("Bearer ").trim();
                
                if let Ok(claims) = auth_service.read().await.validate_token(token).await {
                    // Check permissions using Casbin
                    let resource = extract_resource_from_path(request.uri().path());
                    let action = extract_action_from_method(request.method().as_str());
                    
                    match casbin_service.enforce(&claims.sub, &resource, &action).await {
                        Ok(true) => {
                            // Permission granted
                            return Ok(next.run(request).await);
                        }
                        Ok(false) => {
                            // Permission denied
                            let forbidden_response = Json(ApiResponse::<()>::error(
                                "AUTH_003",
                                "Insufficient permissions"
                            ));
                            let response = Response::builder()
                                .status(StatusCode::FORBIDDEN)
                                .header("content-type", "application/json")
                                .body(axum::body::Body::from(
                                    serde_json::to_vec(&forbidden_response.0).unwrap_or_default()
                                ))
                                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                            
                            return Err(StatusCode::FORBIDDEN);
                        }
                        Err(_) => {
                            // Error checking permission
                            return Err(StatusCode::INTERNAL_SERVER_ERROR);
                        }
                    }
                }
            }
        }
    }
    
    // If we get here, either there's no token or it's invalid
    // Let the auth middleware handle this
    Ok(next.run(request).await)
}

fn extract_resource_from_path(path: &str) -> String {
    // Simple resource extraction - in a real app, you'd have more sophisticated logic
    // Remove the API version and extract the main resource
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() > 3 {
        format!("/{}/{}", parts[2], parts[3]) // e.g., /api/v1/users -> /v1/users
    } else {
        path.to_string()
    }
}

fn extract_action_from_method(method: &str) -> String {
    match method {
        "GET" => "read".to_string(),
        "POST" => "create".to_string(),
        "PUT" => "update".to_string(),
        "DELETE" => "delete".to_string(),
        _ => "other".to_string(),
    }
}