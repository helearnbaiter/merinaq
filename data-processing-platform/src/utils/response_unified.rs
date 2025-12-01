//! Unified response handling for the axum-based data processing platform
//! 
//! This module provides a comprehensive and consistent way to handle API responses
//! throughout the application, following the ApiResponse<T> structure defined in models.rs.

use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use std::collections::HashMap;

use crate::models::{ApiResponse, ApiError};
use crate::utils::error::{ErrorResponse, PlatformError};

/// Trait for creating standardized API responses
pub trait ApiResponseExt<T> {
    /// Create a success response
    fn success_response(self) -> (StatusCode, Json<ApiResponse<T>>);
    
    /// Create a success response with metadata
    fn success_with_meta(self, meta: serde_json::Value) -> (StatusCode, Json<ApiResponse<T>>);
    
    /// Create a paginated success response
    fn paginated_response(
        self,
        page: u32,
        per_page: u32,
        total: u64,
    ) -> (StatusCode, Json<ApiResponse<T>>);
}

impl<T: Serialize> ApiResponseExt<T> for T {
    fn success_response(self) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(self),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::OK, Json(response))
    }
    
    fn success_with_meta(self, meta: serde_json::Value) -> (StatusCode, Json<ApiResponse<T>>) {
        let mut response = ApiResponse {
            success: true,
            data: Some(self),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        // Add meta information to the response
        let mut response_json = serde_json::to_value(response).unwrap();
        if let serde_json::Value::Object(ref mut map) = response_json {
            map.insert("meta".to_string(), meta);
        }
        
        let final_response: ApiResponse<T> = serde_json::from_value(response_json).unwrap();
        (StatusCode::OK, Json(final_response))
    }
    
    fn paginated_response(
        self,
        page: u32,
        per_page: u32,
        total: u64,
    ) -> (StatusCode, Json<ApiResponse<T>>) {
        let meta = serde_json::json!({
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total as f64 / per_page as f64).ceil() as u32,
            }
        });
        
        self.success_with_meta(meta)
    }
}

/// Helper functions for common response patterns
pub mod helpers {
    use super::*;
    
    /// Create a simple success response with no data
    pub fn success_message(message: &str) -> (StatusCode, Json<ApiResponse<serde_json::Value>>) {
        let response = ApiResponse {
            success: true,
            data: Some(serde_json::json!({ "message": message })),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::OK, Json(response))
    }
    
    /// Create a created response (201)
    pub fn created<T: Serialize>(data: T) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::CREATED, Json(response))
    }
    
    /// Create an accepted response (202) for async operations
    pub fn accepted<T: Serialize>(data: T) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::ACCEPTED, Json(response))
    }
    
    /// Create a no content response (204)
    pub fn no_content() -> (StatusCode, Json<ApiResponse<()>>) {
        let response = ApiResponse {
            success: true,
            data: None,
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::NO_CONTENT, Json(response))
    }
    
    /// Create a response with custom status code
    pub fn with_status<T: Serialize>(
        status: StatusCode,
        data: T,
    ) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (status, Json(response))
    }
    
    /// Create an error response from PlatformError
    pub fn error_from_platform_error(
        error: PlatformError,
    ) -> (StatusCode, Json<ErrorResponse>) {
        (error.into_response().0, error.into_response().1)
    }
    
    /// Create a bad request response
    pub fn bad_request<T: Serialize>(message: &str) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: false,
            data: None,
            error: Some(ApiError {
                code: "BAD_REQUEST_400".to_string(),
                message: message.to_string(),
                details: None,
            }),
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::BAD_REQUEST, Json(response))
    }
    
    /// Create a not found response
    pub fn not_found<T: Serialize>(message: &str) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: false,
            data: None,
            error: Some(ApiError {
                code: "NOT_FOUND_404".to_string(),
                message: message.to_string(),
                details: None,
            }),
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::NOT_FOUND, Json(response))
    }
    
    /// Create a forbidden response
    pub fn forbidden<T: Serialize>(message: &str) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: false,
            data: None,
            error: Some(ApiError {
                code: "FORBIDDEN_403".to_string(),
                message: message.to_string(),
                details: None,
            }),
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::FORBIDDEN, Json(response))
    }
    
    /// Create an unauthorized response
    pub fn unauthorized<T: Serialize>(message: &str) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: false,
            data: None,
            error: Some(ApiError {
                code: "UNAUTHORIZED_401".to_string(),
                message: message.to_string(),
                details: None,
            }),
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::UNAUTHORIZED, Json(response))
    }
    
    /// Create an internal server error response
    pub fn internal_server_error<T: Serialize>(message: &str) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: false,
            data: None,
            error: Some(ApiError {
                code: "INTERNAL_ERROR_500".to_string(),
                message: message.to_string(),
                details: None,
            }),
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

/// Middleware for adding request ID to responses
pub async fn request_id_middleware<B>(
    request: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> impl IntoResponse {
    // Generate or extract request ID
    let request_id = uuid::Uuid::new_v4().to_string();
    
    // Add request ID to response headers
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "X-Request-ID",
        axum::http::HeaderValue::from_str(&request_id).unwrap(),
    );
    
    response
}

/// Response builder for complex response structures
pub struct ResponseBuilder<T> {
    data: Option<T>,
    success: bool,
    error: Option<crate::models::ApiError>,
    timestamp: String,
    request_id: Option<String>,
    meta: Option<serde_json::Value>,
}

impl<T: Serialize> ResponseBuilder<T> {
    pub fn new() -> Self {
        Self {
            data: None,
            success: true,
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
            meta: None,
        }
    }
    
    pub fn data(mut self, data: T) -> Self {
        self.data = Some(data);
        self.success = true;
        self
    }
    
    pub fn error(mut self, error: crate::models::ApiError) -> Self {
        self.error = Some(error);
        self.success = false;
        self
    }
    
    pub fn success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }
    
    pub fn request_id<S: Into<String>>(mut self, request_id: S) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
    
    pub fn meta(mut self, meta: serde_json::Value) -> Self {
        self.meta = Some(meta);
        self
    }
    
    pub fn build(self) -> ApiResponse<T> {
        let mut response = ApiResponse {
            success: self.success,
            data: self.data,
            error: self.error,
            timestamp: self.timestamp,
            request_id: self.request_id,
        };
        
        // If there's meta data, we need to extend the response with it
        if let Some(meta) = self.meta {
            let mut response_json = serde_json::to_value(response).unwrap();
            if let serde_json::Value::Object(ref mut map) = response_json {
                map.insert("meta".to_string(), meta);
            }
            response = serde_json::from_value(response_json).unwrap();
        }
        
        response
    }
    
    pub fn build_with_status(self, status: StatusCode) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = self.build();
        (status, Json(response))
    }
}

impl<T: Serialize> Default for ResponseBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to create a response builder
pub fn response_builder<T: Serialize>() -> ResponseBuilder<T> {
    ResponseBuilder::new()
}

/// Standard response codes and messages
pub mod standard {
    use super::*;
    
    pub const SUCCESS: (&str, &str) = ("SUCCESS_001", "Operation completed successfully");
    pub const CREATED: (&str, &str) = ("CREATED_001", "Resource created successfully");
    pub const UPDATED: (&str, &str) = ("UPDATED_001", "Resource updated successfully");
    pub const DELETED: (&str, &str) = ("DELETED_001", "Resource deleted successfully");
    pub const ACCEPTED: (&str, &str) = ("ACCEPTED_001", "Request accepted for processing");
    
    /// Create a standard success response
    pub fn success<T: Serialize>(data: T) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::OK, Json(response))
    }
    
    /// Create a standard created response
    pub fn created<T: Serialize>(data: T) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::CREATED, Json(response))
    }
    
    /// Create a standard accepted response
    pub fn accepted<T: Serialize>(data: T) -> (StatusCode, Json<ApiResponse<T>>) {
        let response = ApiResponse {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::ACCEPTED, Json(response))
    }
    
    /// Create a standard no content response
    pub fn no_content() -> (StatusCode, Json<ApiResponse<()>>) {
        let response = ApiResponse {
            success: true,
            data: None,
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
        };
        
        (StatusCode::NO_CONTENT, Json(response))
    }
}