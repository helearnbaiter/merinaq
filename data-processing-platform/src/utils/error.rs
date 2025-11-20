//! Error Handling System
//! 
//! This module defines a comprehensive error handling system for the data processing platform
//! with different error types for various domains and unified error responses.

use axum::{
    response::{IntoResponse, Response},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

// Unified error types for different domains
#[derive(Error, Debug)]
pub enum PlatformError {
    #[error("Authentication error: {0}")]
    AuthenticationError(String),
    
    #[error("Authorization error: {0}")]
    AuthorizationError(String),
    
    #[error("Validation error: {0}")]
    ValidationError(String),
    
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    
    #[error("Query execution error: {0}")]
    QueryError(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Internal server error: {0}")]
    InternalError(String),
    
    #[error("Not found: {0}")]
    NotFound(String),
    
    #[error("Data source error: {0}")]
    DataSourceError(String),
    
    #[error("Connection pool error: {0}")]
    ConnectionPoolError(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    
    #[error("Arrow error: {0}")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    
    #[error("JWT error: {0}")]
    JWTError(#[from] jsonwebtoken::errors::Error),
    
    #[error("OAuth2 error: {0}")]
    OAuth2Error(String),
    
    #[error("Casbin error: {0}")]
    CasbinError(#[from] casbin::Error),
    
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    
    #[error("URL parsing error: {0}")]
    UrlError(#[from] url::ParseError),
    
    #[error("Flight error: {0}")]
    FlightError(String),
    
    #[error("ADBC error: {0}")]
    AdbcError(String),
}

// Error response structure
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub success: bool,
    pub error: ErrorDetails,
    pub timestamp: String,
    pub request_id: Option<String>,
    pub path: Option<String>,
    pub method: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorDetails {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

// Implementation to convert PlatformError to HTTP Response
impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        let (status, error_code, message) = match &self {
            PlatformError::AuthenticationError(msg) => {
                (StatusCode::UNAUTHORIZED, "AUTH_001".to_string(), msg.clone())
            }
            PlatformError::AuthorizationError(msg) => {
                (StatusCode::FORBIDDEN, "AUTH_002".to_string(), msg.clone())
            }
            PlatformError::ValidationError(msg) => {
                (StatusCode::BAD_REQUEST, "VALIDATION_001".to_string(), msg.clone())
            }
            PlatformError::DatabaseError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "DB_001".to_string(), 
                 "Database error occurred".to_string())
            }
            PlatformError::QueryError(msg) => {
                (StatusCode::BAD_REQUEST, "QUERY_001".to_string(), msg.clone())
            }
            PlatformError::NetworkError(msg) => {
                (StatusCode::BAD_GATEWAY, "NETWORK_001".to_string(), msg.clone())
            }
            PlatformError::ConfigError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "CONFIG_001".to_string(), msg.clone())
            }
            PlatformError::InternalError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_001".to_string(), msg.clone())
            }
            PlatformError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, "NOT_FOUND_001".to_string(), msg.clone())
            }
            PlatformError::DataSourceError(msg) => {
                (StatusCode::BAD_REQUEST, "DATASOURCE_001".to_string(), msg.clone())
            }
            PlatformError::ConnectionPoolError(msg) => {
                (StatusCode::SERVICE_UNAVAILABLE, "POOL_001".to_string(), msg.clone())
            }
            PlatformError::SerializationError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "SERIALIZATION_001".to_string(), 
                 "Serialization error occurred".to_string())
            }
            PlatformError::DataFusionError(_) => {
                (StatusCode::BAD_REQUEST, "DATAFUSION_001".to_string(), 
                 "Query processing error".to_string())
            }
            PlatformError::ArrowError(_) => {
                (StatusCode::BAD_REQUEST, "ARROW_001".to_string(), 
                 "Arrow format error".to_string())
            }
            PlatformError::JWTError(_) => {
                (StatusCode::UNAUTHORIZED, "JWT_001".to_string(), 
                 "Invalid or expired token".to_string())
            }
            PlatformError::OAuth2Error(msg) => {
                (StatusCode::UNAUTHORIZED, "OAUTH2_001".to_string(), msg.clone())
            }
            PlatformError::CasbinError(_) => {
                (StatusCode::FORBIDDEN, "CASBIN_001".to_string(), 
                 "Permission denied".to_string())
            }
            PlatformError::IOError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "IO_001".to_string(), 
                 "IO error occurred".to_string())
            }
            PlatformError::UrlError(_) => {
                (StatusCode::BAD_REQUEST, "URL_001".to_string(), 
                 "Invalid URL format".to_string())
            }
            PlatformError::FlightError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "FLIGHT_001".to_string(), msg.clone())
            }
            PlatformError::AdbcError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "ADBC_001".to_string(), msg.clone())
            }
        };

        let error_response = ErrorResponse {
            success: false,
            error: ErrorDetails {
                code: error_code,
                message,
                details: Some(serde_json::json!({
                    "error_type": std::mem::discriminant(self).as_std(),
                    "full_error": format!("{}", self)
                })),
            },
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None, // Would be populated from request context
            path: None,       // Would be populated from request context
            method: None,     // Would be populated from request context
        };

        (status, Json(error_response)).into_response()
    }
}

// Result type alias for convenience
pub type PlatformResult<T> = Result<T, PlatformError>;

// Error extension trait for adding context
pub trait ErrorContext<T> {
    fn context(self, context: impl Into<String>) -> PlatformResult<T>;
    fn with_context<F, C>(self, context_fn: F) -> PlatformResult<T>
    where
        F: FnOnce() -> C,
        C: Into<String>;
}

impl<T> ErrorContext<T> for PlatformResult<T> {
    fn context(self, context: impl Into<String>) -> PlatformResult<T> {
        self.map_err(|e| PlatformError::InternalError(format!("{}: {}", context.into(), e)))
    }

    fn with_context<F, C>(self, context_fn: F) -> PlatformResult<T>
    where
        F: FnOnce() -> C,
        C: Into<String>,
    {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(PlatformError::InternalError(format!("{}: {}", context_fn().into(), e))),
        }
    }
}

// Error collector for collecting multiple errors
#[derive(Debug, Default)]
pub struct ErrorCollector {
    errors: Vec<PlatformError>,
}

impl ErrorCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_error(&mut self, error: PlatformError) {
        self.errors.push(error);
    }

    pub fn add_errors(&mut self, errors: Vec<PlatformError>) {
        self.errors.extend(errors);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn into_result(self) -> PlatformResult<()> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(PlatformError::InternalError(
                self.errors
                    .into_iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            ))
        }
    }

    pub fn into_validation_result<T>(self, value: T) -> PlatformResult<T> {
        match self.into_result() {
            Ok(()) => Ok(value),
            Err(e) => Err(e),
        }
    }
}

// Error logging utility
pub fn log_error(error: &PlatformError, request_id: Option<&str>) {
    match error {
        PlatformError::AuthenticationError(msg) => {
            tracing::warn!(
                "Authentication error - Request ID: {:?}, Message: {}",
                request_id,
                msg
            );
        }
        PlatformError::AuthorizationError(msg) => {
            tracing::warn!(
                "Authorization error - Request ID: {:?}, Message: {}",
                request_id,
                msg
            );
        }
        PlatformError::ValidationError(msg) => {
            tracing::info!(
                "Validation error - Request ID: {:?}, Message: {}",
                request_id,
                msg
            );
        }
        PlatformError::DatabaseError(_) => {
            tracing::error!(
                "Database error - Request ID: {:?}, Error: {}",
                request_id,
                error
            );
        }
        _ => {
            tracing::error!(
                "Platform error - Request ID: {:?}, Type: {:?}, Message: {}",
                request_id,
                std::mem::discriminant(error),
                error
            );
        }
    }
}

// Error conversion utilities
impl From<std::convert::Infallible> for PlatformError {
    fn from(_: std::convert::Infallible) -> Self {
        PlatformError::InternalError("Infallible conversion error".to_string())
    }
}

impl From<tokio::task::JoinError> for PlatformError {
    fn from(error: tokio::task::JoinError) -> Self {
        PlatformError::InternalError(format!("Task join error: {}", error))
    }
}

impl From<uuid::Error> for PlatformError {
    fn from(error: uuid::Error) -> Self {
        PlatformError::ValidationError(format!("UUID error: {}", error))
    }
}

// Custom error response builder
pub struct ErrorResponseBuilder {
    success: bool,
    code: String,
    message: String,
    details: Option<serde_json::Value>,
    timestamp: String,
    request_id: Option<String>,
    path: Option<String>,
    method: Option<String>,
}

impl ErrorResponseBuilder {
    pub fn new() -> Self {
        Self {
            success: false,
            code: String::new(),
            message: String::new(),
            details: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
            path: None,
            method: None,
        }
    }

    pub fn code<S: Into<String>>(mut self, code: S) -> Self {
        self.code = code.into();
        self
    }

    pub fn message<S: Into<String>>(mut self, message: S) -> Self {
        self.message = message.into();
        self
    }

    pub fn details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn request_id<S: Into<String>>(mut self, request_id: S) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn path<S: Into<String>>(mut self, path: S) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn method<S: Into<String>>(mut self, method: S) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn build(self) -> ErrorResponse {
        ErrorResponse {
            success: self.success,
            error: ErrorDetails {
                code: self.code,
                message: self.message,
                details: self.details,
            },
            timestamp: self.timestamp,
            request_id: self.request_id,
            path: self.path,
            method: self.method,
        }
    }
}

impl Default for ErrorResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Error handling middleware
pub async fn error_handling_middleware<B>(
    request: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> Result<impl axum::response::IntoResponse, PlatformError> {
    // Get request information
    let path = request.uri().path().to_string();
    let method = request.method().clone();
    
    match next.run(request).await {
        Ok(response) => Ok(response),
        Err(err) => {
            // Log the error
            log_error(&err, None); // In a real implementation, you'd extract request ID
            
            // Create an error response with request context
            let error_response = ErrorResponseBuilder::new()
                .code("INTERNAL_001")
                .message("Internal server error occurred".to_string())
                .path(path)
                .method(method.to_string())
                .details(serde_json::json!({
                    "error": format!("{}", err),
                    "error_type": std::mem::discriminant(&err).as_std(),
                }))
                .build();
                
            Err(err)
        }
    }
}

// Standardized error response utilities
pub mod response {
    use super::*;
    
    /// Create a standard error response with specific status code
    pub fn create_error_response(
        status: StatusCode,
        code: &str,
        message: &str,
        details: Option<serde_json::Value>,
    ) -> (StatusCode, Json<ErrorResponse>) {
        let error_response = ErrorResponse {
            success: false,
            error: ErrorDetails {
                code: code.to_string(),
                message: message.to_string(),
                details,
            },
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: None,
            path: None,
            method: None,
        };
        
        (status, Json(error_response))
    }
    
    /// Create a validation error response
    pub fn validation_error(
        field: &str,
        message: &str,
        value: Option<&str>,
    ) -> (StatusCode, Json<ErrorResponse>) {
        create_error_response(
            StatusCode::BAD_REQUEST,
            "VALIDATION_001",
            &format!("Validation failed for field '{}': {}", field, message),
            Some(serde_json::json!({
                "field": field,
                "value": value,
                "validation_message": message,
            })),
        )
    }
    
    /// Create a not found error response
    pub fn not_found(
        resource_type: &str,
        identifier: &str,
    ) -> (StatusCode, Json<ErrorResponse>) {
        create_error_response(
            StatusCode::NOT_FOUND,
            "NOT_FOUND_001",
            &format!("{} with identifier '{}' not found", resource_type, identifier),
            Some(serde_json::json!({
                "resource_type": resource_type,
                "identifier": identifier,
            })),
        )
    }
    
    /// Create an unauthorized error response
    pub fn unauthorized(
        message: &str,
    ) -> (StatusCode, Json<ErrorResponse>) {
        create_error_response(
            StatusCode::UNAUTHORIZED,
            "AUTH_001",
            message,
            None,
        )
    }
    
    /// Create a forbidden error response
    pub fn forbidden(
        message: &str,
    ) -> (StatusCode, Json<ErrorResponse>) {
        create_error_response(
            StatusCode::FORBIDDEN,
            "AUTH_002",
            message,
            None,
        )
    }
}