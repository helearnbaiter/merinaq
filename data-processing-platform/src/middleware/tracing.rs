//! Tracing and Request Context Middleware
//! 
//! Provides comprehensive request tracing, context propagation, and structured logging

use axum::{
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{info, warn, error, span, Level, field};

/// Request context that will be passed through the middleware chain
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
    pub trace_id: String,
    pub start_time: std::time::Instant,
    pub method: String,
    pub path: String,
    pub user_agent: Option<String>,
    pub remote_addr: Option<String>,
}

impl RequestContext {
    pub fn new(
        request_id: String,
        trace_id: String,
        method: String,
        path: String,
        user_agent: Option<String>,
        remote_addr: Option<String>,
    ) -> Self {
        Self {
            request_id,
            trace_id,
            start_time: std::time::Instant::now(),
            method,
            path,
            user_agent,
            remote_addr,
        }
    }

    pub fn duration_ms(&self) -> u128 {
        self.start_time.elapsed().as_millis()
    }
}

/// Middleware for request tracing and context propagation
pub async fn tracing_middleware(request: Request, next: Next) -> Result<Response, StatusCode> {
    let start_time = Instant::now();
    
    // Extract or generate request ID
    let request_id = extract_or_generate_request_id(&request.headers());
    
    // Generate trace ID (for now, using the same as request ID, in production you might want separate trace IDs)
    let trace_id = extract_or_generate_trace_id(&request.headers());
    
    // Extract request information
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let user_agent = extract_user_agent(&request.headers());
    let remote_addr = extract_remote_addr(request.extensions().get::<axum::extract::ConnectInfo<tokio::net::tcp::PeerAddr>>());
    
    // Create request context
    let context = RequestContext::new(
        request_id.clone(),
        trace_id.clone(),
        method.clone(),
        path.clone(),
        user_agent.clone(),
        remote_addr.clone(),
    );
    
    // Create a tracing span for this request
    let span = span!(
        Level::INFO,
        "http_request",
        request_id = context.request_id.as_str(),
        trace_id = context.trace_id.as_str(),
        method = context.method.as_str(),
        path = context.path.as_str(),
        user_agent = field::Empty,
        remote_addr = field::Empty,
        response_time_ms = field::Empty,
        status_code = field::Empty,
    );
    
    let _enter = span.enter();
    
    // Add user agent and remote addr to span if available
    if let Some(ua) = &context.user_agent {
        span.record("user_agent", ua.as_str());
    }
    if let Some(addr) = &context.remote_addr {
        span.record("remote_addr", addr.as_str());
    }
    
    info!(
        "Request started: {} {} from {}",
        context.method,
        context.path,
        context.remote_addr.as_deref().unwrap_or("unknown")
    );
    
    // Add request ID to response headers
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "X-Request-ID",
        HeaderValue::from_str(&context.request_id)
            .unwrap_or_else(|_| HeaderValue::from_static("unknown")),
    );
    
    // Record response time and status
    let duration_ms = start_time.elapsed().as_millis();
    let status_code = response.status().as_u16();
    
    // Update span with response information
    span.record("response_time_ms", duration_ms);
    span.record("status_code", status_code);
    
    info!(
        "Request completed: {} {} - Status: {}, Duration: {}ms",
        context.method,
        context.path,
        status_code,
        duration_ms
    );
    
    // Log slow requests
    if duration_ms > 1000 { // Log requests taking more than 1 second
        warn!(
            "Slow request detected: {} {} - {}ms",
            context.method, context.path, duration_ms
        );
    }
    
    Ok(response)
}

/// Extract request ID from headers or generate a new one
fn extract_or_generate_request_id(headers: &HeaderMap) -> String {
    if let Some(request_id_header) = headers.get("X-Request-ID") {
        if let Ok(request_id) = request_id_header.to_str() {
            if !request_id.is_empty() {
                return request_id.to_string();
            }
        }
    }
    
    // Generate new request ID
    use uuid::Uuid;
    Uuid::new_v4().to_string()
}

/// Extract trace ID from headers or generate a new one
fn extract_or_generate_trace_id(headers: &HeaderMap) -> String {
    if let Some(trace_id_header) = headers.get("X-Trace-ID") {
        if let Ok(trace_id) = trace_id_header.to_str() {
            if !trace_id.is_empty() {
                return trace_id.to_string();
            }
        }
    }
    
    // Generate new trace ID
    use uuid::Uuid;
    Uuid::new_v4().to_string()
}

/// Extract user agent from headers
fn extract_user_agent(headers: &HeaderMap) -> Option<String> {
    headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
}

/// Extract remote address from connection info
fn extract_remote_addr(peer_addr: Option<&axum::extract::ConnectInfo<tokio::net::tcp::PeerAddr>>) -> Option<String> {
    peer_addr.map(|addr| addr.0.to_string())
}

/// Error logging middleware
pub async fn error_logging_middleware<B>(
    request: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> Result<impl axum::response::IntoResponse, (StatusCode, String)> {
    let path = request.uri().path().to_string();
    let method = request.method().clone();
    let request_id = extract_or_generate_request_id(request.headers());
    
    match next.run(request).await {
        Ok(response) => Ok(response),
        Err(err) => {
            error!(
                "Request error: {} {} - Request ID: {}, Error: {}",
                method,
                path,
                request_id,
                err
            );
            
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            ))
        }
    }
}

/// Request context extractor (placeholder for now)
pub struct TracingContext(pub Arc<RequestContext>);

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_extract_or_generate_request_id() {
        let mut headers = HeaderMap::new();
        let request_id = "test-request-id";
        headers.insert("X-Request-ID", HeaderValue::from_str(request_id).unwrap());
        
        assert_eq!(extract_or_generate_request_id(&headers), request_id);
    }
    
    #[tokio::test]
    async fn test_extract_or_generate_request_id_no_header() {
        let headers = HeaderMap::new();
        let result = extract_or_generate_request_id(&headers);
        
        // Should generate a valid UUID
        assert!(!result.is_empty());
        assert!(result.len() == 36); // UUID length
    }
}