//! Health check routes
//! 
//! Provides endpoints for health checking the application

use axum::{
    routing::get,
    Router,
};
use crate::handlers::health::health_check;

/// Create the health check router
pub fn create_router() -> Router {
    Router::new()
        .route("/", get(health_check))
}