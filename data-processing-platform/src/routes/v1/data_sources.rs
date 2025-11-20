//! Data source management routes
//! 
//! Provides endpoints for data source management operations

use axum::{
    routing::{get, post, put, delete},
    Router,
};
use crate::handlers::data_source::{get_data_sources, create_data_source, get_data_source, update_data_source, delete_data_source, test_connection};

/// Create the data source management router
pub fn create_router() -> Router {
    Router::new()
        .route("/", get(get_data_sources))
        .route("/", post(create_data_source))
        .route("/:id", get(get_data_source))
        .route("/:id", put(update_data_source))
        .route("/:id", delete(delete_data_source))
        .route("/:id/test", post(test_connection))
}