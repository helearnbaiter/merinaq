//! Query execution routes
//! 
//! Provides endpoints for query execution operations

use axum::{
    routing::{get, post},
    Router,
};
use crate::handlers::query::{execute_query, execute_sql, get_schema};

/// Create the query execution router
pub fn create_router() -> Router {
    Router::new()
        .route("/", post(execute_query))
        .route("/execute", post(execute_sql))
        .route("/schema", get(get_schema))
}