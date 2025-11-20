//! Policy management routes
//! 
//! Provides endpoints for policy management operations

use axum::{
    routing::{get, post, delete},
    Router,
};

/// Create the policy management router
pub fn create_router() -> Router {
    Router::new()
        .route("/", get(super::super::super::handlers::policy::get_policies))
        .route("/", post(super::super::super::handlers::policy::create_policy))
        .route("/:id", delete(super::super::super::handlers::policy::delete_policy))
        .route("/check", post(super::super::super::handlers::policy::check_permission))
}