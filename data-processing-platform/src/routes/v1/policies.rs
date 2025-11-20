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
        .route("/type/:policy_type", get(super::super::super::handlers::policy::get_policy_by_type))
        .route("/user/:user_id", get(super::super::super::handlers::policy::get_permissions_for_user))
        .route("/resource/:resource", get(super::super::super::handlers::policy::get_permissions_for_resource))
        .route("/", post(super::super::super::handlers::policy::create_policy))
        .route("/bulk", post(super::super::super::handlers::policy::bulk_add_policies))
        .route("/:id", delete(super::super::super::handlers::policy::delete_policy))
        .route("/check", post(super::super::super::handlers::policy::check_permission))
}