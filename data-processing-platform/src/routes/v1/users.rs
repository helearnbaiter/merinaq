//! User management routes
//! 
//! Provides endpoints for user management operations

use axum::{
    routing::{get, post, put, delete},
    Router,
};
use crate::handlers::user::{get_users, create_user, get_user, update_user, delete_user};

/// Create the user management router
pub fn create_router() -> Router {
    Router::new()
        .route("/", get(get_users))
        .route("/", post(create_user))
        .route("/:id", get(get_user))
        .route("/:id", put(update_user))
        .route("/:id", delete(delete_user))
}