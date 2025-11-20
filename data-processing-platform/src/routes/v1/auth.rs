//! Authentication routes
//! 
//! Provides endpoints for user authentication

use axum::{
    routing::post,
    Router,
};
use crate::handlers::auth::{login, refresh_token, logout};

/// Create the authentication router
pub fn create_router() -> Router {
    Router::new()
        .route("/login", post(login))
        .route("/refresh", post(refresh_token))
        .route("/logout", post(logout))
}