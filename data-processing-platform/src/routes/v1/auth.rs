//! Authentication routes
//! 
//! Provides endpoints for user authentication

use axum::{
    routing::{post, get},
    Router,
};
use crate::handlers::auth::{login, refresh_token, logout, oauth2_authorize, oauth2_callback};

/// Create the authentication router
pub fn create_router() -> Router {
    Router::new()
        .route("/login", post(login))
        .route("/refresh", post(refresh_token))
        .route("/logout", post(logout))
        .route("/oauth2/:provider_name", get(oauth2_authorize))
        .route("/oauth2/:provider_name/callback", get(oauth2_callback))
}