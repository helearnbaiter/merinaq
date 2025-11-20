//! API Version 1 Routes
//! 
//! This module defines all routes for version 1 of the API

pub mod auth;
pub mod users;
pub mod data_sources;
pub mod queries;
pub mod policies;
pub mod bi_integration;
pub mod health;

use axum::{
    routing::{get, post, put, delete},
    Router,
};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    services::auth_service::AuthService,
    services::casbin_service::CasbinService,
    services::query_service::QueryService,
    database::DatabasePool,
};

/// Create the version 1 API router with all routes
pub fn create_v1_router(
    db_pool: DatabasePool,
    casbin_enforcer: CasbinService,
    auth_service: Arc<RwLock<AuthService>>,
    query_service: Arc<QueryService>,
) -> Router {
    Router::new()
        // Health check endpoint
        .nest("/health", health::create_router())
        
        // Authentication routes
        .nest("/auth", auth::create_router())
        
        // User management routes
        .nest("/users", users::create_router())
        
        // Data source management routes
        .nest("/data-sources", data_sources::create_router())
        
        // Query execution routes
        .nest("/query", queries::create_router())
        
        // Permission management routes
        .nest("/policies", policies::create_router())
        
        // BI Tool Integration routes
        .nest("/bi", bi_integration::create_router())
        
        // Add shared state
        .layer(axum::extract::Extension(db_pool))
        .layer(axum::extract::Extension(casbin_enforcer))
        .layer(axum::extract::Extension(auth_service))
        .layer(axum::extract::Extension(query_service))
}