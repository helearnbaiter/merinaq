//! Modern Data Processing Platform
//! 
//! A high-performance enterprise data processing platform built with Rust,
//! providing unified data querying, permission management, and visualization.

mod config;
mod database;
mod models;
mod services;
mod middleware;
mod handlers;
mod auth;
mod query_engine;
mod utils;
mod routes;
mod api_version;

use axum::{
    extract::Extension,
    http::Method,
    middleware::{from_fn},
    response::Response,
    routing::{get, post, put, delete},
    Router,
};
use std::sync::Arc;
use tokio;
use tower_http::cors::{CorsLayer, AllowOrigin};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use crate::{
    config::AppConfig,
    database::DatabasePool,
    services::auth_service::AuthService,
    services::casbin_service::CasbinService,
    services::query_service::QueryService,
    utils::error::{PlatformError, PlatformResult},
};

#[tokio::main]
async fn main() -> PlatformResult<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    info!("Starting Data Processing Platform...");

    // Load configuration
    let config = AppConfig::from_env().map_err(|e| PlatformError::ConfigError(e.to_string()))?;
    info!("Configuration loaded: {:?}", config.app.name);

    // Initialize database pool
    let db_pool = DatabasePool::new(&config.database.get_database_url()).await.map_err(|e| PlatformError::DatabaseError(e))?;
    info!("Database pool initialized");

    // Initialize Casbin service
    let casbin_enforcer = CasbinService::new(&config.database.get_database_url()).await.map_err(|e| PlatformError::CasbinError(e))?;
    info!("Casbin service initialized");

    // Initialize Auth service
    let auth_service = AuthService::new(&config.auth);
    info!("Auth service initialized");

    // Initialize Query service
    let query_service = Arc::new(QueryService::new(db_pool.clone()));
    info!("Query service initialized");

    // Build application with shared state using modular routing
    let app = api_version::create_api_router(
        db_pool,
        casbin_enforcer,
        Arc::new(tokio::sync::RwLock::new(auth_service)),
        query_service,
    )
    // Add health check at the root level
    .route("/health", get(handlers::health::health_check))
    // Apply middleware
    .layer(from_fn(middleware::auth::auth_middleware))
    .layer(
        CorsLayer::new()
            .allow_origin(AllowOrigin::any())
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::AUTHORIZATION])
    );

    // Run server
    let listener = tokio::net::TcpListener::bind(&format!("{}:{}", config.server.host, config.server.port))
        .await
        .map_err(|e| PlatformError::InternalError(format!("Failed to bind to address: {}", e)))?;
    
    info!("Server running on http://{}:{}", config.server.host, config.server.port);
    
    axum::serve(listener, app)
        .await
        .map_err(|e| PlatformError::InternalError(format!("Server error: {}", e)))?;

    Ok(())
}