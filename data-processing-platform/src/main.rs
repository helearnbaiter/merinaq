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
    let config = AppConfig::from_env().await.map_err(|e| PlatformError::ConfigError(e.to_string()))?;
    info!("Configuration loaded: {:?}", config.app_name);

    // Initialize database pool
    let db_pool = DatabasePool::new(&config.database_url).await.map_err(|e| PlatformError::DatabaseError(e))?;
    info!("Database pool initialized");

    // Initialize Casbin service
    let casbin_enforcer = CasbinService::new(&config.database_url).await.map_err(|e| PlatformError::CasbinError(e))?;
    info!("Casbin service initialized");

    // Initialize Auth service
    let auth_service = AuthService::new(config.jwt_secret.clone());
    info!("Auth service initialized");

    // Initialize Query service
    let query_service = Arc::new(QueryService::new(db_pool.clone()));
    info!("Query service initialized");

    // Build application with shared state
    let app = Router::new()
        // Health check endpoint
        .route("/health", get(handlers::health::health_check))
        
        // Authentication routes
        .route("/api/v1/auth/login", post(handlers::auth::login))
        .route("/api/v1/auth/refresh", post(handlers::auth::refresh_token))
        .route("/api/v1/auth/logout", post(handlers::auth::logout))
        
        // User management routes
        .route("/api/v1/users", get(handlers::user::get_users))
        .route("/api/v1/users", post(handlers::user::create_user))
        .route("/api/v1/users/:id", get(handlers::user::get_user))
        .route("/api/v1/users/:id", put(handlers::user::update_user))
        .route("/api/v1/users/:id", delete(handlers::user::delete_user))
        
        // Data source management routes
        .route("/api/v1/data-sources", get(handlers::data_source::get_data_sources))
        .route("/api/v1/data-sources", post(handlers::data_source::create_data_source))
        .route("/api/v1/data-sources/:id", get(handlers::data_source::get_data_source))
        .route("/api/v1/data-sources/:id", put(handlers::data_source::update_data_source))
        .route("/api/v1/data-sources/:id", delete(handlers::data_source::delete_data_source))
        .route("/api/v1/data-sources/:id/test", post(handlers::data_source::test_connection))
        
        // Query execution routes
        .route("/api/v1/query", post(handlers::query::execute_query))
        .route("/api/v1/query/execute", post(handlers::query::execute_sql))
        .route("/api/v1/query/schema", get(handlers::query::get_schema))
        
        // Permission management routes
        .route("/api/v1/policies", get(handlers::policy::get_policies))
        .route("/api/v1/policies", post(handlers::policy::create_policy))
        .route("/api/v1/policies/:id", delete(handlers::policy::delete_policy))
        .route("/api/v1/policies/check", post(handlers::policy::check_permission))
        
        // Apply middleware
        .layer(from_fn(middleware::auth::auth_middleware))
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::any())
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers([axum::http::header::CONTENT_TYPE, axum::http::header::AUTHORIZATION])
        )
        // Add shared state
        .layer(Extension(db_pool))
        .layer(Extension(casbin_enforcer))
        .layer(Extension(auth_service))
        .layer(Extension(query_service));

    // Run server
    let listener = tokio::net::TcpListener::bind(&format!("{}:{}", config.server_host, config.server_port))
        .await
        .map_err(|e| PlatformError::InternalError(format!("Failed to bind to address: {}", e)))?;
    
    info!("Server running on http://{}:{}", config.server_host, config.server_port);
    
    axum::serve(listener, app)
        .await
        .map_err(|e| PlatformError::InternalError(format!("Server error: {}", e)))?;

    Ok(())
}