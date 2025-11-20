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

/// Initialize OAuth2 providers
async fn initialize_oauth2_providers(
    auth_service: &Arc<tokio::sync::RwLock<AuthService>>,
    config: &AppConfig,
) -> PlatformResult<()> {
    use crate::auth::{OAuth2Config, OAuth2Provider};
    
    // Initialize Google OAuth2 provider if configured
    if let Some(ref google_config) = config.auth.google {
        let google_provider = OAuth2Provider::new(
            "google".to_string(),
            OAuth2Config::new(
                google_config.client_id.clone(),
                google_config.client_secret.clone(),
                format!("{}/auth/oauth2/google/callback", config.server.get_base_url()),
                "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
                "https://oauth2.googleapis.com/token".to_string(),
                "https://www.googleapis.com/oauth2/v2/userinfo".to_string(),
            )
        );
        auth_service.add_oauth2_provider("google".to_string(), google_provider).await;
        info!("Google OAuth2 provider initialized");
    }
    
    // Initialize GitHub OAuth2 provider if configured
    if let Some(ref github_config) = config.auth.github {
        let github_provider = OAuth2Provider::new(
            "github".to_string(),
            OAuth2Config::new(
                github_config.client_id.clone(),
                github_config.client_secret.clone(),
                format!("{}/auth/oauth2/github/callback", config.server.get_base_url()),
                "https://github.com/login/oauth/authorize".to_string(),
                "https://github.com/login/oauth/access_token".to_string(),
                "https://api.github.com/user".to_string(),
            )
        );
        auth_service.add_oauth2_provider("github".to_string(), github_provider).await;
        info!("GitHub OAuth2 provider initialized");
    }
    
    // Initialize Facebook OAuth2 provider if configured
    if let Some(ref facebook_config) = config.auth.facebook {
        let facebook_provider = OAuth2Provider::new(
            "facebook".to_string(),
            OAuth2Config::new(
                facebook_config.client_id.clone(),
                facebook_config.client_secret.clone(),
                format!("{}/auth/oauth2/facebook/callback", config.server.get_base_url()),
                "https://www.facebook.com/v18.0/dialog/oauth".to_string(),
                "https://graph.facebook.com/v18.0/oauth/access_token".to_string(),
                "https://graph.facebook.com/me?fields=id,name,email".to_string(),
            )
        );
        auth_service.add_oauth2_provider("facebook".to_string(), facebook_provider).await;
        info!("Facebook OAuth2 provider initialized");
    }
    
    Ok(())
}

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
    let auth_service = Arc::new(tokio::sync::RwLock::new(AuthService::new(&config.auth)));
    info!("Auth service initialized");
    
    // Initialize OAuth2 providers
    initialize_oauth2_providers(&auth_service, &config).await?;
    info!("OAuth2 providers initialized");

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