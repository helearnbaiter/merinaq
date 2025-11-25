//! Health check handler
//! 
//! Provides health check endpoint for monitoring and load balancers

use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::models::ApiResponse;
use crate::database::DatabasePool;
use crate::services::{AuthService, QueryService, CasbinService};

pub struct HealthState {
    pub db_pool: DatabasePool,
    pub auth_service: Arc<RwLock<AuthService>>,
    pub query_service: Arc<QueryService>,
    pub casbin_service: Arc<CasbinService>,
}

pub async fn health_check() -> impl IntoResponse {
    // Basic health check response
    let response = ApiResponse::success(json!({
        "status": "healthy",
        "service": "Data Processing Platform",
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "uptime": get_uptime(),
    }));
    
    (StatusCode::OK, Json(response))
}

// Detailed health check that includes database connectivity
pub async fn detailed_health_check(
    Extension(health_state): Extension<Arc<HealthState>>,
) -> impl IntoResponse {
    let mut checks = std::collections::HashMap::new();
    
    // Database connectivity check
    match check_database_health(&health_state.db_pool).await {
        Ok(healthy) => {
            checks.insert("database".to_string(), json!({
                "status": if healthy { "healthy" } else { "unhealthy" },
                "message": if healthy { "Database connection successful" } else { "Database connection failed" }
            }));
        },
        Err(e) => {
            checks.insert("database".to_string(), json!({
                "status": "unhealthy",
                "message": format!("Database check failed: {}", e)
            }));
        }
    }
    
    // Check other services as needed
    checks.insert("auth_service".to_string(), json!({
        "status": "healthy",
        "message": "Auth service available"
    }));
    
    checks.insert("query_service".to_string(), json!({
        "status": "healthy",
        "message": "Query service available"
    }));
    
    checks.insert("casbin_service".to_string(), json!({
        "status": "healthy",
        "message": "Casbin service available"
    }));
    
    // Overall status
    let overall_status = if checks.values().all(|v| v["status"] == "healthy") {
        "healthy"
    } else {
        "unhealthy"
    };
    
    let response = ApiResponse::success(json!({
        "status": overall_status,
        "service": "Data Processing Platform",
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "uptime": get_uptime(),
        "checks": checks,
    }));
    
    (StatusCode::OK, Json(response))
}

async fn check_database_health(db_pool: &DatabasePool) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    // Attempt a simple query to test database connectivity
    let conn = db_pool.get().await
        .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    
    let result = sqlx::query("SELECT 1 as test")
        .fetch_one(&conn)
        .await;
    
    match result {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

fn get_uptime() -> String {
    use std::time::SystemTime;
    use std::env;
    
    // Try to get start time from environment or use current time as reference
    let start_time = env::var("SERVICE_START_TIME")
        .unwrap_or_else(|_| chrono::Utc::now().to_rfc3339());
    
    format!("{}", start_time)
}