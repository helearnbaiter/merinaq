//! Metrics API Routes
//! 
//! Defines routes for metrics and monitoring endpoints

use axum::{
    routing::{get},
    Router,
};

use crate::handlers;

/// Create the metrics router with all metrics-related routes
pub fn create_router() -> Router {
    Router::new()
        .route("/", get(handlers::metrics::metrics_endpoint))
        .route("/pool-stats", get(handlers::metrics::connection_pool_stats))
        .route("/circuit-breaker", get(handlers::metrics::circuit_breaker_status))
        .route("/history", get(handlers::metrics::historical_metrics))
}