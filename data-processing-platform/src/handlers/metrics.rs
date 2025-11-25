//! Metrics and Monitoring Handler
//! 
//! Provides endpoints for metrics collection and monitoring data

use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::{
    models::ApiResponse,
    query_engine::connection_monitor::{ConnectionPoolMonitor, PoolStats},
    services::QueryService,
};

/// Global connection pool monitor - in a real implementation this would be shared across the application
static mut GLOBAL_MONITOR: Option<Arc<ConnectionPoolMonitor>> = None;
static INIT: std::sync::Once = std::sync::Once::new();

/// Set the global monitor instance
pub fn set_global_monitor(monitor: Arc<ConnectionPoolMonitor>) {
    unsafe {
        INIT.call_once(|| {
            GLOBAL_MONITOR = Some(monitor);
        });
    }
}

/// Get the global monitor instance
fn get_global_monitor() -> Option<Arc<ConnectionPoolMonitor>> {
    unsafe { GLOBAL_MONITOR.clone() }
}

/// Metrics endpoint - returns metrics in Prometheus format
pub async fn metrics_endpoint() -> impl IntoResponse {
    let mut metrics_output = String::new();
    
    // Add basic metrics
    metrics_output.push_str("# HELP data_processing_platform_info Platform information\n");
    metrics_output.push_str("# TYPE data_processing_platform_info gauge\n");
    metrics_output.push_str("data_processing_platform_info{version=\"0.1.0\"} 1\n\n");
    
    // Add connection pool metrics if available
    if let Some(monitor) = get_global_monitor() {
        let all_stats = monitor.get_all_pool_stats().await;
        
        metrics_output.push_str("# HELP connection_pool_connections_total Total connections in pool\n");
        metrics_output.push_str("# TYPE connection_pool_connections_total gauge\n");
        
        for stats in all_stats {
            metrics_output.push_str(&format!(
                "connection_pool_connections_total{{pool_id=\"{}\"}} {}\n",
                stats.pool_id, stats.total_connections
            ));
        }
        
        metrics_output.push_str("\n# HELP connection_pool_available_connections Available connections in pool\n");
        metrics_output.push_str("# TYPE connection_pool_available_connections gauge\n");
        
        for stats in &monitor.get_all_pool_stats().await {
            metrics_output.push_str(&format!(
                "connection_pool_available_connections{{pool_id=\"{}\"}} {}\n",
                stats.pool_id, stats.available_connections
            ));
        }
        
        metrics_output.push_str("\n# HELP connection_pool_max_connections Maximum connections allowed in pool\n");
        metrics_output.push_str("# TYPE connection_pool_max_connections gauge\n");
        
        for stats in &monitor.get_all_pool_stats().await {
            metrics_output.push_str(&format!(
                "connection_pool_max_connections{{pool_id=\"{}\"}} {}\n",
                stats.pool_id, stats.max_connections
            ));
        }
        
        metrics_output.push_str("\n# HELP connection_pool_usage_percentage Connection usage percentage\n");
        metrics_output.push_str("# TYPE connection_pool_usage_percentage gauge\n");
        
        for stats in &monitor.get_all_pool_stats().await {
            metrics_output.push_str(&format!(
                "connection_pool_usage_percentage{{pool_id=\"{}\"}} {:.2}\n",
                stats.pool_id, stats.connection_usage_rate
            ));
        }
        
        metrics_output.push_str("\n# HELP connection_pool_waiting_count Number of requests waiting for connections\n");
        metrics_output.push_str("# TYPE connection_pool_waiting_count gauge\n");
        
        for stats in &monitor.get_all_pool_stats().await {
            metrics_output.push_str(&format!(
                "connection_pool_waiting_count{{pool_id=\"{}\"}} {}\n",
                stats.pool_id, stats.waiting_count
            ));
        }
    }
    
    (StatusCode::OK, metrics_output)
}

/// Get detailed connection pool statistics
pub async fn connection_pool_stats() -> impl IntoResponse {
    if let Some(monitor) = get_global_monitor() {
        let stats = monitor.get_all_pool_stats().await;
        
        let response = ApiResponse::success(json!({
            "pools": stats,
            "total_pools": stats.len(),
        }));
        
        (StatusCode::OK, Json(response))
    } else {
        let response = ApiResponse::error("Connection pool monitor not initialized", StatusCode::INTERNAL_SERVER_ERROR.as_u16());
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

/// Get circuit breaker status for all pools
pub async fn circuit_breaker_status() -> impl IntoResponse {
    if let Some(monitor) = get_global_monitor() {
        let breakers = monitor.get_all_circuit_breaker_statuses().await;
        
        let response = ApiResponse::success(json!({
            "circuit_breakers": breakers,
            "total_breakers": breakers.len(),
        }));
        
        (StatusCode::OK, Json(response))
    } else {
        let response = ApiResponse::error("Connection pool monitor not initialized", StatusCode::INTERNAL_SERVER_ERROR.as_u16());
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

/// Get historical metrics
pub async fn historical_metrics() -> impl IntoResponse {
    if let Some(monitor) = get_global_monitor() {
        let history = monitor.get_metrics_history().await;
        
        let response = ApiResponse::success(json!({
            "metrics_history": history,
            "history_count": history.len(),
        }));
        
        (StatusCode::OK, Json(response))
    } else {
        let response = ApiResponse::error("Connection pool monitor not initialized", StatusCode::INTERNAL_SERVER_ERROR.as_u16());
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use tokio;
    use tower::ServiceExt; // for `app.oneshot()`

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let response = metrics_endpoint().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        
        assert!(body_text.contains("data_processing_platform_info"));
    }
}