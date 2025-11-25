//! Metrics and Monitoring Handler
//! 
//! Provides endpoints for metrics collection and monitoring data

use axum::{
    extract::Extension,
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
    monitoring,
};

/// Metrics endpoint - returns metrics in Prometheus format
pub async fn metrics_endpoint() -> impl IntoResponse {
    let prometheus_metrics = monitoring::get_prometheus_metrics();
    (StatusCode::OK, prometheus_metrics)
}

/// Get detailed connection pool statistics
pub async fn connection_pool_stats(
    Extension(monitor): Extension<Arc<ConnectionPoolMonitor>>,
) -> impl IntoResponse {
    let stats = monitor.get_all_pool_stats().await;
    
    let response = ApiResponse::success(json!({
        "pools": stats,
        "total_pools": stats.len(),
    }));
    
    (StatusCode::OK, Json(response))
}

/// Get circuit breaker status for all pools
pub async fn circuit_breaker_status(
    Extension(monitor): Extension<Arc<ConnectionPoolMonitor>>,
) -> impl IntoResponse {
    let breakers = monitor.get_all_circuit_breaker_statuses().await;
    
    let response = ApiResponse::success(json!({
        "circuit_breakers": breakers,
        "total_breakers": breakers.len(),
    }));
    
    (StatusCode::OK, Json(response))
}

/// Get historical metrics
pub async fn historical_metrics(
    Extension(monitor): Extension<Arc<ConnectionPoolMonitor>>,
) -> impl IntoResponse {
    let history = monitor.get_metrics_history().await;
    
    let response = ApiResponse::success(json!({
        "metrics_history": history,
        "history_count": history.len(),
    }));
    
    (StatusCode::OK, Json(response))
}

/// Get active alerts
pub async fn active_alerts(
    Extension(alert_system): Extension<Arc<monitoring::AlertSystem>>,
) -> impl IntoResponse {
    let active_alerts = alert_system.get_active_alerts().await;
    
    let response = ApiResponse::success(json!({
        "active_alerts": active_alerts,
        "alert_count": active_alerts.len(),
    }));
    
    (StatusCode::OK, Json(response))
}

/// Trigger alert check
pub async fn trigger_alert_check(
    Extension(alert_system): Extension<Arc<monitoring::AlertSystem>>,
) -> impl IntoResponse {
    let triggered_alerts = alert_system.check_alerts().await;
    
    let response = ApiResponse::success(json!({
        "triggered_alerts": triggered_alerts,
        "triggered_count": triggered_alerts.len(),
    }));
    
    (StatusCode::OK, Json(response))
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
        
        assert!(body_text.contains("http_requests_total"));
    }
}