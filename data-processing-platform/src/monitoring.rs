//! Comprehensive Monitoring and Alerting System
//!
//! This module provides:
//! - Prometheus metrics collection
//! - Distributed tracing support
//! - Structured logging
//! - Alerting mechanisms

use std::sync::Arc;
use prometheus::{
    Encoder, IntCounter, IntGauge, Histogram, TextEncoder, 
    register_int_counter, register_int_gauge, register_histogram,
};
use tokio::sync::RwLock;
use tracing::{info, warn, error};

use crate::query_engine::connection_monitor::ConnectionPoolMonitor;

lazy_static::lazy_static! {
    // Request metrics
    pub static ref HTTP_REQUESTS_TOTAL: IntCounter = 
        register_int_counter!("http_requests_total", "Total number of HTTP requests").unwrap();
    
    pub static ref HTTP_REQUEST_DURATION: Histogram = 
        register_histogram!("http_request_duration_seconds", "HTTP request duration in seconds").unwrap();
    
    pub static ref ACTIVE_CONNECTIONS: IntGauge = 
        register_int_gauge!("active_connections", "Number of active database connections").unwrap();
    
    pub static ref QUERY_EXECUTIONS_TOTAL: IntCounter = 
        register_int_counter!("query_executions_total", "Total number of query executions").unwrap();
    
    pub static ref QUERY_DURATION: Histogram = 
        register_histogram!("query_duration_seconds", "Query execution duration in seconds").unwrap();
    
    pub static ref CONNECTION_POOL_USAGE: IntGauge = 
        register_int_gauge!("connection_pool_usage_percent", "Connection pool usage percentage").unwrap();
    
    pub static ref SLOW_QUERIES_TOTAL: IntCounter = 
        register_int_counter!("slow_queries_total", "Total number of slow queries").unwrap();
    
    pub static ref FAILED_QUERIES_TOTAL: IntCounter = 
        register_int_counter!("failed_queries_total", "Total number of failed queries").unwrap();
}

/// Alerting system for monitoring
pub struct AlertSystem {
    alerts: Arc<RwLock<Vec<Alert>>>,
}

/// Alert definition
#[derive(Debug, Clone)]
pub struct Alert {
    pub id: String,
    pub name: String,
    pub description: String,
    pub severity: AlertSeverity,
    pub condition: AlertCondition,
    pub active: bool,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Alert severity levels
#[derive(Debug, Clone)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Alert condition types
#[derive(Debug, Clone)]
pub enum AlertCondition {
    Threshold { metric: String, threshold: f64, operator: ComparisonOperator },
    Anomaly { metric: String },
}

/// Comparison operators for alert conditions
#[derive(Debug, Clone)]
pub enum ComparisonOperator {
    GreaterThan,
    LessThan,
    Equal,
    NotEqual,
}

impl AlertSystem {
    pub fn new() -> Self {
        Self {
            alerts: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Add a new alert rule
    pub async fn add_alert_rule(&self, alert: Alert) {
        let mut alerts = self.alerts.write().await;
        alerts.push(alert);
    }

    /// Check if any alerts should be triggered based on current metrics
    pub async fn check_alerts(&self) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        let mut triggered = Vec::new();
        
        for alert in alerts.iter() {
            if self.evaluate_condition(&alert.condition).await {
                let mut alert_clone = alert.clone();
                alert_clone.active = true;
                triggered.push(alert_clone);
            }
        }
        
        triggered
    }

    /// Evaluate an alert condition
    async fn evaluate_condition(&self, condition: &AlertCondition) -> bool {
        match condition {
            AlertCondition::Threshold { metric, threshold, operator } => {
                match metric.as_str() {
                    "connection_pool_usage" => {
                        let current_value = CONNECTION_POOL_USAGE.get() as f64;
                        match operator {
                            ComparisonOperator::GreaterThan => current_value > *threshold,
                            ComparisonOperator::LessThan => current_value < *threshold,
                            ComparisonOperator::Equal => (current_value - threshold).abs() < f64::EPSILON,
                            ComparisonOperator::NotEqual => (current_value - threshold).abs() >= f64::EPSILON,
                        }
                    },
                    "active_connections" => {
                        let current_value = ACTIVE_CONNECTIONS.get() as f64;
                        match operator {
                            ComparisonOperator::GreaterThan => current_value > *threshold,
                            ComparisonOperator::LessThan => current_value < *threshold,
                            ComparisonOperator::Equal => (current_value - threshold).abs() < f64::EPSILON,
                            ComparisonOperator::NotEqual => (current_value - threshold).abs() >= f64::EPSILON,
                        }
                    },
                    _ => false,
                }
            },
            AlertCondition::Anomaly { metric: _ } => {
                // Placeholder for anomaly detection logic
                false
            }
        }
    }

    /// Get all active alerts
    pub async fn get_active_alerts(&self) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts.iter()
            .filter(|alert| alert.active)
            .cloned()
            .collect()
    }
}

/// Initialize the monitoring system
pub async fn init_monitoring_system(connection_monitor: Arc<ConnectionPoolMonitor>) -> Result<(), Box<dyn std::error::Error>> {
    info!("Initializing monitoring system");
    
    // Start background monitoring tasks
    connection_monitor.start_background_tasks().await?;
    
    // Start metrics collection background task
    tokio::spawn(async move {
        loop {
            // Update connection pool metrics periodically
            let all_stats = connection_monitor.get_all_pool_stats().await;
            for stats in all_stats {
                CONNECTION_POOL_USAGE.set(stats.connection_usage_rate as i64);
            }
            
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    });
    
    info!("Monitoring system initialized successfully");
    Ok(())
}

/// Get Prometheus metrics in text format
pub fn get_prometheus_metrics() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    
    String::from_utf8(buffer).unwrap()
}

/// Initialize default alert rules
pub async fn init_default_alerts(alert_system: Arc<AlertSystem>) {
    // High connection pool usage alert
    alert_system.add_alert_rule(Alert {
        id: "high_connection_pool_usage".to_string(),
        name: "High Connection Pool Usage".to_string(),
        description: "Connection pool usage is above 80%".to_string(),
        severity: AlertSeverity::Warning,
        condition: AlertCondition::Threshold {
            metric: "connection_pool_usage".to_string(),
            threshold: 80.0,
            operator: ComparisonOperator::GreaterThan,
        },
        active: false,
        timestamp: chrono::Utc::now(),
    }).await;
    
    // High active connections alert
    alert_system.add_alert_rule(Alert {
        id: "high_active_connections".to_string(),
        name: "High Active Connections".to_string(),
        description: "Active connections are above 100".to_string(),
        severity: AlertSeverity::Warning,
        condition: AlertCondition::Threshold {
            metric: "active_connections".to_string(),
            threshold: 100.0,
            operator: ComparisonOperator::GreaterThan,
        },
        active: false,
        timestamp: chrono::Utc::now(),
    }).await;
    
    info!("Default alert rules initialized");
}