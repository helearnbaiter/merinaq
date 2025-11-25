//! Connection Pool Monitoring and Circuit Breaker
//! 
//! This module provides comprehensive monitoring and protection for connection pools,
//! including real-time metrics, circuit breaking, and automatic recovery mechanisms.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tokio::time::sleep;
use serde::{Deserialize, Serialize};

/// Connection pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    pub pool_id: String,
    pub available_connections: usize,
    pub total_connections: usize,
    pub max_connections: usize,
    pub waiting_count: usize,
    pub requests_per_second: f64,
    pub error_rate: f64,
    pub avg_response_time: f64,
    pub connection_usage_rate: f64, // Percentage of connections in use
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl PoolStats {
    pub fn new(
        pool_id: String,
        available_connections: usize,
        total_connections: usize,
        max_connections: usize,
        waiting_count: usize,
    ) -> Self {
        Self {
            pool_id,
            available_connections,
            total_connections,
            max_connections,
            waiting_count,
            requests_per_second: 0.0,
            error_rate: 0.0,
            avg_response_time: 0.0,
            connection_usage_rate: if max_connections > 0 {
                (total_connections as f64 / max_connections as f64) * 100.0
            } else {
                0.0
            },
            timestamp: chrono::Utc::now(),
        }
    }

    /// Calculate connection usage percentage
    pub fn usage_percentage(&self) -> f64 {
        if self.max_connections > 0 {
            (self.total_connections as f64 / self.max_connections as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Check if the pool is under high pressure
    pub fn is_under_pressure(&self) -> bool {
        self.usage_percentage() > 80.0 || self.waiting_count > 10
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CircuitBreakerState {
    /// Normal operation, requests are allowed
    Closed,
    /// Tripped, requests are blocked
    Open,
    /// Testing if service is recovered
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of failures before tripping the circuit
    pub failure_threshold: u32,
    /// Timeout before attempting to close the circuit again (seconds)
    pub timeout_seconds: u64,
    /// Minimum number of requests needed to evaluate error rate
    pub minimum_request_threshold: u32,
    /// Error rate threshold (percentage) for opening circuit
    pub error_rate_threshold: f64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            timeout_seconds: 30,
            minimum_request_threshold: 10,
            error_rate_threshold: 50.0, // 50% error rate
        }
    }
}

/// Circuit breaker for connection pools
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreaker {
    pub state: CircuitBreakerState,
    pub failure_count: u32,
    pub success_count: u32,
    pub last_failure_time: Option<Instant>,
    pub config: CircuitBreakerConfig,
    pub last_attempt_time: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            config,
            last_attempt_time: None,
        }
    }

    /// Check if a request should be allowed
    pub fn can_make_request(&mut self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                // Check if enough time has passed to try again
                if let Some(last_failure) = self.last_failure_time {
                    if last_failure.elapsed().as_secs() >= self.config.timeout_seconds {
                        self.state = CircuitBreakerState::HalfOpen;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitBreakerState::HalfOpen => {
                // Only allow one request at a time in half-open state
                if self.last_attempt_time.is_none() || 
                   self.last_attempt_time.unwrap().elapsed().as_secs() > 5 {
                    self.last_attempt_time = Some(Instant::now());
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful request
    pub fn record_success(&mut self) {
        match self.state {
            CircuitBreakerState::Closed => {
                self.success_count += 1;
            }
            CircuitBreakerState::HalfOpen => {
                // Success in half-open state means service is healthy again
                self.failure_count = 0;
                self.success_count = 1; // Reset counts
                self.state = CircuitBreakerState::Closed;
                self.last_failure_time = None;
            }
            CircuitBreakerState::Open => {
                // This shouldn't happen, but reset if it does
                self.failure_count = 0;
                self.success_count = 1;
                self.state = CircuitBreakerState::Closed;
                self.last_failure_time = None;
            }
        }
    }

    /// Record a failed request
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure_time = Some(Instant::now());

        // Calculate error rate
        let total_requests = self.failure_count + self.success_count;
        if total_requests >= self.config.minimum_request_threshold {
            let error_rate = (self.failure_count as f64 / total_requests as f64) * 100.0;
            
            // Trip the circuit if error rate exceeds threshold
            if error_rate >= self.config.error_rate_threshold {
                self.state = CircuitBreakerState::Open;
            }
        } else if self.failure_count >= self.config.failure_threshold {
            // Use failure count threshold as fallback
            self.state = CircuitBreakerState::Open;
        }
    }

    /// Get the current error rate
    pub fn error_rate(&self) -> f64 {
        let total_requests = self.failure_count + self.success_count;
        if total_requests > 0 {
            (self.failure_count as f64 / total_requests as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Connection pool monitor that tracks metrics and manages circuit breakers
pub struct ConnectionPoolMonitor {
    pool_stats: Arc<RwLock<HashMap<String, PoolStats>>>,
    circuit_breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    metrics_history: Arc<Mutex<Vec<PoolStats>>>,
    max_history_size: usize,
}

impl ConnectionPoolMonitor {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            pool_stats: Arc::new(RwLock::new(HashMap::new())),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            metrics_history: Arc::new(Mutex::new(Vec::new())),
            max_history_size,
        }
    }

    /// Update pool statistics
    pub async fn update_pool_stats(&self, stats: PoolStats) {
        let mut pool_stats = self.pool_stats.write().await;
        pool_stats.insert(stats.pool_id.clone(), stats.clone());
        
        // Add to history
        let mut history = self.metrics_history.lock().await;
        history.push(stats);
        
        if history.len() > self.max_history_size {
            history.remove(0);
        }
    }

    /// Get current pool statistics
    pub async fn get_pool_stats(&self, pool_id: &str) -> Option<PoolStats> {
        let pool_stats = self.pool_stats.read().await;
        pool_stats.get(pool_id).cloned()
    }

    /// Get all pool statistics
    pub async fn get_all_pool_stats(&self) -> Vec<PoolStats> {
        let pool_stats = self.pool_stats.read().await;
        pool_stats.values().cloned().collect()
    }

    /// Get historical metrics
    pub async fn get_metrics_history(&self) -> Vec<PoolStats> {
        let history = self.metrics_history.lock().await;
        history.clone()
    }

    /// Check if a request should be allowed for a pool
    pub async fn check_circuit_breaker(&self, pool_id: &str) -> Result<(), String> {
        let mut circuit_breakers = self.circuit_breakers.write().await;
        
        let breaker = circuit_breakers
            .entry(pool_id.to_string())
            .or_insert_with(|| CircuitBreaker::new(CircuitBreakerConfig::default()));
        
        if breaker.can_make_request() {
            Ok(())
        } else {
            Err(format!("Circuit breaker is open for pool: {}", pool_id))
        }
    }

    /// Record a successful operation for a pool
    pub async fn record_success(&self, pool_id: &str) {
        let mut circuit_breakers = self.circuit_breakers.write().await;
        
        let breaker = circuit_breakers
            .entry(pool_id.to_string())
            .or_insert_with(|| CircuitBreaker::new(CircuitBreakerConfig::default()));
        
        breaker.record_success();
    }

    /// Record a failed operation for a pool
    pub async fn record_failure(&self, pool_id: &str) {
        let mut circuit_breakers = self.circuit_breakers.write().await;
        
        let breaker = circuit_breakers
            .entry(pool_id.to_string())
            .or_insert_with(|| CircuitBreaker::new(CircuitBreakerConfig::default()));
        
        breaker.record_failure();
    }

    /// Get circuit breaker status for a pool
    pub async fn get_circuit_breaker_status(&self, pool_id: &str) -> Option<CircuitBreaker> {
        let circuit_breakers = self.circuit_breakers.read().await;
        circuit_breakers.get(pool_id).cloned()
    }

    /// Get all circuit breaker statuses
    pub async fn get_all_circuit_breaker_statuses(&self) -> HashMap<String, CircuitBreaker> {
        let circuit_breakers = self.circuit_breakers.read().await;
        circuit_breakers.clone()
    }

    /// Start background monitoring tasks
    pub async fn start_background_tasks(&self) -> Result<(), Box<dyn std::error::Error>> {
        let stats_clone = Arc::clone(&self.pool_stats);
        let breakers_clone = Arc::clone(&self.circuit_breakers);
        
        // Background task for periodic maintenance
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(30)).await;
                
                // Clean up old entries if needed
                let mut stats = stats_clone.write().await;
                stats.retain(|_, stat| {
                    // Keep stats for the last 1 hour
                    stat.timestamp > chrono::Utc::now() - chrono::Duration::hours(1)
                });
                
                let mut breakers = breakers_clone.write().await;
                // Reset success/failure counts periodically to prevent accumulation
                for breaker in breakers.values_mut() {
                    // Only reset if we're in a stable state and have accumulated many requests
                    if breaker.state == CircuitBreakerState::Closed && 
                       (breaker.failure_count + breaker.success_count > 1000) {
                        let success_ratio = breaker.success_count as f64 / 
                                          (breaker.failure_count + breaker.success_count) as f64;
                        // Keep the error rate but reset the counts proportionally
                        breaker.failure_count = (breaker.failure_count as f64 * 0.1) as u32;
                        breaker.success_count = (breaker.success_count as f64 * 0.1) as u32;
                    }
                }
            }
        });

        Ok(())
    }
}

/// Integration with the existing connection pool
impl ConnectionPoolMonitor {
    /// Update connection pool metrics from the connection pool
    pub async fn update_from_connection_pool(
        &self, 
        pool_id: &str, 
        available_connections: usize,
        total_connections: usize,
        max_connections: usize,
        waiting_count: usize
    ) {
        let stats = PoolStats::new(
            pool_id.to_string(),
            available_connections,
            total_connections,
            max_connections,
            waiting_count,
        );
        
        self.update_pool_stats(stats).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_circuit_breaker_states() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            timeout_seconds: 1,
            minimum_request_threshold: 5,
            error_rate_threshold: 50.0,
        };
        
        let mut breaker = CircuitBreaker::new(config);
        
        // Initially should be closed
        assert!(matches!(breaker.state, CircuitBreakerState::Closed));
        assert!(breaker.can_make_request());
        
        // Trip after 3 failures
        breaker.record_failure();
        assert!(breaker.can_make_request());
        breaker.record_failure();
        assert!(breaker.can_make_request());
        breaker.record_failure();
        
        // Should now be open
        assert!(!breaker.can_make_request());
        
        // Wait for timeout and check if it goes to half-open
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(breaker.can_make_request());
        assert!(matches!(breaker.state, CircuitBreakerState::HalfOpen));
        
        // Success should close the circuit
        breaker.record_success();
        assert!(matches!(breaker.state, CircuitBreakerState::Closed));
        assert!(breaker.can_make_request());
    }

    #[tokio::test]
    async fn test_connection_pool_monitor() {
        let monitor = ConnectionPoolMonitor::new(100);
        
        // Add some stats
        let stats = PoolStats::new(
            "test_pool".to_string(),
            5,
            10,
            20,
            2,
        );
        
        monitor.update_pool_stats(stats.clone()).await;
        
        // Check if we can retrieve the stats
        let retrieved = monitor.get_pool_stats("test_pool").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().pool_id, "test_pool");
        
        // Test circuit breaker
        assert!(monitor.check_circuit_breaker("test_pool").await.is_ok());
        
        // Record some failures to trip the breaker
        for _ in 0..5 {
            monitor.record_failure("test_pool").await;
        }
        
        // Now the breaker should be open
        assert!(monitor.check_circuit_breaker("test_pool").await.is_err());
    }
}