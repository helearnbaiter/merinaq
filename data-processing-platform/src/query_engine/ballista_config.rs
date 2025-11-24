//! Ballista Configuration Module
//! 
//! This module provides configuration structures and utilities for Ballista integration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BallistaConfig {
    /// Scheduler host address
    pub scheduler_host: String,
    /// Scheduler port
    pub scheduler_port: u16,
    /// Whether to enable Ballista integration
    pub enabled: bool,
    /// Ballista execution configuration
    pub execution: BallistaExecutionConfig,
    /// Connection pool settings
    pub connection_pool: BallistaConnectionPoolConfig,
    /// Query optimization settings specific to Ballista
    pub optimization: BallistaOptimizationConfig,
}

impl Default for BallistaConfig {
    fn default() -> Self {
        Self {
            scheduler_host: "localhost".to_string(),
            scheduler_port: 50050,
            enabled: false, // Default to disabled until properly configured
            execution: BallistaExecutionConfig::default(),
            connection_pool: BallistaConnectionPoolConfig::default(),
            optimization: BallistaOptimizationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BallistaExecutionConfig {
    /// Number of concurrent tasks per executor
    pub concurrent_tasks: usize,
    /// Memory limit per query (in MB)
    pub memory_limit_mb: usize,
    /// Whether to enable spill to disk for large operations
    pub enable_spill: bool,
    /// Spill threshold (in MB)
    pub spill_threshold_mb: usize,
    /// Maximum number of partitions for distributed operations
    pub max_partitions: usize,
}

impl Default for BallistaExecutionConfig {
    fn default() -> Self {
        Self {
            concurrent_tasks: 4,
            memory_limit_mb: 1024, // 1GB
            enable_spill: true,
            spill_threshold_mb: 512, // 512MB
            max_partitions: 32,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BallistaConnectionPoolConfig {
    /// Maximum number of connections to scheduler
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connection_timeout_sec: u64,
    /// Request timeout in seconds
    pub request_timeout_sec: u64,
    /// Whether to enable connection reuse
    pub enable_reuse: bool,
    /// Keep-alive time for connections (in seconds)
    pub keep_alive_time_sec: u64,
}

impl Default for BallistaConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            connection_timeout_sec: 30,
            request_timeout_sec: 300, // 5 minutes
            enable_reuse: true,
            keep_alive_time_sec: 300, // 5 minutes
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BallistaOptimizationConfig {
    /// Whether to enable query plan optimization
    pub enable_query_optimization: bool,
    /// Whether to enable predicate pushdown
    pub enable_predicate_pushdown: bool,
    /// Whether to enable projection pushdown
    pub enable_projection_pushdown: bool,
    /// Whether to enable filter normalization
    pub enable_filter_normalization: bool,
    /// Whether to enable statistics-based optimization
    pub enable_statistics: bool,
    /// Maximum optimization time allowed (in milliseconds)
    pub max_optimization_time_ms: u64,
}

impl Default for BallistaOptimizationConfig {
    fn default() -> Self {
        Self {
            enable_query_optimization: true,
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
            enable_filter_normalization: true,
            enable_statistics: false, // Disabled by default due to complexity
            max_optimization_time_ms: 5000, // 5 seconds
        }
    }
}

/// Configuration for distributed query planning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedQueryPlanningConfig {
    /// Threshold for when to use distributed execution (estimated row count)
    pub distributed_threshold: u64,
    /// Default partition strategy
    pub default_partition_strategy: String, // "hash", "range", "round_robin"
    /// Whether to auto-partition large tables
    pub auto_partition: bool,
    /// Number of partitions for auto-partitioned tables
    pub auto_partition_count: usize,
    /// Column sampling rate for statistics collection (0.0 to 1.0)
    pub column_sampling_rate: f64,
}

impl Default for DistributedQueryPlanningConfig {
    fn default() -> Self {
        Self {
            distributed_threshold: 100_000, // 100k rows
            default_partition_strategy: "hash".to_string(),
            auto_partition: true,
            auto_partition_count: 8,
            column_sampling_rate: 0.1, // 10% sampling
        }
    }
}

/// Configuration for fault tolerance and recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultToleranceConfig {
    /// Number of retries for failed tasks
    pub task_retry_count: u32,
    /// Initial backoff time for retries (in milliseconds)
    pub initial_backoff_ms: u64,
    /// Maximum backoff time for retries (in milliseconds)
    pub max_backoff_ms: u64,
    /// Whether to enable speculative execution
    pub enable_speculative_execution: bool,
    /// Speculative execution threshold (in milliseconds)
    pub speculative_execution_threshold_ms: u64,
    /// Whether to enable query result caching
    pub enable_result_caching: bool,
    /// Result cache TTL (in seconds)
    pub result_cache_ttl_sec: u64,
}

impl Default for FaultToleranceConfig {
    fn default() -> Self {
        Self {
            task_retry_count: 3,
            initial_backoff_ms: 1000, // 1 second
            max_backoff_ms: 30000,    // 30 seconds
            enable_speculative_execution: false, // Disabled by default
            speculative_execution_threshold_ms: 60000, // 1 minute
            enable_result_caching: true,
            result_cache_ttl_sec: 3600, // 1 hour
        }
    }
}

impl BallistaConfig {
    /// Create a new Ballista configuration from a settings map
    pub fn from_settings(settings: &HashMap<String, String>) -> Self {
        let mut config = Self::default();

        if let Some(host) = settings.get("ballista.scheduler_host") {
            config.scheduler_host = host.clone();
        }

        if let Some(port_str) = settings.get("ballista.scheduler_port") {
            if let Ok(port) = port_str.parse::<u16>() {
                config.scheduler_port = port;
            }
        }

        if let Some(enabled_str) = settings.get("ballista.enabled") {
            if let Ok(enabled) = enabled_str.parse::<bool>() {
                config.enabled = enabled;
            }
        }

        // Execution config
        if let Some(tasks_str) = settings.get("ballista.execution.concurrent_tasks") {
            if let Ok(tasks) = tasks_str.parse::<usize>() {
                config.execution.concurrent_tasks = tasks;
            }
        }

        if let Some(memory_str) = settings.get("ballista.execution.memory_limit_mb") {
            if let Ok(memory) = memory_str.parse::<usize>() {
                config.execution.memory_limit_mb = memory;
            }
        }

        if let Some(enable_spill_str) = settings.get("ballista.execution.enable_spill") {
            if let Ok(enable_spill) = enable_spill_str.parse::<bool>() {
                config.execution.enable_spill = enable_spill;
            }
        }

        // Connection pool config
        if let Some(max_conn_str) = settings.get("ballista.connection_pool.max_connections") {
            if let Ok(max_conn) = max_conn_str.parse::<u32>() {
                config.connection_pool.max_connections = max_conn;
            }
        }

        if let Some(timeout_str) = settings.get("ballista.connection_pool.connection_timeout_sec") {
            if let Ok(timeout) = timeout_str.parse::<u64>() {
                config.connection_pool.connection_timeout_sec = timeout;
            }
        }

        config
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.scheduler_port == 0 {
            return Err("Scheduler port must be greater than 0".to_string());
        }

        if self.execution.concurrent_tasks == 0 {
            return Err("Concurrent tasks must be greater than 0".to_string());
        }

        if self.execution.memory_limit_mb == 0 {
            return Err("Memory limit must be greater than 0".to_string());
        }

        if self.connection_pool.max_connections == 0 {
            return Err("Max connections must be greater than 0".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BallistaConfig::default();
        assert_eq!(config.scheduler_host, "localhost");
        assert_eq!(config.scheduler_port, 50050);
        assert!(!config.enabled); // Should be disabled by default
    }

    #[test]
    fn test_config_from_settings() {
        let mut settings = HashMap::new();
        settings.insert("ballista.scheduler_host".to_string(), "test-host".to_string());
        settings.insert("ballista.scheduler_port".to_string(), "8080".to_string());
        settings.insert("ballista.enabled".to_string(), "true".to_string());
        
        let config = BallistaConfig::from_settings(&settings);
        assert_eq!(config.scheduler_host, "test-host");
        assert_eq!(config.scheduler_port, 8080);
        assert!(config.enabled);
    }

    #[test]
    fn test_config_validation() {
        let mut config = BallistaConfig::default();
        config.scheduler_port = 0;
        assert!(config.validate().is_err());

        config = BallistaConfig::default();
        config.execution.concurrent_tasks = 0;
        assert!(config.validate().is_err());

        config = BallistaConfig::default();
        config.execution.memory_limit_mb = 0;
        assert!(config.validate().is_err());

        config = BallistaConfig::default();
        config.connection_pool.max_connections = 0;
        assert!(config.validate().is_err());

        config = BallistaConfig::default();
        assert!(config.validate().is_ok());
    }
}