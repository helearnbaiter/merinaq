//! Application configuration module
//! 
//! Handles loading and managing application configuration from various sources

use anyhow::Result;
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub app: AppSettings,
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub auth: AuthSettings,
    pub query_engine: QueryEngineSettings,
    pub logging: LoggingSettings,
    pub monitoring: MonitoringSettings,
    pub security: SecuritySettings,
    #[serde(default)]
    pub performance: Option<PerformanceSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettings {
    pub name: String,
    pub version: String,
    pub description: String,
    #[serde(default = "default_debug")]
    pub debug: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSettings {
    pub url: String,
    pub pool_size: u32,
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u64,
    #[serde(default)]
    pub max_connections: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSettings {
    pub jwt_secret: String,
    #[serde(default = "default_jwt_expiration")]
    pub jwt_expiration: u64, // in seconds
    #[serde(default = "default_refresh_token_expiration")]
    pub refresh_token_expiration: u64, // in seconds
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEngineSettings {
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: u32,
    #[serde(default = "default_query_timeout")]
    pub query_timeout: u64, // in seconds
    #[serde(default = "default_result_cache_ttl")]
    pub result_cache_ttl: u64, // in seconds
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
    #[serde(default = "default_false")]
    pub enable_query_cache: bool,
    #[serde(default = "default_false")]
    pub enable_query_optimization: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingSettings {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
    pub file_path: String,
    #[serde(default = "default_false")]
    pub enable_syslog: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringSettings {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_metrics_endpoint")]
    pub metrics_endpoint: String,
    #[serde(default = "default_false")]
    pub tracing_enabled: bool,
    #[serde(default)]
    pub tracing_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySettings {
    #[serde(default = "default_true")]
    pub enable_cors: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default = "default_true")]
    pub enable_rate_limiting: bool,
    #[serde(default = "default_rate_limit_requests")]
    pub rate_limit_requests: u32,
    #[serde(default = "default_rate_limit_window")]
    pub rate_limit_window: u64, // in seconds
    #[serde(default = "default_false")]
    pub enable_request_logging: bool,
    #[serde(default = "default_false")]
    pub ssl_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSettings {
    #[serde(default = "default_true")]
    pub connection_pool_monitoring: bool,
    #[serde(default = "default_true")]
    pub query_performance_monitoring: bool,
    #[serde(default = "default_slow_query_threshold")]
    pub slow_query_threshold: u64, // in milliseconds
}

// Default functions for optional fields
fn default_debug() -> bool { false }
fn default_log_level() -> String { "INFO".to_string() }
fn default_timeout() -> u64 { 30 }
fn default_connection_timeout() -> u64 { 30 }
fn default_jwt_expiration() -> u64 { 3600 } // 1 hour
fn default_refresh_token_expiration() -> u64 { 86400 } // 24 hours
fn default_max_concurrent_queries() -> u32 { 100 }
fn default_query_timeout() -> u64 { 300 } // 5 minutes
fn default_result_cache_ttl() -> u64 { 300 } // 5 minutes
fn default_memory_limit() -> String { "1GB".to_string() }
fn default_log_format() -> String { "json".to_string() }
fn default_metrics_endpoint() -> String { "/metrics".to_string() }
fn default_rate_limit_requests() -> u32 { 100 }
fn default_rate_limit_window() -> u64 { 60 } // 60 seconds
fn default_slow_query_threshold() -> u64 { 5000 } // 5 seconds
fn default_true() -> bool { true }
fn default_false() -> bool { false }

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let environment = env::var("RUN_MODE").unwrap_or_else(|_| "development".to_string());
        
        let mut config_builder = Config::builder()
            // Start with the base configuration
            .add_source(File::with_name("config/base"))
            // Add environment-specific configuration
            .add_source(File::with_name(&format!("config/{}", environment)).required(false))
            // Add in settings from the environment (with a prefix of APP)
            // Eg.. `APP_DEBUG=1` would set the `debug` key
            .add_source(Environment::with_prefix("APP").separator("__"));

        // Load the configuration
        let config = config_builder.build()?;
        let app_config: AppConfig = config.try_deserialize()?;

        Ok(app_config)
    }
}

impl DatabaseSettings {
    pub fn get_database_url(&self) -> String {
        // Expand environment variables in the URL
        let url = &self.url;
        if url.starts_with("${") && url.ends_with("}") {
            let var_name = &url[2..url.len()-1]; // Extract variable name
            env::var(var_name).unwrap_or_else(|_| url.clone())
        } else {
            url.clone()
        }
    }
}