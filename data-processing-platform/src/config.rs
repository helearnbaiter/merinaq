//! Application configuration module
//! 
//! Handles loading and managing application configuration from various sources

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub app_name: String,
    pub server_host: String,
    pub server_port: u16,
    pub database_url: String,
    pub jwt_secret: String,
    pub jwt_expiration: i64, // in seconds
    pub casbin_model_path: String,
    pub casbin_policy_path: String,
    pub oauth2_client_id: String,
    pub oauth2_client_secret: String,
    pub oauth2_redirect_url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub connection_timeout: u64,
}

impl AppConfig {
    pub async fn from_env() -> Result<Self> {
        Ok(AppConfig {
            app_name: env::var("APP_NAME").unwrap_or_else(|_| "Data Processing Platform".to_string()),
            server_host: env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            server_port: env::var("SERVER_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://user:password@localhost/data_platform".to_string()),
            jwt_secret: env::var("JWT_SECRET")
                .unwrap_or_else(|_| "default_secret_key_for_development".to_string()),
            jwt_expiration: env::var("JWT_EXPIRATION")
                .unwrap_or_else(|_| "3600".to_string()) // 1 hour
                .parse()
                .unwrap_or(3600),
            casbin_model_path: env::var("CASBIN_MODEL_PATH")
                .unwrap_or_else(|_| "config/rbac_model.conf".to_string()),
            casbin_policy_path: env::var("CASBIN_POLICY_PATH")
                .unwrap_or_else(|_| "config/policy.csv".to_string()),
            oauth2_client_id: env::var("OAUTH2_CLIENT_ID").unwrap_or_default(),
            oauth2_client_secret: env::var("OAUTH2_CLIENT_SECRET").unwrap_or_default(),
            oauth2_redirect_url: env::var("OAUTH2_REDIRECT_URL")
                .unwrap_or_else(|_| "http://localhost:8080/auth/callback".to_string()),
            max_connections: env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "20".to_string())
                .parse()
                .unwrap_or(20),
            min_connections: env::var("DB_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            connection_timeout: env::var("DB_CONNECTION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub connect_timeout: u64,
    pub idle_timeout: Option<u64>,
    pub statement_timeout: Option<u64>,
}

impl DatabaseConfig {
    pub fn from_env() -> Self {
        DatabaseConfig {
            url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://user:password@localhost/data_platform".to_string()),
            max_connections: env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "20".to_string())
                .parse()
                .unwrap_or(20),
            min_connections: env::var("DB_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            connect_timeout: env::var("DB_CONNECTION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            idle_timeout: env::var("DB_IDLE_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok()),
            statement_timeout: env::var("DB_STATEMENT_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
}