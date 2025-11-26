//! Helper functions and utilities
//! 
//! Contains common utility functions used throughout the application

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

/// Helper function to convert database rows to JSON
pub fn rows_to_json(rows: Vec<tokio_postgres::Row>) -> Result<Vec<Value>> {
    let mut result = Vec::new();
    
    for row in rows {
        let mut row_map = serde_json::Map::new();
        
        for (i, column) in row.columns().iter().enumerate() {
            let value: Value = match row.try_get::<_, Option<Value>>(i)? {
                Some(v) => v,
                None => Value::Null,
            };
            row_map.insert(column.name().to_string(), value);
        }
        
        result.push(Value::Object(row_map));
    }
    
    Ok(result)
}

/// Helper function to validate SQL queries (basic validation)
pub fn validate_sql_query(query: &str) -> Result<()> {
    let query_lower = query.trim().to_lowercase();
    
    // Check for basic dangerous patterns
    if query_lower.contains("drop ") || 
       query_lower.contains("delete ") || 
       query_lower.contains("truncate ") {
        return Err(anyhow::anyhow!("Dangerous SQL operation detected"));
    }
    
    Ok(())
}

/// Helper function to sanitize SQL identifiers
pub fn sanitize_identifier(identifier: &str) -> String {
    // Remove any non-alphanumeric characters except underscores
    identifier
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Helper function to build connection string from configuration
pub fn build_connection_string(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}",
        username, password, host, port, database
    )
}

/// Helper function to format error responses
pub fn format_error_response(error_code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "success": false,
        "data": null,
        "error": {
            "code": error_code,
            "message": message,
            "details": null
        },
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "request_id": null
    })
}

/// Helper function to format success responses
pub fn format_success_response<T: serde::Serialize>(data: T) -> serde_json::Value {
    serde_json::json!({
        "success": true,
        "data": data,
        "error": null,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "request_id": null
    })
}

/// Helper function to validate email format
pub fn is_valid_email(email: &str) -> bool {
    // Simple email validation - in production, use a more robust solution
    email.contains('@') && email.contains('.')
}

/// Helper function to validate username format
pub fn is_valid_username(username: &str) -> bool {
    // Username should be 3-30 characters, alphanumeric and underscores only
    username.len() >= 3 && 
    username.len() <= 30 && 
    username.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Helper function to generate request ID
pub fn generate_request_id() -> String {
    use uuid::Uuid;
    Uuid::new_v4().to_string()
}

/// Helper function to hash sensitive data
pub fn hash_sensitive_data(data: &str) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_email() {
        assert!(is_valid_email("test@example.com"));
        assert!(!is_valid_email("invalid-email"));
    }

    #[test]
    fn test_is_valid_username() {
        assert!(is_valid_username("test_user"));
        assert!(is_valid_username("user123"));
        assert!(!is_valid_username("ab")); // too short
        assert!(!is_valid_username("user@name")); // contains invalid character
    }
}