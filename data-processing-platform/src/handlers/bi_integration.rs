//! BI Tool Integration Handlers
//! 
//! This module provides endpoints for BI tool integration, including:
//! - Standard connection URL configuration
//! - SQL query execution
//! - Data visualization support
//! - Compatibility with mainstream BI and analysis tools

use axum::{
    extract::{Path, Query, Json, Extension},
    http::StatusCode,
    response::{IntoResponse, Response},
    body::Body,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::writer::IpcStreamWriter;
use datafusion::arrow::ipc::writer::WriteOptions;
use std::collections::HashMap;

use crate::{
    database::DatabasePool,
    services::query_service::QueryService,
    utils::error::{PlatformError, PlatformResult},
};

/// BI Tool Connection Configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub driver: String,
    pub ssl_enabled: bool,
    pub connection_timeout: u64,
}

/// BI Tool Query Request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiQueryRequest {
    pub query: String,
    pub params: Option<HashMap<String, String>>,
    pub format: Option<String>, // json, arrow, csv, parquet
    pub limit: Option<usize>,
}

/// BI Tool Query Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BiQueryResponse {
    pub success: bool,
    pub data: Option<serde_json::Value>,
    pub schema: Option<serde_json::Value>,
    pub metadata: Option<QueryMetadata>,
    pub error: Option<String>,
}

/// Query Metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    pub execution_time_ms: u64,
    pub row_count: usize,
    pub column_count: usize,
    pub query_id: String,
}

/// Flight SQL Connection Info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightSqlConnectionInfo {
    pub flight_endpoint: String,
    pub auth_method: String,
    pub token: Option<String>,
    pub database: String,
}

/// Superset Connection Configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupersetConnectionConfig {
    pub connection_url: String,
    pub database_name: String,
    pub driver: String,
    pub extra: Option<serde_json::Value>,
}

/// Get standard connection URL configuration for BI tools
pub async fn get_bi_connection_config(
    Extension(_db_pool): Extension<DatabasePool>,
) -> Result<Json<BiConnectionConfig>, PlatformError> {
    let config = BiConnectionConfig {
        host: std::env::var("SERVER_HOST").unwrap_or_else(|_| "localhost".to_string()),
        port: std::env::var("SERVER_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse()
            .unwrap_or(8080),
        database: std::env::var("DATABASE_NAME").unwrap_or_else(|_| "platform_db".to_string()),
        username: std::env::var("DATABASE_USER").unwrap_or_else(|_| "admin".to_string()),
        password: std::env::var("DATABASE_PASSWORD").unwrap_or_else(|_| "password".to_string()),
        driver: "flight_sql".to_string(),
        ssl_enabled: std::env::var("SSL_ENABLED").unwrap_or_else(|_| "false".to_string()) == "true",
        connection_timeout: 30000,
    };

    Ok(Json(config))
}

/// Execute query for BI tools
pub async fn execute_bi_query(
    Extension(db_pool): Extension<DatabasePool>,
    Extension(query_service): Extension<Arc<QueryService>>,
    Json(payload): Json<BiQueryRequest>,
) -> Result<Response, PlatformError> {
    // Execute the query using the query service
    let start_time = std::time::Instant::now();
    
    use crate::models::ExecuteQueryRequest;
    
    let request = ExecuteQueryRequest {
        sql: payload.query.clone(),
        data_source_id: 1, // Default to first data source - in a real implementation this would come from auth context or request
    };
    
    let result = query_service.execute_query(&request).await;
    
    let execution_time = start_time.elapsed().as_millis() as u64;
    
    match result {
        Ok(query_result) => {
            // Determine response format
            let format = payload.format.unwrap_or_else(|| "json".to_string());
            
            // For Arrow and CSV formats, we need to convert the JSON result back to RecordBatches
            let response = match format.as_str() {
                "arrow" => {
                    // For now, return an error since the conversion is complex and not fully implemented
                    return Err(PlatformError::NotImplemented("Arrow format conversion not fully implemented".to_string()));
                },
                "csv" => {
                    // For now, return an error since the conversion is complex and not fully implemented
                    return Err(PlatformError::NotImplemented("CSV format conversion not fully implemented".to_string()));
                },
                "json" | _ => {
                    let response = BiQueryResponse {
                        success: true,
                        data: query_result.data,
                        schema: extract_schema_from_json(&query_result.data), // Extract schema from the result data
                        metadata: Some(QueryMetadata {
                            execution_time_ms: execution_time,
                            row_count: query_result.data.as_ref().map(|d| match d {
                                serde_json::Value::Array(arr) => arr.len(),
                                _ => 0,
                            }).unwrap_or(0),
                            column_count: if let Some(serde_json::Value::Array(ref arr)) = query_result.data {
                                if !arr.is_empty() {
                                    if let serde_json::Value::Object(ref obj) = arr[0] {
                                        obj.len()
                                    } else {
                                        0
                                    }
                                } else {
                                    0
                                }
                            } else {
                                0
                            },
                            query_id: uuid::Uuid::new_v4().to_string(),
                        }),
                        error: None,
                    };
                    
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response).map_err(|e| {
                            PlatformError::QueryError(format!("Failed to serialize response: {}", e))
                        })?))
                        .map_err(|e| PlatformError::InternalError(e.to_string()))?
                }
            };
            
            Ok(response)
        }
        Err(e) => {
            let response = BiQueryResponse {
                success: false,
                data: None,
                schema: None,
                metadata: None,
                error: Some(e.to_string()),
            };
            
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&response).map_err(|e| {
                    PlatformError::QueryError(format!("Failed to serialize error response: {}", e))
                })?))
                .map_err(|e| PlatformError::InternalError(e.to_string()))?)
        }
    }
}

/// Create Arrow IPC response for efficient data transfer
fn create_arrow_response(batches: &[RecordBatch]) -> Result<Response, PlatformError> {
    if batches.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/vnd.apache.arrow.stream")
            .body(Body::empty())
            .map_err(|e| PlatformError::InternalError(e.to_string()))?);
    }

    // Create in-memory buffer for Arrow IPC stream
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = IpcStreamWriter::try_new(&mut buffer, &batches[0].schema())
            .map_err(|e| PlatformError::QueryError(format!("Failed to create Arrow writer: {}", e)))?;
        
        for batch in batches {
            writer.write(batch)
                .map_err(|e| PlatformError::QueryError(format!("Failed to write batch to Arrow stream: {}", e)))?;
        }
        
        writer.finish()
            .map_err(|e| PlatformError::QueryError(format!("Failed to finish Arrow stream: {}", e)))?;
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/vnd.apache.arrow.stream")
        .body(Body::from(buffer))
        .map_err(|e| PlatformError::InternalError(e.to_string()))?)
}

/// Create CSV response
fn create_csv_response(batches: &[RecordBatch]) -> Result<Response, PlatformError> {
    use std::io::Write;
    use datafusion::arrow::array::*;
    
    let mut buffer: Vec<u8> = Vec::new();
    
    if !batches.is_empty() {
        // Write CSV header
        let schema = &batches[0].schema();
        for (i, field) in schema.fields().iter().enumerate() {
            if i > 0 {
                buffer.write_all(b",").unwrap();
            }
            buffer.write_all(field.name().as_bytes()).unwrap();
        }
        buffer.write_all(b"\n").unwrap();
        
        // Write data rows
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                for (col_idx, column) in batch.columns().iter().enumerate() {
                    if col_idx > 0 {
                        buffer.write_all(b",").unwrap();
                    }
                    
                    // Convert column value to string representation based on data type
                    let value_str = match column.data_type() {
                        arrow::datatypes::DataType::Int8 => {
                            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Int16 => {
                            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Int32 => {
                            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Int64 => {
                            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::UInt8 => {
                            let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::UInt16 => {
                            let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::UInt32 => {
                            let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::UInt64 => {
                            let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Float32 => {
                            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Float64 => {
                            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Utf8 => {
                            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::LargeUtf8 => {
                            let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                        },
                        arrow::datatypes::DataType::Boolean => {
                            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { 
                                if arr.value(row_idx) { "true".to_string() } else { "false".to_string() } 
                            }
                        },
                        arrow::datatypes::DataType::Timestamp(_, _) => {
                            let arr = column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                            if arr.is_null(row_idx) { "NULL".to_string() } else { 
                                // Convert timestamp to readable format
                                let ts = chrono::NaiveDateTime::from_timestamp_opt(arr.value(row_idx) / 1_000_000_000, 
                                    (arr.value(row_idx) % 1_000_000_000) as u32)
                                    .unwrap_or_default();
                                ts.format("%Y-%m-%d %H:%M:%S").to_string()
                            }
                        },
                        _ => format!("Unsupported data type: {:?}", column.data_type()),
                    };
                    
                    buffer.write_all(value_str.as_bytes()).unwrap();
                }
                buffer.write_all(b"\n").unwrap();
            }
        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/csv")
        .header("content-disposition", "attachment; filename=\"query_result.csv\"")
        .body(Body::from(buffer))
        .map_err(|e| PlatformError::InternalError(e.to_string()))?)
}

/// Get Flight SQL connection information
pub async fn get_flight_sql_info(
    Extension(_db_pool): Extension<DatabasePool>,
) -> Result<Json<FlightSqlConnectionInfo>, PlatformError> {
    let flight_endpoint = format!(
        "grpc://{}:{}",
        std::env::var("FLIGHT_HOST").unwrap_or_else(|_| "localhost".to_string()),
        std::env::var("FLIGHT_PORT").unwrap_or_else(|_| "9090".to_string())
    );

    let config = FlightSqlConnectionInfo {
        flight_endpoint,
        auth_method: "bearer".to_string(),
        token: None, // Will be provided by client
        database: std::env::var("DATABASE_NAME").unwrap_or_else(|_| "platform_db".to_string()),
    };

    Ok(Json(config))
}

/// Get Superset connection configuration
pub async fn get_superset_config(
    Extension(_db_pool): Extension<DatabasePool>,
) -> Result<Json<SupersetConnectionConfig>, PlatformError> {
    let connection_url = format!(
        "flight+grpc://{}:{}",
        std::env::var("FLIGHT_HOST").unwrap_or_else(|_| "localhost".to_string()),
        std::env::var("FLIGHT_PORT").unwrap_or_else(|_| "9090".to_string())
    );

    let config = SupersetConnectionConfig {
        connection_url,
        database_name: std::env::var("DATABASE_NAME").unwrap_or_else(|_| "platform_db".to_string()),
        driver: "flight_sql".to_string(),
        extra: Some(serde_json::json!({
            "engine": "flight_sql",
            "supports_dynamic_schema": true,
            "supports_time_grains": true,
            "supports_custom_sql": true
        })),
    };

    Ok(Json(config))
}

/// Get schema information for BI tools
pub async fn get_bi_schema(
    Extension(query_service): Extension<Arc<QueryService>>,
    Extension(db_pool): Extension<DatabasePool>,
) -> Result<Json<serde_json::Value>, PlatformError> {
    // Query system tables to get schema information
    let query = "
        SELECT table_schema, table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, ordinal_position
    ";
    
    let request = ExecuteQueryRequest {
        sql: query.to_string(),
        data_source_id: 1, // Default to first data source
    };
    
    let result = query_service.execute_query(&request).await?;
    
    // Convert to JSON format suitable for BI tools
    let schema_info = serde_json::json!({
        "tables": extract_schema_from_json(&result.data)
    });
    
    Ok(Json(schema_info))
}

/// Helper function to extract table schema from JSON query results
fn extract_schema_from_json(data: &Option<serde_json::Value>) -> serde_json::Value {
    match data {
        Some(serde_json::Value::Array(rows)) => {
            let mut schema_map: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
            
            for row in rows {
                if let serde_json::Value::Object(obj) = row {
                    // Extract schema, table, column and type information from the row
                    if let (Some(serde_json::Value::String(table_schema)), 
                            Some(serde_json::Value::String(table_name)), 
                            Some(serde_json::Value::String(column_name)), 
                            Some(serde_json::Value::String(data_type))) = 
                        (obj.get("table_schema"), obj.get("table_name"), obj.get("column_name"), obj.get("data_type")) {
                        
                        // Create a unique key for the table
                        let table_key = format!("{}.{}", table_schema, table_name);
                        
                        // Get or create the table schema entry
                        let table_entry = schema_map.entry(table_key.clone()).or_insert_with(|| serde_json::json!({}));
                        
                        // Add column information to the table
                        if let serde_json::Value::Object(ref mut table_obj) = table_entry {
                            let mut column_info = serde_json::Map::new();
                            column_info.insert("type".to_string(), serde_json::Value::String(data_type.clone()));
                            column_info.insert("nullable".to_string(), serde_json::Value::Bool(true)); // This would come from the actual schema query
                            
                            table_obj.insert(column_name.clone(), serde_json::Value::Object(column_info));
                        }
                    }
                }
            }
            
            serde_json::json!(schema_map)
        },
        _ => serde_json::json!({}),
    }
}

/// Helper function to extract table schema from query results
fn extract_table_schema(batches: &[RecordBatch]) -> serde_json::Value {
    use datafusion::arrow::array::*;
    use std::collections::HashMap;
    
    let mut schema_map: HashMap<String, serde_json::Value> = HashMap::new();
    
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        
        // Get column information
        let schema = batch.schema();
        
        // Process each row in the batch to extract table and column information
        for row_idx in 0..batch.num_rows() {
            // Extract values from each column for this row
            let table_schema_col = batch.column(0);
            let table_name_col = batch.column(1);
            let column_name_col = batch.column(2);
            let data_type_col = batch.column(3);
            
            // Get the values (assuming they are string arrays)
            let table_schema_array = table_schema_col.as_any().downcast_ref::<StringArray>().unwrap();
            let table_name_array = table_name_col.as_any().downcast_ref::<StringArray>().unwrap();
            let column_name_array = column_name_col.as_any().downcast_ref::<StringArray>().unwrap();
            let data_type_array = data_type_col.as_any().downcast_ref::<StringArray>().unwrap();
            
            let table_schema = table_schema_array.value(row_idx);
            let table_name = table_name_array.value(row_idx);
            let column_name = column_name_array.value(row_idx);
            let data_type = data_type_array.value(row_idx);
            
            // Create a unique key for the table
            let table_key = format!("{}.{}", table_schema, table_name);
            
            // Get or create the table schema entry
            let table_entry = schema_map.entry(table_key.clone()).or_insert_with(|| serde_json::json!({}));
            
            // Add column information to the table
            if let serde_json::Value::Object(ref mut table_obj) = table_entry {
                let mut column_info = serde_json::Map::new();
                column_info.insert("type".to_string(), serde_json::Value::String(data_type.to_string()));
                column_info.insert("nullable".to_string(), serde_json::Value::Bool(true)); // This would come from the actual schema query
                
                table_obj.insert(column_name.to_string(), serde_json::Value::Object(column_info));
            }
        }
    }
    
    serde_json::json!(schema_map)
}

/// Test BI tool connection
pub async fn test_bi_connection(
    Extension(db_pool): Extension<DatabasePool>,
) -> Result<Json<serde_json::Value>, PlatformError> {
    // Test the database connection
    let conn = db_pool.get().await.map_err(|e| {
        PlatformError::DatabaseError(format!("Failed to get database connection: {}", e))
    })?;
    
    // Execute a simple query to test the connection
    let result: Result<Vec<i32>, _> = sqlx::query_scalar("SELECT 1")
        .fetch_all(&conn)
        .await;
    
    match result {
        Ok(_) => {
            Ok(Json(serde_json::json!({
                "success": true,
                "message": "BI connection test successful",
                "connection_type": "flight_sql",
                "timestamp": chrono::Utc::now().to_rfc3339()
            })))
        }
        Err(e) => {
            Err(PlatformError::DatabaseError(format!("BI connection test failed: {}", e)))
        }
    }
}