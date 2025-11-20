//! Query execution service implementation
//! 
//! Handles SQL query execution across multiple data sources using DataFusion

use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use sqlx::{PgPool, Row};
use tracing::info;

use crate::database::DatabasePool;
use crate::models::{ExecuteQueryRequest, ExecuteQueryResponse};
use crate::utils::error::{PlatformError, PlatformResult};
use crate::query_engine::{QueryEngine, DataSourceConfig};

pub struct QueryService {
    db_pool: DatabasePool,
    query_engine: Arc<QueryEngine>,
}

impl QueryService {
    pub fn new(db_pool: DatabasePool) -> Self {
        let query_engine = Arc::new(QueryEngine::new());
        QueryService { 
            db_pool,
            query_engine,
        }
    }

    pub async fn execute_query(&self, request: &ExecuteQueryRequest) -> PlatformResult<ExecuteQueryResponse> {
        use std::time::Instant;
        
        // Get data source configuration
        let data_source = match self.db_pool.get_data_source_by_id(request.data_source_id).await
            .map_err(|e| PlatformError::DatabaseError(e))? {
            Some(ds) => ds,
            None => {
                return Ok(ExecuteQueryResponse {
                    success: false,
                    data: None,
                    error: Some("Data source not found".to_string()),
                    execution_time_ms: None,
                });
            }
        };

        // Convert to our DataSourceConfig
        let config = DataSourceConfig {
            name: data_source.name.clone(),
            source_type: data_source.source_type.clone(),
            connection_config: data_source.connection_config.clone(),
        };

        // Register the data source with the query engine if not already registered
        if let Err(e) = self.query_engine.register_data_source(&config).await {
            return Ok(ExecuteQueryResponse {
                success: false,
                data: None,
                error: Some(format!("Failed to register data source: {}", e)),
                execution_time_ms: None,
            });
        }

        // Execute the query using the DataFusion query engine
        let start_time = Instant::now();
        match self.query_engine.execute_query(&request.sql).await {
            Ok(batches) => {
                let execution_time = start_time.elapsed().as_millis() as u64;
                
                // Convert RecordBatches to JSON
                let mut result_data = Vec::new();
                for batch in batches {
                    let rows = self.record_batch_to_json(&batch)?;
                    result_data.extend(rows);
                }

                Ok(ExecuteQueryResponse {
                    success: true,
                    data: Some(serde_json::Value::Array(result_data)),
                    error: None,
                    execution_time_ms: Some(execution_time),
                })
            }
            Err(e) => {
                Ok(ExecuteQueryResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Query execution failed: {}", e)),
                    execution_time_ms: Some(start_time.elapsed().as_millis() as u64),
                })
            }
        }
    }

    

    pub async fn get_schema(&self, data_source_id: i32) -> PlatformResult<serde_json::Value> {
        // Get data source
        let data_source = match self.db_pool.get_data_source_by_id(data_source_id).await
            .map_err(|e| PlatformError::DatabaseError(e))? {
            Some(ds) => ds,
            None => {
                return Err(PlatformError::NotFound("Data source not found".to_string()));
            }
        };

        // Get schema based on data source type
        match data_source.source_type.as_str() {
            "postgres" => {
                // Query PostgreSQL information schema to get table/column information
                let schema_query = r#"
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                "#;
                
                let rows = sqlx::query(schema_query)
                    .fetch_all(&self.db_pool.pool)
                    .await
                    .map_err(|e| PlatformError::DatabaseError(e))?;
                
                let mut schema = serde_json::Map::new();
                let mut current_table = String::new();
                let mut current_table_cols = serde_json::Map::new();
                
                for row in rows {
                    let table_name: String = row.try_get("table_name")
                        .map_err(|e| PlatformError::DatabaseError(sqlx::Error::ColumnNotFound(e.to_string())))?;
                    let column_name: String = row.try_get("column_name")
                        .map_err(|e| PlatformError::DatabaseError(sqlx::Error::ColumnNotFound(e.to_string())))?;
                    let data_type: String = row.try_get("data_type")
                        .map_err(|e| PlatformError::DatabaseError(sqlx::Error::ColumnNotFound(e.to_string())))?;
                    let is_nullable: String = row.try_get("is_nullable")
                        .map_err(|e| PlatformError::DatabaseError(sqlx::Error::ColumnNotFound(e.to_string())))?;
                    
                    if table_name != current_table && !current_table.is_empty() {
                        schema.insert(current_table.clone(), serde_json::Value::Object(current_table_cols.clone()));
                        current_table_cols.clear();
                    }
                    
                    current_table = table_name.clone();
                    let mut col_info = serde_json::Map::new();
                    col_info.insert("type".to_string(), serde_json::Value::String(data_type));
                    col_info.insert("nullable".to_string(), serde_json::Value::String(is_nullable));
                    current_table_cols.insert(column_name, serde_json::Value::Object(col_info));
                }
                
                // Add the last table
                if !current_table.is_empty() {
                    schema.insert(current_table, serde_json::Value::Object(current_table_cols));
                }
                
                Ok(serde_json::Value::Object(schema))
            }
            _ => {
                Err(PlatformError::ValidationError("Schema introspection not supported for this data source type".to_string()))
            }
        }
    }

    // Helper function to convert RecordBatch to JSON
    fn record_batch_to_json(&self, batch: &datafusion::arrow::record_batch::RecordBatch) -> PlatformResult<Vec<serde_json::Value>> {
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::{DataType, SchemaRef};

        let mut result = Vec::new();
        let schema: SchemaRef = batch.schema();

        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col_name = field.name();
                let array = batch.column(col_idx);

                let value = match array.data_type() {
                    DataType::Int8 => {
                        let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::Int16 => {
                        let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::Int32 => {
                        let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::Int64 => {
                        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::UInt8 => {
                        let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::UInt16 => {
                        let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::UInt32 => {
                        let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::UInt64 => {
                        let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Number(serde_json::Number::from(arr.value(row_idx)))
                        }
                    },
                    DataType::Float32 => {
                        let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            if arr.value(row_idx).is_finite() {
                                serde_json::Value::Number(serde_json::Number::from_f64(arr.value(row_idx) as f64)
                                    .unwrap_or(serde_json::Number::from_f64(0.0).unwrap()))
                            } else {
                                serde_json::Value::Number(serde_json::Number::from_f64(0.0).unwrap())
                            }
                        }
                    },
                    DataType::Float64 => {
                        let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            if arr.value(row_idx).is_finite() {
                                serde_json::Value::Number(serde_json::Number::from_f64(arr.value(row_idx))
                                    .unwrap_or(serde_json::Number::from_f64(0.0).unwrap()))
                            } else {
                                serde_json::Value::Number(serde_json::Number::from_f64(0.0).unwrap())
                            }
                        }
                    },
                    DataType::Utf8 => {
                        let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                    },
                    DataType::LargeUtf8 => {
                        let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                    },
                    DataType::Boolean => {
                        let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            serde_json::Value::Bool(arr.value(row_idx))
                        }
                    },
                    DataType::Timestamp(_, _) => {
                        let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                        if arr.is_null(row_idx) {
                            serde_json::Value::Null
                        } else {
                            // Convert timestamp to ISO string
                            let ts = chrono::NaiveDateTime::from_timestamp_opt(arr.value(row_idx) / 1_000_000_000, 
                                (arr.value(row_idx) % 1_000_000_000) as u32)
                                .unwrap_or_default();
                            serde_json::Value::String(ts.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                        }
                    },
                    _ => serde_json::Value::String(format!("Unsupported data type: {:?}", array.data_type())),
                };

                row.insert(col_name.clone(), value);
            }

            result.push(serde_json::Value::Object(row));
        }

        Ok(result)
    }
}