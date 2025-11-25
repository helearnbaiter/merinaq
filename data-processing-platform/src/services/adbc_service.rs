//! ADBC Service
//! 
//! This service provides a high-level interface for working with ADBC connections,
//! including connection pooling, multi-database support, and query execution.

use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures;

use crate::query_engine::{QueryEngine, adbc::{AdbcResult, AdbcError, AdbcDriver, QueryEngineAdbcDatabase}, adbc_connection_pool::{ConnectionPool, PoolConfig}, adbc_database_adapters::{DatabaseConfig, DatabaseType, DatabaseAdapterFactory}, connection_monitor::ConnectionPoolMonitor};

/// ADBC Service that manages connections and executes queries
pub struct AdbcService {
    driver: AdbcDriver,
    connection_pools: HashMap<String, Arc<ConnectionPool>>,
    adapter_factory: DatabaseAdapterFactory,
    query_engine: Arc<QueryEngine>,
    monitor: Arc<ConnectionPoolMonitor>,
}

impl AdbcService {
    /// Create a new ADBC service
    pub fn new(query_engine: Arc<QueryEngine>) -> Self {
        let mut driver = AdbcDriver::new();
        let connection_pools = HashMap::new();
        let adapter_factory = DatabaseAdapterFactory::new();
        let monitor = Arc::new(ConnectionPoolMonitor::new(1000)); // Keep last 1000 metrics
        
        // Register the default query engine database
        let query_engine_db = Arc::new(QueryEngineAdbcDatabase::new(query_engine.clone()));
        driver.register_database("datafusion", query_engine_db);
        
        Self {
            driver,
            connection_pools,
            adapter_factory,
            query_engine,
            monitor,
        }
    }

    /// Register a new database with connection pooling
    pub async fn register_database(&mut self, name: &str, config: DatabaseConfig) -> AdbcResult<()> {
        // Create an appropriate database adapter based on the configuration
        let adapter = self.adapter_factory.create(config.clone())
            .map_err(|e| AdbcError::Internal(format!("Failed to create database adapter: {:?}", e)))?;
        
        // Register with the driver
        let adapter_arc = Arc::from(adapter);
        self.driver.register_database(name, adapter_arc.clone());
        
        // Create a connection pool for this database if it's not in-memory
        if config.db_type != DatabaseType::DataFusion {
            let pool_config = PoolConfig::default();
            let pool = ConnectionPool::new(adapter_arc, pool_config);
            self.connection_pools.insert(name.to_string(), Arc::new(pool));
        }
        
        Ok(())
    }

    /// Execute a query using ADBC
    pub async fn execute_query(&self, database_name: &str, query: &str) -> AdbcResult<Vec<RecordBatch>> {
        let database = self.driver.get_database(database_name)?;
        
        // Use connection pool if available, otherwise direct connection
        if let Some(pool) = self.connection_pools.get(database_name) {
            let pooled_conn = pool.get_connection().await?;
            // In a real implementation, we'd execute using the pooled connection
            // For now, we'll execute directly through the database
            database.execute_query(query).await
        } else {
            database.execute_query(query).await
        }
    }

    /// Get table schema from a specific database
    pub async fn get_table_schema(&self, database_name: &str, table_name: &str) -> AdbcResult<Schema> {
        let database = self.driver.get_database(database_name)?;
        database.get_table_schema(table_name).await
    }

    /// List tables in a specific database
    pub async fn list_tables(&self, database_name: &str) -> AdbcResult<Vec<String>> {
        let database = self.driver.get_database(database_name)?;
        database.list_tables().await
    }

    /// Get connection pool statistics for a database
    pub async fn get_pool_stats(&self, database_name: &str) -> Option<crate::query_engine::adbc_connection_pool::PoolStats> {
        self.connection_pools.get(database_name)
            .map(|pool| futures::executor::block_on(pool.stats()))
    }

    /// Get a reference to the underlying query engine
    pub fn query_engine(&self) -> &Arc<QueryEngine> {
        &self.query_engine
    }
}

/// ADBC Service Result wrapper for API responses
#[derive(Debug)]
pub struct AdbcQueryResult {
    pub rows: Vec<RecordBatch>,
    pub schema: Schema,
    pub row_count: usize,
}

impl AdbcQueryResult {
    pub fn new(rows: Vec<RecordBatch>) -> Self {
        let schema = rows.first()
            .map(|batch| batch.schema().as_ref().clone())
            .unwrap_or_else(|| Schema::empty());
        
        let row_count = rows.iter().map(|batch| batch.num_rows()).sum();
        
        Self {
            rows,
            schema,
            row_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_adbc_service_creation() {
        let query_engine = Arc::new(QueryEngine::new());
        let service = AdbcService::new(query_engine);
        
        // Service should have been created successfully
        assert_eq!(service.driver.get_database("datafusion").is_ok(), true);
    }

    #[tokio::test]
    async fn test_register_database() {
        let query_engine = Arc::new(QueryEngine::new());
        let mut service = AdbcService::new(query_engine);
        
        let config = DatabaseConfig {
            db_type: DatabaseType::DataFusion,
            connection_string: "".to_string(),
            options: HashMap::new(),
        };
        
        // This should succeed as DataFusion adapter is registered by default
        let result = service.register_database("test_db", config).await;
        assert!(result.is_ok());
    }
}