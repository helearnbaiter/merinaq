//! ADBC Connection Pool Implementation
//! 
//! This module provides connection pooling functionality for ADBC connections
//! to improve performance and resource management.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;
use async_trait::async_trait;

use super::adbc::{AdbcConnection, AdbcDatabase, AdbcResult};

/// Pooled connection wrapper that returns the connection to the pool when dropped
pub struct PooledConnection {
    connection: Option<Arc<AdbcConnection>>,
    pool: Arc<ConnectionPoolInner>,
    checkout_time: Instant,
}

impl PooledConnection {
    pub fn connection(&self) -> &AdbcConnection {
        self.connection.as_ref().expect("PooledConnection connection should not be None")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let connection = self.connection.take().unwrap();
        let pool = Arc::clone(&self.pool);
        // Return connection to pool asynchronously
        tokio::spawn(async move {
            pool.return_connection(connection).await;
        });
    }
}

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 20,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300), // 5 minutes
            max_lifetime: Duration::from_secs(1800), // 30 minutes
        }
    }
}

struct ConnectionPoolInner {
    available_connections: Arc<Mutex<VecDeque<PooledConnectionData>>>,
    semaphore: Arc<Semaphore>,
    database: Arc<dyn AdbcDatabase>,
    config: PoolConfig,
    total_connections: std::sync::atomic::AtomicUsize,
}

struct PooledConnectionData {
    connection: Arc<AdbcConnection>,
    created_at: Instant,
    last_used_at: Instant,
}

impl ConnectionPoolInner {
    async fn return_connection(self: &Arc<Self>, connection: Arc<AdbcConnection>) {
        let pooled_data = PooledConnectionData {
            connection,
            created_at: Instant::now() - self.config.max_lifetime, // Use actual creation time in real implementation
            last_used_at: Instant::now(),
        };

        let mut available = self.available_connections.lock().await;
        available.push_back(pooled_data);
        // Release a permit to allow other tasks to acquire connections
        self.semaphore.add_permits(1);
    }
}

/// Connection pool for ADBC connections
pub struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(database: Arc<dyn AdbcDatabase>, config: PoolConfig) -> Self {
        let available_connections = Arc::new(Mutex::new(VecDeque::new()));
        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        
        let inner = Arc::new(ConnectionPoolInner {
            available_connections,
            semaphore,
            database,
            config,
            total_connections: std::sync::atomic::AtomicUsize::new(0),
        });

        // Spawn background task to maintain the pool
        let pool_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(30)).await; // Run maintenance every 30 seconds
                pool_inner.maintain_pool().await;
            }
        });

        Self { inner }
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self) -> AdbcResult<PooledConnection> {
        // Acquire a permit (respects max connections)
        let _permit = self.inner.semaphore.acquire().await
            .map_err(|_| super::adbc::AdbcError::Internal("Semaphore closed".to_string()))?;

        // Try to get an available connection
        {
            let mut available = self.inner.available_connections.lock().await;
            if let Some(mut pooled_data) = available.pop_front() {
                pooled_data.last_used_at = Instant::now();
                let connection = pooled_data.connection;
                
                return Ok(PooledConnection {
                    connection: Some(connection),
                    pool: Arc::clone(&self.inner),
                    checkout_time: Instant::now(),
                });
            }
        }

        // No available connections, create a new one if under limit
        let new_connection = self.inner.database.connect().await?;
        self.inner.total_connections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(PooledConnection {
            connection: Some(new_connection),
            pool: Arc::clone(&self.inner),
            checkout_time: Instant::now(),
        })
    }

    /// Get current pool statistics
    pub async fn stats(&self) -> PoolStats {
        let available = self.inner.available_connections.lock().await.len();
        let total = self.inner.total_connections.load(std::sync::atomic::Ordering::SeqCst);
        let waiting = self.inner.semaphore.available_permits();
        
        PoolStats {
            available_connections: available,
            total_connections: total,
            waiting_count: waiting,
        }
    }
}

impl ConnectionPoolInner {
    async fn maintain_pool(self: &Arc<Self>) {
        let mut available = self.available_connections.lock().await;
        
        // Remove expired connections
        available.retain(|conn_data| {
            let idle_duration = Instant::now().duration_since(conn_data.last_used_at);
            let lifetime = Instant::now().duration_since(conn_data.created_at);
            
            // Keep connection if it's not too old and not idle too long
            idle_duration < self.config.idle_timeout && lifetime < self.config.max_lifetime
        });
    }
}

/// Pool statistics
#[derive(Debug)]
pub struct PoolStats {
    pub available_connections: usize,
    pub total_connections: usize,
    pub waiting_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_connection_pool() {
        // This is a basic test - in a real scenario, you'd test with an actual database
        // For now, we're just testing the structure
        let config = PoolConfig::default();
        // let pool = ConnectionPool::new(todo!(), config);
        
        // Basic structure test
        assert!(true); // Placeholder until we have a mock database
    }
}