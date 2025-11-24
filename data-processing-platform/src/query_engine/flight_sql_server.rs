//! Flight SQL gRPC Server
//! 
//! This module implements a gRPC server that serves the Flight SQL protocol
//! using Arrow Flight RPC for high-performance data transfer between clients
//! and the query engine.

use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, error};

use crate::query_engine::QueryEngine;
use crate::query_engine::flight_sql::FlightSqlService;

pub struct FlightSqlServer {
    query_engine: Arc<QueryEngine>,
    host: String,
    port: u16,
}

impl FlightSqlServer {
    pub fn new(query_engine: Arc<QueryEngine>, host: String, port: u16) -> Self {
        Self {
            query_engine,
            host,
            port,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = format!("{}:{}", self.host, self.port).parse()
            .map_err(|e| format!("Invalid address: {}:{}", self.host, self.port))?;
        
        let service = FlightSqlService::new(self.query_engine.clone());
        
        info!("Starting Flight SQL server on {}", addr);
        
        Server::builder()
            .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(service))
            .serve(addr)
            .await
            .map_err(|e| {
                error!("Flight SQL server error: {}", e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
        
        Ok(())
    }
}

/// Helper function to start the Flight SQL server in a background task
pub async fn start_flight_sql_server(query_engine: Arc<QueryEngine>, host: String, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = FlightSqlServer::new(query_engine, host, port);
    server.start().await
}