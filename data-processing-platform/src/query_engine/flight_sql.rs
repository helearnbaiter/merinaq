//! Flight SQL Service Implementation
//! 
//! This module implements the Flight SQL protocol using Arrow Flight RPC for high-performance
//! data transfer between clients and the query engine.

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    Location, Ticket, PollInfo, PutResult, SchemaResult, Criteria,
    ActionType, CreatePreparedStatementRequest, CreatePreparedStatementResult,
    ClosePreparedStatementRequest, Command, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
    CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo,
    CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementQuery, CommandStatementUpdate, FlightEndpoint, PollInfo,
    Result, SubmitActionRequest, SubmitActionResult, SchemaAsIpc,
};
use arrow_ipc::convert::try_schema_from_ipc_buffer;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::error::ArrowError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

// Flight SQL service implementation
pub struct FlightSqlService {
    query_engine: Arc<crate::query_engine::QueryEngine>,
    prepared_statements: Arc<RwLock<HashMap<String, String>>>, // statement_id -> sql
    auth_tokens: Arc<RwLock<HashMap<String, AuthToken>>>,     // token -> token info
}

#[derive(Debug, Clone)]
struct AuthToken {
    user_id: String,
    permissions: Vec<String>,
    expires_at: std::time::SystemTime,
}

impl FlightSqlService {
    pub fn new(query_engine: Arc<crate::query_engine::QueryEngine>) -> Self {
        Self {
            query_engine,
            prepared_statements: Arc::new(RwLock::new(HashMap::new())),
            auth_tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Validate authentication token
    async fn validate_token(&self, token: &str) -> Result<(), Status> {
        let tokens = self.auth_tokens.read().await;
        match tokens.get(token) {
            Some(auth_token) => {
                if auth_token.expires_at > std::time::SystemTime::now() {
                    Ok(())
                } else {
                    Err(Status::unauthenticated("Token has expired"))
                }
            }
            None => Err(Status::unauthenticated("Invalid token")),
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightSqlService {
    type HandshakeStream = Box<dyn Streaming<Item = Result<HandshakeResponse>> + Send + Unpin>;
    type ListFlightsStream = Box<dyn Streaming<Item = Result<FlightInfo>> + Send + Unpin>;
    type DoGetStream = Box<dyn Streaming<Item = Result<FlightData>> + Send + Unpin>;
    type DoPutStream = Box<dyn Streaming<Item = Result<PutResult>> + Send + Unpin>;
    type DoActionStream = Box<dyn Streaming<Item = Result<arrow_flight::Result>> + Send + Unpin>;
    type ListActionsStream = Box<dyn Streaming<Item = Result<ActionType>> + Send + Unpin>;
    type DoExchangeStream = Box<dyn Streaming<Item = Result<FlightData>> + Send + Unpin>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let query = descriptor
            .cmd
            .ok_or_else(|| Status::invalid_argument("FlightDescriptor command is required"))?;
        
        let query_str = String::from_utf8(query)
            .map_err(|_| Status::invalid_argument("Invalid query in command"))?;

        // Execute the query to get schema
        let batches = self.query_engine.execute_query(&query_str).await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?;

        if batches.is_empty() {
            return Err(Status::not_found("No results for query"));
        }

        let schema = batches[0].schema();
        let options = IpcWriteOptions::default();
        let schema_flight_data = SchemaAsIpc::new(&schema, &options).try_into()
            .map_err(|_| Status::internal("Failed to serialize schema"))?;

        Ok(Response::new(schema_flight_data))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let query = String::from_utf8(ticket.ticket)
            .map_err(|_| Status::invalid_argument("Invalid ticket"))?;

        // Execute the query and return results as a stream
        let batches = self.query_engine.execute_query(&query).await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?;

        // Convert batches to FlightData stream
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        // Spawn a task to send the data
        let batches_clone = batches.clone(); // Clone the batches to move into async block
        tokio::spawn(async move {
            if !batches_clone.is_empty() {
                // Send schema first
                let schema = batches_clone[0].schema();
                let options = IpcWriteOptions::default();
                let schema_flight_data = SchemaAsIpc::new(&schema, &options).try_into();
                
                if let Ok(data) = schema_flight_data {
                    if tx.send(Ok(data)).await.is_err() {
                        return;
                    }
                }

                // Send record batches
                for batch in batches_clone {
                    // Serialize record batch to IPC format
                    let mut buf: Vec<u8> = Vec::new();
                    {
                        let mut writer = FileWriter::try_new(&mut buf, batch.schema()).unwrap();
                        writer.write(&batch).unwrap();
                        writer.finish().unwrap();
                    }

                    let flight_data = FlightData {
                        flight_descriptor: None,
                        data_header: buf,
                        data_body: vec![], // Data is in data_header for record batches
                        app_metadata: vec![],
                    };

                    if tx.send(Ok(flight_data)).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut stream = request.into_inner();
        
        // Get the handshake request from the client
        if let Some(handshake_req) = stream.message().await.map_err(|_| Status::invalid_argument("Invalid handshake request"))? {
            // Extract authentication token from the payload
            let auth_payload = String::from_utf8(handshake_req.payload)
                .map_err(|_| Status::invalid_argument("Invalid authentication payload"))?;
            
            // For now, we'll just validate the token format
            // In a real implementation, we would validate against a token store
            let token = auth_payload.trim_start_matches("Bearer ").to_string();
            
            // Create a simple auth token entry
            let auth_token = AuthToken {
                user_id: "flight_user".to_string(),
                permissions: vec!["read".to_string(), "write".to_string()],
                expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(3600), // 1 hour
            };
            
            {
                let mut tokens = self.auth_tokens.write().await;
                tokens.insert(token.clone(), auth_token);
            }
            
            // Create handshake response with a token
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            
            tokio::spawn(async move {
                let response = HandshakeResponse {
                    protocol_version: 0,
                    payload: format!("Bearer:{}", token).into_bytes(),
                };
                
                let _ = tx.send(Ok(response)).await;
            });

            return Ok(Response::new(Box::new(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            )));
        }
        
        Err(Status::unauthenticated("Handshake failed"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // For now, return an empty list - in a real implementation, this would list available flights
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        tokio::spawn(async move {
            // This would normally return actual flight information based on criteria
        });

        Ok(Response::new(Box::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let query = descriptor
            .cmd
            .ok_or_else(|| Status::invalid_argument("FlightDescriptor command is required"))?;
        
        let query_str = String::from_utf8(query)
            .map_err(|_| Status::invalid_argument("Invalid query in command"))?;

        // Execute the query to get schema for flight info
        let batches = self.query_engine.execute_query(&query_str).await
            .map_err(|e| Status::internal(format!("Query execution failed: {}", e)))?;

        if batches.is_empty() {
            return Err(Status::not_found("No results for query"));
        }

        let schema = batches[0].schema();
        let options = IpcWriteOptions::default();
        let schema_bytes = SchemaAsIpc::new(&schema, &options).try_into()
            .map_err(|_| Status::internal("Failed to serialize schema"))?
            .data_header;

        // Create flight info with schema and endpoints
        let flight_info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(descriptor),
            endpoint: vec![
                FlightEndpoint {
                    ticket: Some(Ticket {
                        ticket: query_str.as_bytes().to_vec(),
                    }),
                    location: vec![Location {
                        uri: format!("grpc://{}:{}", 
                            std::env::var("FLIGHT_HOST").unwrap_or_else(|_| "localhost".to_string()),
                            std::env::var("FLIGHT_PORT").unwrap_or_else(|_| "9090".to_string())
                        ),
                    }],
                    expiration_time: None,
                    app_metadata: vec![],
                }
            ],
            total_records: -1, // Unknown
            total_bytes: -1,   // Unknown
            ordered: false,
        };

        Ok(Response::new(flight_info))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(
        &self,
        request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        match action.r#type.as_str() {
            "createPreparedStatement" => {
                // Parse the request to extract SQL
                let req: CreatePreparedStatementRequest = 
                    match try_schema_from_ipc_buffer(&action.body) {
                        Ok(req) => req,
                        Err(_) => {
                            let _ = tx.send(Err(Status::invalid_argument("Invalid request"))).await;
                            return Ok(Response::new(Box::new(
                                tokio_stream::wrappers::ReceiverStream::new(rx),
                            )));
                        }
                    };

                // Create a prepared statement
                let statement_id = uuid::Uuid::new_v4().to_string();
                {
                    let mut statements = self.prepared_statements.write().await;
                    statements.insert(statement_id.clone(), req.query);
                }

                let result = CreatePreparedStatementResult {
                    prepared_statement_handle: statement_id.into_bytes(),
                };

                let result_bytes = bincode::serialize(&result)
                    .map_err(|_| Status::internal("Serialization failed"))?;

                let response = arrow_flight::Result {
                    body: result_bytes,
                };

                let _ = tx.send(Ok(response)).await;
            }
            "closePreparedStatement" => {
                // Parse the request to extract statement handle
                let req: ClosePreparedStatementRequest = 
                    match try_schema_from_ipc_buffer(&action.body) {
                        Ok(req) => req,
                        Err(_) => {
                            let _ = tx.send(Err(Status::invalid_argument("Invalid request"))).await;
                            return Ok(Response::new(Box::new(
                                tokio_stream::wrappers::ReceiverStream::new(rx),
                            )));
                        }
                    };

                // Remove the prepared statement
                {
                    let mut statements = self.prepared_statements.write().await;
                    statements.remove(&String::from_utf8_lossy(&req.prepared_statement_handle));
                }

                let response = arrow_flight::Result {
                    body: b"closed".to_vec(),
                };

                let _ = tx.send(Ok(response)).await;
            }
            _ => {
                let _ = tx.send(Err(Status::unimplemented(format!("Action {} not implemented", action.r#type)))).await;
            }
        }

        Ok(Response::new(Box::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn list_actions(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "createPreparedStatement".to_string(),
                description: "Create a prepared statement".to_string(),
            },
            ActionType {
                r#type: "closePreparedStatement".to_string(),
                description: "Close a prepared statement".to_string(),
            },
        ];

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        tokio::spawn(async move {
            for action in actions {
                if tx.send(Ok(action)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}

// Helper function to execute a prepared statement
pub async fn execute_prepared_statement(
    flight_service: &FlightSqlService,
    statement_id: &str,
    params: &[datafusion::arrow::array::ArrayRef],
) -> Result<Vec<RecordBatch>, ArrowError> {
    let statements = flight_service.prepared_statements.read().await;
    let sql = statements.get(statement_id)
        .ok_or(ArrowError::ComputeError("Prepared statement not found".to_string()))?;

    // For now, just execute the SQL directly
    // In a real implementation, we would bind the parameters
    let query_engine = &flight_service.query_engine;
    let batches = query_engine.execute_query(sql).await
        .map_err(|e| ArrowError::ComputeError(format!("Query execution failed: {}", e)))?;

    Ok(batches)
}