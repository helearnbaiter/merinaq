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
}

impl FlightSqlService {
    pub fn new(query_engine: Arc<crate::query_engine::QueryEngine>) -> Self {
        Self {
            query_engine,
            prepared_statements: Arc::new(RwLock::new(HashMap::new())),
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
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
        });

        Ok(Response::new(Box::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // For now, return a simple handshake response
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        
        tokio::spawn(async move {
            let response = HandshakeResponse {
                protocol_version: 0,
                payload: b"handshake_successful".to_vec(),
            };
            
            let _ = tx.send(Ok(response)).await;
        });

        Ok(Response::new(Box::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
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