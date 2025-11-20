//! BI Integration routes
//! 
//! Provides endpoints for business intelligence tool integration

use axum::{
    routing::{get, post},
    Router,
};

/// Create the BI integration router
pub fn create_router() -> Router {
    Router::new()
        .route("/config", get(super::super::super::handlers::bi_integration::get_bi_connection_config))
        .route("/query", post(super::super::super::handlers::bi_integration::execute_bi_query))
        .route("/flight-info", get(super::super::super::handlers::bi_integration::get_flight_sql_info))
        .route("/superset-config", get(super::super::super::handlers::bi_integration::get_superset_config))
        .route("/schema", get(super::super::super::handlers::bi_integration::get_bi_schema))
        .route("/connection-test", get(super::super::super::handlers::bi_integration::test_bi_connection))
}