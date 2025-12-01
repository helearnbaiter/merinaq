# Unified Response System for Data Processing Platform

This document describes the unified response system implemented in the axum-based data processing platform.

## Overview

The response system provides a consistent way to handle API responses throughout the application using the `ApiResponse<T>` structure.

## Structure

- `ApiResponse<T>`: Defined in `models.rs`, contains the standard response format
- Response utilities: Located in `utils/response.rs`
- Helper functions: Available in `utils/response.rs` and `utils/response_unified.rs`

## Usage

### Basic Usage

```rust
use crate::models::ApiResponse;

// Success response
let response = ApiResponse::success(data);

// Error response
let response = ApiResponse::error("CODE_001", "Error message");
```

### Using Helper Functions

```rust
use crate::utils::response::helpers;

// Standard responses
let success = helpers::success_message("Operation completed");
let created = helpers::created(data);
let not_found = helpers::not_found("Resource not found");
let bad_request = helpers::bad_request("Invalid input");
```

### Using the ApiResponseExt Trait

```rust
use crate::utils::response::ApiResponseExt;

// Any serializable value can be converted to a response
let data = vec![user1, user2];
let response = data.success_response();
```

### Using Response Builder

```rust
use crate::utils::response::response_builder;

let response = response_builder()
    .data(some_data)
    .request_id("req-12345")
    .meta(serde_json::json!({"pagination": {"page": 1, "total": 10}}))
    .build_with_status(StatusCode::OK);
```

## Response Format

```json
{
  "success": true,
  "data": { /* response data */ },
  "error": { /* error details */ },
  "timestamp": "2023-01-01T00:00:00Z",
  "request_id": "req-12345"
}
```

## Standard Error Codes

- `BAD_REQUEST_400`: Bad request
- `UNAUTHORIZED_401`: Unauthorized access
- `FORBIDDEN_403`: Forbidden access
- `NOT_FOUND_404`: Resource not found
- `INTERNAL_ERROR_500`: Internal server error