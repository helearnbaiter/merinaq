# Modern Data Processing Platform

A high-performance enterprise data processing platform built with Rust, providing unified data querying, permission management, and visualization capabilities.

## Features

- **High Performance**: Built with Rust for memory safety and performance
- **Multi-Environment Configuration**: Supports development, testing, and production environments
- **Unified Data Access**: Query multiple data sources through a single interface
- **Enterprise Security**: Role-based access control with Casbin
- **Authentication**: JWT-based authentication with OAuth2 support
- **Multi-Source Queries**: Support for PostgreSQL, MySQL, CSV, Parquet, and more
- **API-First**: RESTful API design with comprehensive endpoints
- **Extensible Architecture**: Plugin-based data source support

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │◄──►│   API Gateway    │◄──►│  Data Sources   │
│   (React/Vue)   │    │   (Axum)         │    │ (PostgreSQL,   │
└─────────────────┘    └──────────────────┘    │  MySQL, CSV,    │
                                              │  Parquet, etc.) │
┌─────────────────┐    ┌──────────────────┐    └─────────────────┘
│   BI Tools      │◄──►│ Authentication   │
│   (Superset,    │    │   & Authorization│
│   Tableau, etc.)│    │   (Casbin, JWT)  │
└─────────────────┘    └──────────────────┘
                        │ Query Engine     │
                        │   (DataFusion)   │
                        └──────────────────┘
```

## Tech Stack

- **Backend**: Rust
- **Web Framework**: Axum
- **Authentication**: JWT, OAuth2
- **Authorization**: Casbin
- **Database**: PostgreSQL (with SQLx)
- **ORM**: SeaORM
- **Query Engine**: Apache DataFusion
- **Serialization**: Serde
- **HTTP Client**: Reqwest

## Project Structure

```
data-processing-platform/
├── src/
│   ├── main.rs              # Application entry point
│   ├── config.rs            # Configuration management
│   ├── database.rs          # Database connection and migrations
│   ├── models.rs            # Data models
│   ├── auth.rs              # Authentication utilities
│   ├── handlers/            # API route handlers
│   ├── services/            # Business logic services
│   ├── middleware/          # Request middleware
│   ├── query_engine/        # Query execution engine
│   └── utils/               # Utility functions
├── config/
│   └── rbac_model.conf      # Casbin RBAC model
├── database_init.sql        # Database initialization script
├── Cargo.toml              # Project dependencies
└── README.md               # This file
```

## API Endpoints

### Authentication
- `POST /api/v1/auth/login` - User login
- `POST /api/v1/auth/refresh` - Token refresh
- `POST /api/v1/auth/logout` - User logout

### Users
- `GET /api/v1/users` - Get all users
- `POST /api/v1/users` - Create user
- `GET /api/v1/users/{id}` - Get user by ID
- `PUT /api/v1/users/{id}` - Update user
- `DELETE /api/v1/users/{id}` - Delete user

### Data Sources
- `GET /api/v1/data-sources` - Get all data sources
- `POST /api/v1/data-sources` - Create data source
- `GET /api/v1/data-sources/{id}` - Get data source by ID
- `PUT /api/v1/data-sources/{id}` - Update data source
- `DELETE /api/v1/data-sources/{id}` - Delete data source
- `POST /api/v1/data-sources/{id}/test` - Test data source connection

### Query Execution
- `POST /api/v1/query` - Execute query
- `POST /api/v1/query/execute` - Execute SQL
- `GET /api/v1/query/schema` - Get schema information

### Policies (Casbin)
- `GET /api/v1/policies` - Get all policies
- `POST /api/v1/policies` - Create policy
- `DELETE /api/v1/policies/{id}` - Delete policy
- `POST /api/v1/policies/check` - Check permission

### BI Tool Integration
- `GET /api/v1/bi/config` - Get standard connection configuration for BI tools
- `POST /api/v1/bi/query` - Execute query for BI tools
- `GET /api/v1/bi/flight-info` - Get Flight SQL connection information
- `GET /api/v1/bi/superset-config` - Get Superset connection configuration
- `GET /api/v1/bi/schema` - Get schema information for BI tools
- `GET /api/v1/bi/connection-test` - Test BI tool connection

## Configuration

The application supports multiple environments through a hierarchical configuration system:

```
config/
├── base.toml          # Common configuration for all environments
├── development.toml   # Development-specific settings
├── testing.toml       # Testing-specific settings
└── production.toml    # Production-specific settings
```

Configuration is loaded based on the `RUN_MODE` environment variable:

```bash
# Default to development
RUN_MODE=development cargo run

# For production
RUN_MODE=production cargo run

# For testing
RUN_MODE=testing cargo run
```

You can also override configuration values using environment variables with the `APP__` prefix:

```bash
APP__server__port=9000 RUN_MODE=production cargo run
```

For backward compatibility, you can still use environment variables as defined below.

## Database Setup

1. Install PostgreSQL
2. Create the database:
   ```sql
   CREATE DATABASE data_platform;
   ```
3. Run the initialization script:
   ```bash
   psql -d data_platform -f database_init.sql
   ```

## Running the Application

### Prerequisites
- Rust (latest stable version)

### Build and Run
```bash
# Clone the repository
git clone <repository-url>
cd data-processing-platform

# Build the project
cargo build

# Run the application
cargo run
```

The application will start on `http://localhost:8080`

## Development

### Running Tests
```bash
cargo test
```

### Code Formatting
```bash
cargo fmt
```

### Linting
```bash
cargo clippy
```

## Deployment

### Docker
```dockerfile
FROM rust:latest as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:buster-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/data-processing-platform /usr/local/bin/data-processing-platform

EXPOSE 8080
CMD ["data-processing-platform"]
```

### Environment Variables for Production
```bash
APP_NAME="Production Data Platform"
SERVER_HOST="0.0.0.0"
SERVER_PORT=8080
DATABASE_URL="postgresql://user:pass@prod-db:5432/data_platform"
JWT_SECRET="production-jwt-secret-key"
```

## Security

- All API endpoints are protected with JWT authentication
- Role-based access control using Casbin
- Environment-specific security configurations
- SQL injection protection through parameterized queries
- Input validation and sanitization
- Secure password hashing with bcrypt

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.