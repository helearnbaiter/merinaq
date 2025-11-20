# Multi-Environment Configuration System

This document explains how to use the multi-environment configuration system in the Data Processing Platform.

## Configuration Structure

The application supports multiple environments through a hierarchical configuration system:

```
config/
├── base.toml          # Common configuration for all environments
├── development.toml   # Development-specific settings
├── testing.toml       # Testing-specific settings
├── production.toml    # Production-specific settings
└── rbac_model.conf    # Casbin RBAC model configuration
```

## Environment Selection

The application determines which environment to use based on the `RUN_MODE` environment variable:

- `RUN_MODE=development` (default) - Loads `config/development.toml`
- `RUN_MODE=testing` - Loads `config/testing.toml`
- `RUN_MODE=production` - Loads `config/production.toml`

## Configuration Loading Order

1. **Base Configuration**: Always loads `config/base.toml`
2. **Environment Override**: Loads environment-specific file (e.g., `config/production.toml`)
3. **Environment Variables**: Applies environment variables with `APP__` prefix

## Example Usage

### Running in Different Environments

```bash
# Development mode (default)
cargo run

# Production mode
RUN_MODE=production cargo run

# Testing mode
RUN_MODE=testing cargo run
```

### Environment Variable Override

You can override any configuration value using environment variables with the `APP__` separator:

```bash
# Override JWT expiration in production
APP__auth__jwt_expiration=7200 RUN_MODE=production cargo run

# Override server port
APP__server__port=9000 cargo run
```

## Configuration Schema

The configuration is structured as follows:

```rust
pub struct AppConfig {
    pub app: AppSettings,
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub auth: AuthSettings,
    pub query_engine: QueryEngineSettings,
    pub logging: LoggingSettings,
    pub monitoring: MonitoringSettings,
    pub security: SecuritySettings,
    pub performance: Option<PerformanceSettings>,
}
```

## Environment-Specific Features

### Development Environment (`development.toml`)
- Lower resource limits
- Debug logging enabled
- CORS enabled for local development
- Rate limiting disabled

### Production Environment (`production.toml`)
- Higher resource limits
- Environment variable support for secrets
- Security features enabled
- Performance monitoring enabled
- Proper logging format

### Testing Environment (`testing.toml`)
- Isolated test database
- Lower resource usage
- Trace-level logging
- Different port to avoid conflicts

## Best Practices

1. **Secrets Management**: Use environment variables for sensitive data (JWT secrets, database URLs)
2. **Environment Variables**: Use `APP__section__key=value` format for overrides
3. **Default Values**: All configurations have sensible defaults
4. **Type Safety**: All configuration values are type-checked at compile time

## Example Override

To override the database URL in production:

```bash
export DATABASE_URL="postgresql://user:pass@prod-db:5432/platform"
APP__database__url='${DATABASE_URL}' RUN_MODE=production cargo run
```

This configuration system provides flexibility for different deployment scenarios while maintaining security and performance requirements.