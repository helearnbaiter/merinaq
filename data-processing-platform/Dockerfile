# Multi-stage Docker build for production
FROM rust:1.75 AS builder

# Set working directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create a dummy main to allow cargo to download dependencies
RUN mkdir src && \
    echo "fn main() { println!(\"Dummy build\"); }" > src/main.rs

# Download and build dependencies
RUN cargo build --release && rm -rf src

# Copy the actual source code
COPY . .

# Build the application
RUN cargo build --release

# Production stage
FROM debian:bookworm-slim AS runtime

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/data-processing-platform /usr/local/bin/data-processing-platform

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Change ownership of the binary
RUN chown appuser:appuser /usr/local/bin/data-processing-platform

# Create config directory and copy config files
RUN mkdir -p /app/config
COPY config/ /app/config/
RUN chown -R appuser:appuser /app/config

# Switch to non-root user
USER appuser

# Expose the application port
EXPOSE 8080

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the application
CMD ["data-processing-platform"]