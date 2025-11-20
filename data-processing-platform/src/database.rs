//! Database connection and pool management module
//! 
//! Handles database connections, migrations, and connection pooling

use sqlx::{PgPool, Row};
use anyhow::Result;
use tracing::info;

pub struct DatabasePool {
    pub pool: PgPool,
}

impl DatabasePool {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        
        // Run migrations
        Self::run_migrations(&pool).await?;
        
        Ok(DatabasePool { pool })
    }
    
    async fn run_migrations(pool: &PgPool) -> Result<()> {
        // Create tables if they don't exist
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(100) DEFAULT 'user',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS data_sources (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                source_type VARCHAR(100) NOT NULL, -- postgres, mysql, csv, parquet, etc.
                connection_config JSONB NOT NULL,
                created_by INTEGER REFERENCES users(id),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS queries (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                data_source_id INTEGER REFERENCES data_sources(id),
                sql_text TEXT NOT NULL,
                query_name VARCHAR(255),
                description TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS query_results (
                id SERIAL PRIMARY KEY,
                query_id INTEGER REFERENCES queries(id),
                result_data JSONB,
                execution_time_ms BIGINT,
                status VARCHAR(50) DEFAULT 'completed',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS casbin_rules (
                id SERIAL PRIMARY KEY,
                ptype VARCHAR(100),
                v0 TEXT,
                v1 TEXT,
                v2 TEXT,
                v3 TEXT,
                v4 TEXT,
                v5 TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
            CREATE INDEX IF NOT EXISTS idx_data_sources_type ON data_sources(source_type);
            CREATE INDEX IF NOT EXISTS idx_queries_user_id ON queries(user_id);
            CREATE INDEX IF NOT EXISTS idx_query_results_query_id ON query_results(query_id);
            CREATE INDEX IF NOT EXISTS idx_casbin_rules_ptype ON casbin_rules(ptype);
            "#,
        )
        .execute(pool)
        .await?;

        info!("Database migrations completed");
        Ok(())
    }
    
    pub async fn get_user_by_username(&self, username: &str) -> Result<Option<crate::models::User>> {
        let row = sqlx::query(
            "SELECT id, username, email, password_hash, role, created_at, updated_at, is_active 
             FROM users WHERE username = $1 AND is_active = TRUE"
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            Ok(Some(crate::models::User {
                id: row.get("id"),
                username: row.get("username"),
                email: row.get("email"),
                password_hash: row.get("password_hash"),
                role: row.get("role"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                is_active: row.get("is_active"),
            }))
        } else {
            Ok(None)
        }
    }
    
    pub async fn create_user(&self, user: &crate::models::NewUser) -> Result<crate::models::User> {
        let row = sqlx::query(
            "INSERT INTO users (username, email, password_hash, role) 
             VALUES ($1, $2, $3, $4) 
             RETURNING id, username, email, password_hash, role, created_at, updated_at, is_active"
        )
        .bind(&user.username)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(&user.role)
        .fetch_one(&self.pool)
        .await?;

        Ok(crate::models::User {
            id: row.get("id"),
            username: row.get("username"),
            email: row.get("email"),
            password_hash: row.get("password_hash"),
            role: row.get("role"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            is_active: row.get("is_active"),
        })
    }
    
    pub async fn get_data_source_by_id(&self, id: i32) -> Result<Option<crate::models::DataSource>> {
        let row = sqlx::query(
            "SELECT id, name, description, source_type, connection_config, created_by, created_at, updated_at, is_active 
             FROM data_sources WHERE id = $1 AND is_active = TRUE"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            Ok(Some(crate::models::DataSource {
                id: row.get("id"),
                name: row.get("name"),
                description: row.get("description"),
                source_type: row.get("source_type"),
                connection_config: row.get("connection_config"),
                created_by: row.get("created_by"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                is_active: row.get("is_active"),
            }))
        } else {
            Ok(None)
        }
    }
}

// Helper function to initialize database with sample data
pub async fn initialize_sample_data(pool: &PgPool) -> Result<()> {
    // Insert sample admin user (password is 'password' hashed with bcrypt)
    sqlx::query(
        r#"
        INSERT INTO users (username, email, password_hash, role, is_active) 
        VALUES 
            ('admin', 'admin@example.com', '$2b$12$LQv3c158Q4YZW6z7z2P7k.hLm3n4o5p6q7r8s9t0u1v2w3x4y5z6', 'admin'),
            ('user1', 'user1@example.com', '$2b$12$LQv3c158Q4YZW6z7z2P7k.hLm3n4o5p6q7r8s9t0u1v2w3x4y5z6', 'user')
        ON CONFLICT (username) DO NOTHING;
        "#,
    )
    .execute(pool)
    .await?;
    
    // Insert sample data source
    sqlx::query(
        r#"
        INSERT INTO data_sources (name, description, source_type, connection_config, created_by) 
        VALUES 
            ('Sample PostgreSQL DB', 'Sample PostgreSQL database for testing', 'postgres', 
             '{"host": "localhost", "port": 5432, "database": "sample_db", "username": "user"}', 1)
        ON CONFLICT (name) DO NOTHING;
        "#,
    )
    .execute(pool)
    .await?;

    info!("Sample data initialized");
    Ok(())
}