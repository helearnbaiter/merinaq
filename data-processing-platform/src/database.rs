//! Database connection and pool management module
//! 
//! Handles database connections, migrations, and connection pooling

use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use anyhow::Result;
use tracing::info;
use crate::config::DatabaseSettings;

pub struct DatabasePool {
    pub pool: PgPool,
}

impl DatabasePool {
    pub async fn new(config: &DatabaseSettings) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.pool_size)
            .connect(config.get_database_url().as_str())
            .await?;
        
        Ok(DatabasePool { pool })
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
        .bind(&user.password)
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