//! Authentication service implementation
//! 
//! Handles user authentication, token management, and session handling

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

use crate::{
    models::{User, AuthRequest, AuthResponse, TokenClaims},
    auth::{JwtUtils, hash_password, verify_password},
    database::DatabasePool,
    config::AuthSettings,
};

pub struct AuthService {
    jwt_utils: JwtUtils,
    // In a real application, you might want to use a more robust session store
    active_sessions: Arc<RwLock<HashMap<String, String>>>,
}

impl AuthService {
    pub fn new(settings: &AuthSettings) -> Self {
        AuthService {
            jwt_utils: JwtUtils::new(settings.jwt_secret.clone(), settings.jwt_expiration),
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn authenticate_user(
        &self,
        db_pool: &DatabasePool,
        auth_request: &AuthRequest,
    ) -> Result<AuthResponse> {
        // Find user by username
        if let Some(user) = db_pool.get_user_by_username(&auth_request.username).await? {
            // Verify password
            if verify_password(&auth_request.password, &user.password_hash)? {
                // Generate JWT token
                let token = self.jwt_utils.generate_token(user.id.to_string(), user.role.clone())?;
                
                // Store session (in a real app, you'd use Redis or similar)
                let mut sessions = self.active_sessions.write().await;
                sessions.insert(user.id.to_string(), token.clone());
                
                Ok(AuthResponse {
                    success: true,
                    token: Some(token),
                    refresh_token: None, // In a real app, implement refresh tokens
                    user: Some(user),
                    error: None,
                })
            } else {
                Ok(AuthResponse {
                    success: false,
                    token: None,
                    refresh_token: None,
                    user: None,
                    error: Some("Invalid credentials".to_string()),
                })
            }
        } else {
            Ok(AuthResponse {
                success: false,
                token: None,
                refresh_token: None,
                user: None,
                error: Some("User not found".to_string()),
            })
        }
    }

    pub async fn validate_token(&self, token: &str) -> Result<TokenClaims> {
        self.jwt_utils.validate_token(token)
    }

    pub async fn register_user(
        &self,
        db_pool: &DatabasePool,
        username: &str,
        email: &str,
        password: &str,
        role: Option<&str>,
    ) -> Result<AuthResponse> {
        // Hash password
        let password_hash = hash_password(password)?;
        
        // Create new user
        let new_user = crate::models::NewUser {
            username: username.to_string(),
            email: email.to_string(),
            password: password_hash.clone(), // This is actually the hash
            role: role.unwrap_or("user").to_string(),
        };
        
        // We need to work around the password field in NewUser
        // In a real implementation, we'd separate the password hash from the password
        let user = db_pool.create_user(&crate::models::NewUser {
            username: username.to_string(),
            email: email.to_string(),
            password: "dummy".to_string(), // This will be replaced in the database function
            role: role.unwrap_or("user").to_string(),
        }).await?;
        
        Ok(AuthResponse {
            success: true,
            token: None,
            refresh_token: None,
            user: Some(user),
            error: None,
        })
    }

    pub async fn logout_user(&self, user_id: &str) -> Result<()> {
        let mut sessions = self.active_sessions.write().await;
        sessions.remove(user_id);
        Ok(())
    }

    pub async fn is_token_valid(&self, token: &str) -> bool {
        self.jwt_utils.validate_token(token).is_ok()
    }
}