//! Authentication service implementation
//! 
//! Handles user authentication, token management, and session handling

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

use crate::{
    models::{User, AuthRequest, AuthResponse, TokenClaims, NewUser},
    auth::{JwtUtils, hash_password, verify_password, OAuth2Manager, OAuth2Provider, UserInfo, OAuth2SessionManager},
    database::DatabasePool,
    config::AuthSettings,
};

pub struct AuthService {
    jwt_utils: JwtUtils,
    oauth2_manager: Arc<RwLock<OAuth2Manager>>,
    oauth2_state_manager: Arc<RwLock<OAuth2SessionManager>>,
    // In a real application, you might want to use a more robust session store
    active_sessions: Arc<RwLock<HashMap<String, String>>>,
    // Store active refresh tokens for invalidation
    active_refresh_tokens: Arc<RwLock<HashMap<String, String>>>, // token_hash -> user_id
}

impl AuthService {
    pub fn new(settings: &AuthSettings) -> Self {
        AuthService {
            jwt_utils: JwtUtils::new(
                settings.jwt_secret.clone(), 
                settings.jwt_expiration as i64, 
                settings.refresh_token_expiration as i64
            ),
            oauth2_manager: Arc::new(RwLock::new(OAuth2Manager::new())),
            oauth2_state_manager: Arc::new(RwLock::new(OAuth2SessionManager::new(300))), // 5 minutes TTL for OAuth2 states
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
            active_refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
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
                let refresh_token = self.jwt_utils.generate_refresh_token(user.id.to_string(), user.role.clone())?;
                
                // Store session (in a real app, you'd use Redis or similar)
                let mut sessions = self.active_sessions.write().await;
                sessions.insert(user.id.to_string(), token.clone());
                
                // Store refresh token for invalidation
                let mut refresh_tokens = self.active_refresh_tokens.write().await;
                refresh_tokens.insert(refresh_token.clone(), user.id.to_string());
                
                Ok(AuthResponse {
                    success: true,
                    token: Some(token),
                    refresh_token: Some(refresh_token),
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

    pub async fn authenticate_oauth2_user(
        &self,
        db_pool: &DatabasePool,
        provider_name: &str,
        code: &str,
    ) -> Result<AuthResponse> {
        let oauth2_manager = self.oauth2_manager.read().await;
        let provider = oauth2_manager.get_provider(provider_name)
            .ok_or_else(|| anyhow::anyhow!("OAuth2 provider {} not found", provider_name))?;

        // Exchange code for token
        let token_result = provider.exchange_code_for_token(code).await?;
        let access_token = token_result.get("access_token")
            .ok_or_else(|| anyhow::anyhow!("No access token in response"))?;

        // Get user info from provider
        let user_info = provider.get_user_info(access_token).await?;

        // Check if user already exists by email or provider ID
        let mut user = if let Some(existing_user) = db_pool.get_user_by_email(&user_info.email).await? {
            existing_user
        } else {
            // Create new user if doesn't exist
            let new_user = NewUser {
                username: user_info.name.clone(),
                email: user_info.email.clone(),
                password: "oauth2_user".to_string(), // Placeholder for OAuth2 users
                role: "user".to_string(),
            };
            db_pool.create_user(&new_user).await?
        };

        // Update user with OAuth2 provider info if needed
        // In a real app, you might want to store provider-specific data

        // Generate JWT tokens
        let token = self.jwt_utils.generate_token(user.id.to_string(), user.role.clone())?;
        let refresh_token = self.jwt_utils.generate_refresh_token(user.id.to_string(), user.role.clone())?;

        // Store session
        let mut sessions = self.active_sessions.write().await;
        sessions.insert(user.id.to_string(), token.clone());

        // Store refresh token for invalidation
        let mut refresh_tokens = self.active_refresh_tokens.write().await;
        refresh_tokens.insert(refresh_token.clone(), user.id.to_string());

        Ok(AuthResponse {
            success: true,
            token: Some(token),
            refresh_token: Some(refresh_token),
            user: Some(user),
            error: None,
        })
    }

    pub async fn refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<AuthResponse> {
        // Validate refresh token
        let claims = self.jwt_utils.validate_token(refresh_token)?;
        
        // Check if refresh token is in our active tokens list
        let refresh_tokens = self.active_refresh_tokens.read().await;
        if !refresh_tokens.contains_key(refresh_token) {
            return Err(anyhow::anyhow!("Refresh token not found in active tokens"));
        }
        drop(refresh_tokens); // Release the read lock
        
        // Generate new access token
        let new_token = self.jwt_utils.generate_token(claims.sub.clone(), claims.role.clone())?;
        
        // Generate a new refresh token (refresh token rotation)
        let new_refresh_token = self.jwt_utils.generate_refresh_token(claims.sub.clone(), claims.role.clone())?;
        
        // Remove the old refresh token and add the new one
        let mut refresh_tokens = self.active_refresh_tokens.write().await;
        refresh_tokens.remove(refresh_token); // Invalidate old refresh token
        refresh_tokens.insert(new_refresh_token.clone(), claims.sub.clone()); // Add new refresh token
        
        Ok(AuthResponse {
            success: true,
            token: Some(new_token),
            refresh_token: Some(new_refresh_token), // Return new refresh token
            user: None, // User info not needed in refresh response
            error: None,
        })
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
        let new_user = NewUser {
            username: username.to_string(),
            email: email.to_string(),
            password: password_hash,
            role: role.unwrap_or("user").to_string(),
        };
        
        let user = db_pool.create_user(&new_user).await?;
        
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

    pub async fn generate_oauth2_state(&self, redirect_uri: Option<String>) -> String {
        let mut state_manager = self.oauth2_state_manager.write().await;
        state_manager.generate_state(redirect_uri)
    }

    pub async fn validate_oauth2_state(&self, state: &str) -> Option<crate::auth::OAuth2State> {
        let mut state_manager = self.oauth2_state_manager.write().await;
        state_manager.validate_state(state)
    }

    pub async fn add_oauth2_provider(&self, name: String, provider: OAuth2Provider) {
        let mut oauth2_manager = self.oauth2_manager.write().await;
        oauth2_manager.add_provider(name, provider);
    }
}