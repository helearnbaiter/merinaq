//! Authentication and authorization utilities
//! 
//! Contains JWT token handling, password hashing, and OAuth2 utilities

use jsonwebtoken::{encode, decode, Header, Validation, Algorithm, EncodingKey, DecodingKey};
use serde::{Deserialize, Serialize};
use bcrypt::{hash, verify, DEFAULT_COST};
use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use reqwest::Client;
use serde_json::Value;
use uuid::Uuid;

use crate::models::TokenClaims;

#[derive(Debug, Clone)]
pub struct JwtUtils {
    secret: String,
    expiration: i64,
    refresh_expiration: i64, // For refresh tokens
}

impl JwtUtils {
    pub fn new(secret: String, expiration: i64, refresh_expiration: i64) -> Self {
        JwtUtils { secret, expiration, refresh_expiration }
    }

    pub fn generate_token(&self, user_id: String, role: String) -> Result<String> {
        let expiration = Utc::now().timestamp() + self.expiration;
        let claims = TokenClaims {
            sub: user_id,
            exp: expiration,
            iat: Utc::now().timestamp(),
            role,
        };

        let header = Header::new(Algorithm::HS256);
        let encoding_key = EncodingKey::from_secret(self.secret.as_ref());
        let token = encode(&header, &claims, &encoding_key)?;
        Ok(token)
    }

    pub fn generate_refresh_token(&self, user_id: String, role: String) -> Result<String> {
        let expiration = Utc::now().timestamp() + self.refresh_expiration;
        let claims = TokenClaims {
            sub: user_id,
            exp: expiration,
            iat: Utc::now().timestamp(),
            role,
        };

        let header = Header::new(Algorithm::HS256);
        let encoding_key = EncodingKey::from_secret(self.secret.as_ref());
        let token = encode(&header, &claims, &encoding_key)?;
        Ok(token)
    }

    pub fn validate_token(&self, token: &str) -> Result<TokenClaims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        let decoding_key = DecodingKey::from_secret(self.secret.as_ref());
        let token_data = decode::<TokenClaims>(token, &decoding_key, &validation)?;
        Ok(token_data.claims)
    }
}

#[derive(Debug, Clone)]
pub struct OAuth2State {
    pub state: String,
    pub timestamp: i64,
    pub redirect_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OAuth2SessionManager {
    // In a real application, this would be Redis or another distributed store
    active_states: HashMap<String, OAuth2State>,
    state_ttl: i64, // Time to live for OAuth2 states in seconds
}

impl OAuth2SessionManager {
    pub fn new(state_ttl: i64) -> Self {
        OAuth2SessionManager {
            active_states: HashMap::new(),
            state_ttl,
        }
    }

    pub fn generate_state(&mut self, redirect_uri: Option<String>) -> String {
        let state = Uuid::new_v4().to_string();
        let timestamp = Utc::now().timestamp();
        
        self.active_states.insert(state.clone(), OAuth2State {
            state: state.clone(),
            timestamp,
            redirect_uri,
        });
        
        state
    }

    pub fn validate_state(&mut self, state: &str) -> Option<OAuth2State> {
        if let Some(oauth2_state) = self.active_states.get(state) {
            let current_time = Utc::now().timestamp();
            if current_time - oauth2_state.timestamp <= self.state_ttl {
                // Remove the state after validation (one-time use)
                return self.active_states.remove(state);
            }
        }
        None
    }
}

pub fn hash_password(password: &str) -> Result<String> {
    let hashed = hash(password, DEFAULT_COST)?;
    Ok(hashed)
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool> {
    let verified = verify(password, hash)?;
    Ok(verified)
}

#[derive(Debug, Clone)]
pub struct OAuth2Config {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_url: String,
    pub auth_url: String,
    pub token_url: String,
    pub user_info_url: String,
}

impl OAuth2Config {
    pub fn new(
        client_id: String,
        client_secret: String,
        redirect_url: String,
        auth_url: String,
        token_url: String,
        user_info_url: String,
    ) -> Self {
        OAuth2Config {
            client_id,
            client_secret,
            redirect_url,
            auth_url,
            token_url,
            user_info_url,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OAuth2Provider {
    pub name: String,
    pub config: OAuth2Config,
    pub client: Client,
}

impl OAuth2Provider {
    pub fn new(name: String, config: OAuth2Config) -> Self {
        OAuth2Provider { 
            name, 
            config, 
            client: Client::new(),
        }
    }

    pub async fn get_authorization_url(&self, state: &str, scopes: Option<&[&str]>) -> String {
        let scope_str = if let Some(scopes) = scopes {
            format!("&scope={}", scopes.join("+"))
        } else {
            "".to_string()
        };
        
        format!(
            "{}?client_id={}&redirect_uri={}&response_type=code&state={}{}",
            self.config.auth_url, 
            self.config.client_id, 
            self.config.redirect_url, 
            state,
            scope_str
        )
    }

    pub async fn exchange_code_for_token(&self, code: &str) -> Result<HashMap<String, String>> {
        let params = [
            ("grant_type", "authorization_code"),
            ("client_id", &self.config.client_id),
            ("client_secret", &self.config.client_secret),
            ("code", code),
            ("redirect_uri", &self.config.redirect_url),
        ];

        let response = self.client
            .post(&self.config.token_url)
            .form(&params)
            .send()
            .await?;

        let response_text = response.text().await?;
        let token_response: Value = serde_json::from_str(&response_text)?;

        let mut result = HashMap::new();
        if let Some(access_token) = token_response.get("access_token").and_then(|v| v.as_str()) {
            result.insert("access_token".to_string(), access_token.to_string());
        }
        if let Some(refresh_token) = token_response.get("refresh_token").and_then(|v| v.as_str()) {
            result.insert("refresh_token".to_string(), refresh_token.to_string());
        }
        if let Some(expires_in) = token_response.get("expires_in").and_then(|v| v.as_i64()) {
            result.insert("expires_in".to_string(), expires_in.to_string());
        }

        Ok(result)
    }

    pub async fn get_user_info(&self, access_token: &str) -> Result<UserInfo> {
        let response = self.client
            .get(&self.config.user_info_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .send()
            .await?;

        let user_data: Value = response.json().await?;
        
        Ok(UserInfo {
            id: user_data.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            email: user_data.get("email").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            name: user_data.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            provider: self.name.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct UserInfo {
    pub id: String,
    pub email: String,
    pub name: String,
    pub provider: String,
}

#[derive(Debug, Clone)]
pub struct OAuth2Manager {
    pub providers: HashMap<String, OAuth2Provider>,
}

impl OAuth2Manager {
    pub fn new() -> Self {
        OAuth2Manager {
            providers: HashMap::new(),
        }
    }

    pub fn add_provider(&mut self, name: String, provider: OAuth2Provider) {
        self.providers.insert(name, provider);
    }

    pub fn get_provider(&self, name: &str) -> Option<&OAuth2Provider> {
        self.providers.get(name)
    }
}