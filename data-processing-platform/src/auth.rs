//! Authentication and authorization utilities
//! 
//! Contains JWT token handling, password hashing, and OAuth2 utilities

use jsonwebtoken::{encode, decode, Header, Validation};
use serde::{Deserialize, Serialize};
use bcrypt::{hash, verify, DEFAULT_COST};
use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;

use crate::models::TokenClaims;

#[derive(Debug, Clone)]
pub struct JwtUtils {
    secret: String,
    expiration: i64,
}

impl JwtUtils {
    pub fn new(secret: String, expiration: i64) -> Self {
        JwtUtils { secret, expiration }
    }

    pub fn generate_token(&self, user_id: String, role: String) -> Result<String> {
        let expiration = Utc::now().timestamp() + self.expiration;
        let claims = TokenClaims {
            sub: user_id,
            exp: expiration,
            iat: Utc::now().timestamp(),
            role,
        };

        let token = encode(&Header::default(), &claims, &self.secret.as_ref().into())?;
        Ok(token)
    }

    pub fn validate_token(&self, token: &str) -> Result<TokenClaims> {
        let validation = Validation::default();
        let token_data = decode::<TokenClaims>(token, &self.secret.as_ref().into(), &validation)?;
        Ok(token_data.claims)
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
}

impl OAuth2Config {
    pub fn new(
        client_id: String,
        client_secret: String,
        redirect_url: String,
        auth_url: String,
        token_url: String,
    ) -> Self {
        OAuth2Config {
            client_id,
            client_secret,
            redirect_url,
            auth_url,
            token_url,
        }
    }
}

// Mock OAuth2 provider for demonstration
#[derive(Debug, Clone)]
pub struct OAuth2Provider {
    pub name: String,
    pub config: OAuth2Config,
}

impl OAuth2Provider {
    pub fn new(name: String, config: OAuth2Config) -> Self {
        OAuth2Provider { name, config }
    }

    pub async fn get_authorization_url(&self, state: &str) -> String {
        format!(
            "{}?client_id={}&redirect_uri={}&response_type=code&state={}",
            self.config.auth_url, self.config.client_id, self.config.redirect_url, state
        )
    }

    pub async fn exchange_code_for_token(&self, code: &str) -> Result<HashMap<String, String>> {
        // This is a simplified implementation
        // In a real application, you would make an HTTP request to the token endpoint
        let mut response = HashMap::new();
        response.insert("access_token".to_string(), "mock_access_token".to_string());
        response.insert("refresh_token".to_string(), "mock_refresh_token".to_string());
        response.insert("expires_in".to_string(), "3600".to_string());
        Ok(response)
    }
}