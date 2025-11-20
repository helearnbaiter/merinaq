# OAuth2 + JWT Authentication System Implementation

## Overview

This document outlines the implementation of a comprehensive OAuth2 + JWT authentication system with support for multiple authentication providers.

## Features

1. **Complete OAuth2 Flow**: Implementation of the full OAuth2 authorization code flow
2. **JWT Token Management**: Access tokens with automatic refresh mechanism
3. **Multi-Provider Support**: Integration with Google, GitHub, Facebook and other OAuth2 providers
4. **Automatic Token Refresh**: Refresh token mechanism for seamless user experience
5. **User Profile Management**: Automatic user creation and linking based on OAuth2 provider data

## Architecture

### Core Components

1. **OAuth2Manager**: Manages multiple OAuth2 providers
2. **OAuth2Provider**: Represents individual OAuth2 providers (Google, GitHub, etc.)
3. **OAuth2Config**: Configuration for each OAuth2 provider
4. **AuthService**: Main authentication service handling both traditional and OAuth2 authentication

### Data Flow

1. User initiates OAuth2 flow by accessing `/auth/oauth2/{provider_name}`
2. System redirects to the provider's authorization endpoint
3. User authenticates with the provider and grants permissions
4. Provider redirects back to `/auth/oauth2/{provider_name}/callback` with authorization code
5. System exchanges code for access token
6. System fetches user information from provider
7. System creates/links user account and generates JWT tokens
8. Returns JWT access and refresh tokens to client

## Implementation Details

### OAuth2 Provider Configuration

The system supports configuration of multiple OAuth2 providers via the configuration file:

```json
{
  "auth": {
    "jwt_secret": "your-super-secret-jwt-key-change-this-in-production",
    "jwt_expiration": 3600,
    "refresh_token_expiration": 86400,
    "google": {
      "client_id": "your-google-client-id",
      "client_secret": "your-google-client-secret"
    },
    "github": {
      "client_id": "your-github-client-id",
      "client_secret": "your-github-client-secret"
    },
    "facebook": {
      "client_id": "your-facebook-client-id",
      "client_secret": "your-facebook-client-secret"
    }
  }
}
```

### API Endpoints

- `POST /auth/login` - Traditional username/password authentication
- `POST /auth/refresh` - JWT token refresh
- `POST /auth/logout` - User logout
- `GET /auth/oauth2/{provider_name}` - Initiate OAuth2 flow for a provider
- `GET /auth/oauth2/{provider_name}/callback` - Handle OAuth2 callback

### OAuth2 Flow

1. **Authorization Request**:
   - Client calls `GET /auth/oauth2/google` (or github/facebook)
   - Server returns authorization URL for the provider
   - Client redirects user to provider's authorization page

2. **Callback Handling**:
   - Provider redirects user back to `GET /auth/oauth2/google/callback?code=...`
   - Server exchanges authorization code for access token
   - Server fetches user information from provider
   - Server creates/updates user account
   - Server generates JWT tokens and returns to client

3. **Token Management**:
   - Access tokens have short expiration (1 hour by default)
   - Refresh tokens have longer expiration (24 hours by default)
   - Client can refresh access token using `POST /auth/refresh`

## Security Considerations

1. **Secure Token Storage**: JWT tokens should be stored securely on the client side
2. **HTTPS Required**: All authentication endpoints should be served over HTTPS
3. **State Parameter**: OAuth2 flows include state parameter to prevent CSRF attacks
4. **Provider Validation**: Only configured OAuth2 providers are allowed
5. **Rate Limiting**: Authentication endpoints should be rate-limited to prevent abuse

## Extensibility

The system is designed to support additional OAuth2 providers:

1. Add provider configuration to `OAuth2ProviderConfig`
2. Update the initialization function in `main.rs`
3. The system will automatically handle the new provider

## Error Handling

The system provides comprehensive error handling with specific error codes:

- `AUTH_001`: Invalid credentials (traditional login)
- `AUTH_002`: Internal authentication error
- `AUTH_004`: OAuth2 provider not found
- `AUTH_005`: OAuth2 authentication failed
- `AUTH_006`: Internal OAuth2 error
- `AUTH_007`: No authorization code provided
- `AUTH_008`: Invalid refresh token
- `AUTH_009`: Internal token refresh error

## Usage Examples

### Initiating Google OAuth2 Flow

```bash
GET /auth/oauth2/google?state=random_state_string&scopes=email,profile
```

### Handling OAuth2 Callback

```bash
GET /auth/oauth2/google/callback?code=authorization_code&state=original_state
```

### Refreshing JWT Token

```bash
POST /auth/refresh
Content-Type: application/json

{
  "refresh_token": "your_refresh_token"
}
```

## Configuration

The system requires proper configuration of OAuth2 providers in the application configuration. Each provider needs:

1. Client ID
2. Client Secret
3. Properly configured redirect URIs in the provider's developer console

## Database Integration

OAuth2 users are stored in the same user table as traditional users, with provider-specific information stored in extended user profiles. The system can link existing accounts with the same email address to prevent duplicate accounts.