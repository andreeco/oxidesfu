//! LiveKit-compatible JWT validation and grant checks for OxideSFU.

use std::collections::HashMap;

use base64::{Engine, engine::general_purpose};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const BEARER_PREFIX: &str = "Bearer ";

/// LiveKit-compatible video grants.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct VideoGrants {
    pub room_create: bool,
    pub room_list: bool,
    pub room_record: bool,
    pub room_admin: bool,
    pub room_join: bool,
    pub room: String,
    pub destination_room: String,
    #[serde(default = "default_true")]
    pub can_publish: bool,
    #[serde(default = "default_true")]
    pub can_subscribe: bool,
    #[serde(default = "default_true")]
    pub can_publish_data: bool,
    pub can_publish_sources: Vec<String>,
    pub can_update_own_metadata: bool,
    pub ingress_admin: bool,
    pub hidden: bool,
    pub recorder: bool,
    pub agent: bool,
}

impl Default for VideoGrants {
    fn default() -> Self {
        Self {
            room_create: false,
            room_list: false,
            room_record: false,
            room_admin: false,
            room_join: false,
            room: String::new(),
            destination_room: String::new(),
            can_publish: true,
            can_subscribe: true,
            can_publish_data: true,
            can_publish_sources: Vec::new(),
            can_update_own_metadata: false,
            ingress_admin: false,
            hidden: false,
            recorder: false,
            agent: false,
        }
    }
}

/// LiveKit-compatible SIP grants.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct SipGrants {
    pub admin: bool,
    pub call: bool,
}

/// LiveKit-compatible JWT claims used by OxideSFU.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct Claims {
    pub exp: usize,
    pub iss: String,
    pub nbf: usize,
    pub sub: String,
    pub identity: String,
    pub name: String,
    pub kind: String,
    pub kind_details: Vec<String>,
    pub video: VideoGrants,
    pub sip: SipGrants,
    pub sha256: String,
    pub metadata: String,
    pub attributes: HashMap<String, String>,
    pub room_config: Option<serde_json::Value>,
}

/// Verified authentication context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub api_key: String,
    pub claims: Claims,
}

/// In-memory API key provider for development and tests.
#[derive(Debug, Clone, Default)]
pub struct ApiKeyStore {
    secrets: HashMap<String, String>,
}

impl ApiKeyStore {
    /// Creates an empty API key store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces an API key/secret pair.
    pub fn insert(&mut self, api_key: impl Into<String>, api_secret: impl Into<String>) {
        self.secrets.insert(api_key.into(), api_secret.into());
    }

    fn secret_for(&self, api_key: &str) -> Option<&str> {
        self.secrets.get(api_key).map(String::as_str)
    }
}

/// Errors produced while validating LiveKit-compatible auth.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("invalid authorization header. Must start with Bearer ")]
    MissingBearer,
    #[error("invalid authorization token")]
    InvalidToken,
    #[error("invalid API key")]
    InvalidApiKey,
    #[error("permissions denied")]
    PermissionDenied,
}

/// Verifies LiveKit-compatible access tokens.
#[derive(Debug, Clone)]
pub struct TokenVerifier {
    keys: ApiKeyStore,
}

impl TokenVerifier {
    /// Creates a verifier backed by an in-memory API key store.
    pub fn new(keys: ApiKeyStore) -> Self {
        Self { keys }
    }

    /// Issues a JWT signed with the secret configured for `api_key`.
    pub fn issue_token(&self, api_key: &str, claims: &Claims) -> Result<String, AuthError> {
        let secret = self
            .keys
            .secret_for(api_key)
            .ok_or(AuthError::InvalidApiKey)?;

        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .map_err(|_| AuthError::InvalidToken)
    }

    /// Verifies an HTTP `Authorization` header with the `Bearer` scheme.
    pub fn verify_authorization_header(&self, header: &str) -> Result<AuthContext, AuthError> {
        let token = header
            .strip_prefix(BEARER_PREFIX)
            .ok_or(AuthError::MissingBearer)?;
        self.verify_token(token)
    }

    /// Verifies a raw JWT access token.
    pub fn verify_token(&self, token: &str) -> Result<AuthContext, AuthError> {
        let unverified = decode_unverified_claims(token)?;
        let secret = self
            .keys
            .secret_for(&unverified.iss)
            .ok_or(AuthError::InvalidApiKey)?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_issuer(&[unverified.iss.as_str()]);

        let mut claims = jsonwebtoken::decode::<Claims>(
            token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )
        .map_err(|_| AuthError::InvalidToken)?
        .claims;

        if claims.sub.is_empty() && !claims.identity.is_empty() {
            claims.sub = claims.identity.clone();
        }

        Ok(AuthContext {
            api_key: claims.iss.clone(),
            claims,
        })
    }
}

impl VideoGrants {
    /// Returns whether media publish is allowed, using LiveKit defaults.
    pub fn get_can_publish(&self) -> bool {
        self.can_publish
    }

    /// Returns whether subscribe is allowed, using LiveKit defaults.
    pub fn get_can_subscribe(&self) -> bool {
        self.can_subscribe
    }

    /// Returns whether data publish is allowed.
    pub fn get_can_publish_data(&self) -> bool {
        self.can_publish_data
    }

    /// Returns whether metadata updates are allowed.
    pub fn get_can_update_own_metadata(&self) -> bool {
        self.can_update_own_metadata
    }

    /// Returns whether a specific track source is publishable.
    pub fn get_can_publish_source(&self, source: &str) -> bool {
        if !self.get_can_publish() {
            return false;
        }
        if self.can_publish_sources.is_empty() {
            return true;
        }
        self.can_publish_sources
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(source))
    }
}

impl AuthContext {
    /// Returns the participant identity from `sub` or fallback `identity` claim.
    pub fn participant_identity(&self) -> &str {
        if !self.claims.sub.is_empty() {
            &self.claims.sub
        } else {
            &self.claims.identity
        }
    }

    /// Ensures the token permits agent worker websocket access.
    pub fn ensure_agent_permission(&self) -> Result<(), AuthError> {
        self.claims
            .video
            .agent
            .then_some(())
            .ok_or(AuthError::PermissionDenied)
    }

    /// Ensures the token permits creating rooms.
    pub fn ensure_create_permission(&self) -> Result<(), AuthError> {
        self.claims
            .video
            .room_create
            .then_some(())
            .ok_or(AuthError::PermissionDenied)
    }

    /// Ensures the token permits listing rooms.
    pub fn ensure_list_permission(&self) -> Result<(), AuthError> {
        self.claims
            .video
            .room_list
            .then_some(())
            .ok_or(AuthError::PermissionDenied)
    }

    /// Ensures the token permits egress record operations.
    pub fn ensure_record_permission(&self) -> Result<(), AuthError> {
        self.claims
            .video
            .room_record
            .then_some(())
            .ok_or(AuthError::PermissionDenied)
    }

    /// Ensures the token permits ingress admin operations.
    pub fn ensure_ingress_admin_permission(&self) -> Result<(), AuthError> {
        self.claims
            .video
            .ingress_admin
            .then_some(())
            .ok_or(AuthError::PermissionDenied)
    }

    /// Ensures the token permits joining its configured room and returns that room name.
    pub fn ensure_join_permission(&self) -> Result<&str, AuthError> {
        if self.claims.video.room_join
            && !self.claims.video.room.is_empty()
            && !self.participant_identity().is_empty()
        {
            return Ok(&self.claims.video.room);
        }
        Err(AuthError::PermissionDenied)
    }

    /// Ensures the token permits admin operations for `room`.
    pub fn ensure_admin_permission(&self, room: &str) -> Result<(), AuthError> {
        if self.claims.video.room_admin && self.claims.video.room == room {
            return Ok(());
        }
        Err(AuthError::PermissionDenied)
    }

    /// Ensures the token permits moving or forwarding from `room` to `destination_room`.
    pub fn ensure_destination_room_permission(
        &self,
        room: &str,
        destination_room: &str,
    ) -> Result<(), AuthError> {
        if self.claims.video.room_admin
            && self.claims.video.room == room
            && self.claims.video.destination_room == destination_room
        {
            return Ok(());
        }
        Err(AuthError::PermissionDenied)
    }
}

fn decode_unverified_claims(token: &str) -> Result<Claims, AuthError> {
    let mut parts = token.split('.');
    let _header = parts.next().ok_or(AuthError::InvalidToken)?;
    let payload = parts.next().ok_or(AuthError::InvalidToken)?;
    let _signature = parts.next().ok_or(AuthError::InvalidToken)?;
    if parts.next().is_some() {
        return Err(AuthError::InvalidToken);
    }

    let payload = general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| general_purpose::URL_SAFE.decode(payload))
        .map_err(|_| AuthError::InvalidToken)?;
    serde_json::from_slice(&payload).map_err(|_| AuthError::InvalidToken)
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use jsonwebtoken::{EncodingKey, Header};

    use super::*;

    const API_KEY: &str = "devkey";
    const API_SECRET: &str = "secret";

    fn now() -> usize {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_secs() as usize
    }

    fn verifier() -> TokenVerifier {
        let mut keys = ApiKeyStore::new();
        keys.insert(API_KEY, API_SECRET);
        TokenVerifier::new(keys)
    }

    fn token(mut claims: Claims, secret: &str) -> String {
        if claims.iss.is_empty() {
            claims.iss = API_KEY.to_string();
        }
        if claims.exp == 0 {
            claims.exp = now() + Duration::from_secs(60).as_secs() as usize;
        }
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("test token should encode")
    }

    #[test]
    fn issues_token_with_configured_api_key_secret() {
        let verifier = verifier();
        let claims = Claims {
            iss: API_KEY.to_string(),
            sub: "alice".to_string(),
            name: "Alice".to_string(),
            exp: now() + Duration::from_secs(60).as_secs() as usize,
            video: VideoGrants {
                room_join: true,
                room: "test-room".to_string(),
                can_publish: false,
                can_subscribe: true,
                can_publish_data: false,
                ..Default::default()
            },
            metadata: "meta".to_string(),
            ..Default::default()
        };

        let jwt = verifier
            .issue_token(API_KEY, &claims)
            .expect("token should issue");
        let verified = verifier.verify_token(&jwt).expect("token should verify");
        assert_eq!(verified.claims.sub, "alice");
        assert_eq!(verified.claims.metadata, "meta");
        assert!(!verified.claims.video.can_publish);
        assert!(verified.claims.video.can_subscribe);
        assert!(!verified.claims.video.can_publish_data);
    }

    #[test]
    fn verifies_valid_bearer_token() {
        let jwt = token(
            Claims {
                sub: "alice".to_string(),
                name: "Alice".to_string(),
                video: VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier()
            .verify_authorization_header(&format!("Bearer {jwt}"))
            .expect("token should verify");

        assert_eq!(ctx.api_key, API_KEY);
        assert_eq!(ctx.claims.sub, "alice");
        assert_eq!(ctx.ensure_join_permission(), Ok("test-room"));
    }

    #[test]
    fn rejects_missing_bearer_prefix() {
        let err = verifier()
            .verify_authorization_header("not-a-bearer-token")
            .expect_err("missing bearer prefix should fail");

        assert_eq!(err, AuthError::MissingBearer);
    }

    #[test]
    fn rejects_wrong_secret() {
        let jwt = token(
            Claims {
                video: VideoGrants {
                    room_create: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            "wrong-secret",
        );

        let err = verifier()
            .verify_token(&jwt)
            .expect_err("wrong secret should fail");

        assert_eq!(err, AuthError::InvalidToken);
    }

    #[test]
    fn rejects_unknown_api_key() {
        let jwt = token(
            Claims {
                iss: "unknown".to_string(),
                video: VideoGrants {
                    room_create: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );

        let err = verifier()
            .verify_token(&jwt)
            .expect_err("unknown key should fail");

        assert_eq!(err, AuthError::InvalidApiKey);
    }

    #[test]
    fn enforces_room_grants() {
        let jwt = token(
            Claims {
                video: VideoGrants {
                    room_create: true,
                    room_list: false,
                    room_record: true,
                    ingress_admin: true,
                    room_admin: true,
                    room: "admin-room".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );
        let ctx = verifier().verify_token(&jwt).expect("token should verify");

        assert_eq!(ctx.ensure_create_permission(), Ok(()));
        assert_eq!(
            ctx.ensure_list_permission(),
            Err(AuthError::PermissionDenied)
        );
        assert_eq!(ctx.ensure_record_permission(), Ok(()));
        assert_eq!(ctx.ensure_ingress_admin_permission(), Ok(()));
        assert_eq!(ctx.ensure_admin_permission("admin-room"), Ok(()));
        assert_eq!(
            ctx.ensure_admin_permission("other-room"),
            Err(AuthError::PermissionDenied)
        );
        assert_eq!(
            ctx.ensure_join_permission(),
            Err(AuthError::PermissionDenied)
        );
    }

    #[test]
    fn video_grants_defaults_match_livekit_semantics() {
        let grants = VideoGrants::default();
        assert!(grants.get_can_publish());
        assert!(grants.get_can_subscribe());
        assert!(grants.get_can_publish_data());
        assert!(!grants.get_can_update_own_metadata());
        assert!(!grants.agent);
    }

    #[test]
    fn ensure_agent_permission_requires_agent_grant() {
        let jwt = token(
            Claims {
                sub: "agent-worker".to_string(),
                video: VideoGrants {
                    agent: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier().verify_token(&jwt).expect("token should verify");
        assert_eq!(ctx.ensure_agent_permission(), Ok(()));

        let non_agent_jwt = token(
            Claims {
                sub: "alice".to_string(),
                video: VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );
        let non_agent_ctx = verifier()
            .verify_token(&non_agent_jwt)
            .expect("non-agent token should verify");
        assert_eq!(
            non_agent_ctx.ensure_agent_permission(),
            Err(AuthError::PermissionDenied)
        );
    }

    #[test]
    fn can_publish_data_follows_explicit_value() {
        let grants = VideoGrants {
            can_publish: false,
            can_publish_data: false,
            ..Default::default()
        };
        assert!(!grants.get_can_publish_data());
    }

    #[test]
    fn can_publish_sources_respects_can_publish_gate() {
        let grants = VideoGrants {
            can_publish: false,
            can_publish_sources: vec!["camera".to_string()],
            ..Default::default()
        };
        assert!(!grants.get_can_publish_source("camera"));
    }

    #[test]
    fn rejects_expired_token() {
        let jwt = token(
            Claims {
                exp: now().saturating_sub(120),
                ..Default::default()
            },
            API_SECRET,
        );

        assert_eq!(
            verifier().verify_token(&jwt),
            Err(AuthError::InvalidToken),
            "expired token should be rejected"
        );
    }

    #[test]
    fn rejects_token_before_not_before_time() {
        let jwt = token(
            Claims {
                nbf: now() + 120,
                ..Default::default()
            },
            API_SECRET,
        );

        assert_eq!(
            verifier().verify_token(&jwt),
            Err(AuthError::InvalidToken),
            "future nbf token should be rejected"
        );
    }

    #[test]
    fn rejects_token_without_exp() {
        let jwt = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &Claims {
                iss: API_KEY.to_string(),
                exp: 0,
                ..Default::default()
            },
            &EncodingKey::from_secret(API_SECRET.as_bytes()),
        )
        .expect("token should encode");

        assert_eq!(
            verifier().verify_token(&jwt),
            Err(AuthError::InvalidToken),
            "token without exp should be rejected"
        );
    }

    #[test]
    fn rejects_token_with_missing_issuer() {
        let claims = Claims {
            iss: String::new(),
            exp: now() + 60,
            ..Default::default()
        };
        let jwt = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(API_SECRET.as_bytes()),
        )
        .expect("test token should encode");

        assert_eq!(
            verifier().verify_token(&jwt),
            Err(AuthError::InvalidApiKey),
            "missing issuer should fail API key lookup"
        );
    }

    #[test]
    fn ensure_join_permission_rejects_missing_identity() {
        let jwt = token(
            Claims {
                sub: String::new(),
                identity: String::new(),
                video: VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );
        let ctx = verifier().verify_token(&jwt).expect("token should verify");
        assert_eq!(
            ctx.ensure_join_permission(),
            Err(AuthError::PermissionDenied)
        );
    }

    #[test]
    fn uses_identity_claim_when_sub_is_missing() {
        let jwt = token(
            Claims {
                sub: String::new(),
                identity: "alice".to_string(),
                video: VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier().verify_token(&jwt).expect("token should verify");
        assert_eq!(ctx.participant_identity(), "alice");
        assert_eq!(ctx.ensure_join_permission(), Ok("test-room"));
    }

    #[test]
    fn rejects_token_with_missing_signature() {
        let claims = Claims {
            iss: API_KEY.to_string(),
            exp: now() + 60,
            ..Default::default()
        };
        let payload = serde_json::to_vec(&claims).expect("claims should serialize");
        let header = br#"{"alg":"HS256","typ":"JWT"}"#;
        let token = format!(
            "{}.{}.",
            general_purpose::URL_SAFE_NO_PAD.encode(header),
            general_purpose::URL_SAFE_NO_PAD.encode(payload)
        );

        assert_eq!(
            verifier().verify_token(&token),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn rejects_token_with_wrong_algorithm() {
        let claims = Claims {
            iss: API_KEY.to_string(),
            exp: now() + 60,
            ..Default::default()
        };
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS384),
            &claims,
            &EncodingKey::from_secret(API_SECRET.as_bytes()),
        )
        .expect("token should encode");

        assert_eq!(
            verifier().verify_token(&token),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn accepts_token_with_current_validity_window() {
        let jwt = token(
            Claims {
                nbf: now().saturating_sub(1),
                exp: now() + 60,
                sub: "alice".to_string(),
                video: VideoGrants {
                    room_join: true,
                    room: "test-room".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier()
            .verify_token(&jwt)
            .expect("current validity window should verify");
        assert_eq!(ctx.participant_identity(), "alice");
    }

    #[test]
    fn extracts_name_metadata_and_attributes_from_token() {
        let jwt = token(
            Claims {
                sub: "alice".to_string(),
                name: "Alice Display".to_string(),
                metadata: "{\"tier\":\"pro\"}".to_string(),
                attributes: HashMap::from([
                    ("role".to_string(), "speaker".to_string()),
                    ("lang".to_string(), "en".to_string()),
                ]),
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier().verify_token(&jwt).expect("token should verify");
        assert_eq!(ctx.claims.name, "Alice Display");
        assert_eq!(ctx.claims.metadata, "{\"tier\":\"pro\"}");
        assert_eq!(
            ctx.claims.attributes.get("role"),
            Some(&"speaker".to_string())
        );
        assert_eq!(ctx.claims.attributes.get("lang"), Some(&"en".to_string()));
    }

    #[test]
    fn extracts_participant_kind_from_token() {
        let jwt = token(
            Claims {
                sub: "alice".to_string(),
                kind: "agent".to_string(),
                kind_details: vec!["weather".to_string(), "assistant".to_string()],
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier().verify_token(&jwt).expect("token should verify");
        assert_eq!(ctx.claims.kind, "agent");
        assert_eq!(
            ctx.claims.kind_details,
            vec!["weather".to_string(), "assistant".to_string()]
        );
    }

    #[test]
    fn extracts_room_configuration_from_token() {
        let room_config = serde_json::json!({
            "emptyTimeout": 30,
            "maxParticipants": 15,
            "metadata": "seed"
        });
        let jwt = token(
            Claims {
                sub: "alice".to_string(),
                room_config: Some(room_config.clone()),
                ..Default::default()
            },
            API_SECRET,
        );

        let ctx = verifier().verify_token(&jwt).expect("token should verify");
        assert_eq!(ctx.claims.room_config, Some(room_config));
    }
}
