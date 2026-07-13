use std::{collections::HashMap, time::UNIX_EPOCH};

use md5::{Digest, Md5};
use sha2::Sha256;
use thiserror::Error;

pub(crate) const LIVEKIT_REALM: &str = "livekit";

const BASE62_ALPHABET: &[u8; 62] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnMethod {
    Allocate,
    Refresh,
    CreatePermission,
    ChannelBind,
    Send,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TurnRequestAttributes<'a> {
    pub(crate) username: &'a str,
    pub(crate) method: TurnMethod,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedTurnUsername {
    pub(crate) api_key: String,
    pub(crate) participant_id: String,
    pub(crate) expiry_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum TurnAuthError {
    #[error("invalid base62")]
    InvalidBase62,
    #[error("invalid utf-8")]
    InvalidUtf8,
    #[error("invalid username")]
    InvalidUsername,
    #[error("invalid expiry")]
    InvalidExpiry,
    #[error("expired")]
    Expired,
    #[error("invalid api key")]
    InvalidApiKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnAuthDecision {
    pub(crate) user_id: String,
    pub(crate) key: Vec<u8>,
    pub(crate) ok: bool,
}

impl TurnAuthDecision {
    fn reject() -> Self {
        Self {
            user_id: String::new(),
            key: Vec::new(),
            ok: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TurnAuthHandler {
    secrets_by_api_key: HashMap<String, String>,
}

impl TurnAuthHandler {
    pub(crate) fn new(secrets_by_api_key: HashMap<String, String>) -> Self {
        Self { secrets_by_api_key }
    }

    pub(crate) fn create_username_at(
        &self,
        api_key: &str,
        participant_id: &str,
        ttl_seconds: i64,
        now_unix_seconds: i64,
    ) -> (String, i64) {
        let expiry = now_unix_seconds.saturating_add(ttl_seconds);
        let raw = format!("{api_key}|{participant_id}|{expiry}");
        (encode_base62_bytes(raw.as_bytes()), expiry)
    }

    pub(crate) fn parse_username(
        &self,
        username: &str,
    ) -> Result<ParsedTurnUsername, TurnAuthError> {
        let decoded = decode_base62_to_bytes(username)?;
        let decoded = String::from_utf8(decoded).map_err(|_| TurnAuthError::InvalidUtf8)?;
        let parts: Vec<&str> = decoded.split('|').collect();
        if parts.len() != 3 {
            return Err(TurnAuthError::InvalidUsername);
        }

        let expiry = parts[2]
            .parse::<i64>()
            .map_err(|_| TurnAuthError::InvalidExpiry)?;
        if expiry == 0 {
            return Err(TurnAuthError::Expired);
        }

        Ok(ParsedTurnUsername {
            api_key: parts[0].to_string(),
            participant_id: parts[1].to_string(),
            expiry_unix_seconds: expiry,
        })
    }

    pub(crate) fn create_password_at(
        &self,
        api_key: &str,
        participant_id: &str,
        expiry_unix_seconds: i64,
        now_unix_seconds: i64,
    ) -> Result<String, TurnAuthError> {
        if expiry_unix_seconds == 0 || now_unix_seconds > expiry_unix_seconds {
            return Err(TurnAuthError::Expired);
        }
        self.compute_password(api_key, participant_id, expiry_unix_seconds)
    }

    pub(crate) fn password_for(
        &self,
        api_key: &str,
        participant_id: &str,
        expiry_unix_seconds: i64,
    ) -> Result<String, TurnAuthError> {
        self.compute_password(api_key, participant_id, expiry_unix_seconds)
    }

    fn compute_password(
        &self,
        api_key: &str,
        participant_id: &str,
        expiry_unix_seconds: i64,
    ) -> Result<String, TurnAuthError> {
        let Some(secret) = self.secrets_by_api_key.get(api_key) else {
            return Err(TurnAuthError::InvalidApiKey);
        };

        let key_input = format!("{secret}|{participant_id}|{expiry_unix_seconds}");
        let digest = Sha256::digest(key_input.as_bytes());
        Ok(encode_base62_bytes(&digest))
    }

    pub(crate) fn handle_auth_at(
        &self,
        request: TurnRequestAttributes<'_>,
        now_unix_seconds: i64,
    ) -> TurnAuthDecision {
        let parsed = match self.parse_username(request.username) {
            Ok(parsed) => parsed,
            Err(_) => return TurnAuthDecision::reject(),
        };

        if now_unix_seconds > parsed.expiry_unix_seconds && request.method == TurnMethod::Allocate {
            return TurnAuthDecision::reject();
        }

        let password = match self.compute_password(
            &parsed.api_key,
            &parsed.participant_id,
            parsed.expiry_unix_seconds,
        ) {
            Ok(password) => password,
            Err(_) => return TurnAuthDecision::reject(),
        };

        let auth_key = generate_turn_auth_key(request.username, LIVEKIT_REALM, &password);
        TurnAuthDecision {
            user_id: parsed.participant_id,
            key: auth_key,
            ok: true,
        }
    }

    pub(crate) fn now_unix_seconds() -> i64 {
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs() as i64)
    }
}

fn generate_turn_auth_key(username: &str, realm: &str, password: &str) -> Vec<u8> {
    let mut hasher = Md5::new();
    hasher.update(username.as_bytes());
    hasher.update(b":");
    hasher.update(realm.as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());
    hasher.finalize().to_vec()
}

fn encode_base62_bytes(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }

    let zero_count = data.iter().take_while(|&&byte| byte == 0).count();
    let mut input = data.to_vec();
    let mut encoded = Vec::new();
    let mut start = zero_count;

    while start < input.len() {
        let mut remainder: u32 = 0;
        for byte in input.iter_mut().skip(start) {
            let accumulator = (remainder << 8) + u32::from(*byte);
            *byte = (accumulator / 62) as u8;
            remainder = accumulator % 62;
        }
        encoded.push(BASE62_ALPHABET[remainder as usize]);
        while start < input.len() && input[start] == 0 {
            start += 1;
        }
    }

    encoded.extend(std::iter::repeat_n(BASE62_ALPHABET[0], zero_count));
    encoded.reverse();
    String::from_utf8(encoded).expect("base62 alphabet should always produce valid utf-8")
}

fn decode_base62_to_bytes(encoded: &str) -> Result<Vec<u8>, TurnAuthError> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }

    let encoded = encoded.as_bytes();
    let zero_count = encoded
        .iter()
        .take_while(|&&byte| byte == BASE62_ALPHABET[0])
        .count();

    let mut input = Vec::with_capacity(encoded.len());
    for &byte in encoded {
        let value = base62_value(byte).ok_or(TurnAuthError::InvalidBase62)?;
        input.push(value);
    }

    let mut decoded = Vec::new();
    let mut start = zero_count;

    while start < input.len() {
        let mut remainder: u32 = 0;
        for digit in input.iter_mut().skip(start) {
            let accumulator = remainder * 62 + u32::from(*digit);
            *digit = (accumulator / 256) as u8;
            remainder = accumulator % 256;
        }
        decoded.push(remainder as u8);
        while start < input.len() && input[start] == 0 {
            start += 1;
        }
    }

    decoded.extend(std::iter::repeat_n(0_u8, zero_count));
    decoded.reverse();
    Ok(decoded)
}

fn base62_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'Z' => Some(byte - b'A' + 10),
        b'a'..=b'z' => Some(byte - b'a' + 36),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
