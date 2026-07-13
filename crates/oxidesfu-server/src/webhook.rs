use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use axum::http::{Method, Request, header};
use oxidesfu_auth::Claims;
use oxidesfu_core::ServerConfig;
use sha2::Digest;

pub const WEBHOOK_CONTENT_TYPE: &str = "application/webhook+json";
const DEFAULT_WEBHOOK_TOKEN_TTL: Duration = Duration::from_secs(5 * 60);
static NEXT_WEBHOOK_EVENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDispatchTarget {
    pub url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookBuildError {
    #[error("webhook event JSON serialization failed")]
    Serialize(#[from] serde_json::Error),
    #[error("webhook JWT signing failed")]
    Sign(#[from] jsonwebtoken::errors::Error),
    #[error("invalid webhook destination URL")]
    InvalidUrl(#[from] axum::http::uri::InvalidUri),
    #[error("invalid webhook authorization header value")]
    InvalidAuthorizationHeader(#[from] axum::http::header::InvalidHeaderValue),
    #[error("failed building webhook request")]
    BuildRequest,
}

#[derive(Debug, Clone)]
pub struct WebhookSigner {
    api_key: String,
    api_secret: String,
    token_ttl: Duration,
}

impl WebhookSigner {
    pub fn from_server_config(config: &ServerConfig) -> Option<Self> {
        let webhook_api_key = config.webhook_api_key.clone()?;
        if config.webhook_urls.is_empty() {
            return None;
        }

        Some(Self {
            api_key: webhook_api_key,
            api_secret: config.api_secret.clone(),
            token_ttl: DEFAULT_WEBHOOK_TOKEN_TTL,
        })
    }

    fn sign_payload(&self, payload: &[u8]) -> Result<String, jsonwebtoken::errors::Error> {
        let payload_hash = {
            use base64::Engine;
            let digest = sha2::Sha256::digest(payload);
            base64::engine::general_purpose::STANDARD.encode(digest)
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as usize;

        let claims = Claims {
            iss: self.api_key.clone(),
            exp: now + self.token_ttl.as_secs() as usize,
            sha256: payload_hash,
            ..Default::default()
        };

        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(self.api_secret.as_bytes()),
        )
    }
}

pub fn webhook_dispatch_targets(config: &ServerConfig) -> Vec<WebhookDispatchTarget> {
    if WebhookSigner::from_server_config(config).is_none() {
        return Vec::new();
    }

    config
        .webhook_urls
        .iter()
        .cloned()
        .map(|url| WebhookDispatchTarget { url })
        .collect()
}

pub fn build_signed_webhook_request(
    target: &WebhookDispatchTarget,
    event: &livekit_protocol::WebhookEvent,
    signer: &WebhookSigner,
) -> Result<Request<Vec<u8>>, WebhookBuildError> {
    let body = serde_json::to_vec(event)?;
    let token = signer.sign_payload(&body)?;

    let request = Request::builder()
        .method(Method::POST)
        .uri(target.url.parse::<axum::http::Uri>()?)
        .header(header::CONTENT_TYPE, WEBHOOK_CONTENT_TYPE)
        .header(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&token)?,
        )
        .body(body)
        .map_err(|_| WebhookBuildError::BuildRequest)?;

    Ok(request)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookRetryPolicy {
    pub max_attempts: usize,
    pub retry_backoff: Duration,
}

impl Default for WebhookRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum WebhookSendError {
    #[error("transport error")]
    Transport,
}

#[async_trait]
pub trait WebhookHttpSender: Send + Sync {
    async fn send(&self, request: Request<Vec<u8>>) -> Result<u16, WebhookSendError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookDispatchOutcome {
    pub delivered: bool,
    pub attempts: usize,
}

pub async fn dispatch_webhook_event_with_retry(
    sender: &dyn WebhookHttpSender,
    target: &WebhookDispatchTarget,
    event: &livekit_protocol::WebhookEvent,
    signer: &WebhookSigner,
    retry: WebhookRetryPolicy,
) -> Result<WebhookDispatchOutcome, WebhookBuildError> {
    let max_attempts = retry.max_attempts.max(1);

    for attempt in 1..=max_attempts {
        let request = build_signed_webhook_request(target, event, signer)?;
        let response = sender.send(request).await;

        match response {
            Ok(status) if (200..=299).contains(&status) => {
                return Ok(WebhookDispatchOutcome {
                    delivered: true,
                    attempts: attempt,
                });
            }
            Ok(status) if status >= 500 => {
                if attempt < max_attempts {
                    tokio::time::sleep(retry.retry_backoff).await;
                    continue;
                }
                return Ok(WebhookDispatchOutcome {
                    delivered: false,
                    attempts: attempt,
                });
            }
            Ok(_) => {
                return Ok(WebhookDispatchOutcome {
                    delivered: false,
                    attempts: attempt,
                });
            }
            Err(_) => {
                if attempt < max_attempts {
                    tokio::time::sleep(retry.retry_backoff).await;
                    continue;
                }
                return Ok(WebhookDispatchOutcome {
                    delivered: false,
                    attempts: attempt,
                });
            }
        }
    }

    Ok(WebhookDispatchOutcome {
        delivered: false,
        attempts: max_attempts,
    })
}

struct QueuedWebhookEvent {
    event: livekit_protocol::WebhookEvent,
    completion: tokio::sync::oneshot::Sender<Result<WebhookDispatchOutcome, WebhookBuildError>>,
}

#[derive(Clone)]
pub struct WebhookUrlDispatcher {
    queue: tokio::sync::mpsc::Sender<QueuedWebhookEvent>,
}

impl WebhookUrlDispatcher {
    pub fn spawn(
        sender: Arc<dyn WebhookHttpSender>,
        target: WebhookDispatchTarget,
        signer: WebhookSigner,
        retry: WebhookRetryPolicy,
        queue_capacity: usize,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<QueuedWebhookEvent>(queue_capacity);
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let result = dispatch_webhook_event_with_retry(
                    sender.as_ref(),
                    &target,
                    &job.event,
                    &signer,
                    retry,
                )
                .await;
                let _ = job.completion.send(result);
            }
        });

        Self { queue: tx }
    }

    pub async fn dispatch(
        &self,
        event: livekit_protocol::WebhookEvent,
    ) -> Result<WebhookDispatchOutcome, WebhookBuildError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self
            .queue
            .send(QueuedWebhookEvent {
                event,
                completion: tx,
            })
            .await;
        rx.await.unwrap_or(Ok(WebhookDispatchOutcome {
            delivered: false,
            attempts: 0,
        }))
    }
}

#[derive(Clone)]
pub struct ReqwestWebhookSender {
    client: reqwest::Client,
}

impl Default for ReqwestWebhookSender {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl WebhookHttpSender for ReqwestWebhookSender {
    async fn send(&self, request: Request<Vec<u8>>) -> Result<u16, WebhookSendError> {
        let mut builder = self
            .client
            .request(reqwest::Method::POST, request.uri().to_string());
        for (name, value) in request.headers() {
            builder = builder.header(name, value);
        }
        let response = builder
            .body(request.into_body())
            .send()
            .await
            .map_err(|_| WebhookSendError::Transport)?;
        Ok(response.status().as_u16())
    }
}

#[derive(Clone)]
pub struct WebhookDispatcher {
    url_dispatchers: Vec<WebhookUrlDispatcher>,
}

impl WebhookDispatcher {
    pub fn from_server_config(config: &ServerConfig) -> Option<Self> {
        let signer = WebhookSigner::from_server_config(config)?;
        let targets = webhook_dispatch_targets(config);
        if targets.is_empty() {
            return None;
        }

        let sender: Arc<dyn WebhookHttpSender> = Arc::new(ReqwestWebhookSender::default());
        let retry_policy = WebhookRetryPolicy::default();
        let url_dispatchers = targets
            .into_iter()
            .map(|target| {
                WebhookUrlDispatcher::spawn(
                    sender.clone(),
                    target,
                    signer.clone(),
                    retry_policy,
                    256,
                )
            })
            .collect();

        Some(Self { url_dispatchers })
    }

    pub fn emit(&self, event: livekit_protocol::WebhookEvent) {
        for dispatcher in self.url_dispatchers.clone() {
            let event = event.clone();
            tokio::spawn(async move {
                let _ = dispatcher.dispatch(event).await;
            });
        }
    }

    pub fn signal_webhook_handler(
        &self,
    ) -> Arc<dyn Fn(livekit_protocol::WebhookEvent) + Send + Sync> {
        let dispatcher = self.clone();
        Arc::new(move |event| dispatcher.emit(event))
    }
}

pub fn room_finished_webhook_event(room: livekit_protocol::Room) -> livekit_protocol::WebhookEvent {
    livekit_protocol::WebhookEvent {
        event: "room_finished".to_string(),
        room: Some(room),
        id: format!(
            "EV_{:016x}",
            NEXT_WEBHOOK_EVENT_ID.fetch_add(1, Ordering::Relaxed)
        ),
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
            .unwrap_or_default(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
    };

    use super::*;

    use oxidesfu_auth::{ApiKeyStore, TokenVerifier};
    use sha2::Digest;

    #[derive(Clone, Default)]
    struct FakeWebhookSender {
        statuses_by_event: Arc<Mutex<HashMap<String, VecDeque<Result<u16, WebhookSendError>>>>>,
        attempt_log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl WebhookHttpSender for FakeWebhookSender {
        async fn send(&self, request: Request<Vec<u8>>) -> Result<u16, WebhookSendError> {
            let event: livekit_protocol::WebhookEvent =
                serde_json::from_slice(request.body()).expect("webhook body should decode");
            self.attempt_log
                .lock()
                .expect("attempt log lock should not be poisoned")
                .push(event.id.clone());

            let mut statuses = self
                .statuses_by_event
                .lock()
                .expect("statuses lock should not be poisoned");
            let queue = statuses
                .get_mut(&event.id)
                .expect("event id should have predefined statuses");
            queue
                .pop_front()
                .expect("event id should have an available response")
        }
    }

    fn configured_webhook_config() -> ServerConfig {
        let mut config = ServerConfig::development();
        config.api_key = "devkey".to_string();
        config.api_secret = "secret".to_string();
        config.webhook_api_key = Some("devkey".to_string());
        config.webhook_urls = vec!["https://hooks.example.test/events".to_string()];
        config
    }

    fn test_event() -> livekit_protocol::WebhookEvent {
        test_event_with_id("evt_test_1")
    }

    fn test_event_with_id(id: &str) -> livekit_protocol::WebhookEvent {
        livekit_protocol::WebhookEvent {
            event: "room_started".to_string(),
            id: id.to_string(),
            created_at: 1_720_000_000,
            room: Some(livekit_protocol::Room {
                sid: "RM_test".to_string(),
                name: "test-room".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn fake_sender_with_statuses(
        statuses_by_event: impl IntoIterator<Item = (&'static str, Vec<Result<u16, WebhookSendError>>)>,
    ) -> FakeWebhookSender {
        let mut map = HashMap::new();
        for (event_id, statuses) in statuses_by_event {
            map.insert(event_id.to_string(), statuses.into());
        }
        FakeWebhookSender {
            statuses_by_event: Arc::new(Mutex::new(map)),
            attempt_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[test]
    fn webhook_dispatcher_not_started_when_webhooks_disabled() {
        let config = ServerConfig::development();
        assert!(webhook_dispatch_targets(&config).is_empty());
    }

    #[test]
    fn webhook_dispatcher_started_when_configured() {
        let config = configured_webhook_config();
        let targets = webhook_dispatch_targets(&config);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].url, "https://hooks.example.test/events");
    }

    #[test]
    fn webhook_request_uses_post_method() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");

        let request =
            build_signed_webhook_request(&target, &test_event(), &signer).expect("request builds");

        assert_eq!(request.method(), Method::POST);
    }

    #[test]
    fn webhook_request_content_type_is_application_webhook_json() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");

        let request =
            build_signed_webhook_request(&target, &test_event(), &signer).expect("request builds");

        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .expect("content type header should exist");
        assert_eq!(content_type, WEBHOOK_CONTENT_TYPE);
    }

    #[test]
    fn webhook_body_is_valid_webhook_event_json() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let event = test_event();

        let request =
            build_signed_webhook_request(&target, &event, &signer).expect("request builds");
        let decoded: livekit_protocol::WebhookEvent =
            serde_json::from_slice(request.body()).expect("body should decode into WebhookEvent");

        assert_eq!(decoded.event, event.event);
        assert_eq!(decoded.id, event.id);
        assert_eq!(decoded.created_at, event.created_at);
    }

    #[test]
    fn webhook_authorization_header_is_signed_jwt_token() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");

        let request =
            build_signed_webhook_request(&target, &test_event(), &signer).expect("request builds");
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .expect("authorization header should exist");

        assert_eq!(token.split('.').count(), 3, "webhook auth should be JWT");
    }

    #[test]
    fn webhook_jwt_issuer_is_configured_webhook_api_key() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");

        let request =
            build_signed_webhook_request(&target, &test_event(), &signer).expect("request builds");
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .expect("authorization header should exist");

        let mut keys = ApiKeyStore::new();
        keys.insert("devkey", "secret");
        let verifier = TokenVerifier::new(keys);
        let verified = verifier.verify_token(token).expect("token should verify");
        assert_eq!(verified.claims.iss, "devkey");
    }

    #[test]
    fn webhook_jwt_contains_sha256_payload_hash() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");

        let request =
            build_signed_webhook_request(&target, &test_event(), &signer).expect("request builds");
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .expect("authorization header should exist");

        let mut keys = ApiKeyStore::new();
        keys.insert("devkey", "secret");
        let verifier = TokenVerifier::new(keys);
        let verified = verifier.verify_token(token).expect("token should verify");

        let expected_hash = {
            use base64::Engine;
            let digest = sha2::Sha256::digest(request.body());
            base64::engine::general_purpose::STANDARD.encode(digest)
        };

        assert_eq!(verified.claims.sha256, expected_hash);
    }

    #[test]
    fn webhook_signature_validation_fails_if_body_modified() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");

        let request =
            build_signed_webhook_request(&target, &test_event(), &signer).expect("request builds");
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .expect("authorization header should exist");

        let mut keys = ApiKeyStore::new();
        keys.insert("devkey", "secret");
        let verifier = TokenVerifier::new(keys);
        let verified = verifier.verify_token(token).expect("token should verify");

        let mut tampered = request.body().clone();
        tampered.push(b' ');
        let tampered_hash = {
            use base64::Engine;
            let digest = sha2::Sha256::digest(&tampered);
            base64::engine::general_purpose::STANDARD.encode(digest)
        };

        assert_ne!(verified.claims.sha256, tampered_hash);
    }

    #[tokio::test]
    async fn webhook_delivery_succeeds_on_2xx_response() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let sender = fake_sender_with_statuses([("evt_test_1", vec![Ok(204)])]);

        let outcome = dispatch_webhook_event_with_retry(
            &sender,
            &target,
            &test_event(),
            &signer,
            WebhookRetryPolicy {
                max_attempts: 3,
                retry_backoff: Duration::from_millis(0),
            },
        )
        .await
        .expect("dispatch should complete");

        assert!(outcome.delivered);
        assert_eq!(outcome.attempts, 1);
    }

    #[tokio::test]
    async fn webhook_delivery_retries_on_5xx_response() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let sender = fake_sender_with_statuses([("evt_test_1", vec![Ok(500), Ok(502), Ok(200)])]);

        let outcome = dispatch_webhook_event_with_retry(
            &sender,
            &target,
            &test_event(),
            &signer,
            WebhookRetryPolicy {
                max_attempts: 4,
                retry_backoff: Duration::from_millis(0),
            },
        )
        .await
        .expect("dispatch should complete");

        assert!(outcome.delivered);
        assert_eq!(outcome.attempts, 3);
    }

    #[tokio::test]
    async fn webhook_delivery_retries_on_connection_failure() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let sender = fake_sender_with_statuses([(
            "evt_test_1",
            vec![Err(WebhookSendError::Transport), Ok(200)],
        )]);

        let outcome = dispatch_webhook_event_with_retry(
            &sender,
            &target,
            &test_event(),
            &signer,
            WebhookRetryPolicy {
                max_attempts: 3,
                retry_backoff: Duration::from_millis(0),
            },
        )
        .await
        .expect("dispatch should complete");

        assert!(outcome.delivered);
        assert_eq!(outcome.attempts, 2);
    }

    #[tokio::test]
    async fn webhook_delivery_abandons_after_max_attempts() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let sender = fake_sender_with_statuses([("evt_test_1", vec![Ok(500), Ok(502), Ok(503)])]);

        let outcome = dispatch_webhook_event_with_retry(
            &sender,
            &target,
            &test_event(),
            &signer,
            WebhookRetryPolicy {
                max_attempts: 3,
                retry_backoff: Duration::from_millis(0),
            },
        )
        .await
        .expect("dispatch should complete");

        assert!(!outcome.delivered);
        assert_eq!(outcome.attempts, 3);
    }

    #[tokio::test]
    async fn webhook_queue_preserves_order_for_single_url() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let sender = fake_sender_with_statuses([
            ("evt_first", vec![Ok(200)]),
            ("evt_second", vec![Ok(200)]),
        ]);

        let dispatcher = WebhookUrlDispatcher::spawn(
            Arc::new(sender.clone()),
            target,
            signer,
            WebhookRetryPolicy {
                max_attempts: 2,
                retry_backoff: Duration::from_millis(0),
            },
            8,
        );

        let first = dispatcher.dispatch(test_event_with_id("evt_first"));
        let second = dispatcher.dispatch(test_event_with_id("evt_second"));

        let first_outcome = first.await.expect("first dispatch should complete");
        let second_outcome = second.await.expect("second dispatch should complete");
        assert!(first_outcome.delivered);
        assert!(second_outcome.delivered);

        let attempts = sender
            .attempt_log
            .lock()
            .expect("attempt log lock should not be poisoned")
            .clone();
        assert_eq!(attempts, vec!["evt_first", "evt_second"]);
    }

    #[tokio::test]
    async fn webhook_queue_delays_newer_event_until_older_delivered_or_abandoned() {
        let config = configured_webhook_config();
        let signer = WebhookSigner::from_server_config(&config).expect("signer should be enabled");
        let target = webhook_dispatch_targets(&config)
            .into_iter()
            .next()
            .expect("target should exist");
        let sender = fake_sender_with_statuses([
            ("evt_first", vec![Ok(500), Ok(500), Ok(200)]),
            ("evt_second", vec![Ok(200)]),
        ]);

        let dispatcher = WebhookUrlDispatcher::spawn(
            Arc::new(sender.clone()),
            target,
            signer,
            WebhookRetryPolicy {
                max_attempts: 4,
                retry_backoff: Duration::from_millis(0),
            },
            8,
        );

        let first = dispatcher.dispatch(test_event_with_id("evt_first"));
        let second = dispatcher.dispatch(test_event_with_id("evt_second"));

        let _ = first.await.expect("first dispatch should complete");
        let _ = second.await.expect("second dispatch should complete");

        let attempts = sender
            .attempt_log
            .lock()
            .expect("attempt log lock should not be poisoned")
            .clone();

        assert_eq!(
            attempts,
            vec!["evt_first", "evt_first", "evt_first", "evt_second"]
        );
    }

    #[test]
    fn room_finished_webhook_event_sets_required_fields() {
        let room = livekit_protocol::Room {
            sid: "RM_test".to_string(),
            name: "room-a".to_string(),
            ..Default::default()
        };

        let event = room_finished_webhook_event(room.clone());
        assert_eq!(event.event, "room_finished");
        assert_eq!(event.room, Some(room));
        assert!(!event.id.is_empty());
        assert!(event.created_at > 0);
    }
}
