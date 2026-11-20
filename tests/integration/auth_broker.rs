//! A standalone Pulsar broker that takes JSON Web Tokens, for the tests of
//! credentials that expire or are rejected. It checks every second whether a
//! connection's token expired, as a strict production broker would.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, LazyLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use pulsar::{
    authentication::Authentication, error::AuthenticationError, OperationRetryOptions, Pulsar,
    TokioExecutor,
};
use sha2::Sha256;
use tokio::sync::OnceCell;

use crate::cluster::{
    docker_logs, docker_ok, ensure_network, eventually, http, path, up, Container, TopicStats,
    BOOT_TIMEOUT, NETWORK,
};

const NAME: &str = "rprs-auth";
const SERVICE_PORT: usize = 6652;
const WEB_PORT: usize = 8082;
/// The key every token is signed with; exactly 32 bytes for HS256.
const SECRET: &[u8] = b"rprs-integration-test-secret-key";
const ADMIN: &str = "admin";
/// The superuser's token, which never expires.
static ADMIN_TOKEN: LazyLock<String> = LazyLock::new(|| token(SECRET, ADMIN, None));

pub struct AuthBroker;

static BROKER: OnceCell<AuthBroker> = OnceCell::const_new();

/// The JWT broker, started once per test run and reused across runs.
pub async fn auth_broker() -> &'static AuthBroker {
    BROKER.get_or_init(boot).await
}

/// A JSON Web Token for `subject`, signed with `key`, that expires after
/// `lifetime` if one is given.
fn token(key: &[u8], subject: &str, lifetime: Option<Duration>) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256"}"#);
    let claims = match lifetime {
        Some(lifetime) => {
            let expiry = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .saturating_add(lifetime)
                .as_secs();
            format!(r#"{{"sub":"{subject}","exp":{expiry}}}"#)
        }
        None => format!(r#"{{"sub":"{subject}"}}"#),
    };
    let signed = format!("{header}.{}", URL_SAFE_NO_PAD.encode(claims));
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(signed.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{signed}.{signature}")
}

async fn boot() -> AuthBroker {
    let _ = env_logger::builder().is_test(true).try_init();
    ensure_network().await;
    let exists = docker_ok(&["inspect", NAME]).await;
    let secret = format!(
        "data:;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(SECRET)
    );
    let admin = format!("token:{}", *ADMIN_TOKEN);
    let service_port = SERVICE_PORT.to_string();
    let web_port = WEB_PORT.to_string();
    up(
        exists,
        Container {
            name: NAME,
            network: NETWORK,
            ports: &[SERVICE_PORT, WEB_PORT],
            env: &[
                (
                    "PULSAR_MEM",
                    "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=256m",
                ),
                ("PULSAR_PREFIX_advertisedAddress", "127.0.0.1"),
                ("PULSAR_PREFIX_brokerServicePort", &service_port),
                ("PULSAR_PREFIX_webServicePort", &web_port),
                ("PULSAR_PREFIX_authenticationEnabled", "true"),
                (
                    "PULSAR_PREFIX_authenticationProviders",
                    "org.apache.pulsar.broker.authentication.AuthenticationProviderToken",
                ),
                ("PULSAR_PREFIX_tokenSecretKey", &secret),
                ("PULSAR_PREFIX_superUserRoles", ADMIN),
                ("PULSAR_PREFIX_authenticationRefreshCheckSeconds", "1"),
                (
                    "PULSAR_PREFIX_brokerClientAuthenticationPlugin",
                    "org.apache.pulsar.client.impl.auth.AuthenticationToken",
                ),
                ("PULSAR_PREFIX_brokerClientAuthenticationParameters", &admin),
            ],
            script: "bin/apply-config-from-env.py conf/standalone.conf \
                     && exec bin/pulsar standalone -nfw -nss",
        },
    )
    .await;
    let broker = AuthBroker;
    let ready = eventually(BOOT_TIMEOUT, || async {
        broker
            .get("namespaces/public")
            .await
            .is_ok_and(|body| body.contains("public/default"))
    })
    .await;
    assert!(
        ready,
        "{NAME} not ready within {BOOT_TIMEOUT:?}:\n{}",
        docker_logs(NAME).await
    );
    broker
}

impl AuthBroker {
    /// A client that authenticates with `tokens`, and gives up an operation
    /// after five seconds so a rejected credential shows soon.
    pub async fn client(&self, tokens: MintedTokens) -> Pulsar<TokioExecutor> {
        Pulsar::builder(format!("pulsar://127.0.0.1:{SERVICE_PORT}"), TokioExecutor)
            .with_auth_provider(Box::new(tokens))
            .with_operation_retry_options(OperationRetryOptions {
                operation_timeout: Duration::from_secs(5),
                ..Default::default()
            })
            .build()
            .await
            .expect("connect to the auth broker")
    }

    /// When the broker last saw the one consumer of `subscription` connect.
    pub async fn connected_since(&self, topic: &str, subscription: &str) -> Option<String> {
        let body = self.get(&format!("{}/stats", path(topic))).await.ok()?;
        let stats: TopicStats = serde_json::from_str(&body).ok()?;
        let consumers = &stats.subscriptions.get(subscription)?.consumers;
        Some(consumers.first()?.connected_since.clone())
    }

    /// The body of an admin API `GET` under `admin/v2/`.
    async fn get(&self, path: &str) -> Result<String, String> {
        let response = http()
            .get(format!("http://127.0.0.1:{WEB_PORT}/admin/v2/{path}"))
            .bearer_auth(&*ADMIN_TOKEN)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body = response.text().await.map_err(|e| e.to_string())?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(format!("{status}: {body}"))
        }
    }
}

/// Mints a token for each authentication the client makes, valid for
/// `lifetime`, or signed with the wrong key once made invalid.
#[derive(Clone)]
pub struct MintedTokens {
    lifetime: Duration,
    valid: Arc<AtomicBool>,
    minted: Arc<AtomicUsize>,
}

impl MintedTokens {
    pub fn new(lifetime: Duration) -> Self {
        MintedTokens {
            lifetime,
            valid: Arc::new(AtomicBool::new(true)),
            minted: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// How many tokens were handed out.
    pub fn minted(&self) -> usize {
        self.minted.load(Ordering::SeqCst)
    }

    pub fn set_valid(&self, valid: bool) {
        self.valid.store(valid, Ordering::SeqCst);
    }
}

#[async_trait]
impl Authentication for MintedTokens {
    fn auth_method_name(&self) -> String {
        "token".to_string()
    }

    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        Ok(())
    }

    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        self.minted.fetch_add(1, Ordering::SeqCst);
        let key: &[u8] = if self.valid.load(Ordering::SeqCst) {
            SECRET
        } else {
            b"not-the-key-the-broker-checks!!!"
        };
        Ok(token(key, "client", Some(self.lifetime)).into_bytes())
    }
}
