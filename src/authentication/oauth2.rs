//! OAuth2 Authenticator

use std::{
    error::Error as _,
    fmt::{Display, Formatter},
    fs,
    time::Instant,
};

use async_trait::async_trait;
use data_url::DataUrl;
use nom::lib::std::ops::Add;
use oauth2::{
    basic::{BasicClient, BasicTokenResponse},
    reqwest,
    AuthType::RequestBody,
    AuthUrl, ClientId, ClientSecret, RequestTokenError, Scope, TokenResponse, TokenUrl,
};
use openidconnect::{core::CoreProviderMetadata, IssuerUrl};
use serde::{de::DeserializeOwned, Deserialize};
use url::Url;

use crate::{authentication::Authentication, error::AuthenticationError, message::proto::Redacted};

/// A failure to parse JSON that holds a secret, the credentials or a token
/// answer, kept to its kind and position: serde_json's own message quotes the
/// value it rejected, which can be the secret.
#[derive(Debug)]
struct SecretJsonError {
    category: serde_json::error::Category,
    line: usize,
    column: usize,
}

impl From<serde_json::Error> for SecretJsonError {
    fn from(error: serde_json::Error) -> Self {
        SecretJsonError {
            category: error.classify(),
            line: error.line(),
            column: error.column(),
        }
    }
}

impl Display for SecretJsonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} error at line {} column {}",
            self.category, self.line, self.column
        )
    }
}

impl std::error::Error for SecretJsonError {}

fn parse_secret_json<T: DeserializeOwned>(json: &[u8]) -> Result<T, SecretJsonError> {
    Ok(serde_json::from_slice(json)?)
}

#[derive(Deserialize, Clone)]
struct OAuth2PrivateParams {
    client_id: String,
    client_secret: String,
    #[allow(dead_code)]
    client_email: Option<String>,
    issuer_url: Option<String>,
}

// Without the client secret. Names every field, so one added here does not
// compile until it is placed.
impl std::fmt::Debug for OAuth2PrivateParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            client_id,
            client_secret: _,
            client_email,
            issuer_url,
        } = self;
        f.debug_struct("OAuth2PrivateParams")
            .field("client_id", client_id)
            .field("client_secret", &Redacted)
            .field("client_email", client_email)
            .field("issuer_url", issuer_url)
            .finish()
    }
}

#[derive(Deserialize, Clone)]
pub struct OAuth2Params {
    pub issuer_url: String,
    pub credentials_url: String,
    pub audience: Option<String>,
    pub scope: Option<String>,
}

impl Display for OAuth2Params {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OAuth2Params({}, {}, {:?}, {:?})",
            self.issuer_url,
            self.credentials_hint(),
            self.audience,
            self.scope
        )
    }
}

// Shows the credentials URL as credentials_hint does: a data: URL holds the
// client secret.
// Names every field, so one added here does not compile until it is placed.
impl std::fmt::Debug for OAuth2Params {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Self {
            issuer_url,
            credentials_url: _,
            audience,
            scope,
        } = self;
        f.debug_struct("OAuth2Params")
            .field("issuer_url", issuer_url)
            .field("credentials_url", &self.credentials_hint())
            .field("audience", audience)
            .field("scope", scope)
            .finish()
    }
}

pub struct CachedToken {
    token_secret: Vec<u8>,
    expiring_at: Option<Instant>,
    expired_at: Option<Instant>,
}

impl From<BasicTokenResponse> for CachedToken {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(resp: BasicTokenResponse) -> Self {
        let now = Instant::now();
        CachedToken {
            expiring_at: resp.expires_in().map(|d| now.add(d.mul_f32(0.9))),
            expired_at: resp.expires_in().map(|d| now.add(d)),
            token_secret: resp.access_token().secret().clone().into_bytes(),
        }
    }
}

impl CachedToken {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn is_expiring(&self) -> bool {
        match &self.expiring_at {
            Some(expiring_at) => Instant::now().ge(expiring_at),
            None => false,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn is_expired(&self) -> bool {
        match &self.expired_at {
            Some(expired_at) => Instant::now().ge(expired_at),
            None => false,
        }
    }
}

pub struct OAuth2Authentication {
    params: OAuth2Params,
    private_params: Option<OAuth2PrivateParams>,
    token_url: Option<TokenUrl>,
    token: Option<CachedToken>,
}

impl OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn client_credentials(params: OAuth2Params) -> Box<dyn Authentication> {
        Box::new(OAuth2Authentication {
            params,
            private_params: None,
            token_url: None,
            token: None,
        })
    }
}

impl OAuth2Params {
    /// The credentials URL to show: a file URL whole, or only the scheme of
    /// any other URL, since a data: URL holds the client secret itself.
    fn credentials_hint(&self) -> String {
        match Url::parse(self.credentials_url.as_str()) {
            Ok(url) => match url.scheme() {
                "file" => url.into(),
                scheme => format!("{scheme}:<redacted>"),
            },
            Err(_) => String::from("<invalid credentials_url>"),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn read_private_params(&self) -> Result<OAuth2PrivateParams, Box<dyn std::error::Error>> {
        let credentials_url = Url::parse(self.credentials_url.as_str())?;
        match credentials_url.scheme() {
            "file" => {
                let path = credentials_url.to_file_path().map_err(|()| {
                    format!("credentials url [{credentials_url}] names no local file")
                })?;
                Ok(parse_secret_json(&fs::read(path)?)?)
            }
            "data" => {
                let data_url = match DataUrl::process(self.credentials_url.as_str()) {
                    Ok(data_url) => data_url,
                    Err(err) => return Err(Box::from(format!("invalid data url: {err}"))),
                };
                let body = match data_url.decode_to_vec() {
                    Ok((body, _)) => body,
                    Err(err) => return Err(Box::from(format!("invalid data url: {err}"))),
                };

                Ok(parse_secret_json(&body)?)
            }
            scheme => Err(Box::from(format!(
                "unsupported credentials url scheme [{scheme}]"
            ))),
        }
    }
}

#[async_trait]
impl Authentication for OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn auth_method_name(&self) -> String {
        String::from("token")
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        match self.params.read_private_params() {
            Ok(private_params) => self.private_params = Some(private_params),
            Err(e) => {
                return Err(AuthenticationError::Custom(format!(
                    "failed to read OAuth2 credentials from {}: {e}",
                    self.params.credentials_hint()
                )));
            }
        }
        if let Err(e) = self.token_url().await {
            return Err(AuthenticationError::Custom(format!(
                "failed to discover OAuth2 token endpoint for issuer [{}]: {e}",
                self.params.issuer_url
            )));
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        if self.private_params.is_none() {
            return Err(AuthenticationError::Custom("not initialized".to_string()));
        }
        let mut need_token = false;
        let mut none_or_expired = true;
        if let Some(token) = self.token.as_ref() {
            none_or_expired = token.is_expired();
            if none_or_expired || token.is_expiring() {
                need_token = true;
            }
        } else {
            need_token = true;
        }
        if need_token {
            match self.fetch_token().await {
                Ok(token) => {
                    self.token = Some(token.into());
                }
                Err(e) => {
                    if none_or_expired {
                        // invalidate the expired token
                        self.token = None;
                        if let Some(auth_err) = e.downcast_ref::<AuthenticationError>() {
                            return Err(auth_err.clone());
                        }
                        return Err(AuthenticationError::Custom(format!(
                            "failed to fetch OAuth2 access token for issuer [{}] (audience={:?}, scope={:?}): {e}",
                            self.params.issuer_url,
                            self.params.audience,
                            self.params.scope
                        )));
                    } else {
                        warn!(
                            "failed to refresh OAuth2 token for issuer [{}] (audience={:?}, scope={:?}): {e}. Using existing non-expired token",
                            self.params.issuer_url,
                            self.params.audience,
                            self.params.scope
                        );
                    }
                }
            }
        }
        Ok(self.token.as_ref().unwrap().token_secret.clone())
    }
}

/// The HTTP client of the discovery and token requests. It follows no
/// redirect: the token request carries the client secret in its body, which a
/// redirect would send on to its target, and the token endpoint is the one
/// the discovery document names, so that document has to come from the
/// issuer's own URL.
fn issuer_http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

impl OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn token_url(&mut self) -> Result<TokenUrl, Box<dyn std::error::Error>> {
        match &self.token_url {
            Some(url) => Ok(url.clone()),
            None => {
                let client = issuer_http_client()?;
                let metadata = CoreProviderMetadata::discover_async(
                    IssuerUrl::from_url(Url::parse(self.params.issuer_url.as_str())?),
                    &client,
                )
                .await?;
                if let Some(token_endpoint) = metadata.token_endpoint() {
                    self.token_url = Some(token_endpoint.clone());
                    Ok(token_endpoint.clone())
                } else {
                    Err(Box::from("token url not exists"))
                }
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn fetch_token(&mut self) -> Result<BasicTokenResponse, Box<dyn std::error::Error>> {
        let token_url = self.token_url().await.map_err(|e| {
            AuthenticationError::Retriable(format!(
                "failed to discover OAuth2 token endpoint for issuer [{}]: {e}",
                self.params.issuer_url
            ))
        })?;

        let private_params = self
            .private_params
            .as_ref()
            .expect("oauth2 provider is uninitialized");

        let issuer_url = if let Some(url) = private_params.issuer_url.as_ref() {
            url.as_str()
        } else {
            self.params.issuer_url.as_str()
        };

        let client = BasicClient::new(ClientId::new(private_params.client_id.clone()))
            .set_client_secret(ClientSecret::new(private_params.client_secret.clone()))
            .set_auth_uri(AuthUrl::from_url(Url::parse(issuer_url)?))
            .set_token_uri(token_url)
            .set_auth_type(RequestBody);

        let mut request = client.exchange_client_credentials();

        if let Some(audience) = &self.params.audience {
            request = request.add_extra_param("audience", audience.clone());
        }

        if let Some(scope) = &self.params.scope {
            request = request.add_scope(Scope::new(scope.clone()));
        }

        let client = issuer_http_client()?;
        let token = request.request_async(&client).await.map_err(|e| {
            let context = format!(
                "token endpoint request failed for issuer [{}] (audience={:?}, scope={:?})",
                self.params.issuer_url, self.params.audience, self.params.scope
            );
            // No arm prints the answer's body: on a 200 it holds the access token.
            match e {
                RequestTokenError::ServerResponse(response) => AuthenticationError::Custom(
                    format!("{context}: the server answered with an error: {response}"),
                ),
                RequestTokenError::Request(error) => {
                    let mut message = format!("{context}: the request failed: {error}");
                    let mut cause = error.source();
                    while let Some(source) = cause {
                        message.push_str(&format!(": {source}"));
                        cause = source.source();
                    }
                    AuthenticationError::Retriable(message)
                }
                RequestTokenError::Parse(error, _body) => AuthenticationError::Retriable(format!(
                    "{context}: the answer could not be parsed: {}",
                    SecretJsonError::from(error.into_inner())
                )),
                RequestTokenError::Other(message) => {
                    AuthenticationError::Retriable(format!("{context}: {message}"))
                }
            }
        })?;
        debug!("Got a new oauth2 token for [{}]", self.params);
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::mpsc};

    use oauth2::TokenUrl;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use url::Url;

    use crate::{
        authentication::{
            oauth2::{OAuth2Authentication, OAuth2Params, OAuth2PrivateParams},
            Authentication,
        },
        error::{AuthenticationError, ConnectionError},
    };

    #[test]
    fn parse_data_url() {
        let params = OAuth2Params {
            issuer_url: "".to_string(),
            credentials_url: "data:application/json;base64,eyJjbGllbnRfaWQiOiJjbGllbnQtaWQiLCJjbGllbnRfc2VjcmV0IjoiY2xpZW50LXNlY3JldCJ9Cg==".to_string(),
            audience: None,
            scope: None,
        };
        let private_params = params.read_private_params().unwrap();
        assert_eq!(private_params.client_id, "client-id");
        assert_eq!(private_params.client_secret, "client-secret");
        assert_eq!(private_params.client_email, None);
        assert_eq!(private_params.issuer_url, None);
    }

    #[tokio::test]
    async fn auth_data_returns_retriable_error() {
        let params = OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url: "data:application/json;base64,eyJjbGllbnRfaWQiOiJpZCIsImNsaWVudF9zZWNyZXQiOiJzZWNyZXQifQ==".to_string(),
            audience: None,
            scope: None,
        };

        let private_params = OAuth2PrivateParams {
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            client_email: None,
            issuer_url: None,
        };

        let assert_retriable_error = |err| match err {
            AuthenticationError::Retriable(_) => {
                let err: ConnectionError = err.into();
                assert!(err.establish_retryable());
            }
            other => panic!("expected retriable error, got {other:?}"),
        };

        let mut auth = OAuth2Authentication {
            params: params.clone(),
            private_params: Some(private_params.clone()),
            token_url: None, // token_url is not set to trigger discovery
            token: None,
        };
        assert_retriable_error(auth.auth_data().await.unwrap_err());

        let token_url = TokenUrl::new("http://127.0.0.1:1/token".to_string()).unwrap();

        let mut auth = OAuth2Authentication {
            params,
            private_params: Some(private_params),
            token_url: Some(token_url),
            token: None,
        };
        assert_retriable_error(auth.auth_data().await.unwrap_err());
    }

    #[test]
    fn reads_a_credentials_file_whose_path_needs_percent_encoding() {
        let dir = std::env::temp_dir().join(format!("oauth2 100% {}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let file = dir.join("credentials.json");
        std::fs::write(
            &file,
            r#"{"client_id":"client-id","client_secret":"client-secret"}"#,
        )
        .unwrap();
        let params = OAuth2Params {
            issuer_url: "".to_string(),
            credentials_url: Url::from_file_path(&file).unwrap().into(),
            audience: None,
            scope: None,
        };

        let read = params.read_private_params().map_err(|e| e.to_string());
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(read.unwrap().client_id, "client-id");
    }

    #[cfg(unix)]
    #[test]
    fn a_file_url_without_a_local_path_is_named_in_the_error() {
        let params = OAuth2Params {
            issuer_url: "".to_string(),
            credentials_url: "file://remote-host/credentials.json".to_string(),
            audience: None,
            scope: None,
        };

        let error = params.read_private_params().unwrap_err().to_string();

        assert!(
            error.contains("[file://remote-host/credentials.json]"),
            "{error}"
        );
    }

    /// Answers one HTTP request on a local port with `head`, a status line
    /// and any headers, followed by `body`.
    async fn serve_one_answer(head: &str, body: &str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let answer = format!(
            "{head}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let read = stream.read(&mut request).await.unwrap();
            assert_ne!(read, 0, "the client hung up without a request");
            stream.write_all(answer.as_bytes()).await.unwrap();
            // read on until the client hangs up, so no unread request bytes reset the connection
            while stream.read(&mut request).await.unwrap() > 0 {}
        });
        address
    }

    /// The error of a first token request to a token endpoint that answers
    /// with `head` and `body`.
    async fn token_request_error(head: &str, body: &str) -> AuthenticationError {
        let address = serve_one_answer(head, body).await;
        let mut auth = OAuth2Authentication {
            params: OAuth2Params {
                issuer_url: "http://issuer.example".to_string(),
                credentials_url: "data:,".to_string(),
                audience: None,
                scope: None,
            },
            private_params: Some(OAuth2PrivateParams {
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                client_email: None,
                issuer_url: None,
            }),
            token_url: Some(TokenUrl::new(format!("http://{address}/token")).unwrap()),
            token: None,
        };
        auth.auth_data().await.unwrap_err()
    }

    #[tokio::test]
    async fn a_token_endpoint_error_does_not_show_the_answer() {
        const MARKER: &str = "an-access-token-marker";
        // a 200 whose body is no token response: expires_in is not a number
        let body =
            format!(r#"{{"access_token":"{MARKER}","token_type":"bearer","expires_in":"soon"}}"#);

        let error =
            token_request_error("HTTP/1.1 200 OK\r\nContent-Type: application/json", &body).await;

        let shown = format!("{error} {error:?}");
        let bytes = format!("{:?}", MARKER.as_bytes());
        assert!(!shown.contains(MARKER), "{shown}");
        assert!(!shown.contains(bytes.trim_matches(['[', ']'])), "{shown}");
        assert!(shown.contains("could not be parsed"), "{shown}");
        assert!(
            matches!(error, AuthenticationError::Retriable(_)),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_token_endpoint_that_rejects_the_client_is_not_retried() {
        let error = token_request_error(
            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json",
            r#"{"error":"invalid_client"}"#,
        )
        .await;

        assert!(matches!(error, AuthenticationError::Custom(_)), "{error:?}");
        assert!(error.to_string().contains("invalid_client"), "{error}");
    }

    #[tokio::test]
    async fn a_token_endpoint_error_without_a_body_is_retried() {
        let error = token_request_error("HTTP/1.1 503 Service Unavailable", "").await;

        assert!(
            matches!(error, AuthenticationError::Retriable(_)),
            "{error:?}"
        );
    }

    #[test]
    fn no_rendering_of_the_params_shows_inline_credentials() {
        let params = OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url:
                r#"data:application/json,{"client_id":"id","client_secret":"a-client-secret"}"#
                    .to_string(),
            audience: None,
            scope: None,
        };

        let shown = format!("{params} {params:?}");

        assert!(!shown.contains("a-client-secret"), "{shown}");
        assert!(shown.contains("http://issuer.example"), "{shown}");
    }

    #[tokio::test]
    async fn an_invalid_data_url_error_does_not_show_its_content() {
        let mut auth = OAuth2Authentication::client_credentials(OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url: "data:application/json;base64,a-client-secret".to_string(),
            audience: None,
            scope: None,
        });

        let error = auth.initialize().await.unwrap_err();

        let shown = format!("{error} {error:?}");
        assert!(!shown.contains("a-client-secret"), "{shown}");
        assert!(shown.contains("invalid data url"), "{shown}");
    }

    #[test]
    fn debug_of_the_private_params_does_not_show_the_secret() {
        let private_params = OAuth2PrivateParams {
            client_id: "client-id".to_string(),
            client_secret: "a-client-secret".to_string(),
            client_email: None,
            issuer_url: None,
        };

        let shown = format!("{private_params:?}");

        assert!(!shown.contains("a-client-secret"), "{shown}");
        assert!(shown.contains("client_secret: <redacted>"), "{shown}");
        assert!(shown.contains("client_id: \"client-id\""), "{shown}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_url_with_a_host_is_shown_whole() {
        let params = OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url: "file://remote-host/credentials.json".to_string(),
            audience: None,
            scope: None,
        };
        let mut auth = OAuth2Authentication::client_credentials(params.clone());

        let error = auth.initialize().await.unwrap_err();

        for shown in [params.to_string(), format!("{params:?}"), error.to_string()] {
            assert!(
                shown.contains("file://remote-host/credentials.json"),
                "{shown}"
            );
            assert!(!shown.contains("file:/credentials.json"), "{shown}");
        }
    }

    /// The error of initialize() with a credentials file that holds `json`.
    async fn initialize_error_for_a_credentials_file(json: &str) -> AuthenticationError {
        let dir = std::env::temp_dir().join(format!("oauth2-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let file = dir.join("credentials.json");
        std::fs::write(&file, json).unwrap();
        let mut auth = OAuth2Authentication::client_credentials(OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url: Url::from_file_path(&file).unwrap().into(),
            audience: None,
            scope: None,
        });

        let error = auth.initialize().await.unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        error
    }

    #[tokio::test]
    async fn a_numeric_client_secret_is_not_shown_in_the_parse_error() {
        let error = initialize_error_for_a_credentials_file(
            r#"{"client_id":"id","client_secret":83749201}"#,
        )
        .await;

        let shown = format!("{error} {error:?}");
        assert!(!shown.contains("83749201"), "{shown}");
        assert!(!shown.contains("invalid type"), "{shown}");
        assert!(shown.contains("line 1 column"), "{shown}");
    }

    #[tokio::test]
    async fn a_boolean_client_secret_is_not_shown_in_the_parse_error() {
        let error =
            initialize_error_for_a_credentials_file(r#"{"client_id":"id","client_secret":true}"#)
                .await;

        let shown = format!("{error} {error:?}");
        assert!(!shown.contains("`true`"), "{shown}");
        assert!(!shown.contains("boolean"), "{shown}");
        assert!(!shown.contains("invalid type"), "{shown}");
        assert!(shown.contains("line 1 column"), "{shown}");
    }

    /// A local listener that records each request it receives and answers
    /// it with a 404.
    async fn recording_listener() -> (SocketAddr, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, received) = mpsc::channel();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 4096];
                let read = stream.read(&mut request).await.unwrap();
                sender
                    .send(String::from_utf8_lossy(&request[..read]).into_owned())
                    .unwrap();
                stream
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        });
        (address, received)
    }

    #[tokio::test]
    async fn the_token_request_follows_no_redirect() {
        let (target, received) = recording_listener().await;
        let token_endpoint = serve_one_answer(
            &format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target}/token"),
            "",
        )
        .await;
        let mut auth = OAuth2Authentication {
            params: OAuth2Params {
                issuer_url: "http://issuer.example".to_string(),
                credentials_url: "data:,".to_string(),
                audience: None,
                scope: None,
            },
            private_params: Some(OAuth2PrivateParams {
                client_id: "id".to_string(),
                client_secret: "a-client-secret".to_string(),
                client_email: None,
                issuer_url: None,
            }),
            token_url: Some(TokenUrl::new(format!("http://{token_endpoint}/token")).unwrap()),
            token: None,
        };

        auth.auth_data().await.unwrap_err();

        let resent = received.try_recv();
        assert!(resent.is_err(), "the redirect target received {resent:?}");
    }

    #[tokio::test]
    async fn discovery_follows_no_redirect() {
        let (target, received) = recording_listener().await;
        let issuer = serve_one_answer(
            &format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target}/.well-known/openid-configuration"
            ),
            "",
        )
        .await;
        let mut auth = OAuth2Authentication::client_credentials(OAuth2Params {
            issuer_url: format!("http://{issuer}"),
            credentials_url:
                r#"data:application/json,{"client_id":"id","client_secret":"a-client-secret"}"#
                    .to_string(),
            audience: None,
            scope: None,
        });

        auth.initialize().await.unwrap_err();

        let followed = received.try_recv();
        assert!(
            followed.is_err(),
            "the redirect target received {followed:?}"
        );
    }
}
