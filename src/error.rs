//! Error types
use std::{fmt, io, sync::Arc};

use crate::{message::proto::ServerError, producer::SendFuture};

#[derive(Debug)]
pub enum Error {
    Connection(ConnectionError),
    Consumer(ConsumerError),
    Producer(ProducerError),
    ServiceDiscovery(ServiceDiscoveryError),
    Authentication(AuthenticationError),
    Custom(String),
    Executor,
    #[cfg(feature = "admin-api")]
    Admin(AdminError),
}

impl From<ConnectionError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        Error::Connection(err)
    }
}

impl From<ConsumerError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConsumerError) -> Self {
        Error::Consumer(err)
    }
}

impl From<ProducerError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ProducerError) -> Self {
        Error::Producer(err)
    }
}

impl From<ServiceDiscoveryError> for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ServiceDiscoveryError) -> Self {
        Error::ServiceDiscovery(err)
    }
}

/// What an operation is doing when it fails, which decides what is retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Creating a consumer or producer, or looking a topic up for that.
    Create,
    /// Getting back what was created: after a reconnect, a seek, a re-creation.
    Reconnect,
}

/// Whether an operation the broker answered with `kind` may succeed if tried
/// again soon. A busy consumer or producer, and a few more answers, are worth
/// waiting out only for what was created already: at creation the other one
/// holds it on purpose.
// Java: PulsarClientException.isRetriableError, which a reconnect ignores
// once subscribeFuture is done
fn retriable_answer(kind: ServerError, phase: Phase) -> bool {
    let ends = match phase {
        Phase::Create => is_permanent(kind),
        Phase::Reconnect => ends_after_creation(kind),
    };
    !ends
        && (phase == Phase::Reconnect
            || !matches!(
                kind,
                ServerError::ConsumerBusy
                    | ServerError::ProducerBusy
                    | ServerError::SubscriptionNotFound
                    | ServerError::UnsupportedVersionError
                    | ServerError::ChecksumError
                    | ServerError::ConsumerAssignError
                    | ServerError::TransactionConflict
            ))
}

/// Broker errors that no retry can fix when creating a consumer or producer
/// (see [`ends_after_creation`] for after). An
/// authentication failure is not one, as in Java's isRetriableError: the
/// auth provider may hand out a valid credential on the next attempt.
pub(crate) fn is_permanent(kind: ServerError) -> bool {
    ends_after_creation(kind)
        || matches!(
            kind,
            ServerError::AuthorizationError
                | ServerError::IncompatibleSchema
                | ServerError::NotAllowedError
                | ServerError::InvalidTopicName
        )
}

/// Broker errors that end a consumer or producer that already exists. Java
/// retries everything else once a consumer subscribed: an authorization or
/// policy refusal can lift, and a broker answers AuthorizationError whenever
/// its authorization check fails, a metadata timeout say.
// Java: ConsumerImpl.connectionOpened (TopicDoesNotExistException) and
// ProducerImpl.connectionOpened (ProducerFenced, TopicTerminated)
pub(crate) fn ends_after_creation(kind: ServerError) -> bool {
    matches!(
        kind,
        ServerError::TopicNotFound
            | ServerError::TopicTerminatedError
            | ServerError::ProducerFenced
    )
}

impl Error {
    /// The error alone, without the topic a consumer tags it with.
    pub fn without_topic(&self) -> &Error {
        match self {
            Error::Consumer(ConsumerError::Topic { error, .. }) => error.without_topic(),
            other => other,
        }
    }

    /// The error the broker answered with, if this error carries one.
    pub fn server_error(&self) -> Option<ServerError> {
        match self {
            Error::ServiceDiscovery(ServiceDiscoveryError::Query(kind, _)) => *kind,
            _ => self.connection_error()?.server_error(),
        }
    }

    /// The connection error inside, through every layer that wraps one.
    fn connection_error(&self) -> Option<&ConnectionError> {
        match self {
            Error::Connection(e)
            | Error::Consumer(ConsumerError::Connection(e))
            | Error::Producer(ProducerError::Connection(e))
            | Error::ServiceDiscovery(ServiceDiscoveryError::Connection(e)) => Some(e),
            Error::Consumer(ConsumerError::Topic { error, .. }) => error.connection_error(),
            Error::Producer(ProducerError::Batch(error)) => error.connection_error(),
            _ => None,
        }
    }

    /// Whether an operation that failed with this may succeed if tried again
    /// soon.
    pub(crate) fn may_retry(&self, phase: Phase) -> bool {
        match self {
            Error::ServiceDiscovery(e) => e.may_retry(phase),
            _ => self.connection_error().is_some_and(|e| e.may_retry(phase)),
        }
    }

    /// Whether a consumer or producer that already exists ends on this error:
    /// the broker says its topic is deleted or terminated, or fenced the
    /// producer. A consumer hands the error to the application and ends that
    /// topic or partition for good, [`ConsumerError::Topic`] naming it; a
    /// consumer or reader of several reads the others on.
    pub fn is_final(&self) -> bool {
        self.server_error().is_some_and(ends_after_creation)
    }

    /// Whether someone has to act for the client to get through: a credential
    /// that the broker refused, or that the auth provider failed to make, or
    /// a refusal by authorization or policy.
    pub(crate) fn needs_attention(&self) -> bool {
        self.connection_error()
            .is_some_and(ConnectionError::refuses_credential)
            || self.server_error().is_some_and(is_permanent)
    }
}

impl fmt::Display for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Connection(e) => write!(f, "Connection error: {e}"),
            Error::Consumer(e) => write!(f, "consumer error: {e}"),
            Error::Producer(e) => write!(f, "producer error: {e}"),
            Error::ServiceDiscovery(e) => write!(f, "service discovery error: {e}"),
            Error::Authentication(e) => write!(f, "authentication error: {e}"),
            Error::Custom(e) => write!(f, "error: {e}"),
            Error::Executor => write!(f, "could not spawn task"),
            #[cfg(feature = "admin-api")]
            Error::Admin(e) => write!(f, "admin error: {e}"),
        }
    }
}

impl std::error::Error for Error {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Connection(e) => e.source(),
            Error::Consumer(e) => e.source(),
            Error::Producer(e) => e.source(),
            Error::ServiceDiscovery(e) => e.source(),
            Error::Authentication(e) => e.source(),
            Error::Custom(_) => None,
            Error::Executor => None,
            #[cfg(feature = "admin-api")]
            Error::Admin(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    SlowDown,
    Disconnected,
    PulsarError(Option<crate::message::proto::ServerError>, Option<String>),
    Unexpected(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    #[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
    Tls(native_tls::Error),
    #[cfg(all(
        any(
            feature = "tokio-rustls-runtime-aws-lc-rs",
            feature = "tokio-rustls-runtime-ring",
            feature = "async-std-rustls-runtime-aws-lc-rs",
            feature = "async-std-rustls-runtime-ring",
        ),
        not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
    ))]
    Tls(rustls::Error),
    #[cfg(any(
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring",
    ))]
    DnsName(rustls::pki_types::InvalidDnsNameError),
    Authentication(AuthenticationError),
    NotFound,
    Canceled,
    Shutdown,
}

impl ConnectionError {
    /// The error the broker answered with, also behind a timeout it caused.
    pub fn server_error(&self) -> Option<ServerError> {
        match self {
            ConnectionError::PulsarError(kind, _) => *kind,
            ConnectionError::Io(e) => e
                .get_ref()?
                .downcast_ref::<TimedOutBecause>()?
                .cause
                .server_error(),
            _ => None,
        }
    }

    /// Whether an operation that failed with this may succeed if tried again
    /// soon: a passing broker answer, or any trouble on the network. Java
    /// retries every network error; what no retry fixes is a broken
    /// configuration or protocol, and a client that is shutting down.
    pub(crate) fn may_retry(&self, phase: Phase) -> bool {
        match self {
            ConnectionError::PulsarError(kind, _) => {
                kind.is_some_and(|kind| retriable_answer(kind, phase))
            }
            ConnectionError::Unexpected(_)
            | ConnectionError::Decoding(_)
            | ConnectionError::Encoding(_)
            | ConnectionError::UnexpectedResponse(_)
            | ConnectionError::Tls(_)
            | ConnectionError::NotFound
            | ConnectionError::Shutdown => false,
            #[allow(unreachable_patterns)]
            _ => !self.is_configuration_error(),
        }
    }

    /// Errors in how the client is set up, which no retry fixes.
    fn is_configuration_error(&self) -> bool {
        match self {
            ConnectionError::Io(e) => matches!(
                e.kind(),
                io::ErrorKind::InvalidInput
                    | io::ErrorKind::InvalidData
                    | io::ErrorKind::PermissionDenied
                    | io::ErrorKind::Unsupported
            ),
            #[cfg(all(
                any(
                    feature = "tokio-rustls-runtime-aws-lc-rs",
                    feature = "tokio-rustls-runtime-ring",
                    feature = "async-std-rustls-runtime-aws-lc-rs",
                    feature = "async-std-rustls-runtime-ring",
                ),
                not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
            ))]
            ConnectionError::DnsName(_) => true,
            _ => false,
        }
    }

    /// Whether the broker refused the credential, or the auth provider failed
    /// to make one; a passing provider failure is not it.
    pub(crate) fn refuses_credential(&self) -> bool {
        matches!(
            self,
            ConnectionError::PulsarError(Some(ServerError::AuthenticationError), _)
                | ConnectionError::Authentication(AuthenticationError::Custom(_))
        )
    }

    /// The same error again: for each task that waited on the connect that
    /// failed with it, and for each reader of a broken connection's error.
    pub(crate) fn duplicate(&self) -> ConnectionError {
        match self {
            ConnectionError::PulsarError(kind, message) => {
                ConnectionError::PulsarError(*kind, message.clone())
            }
            ConnectionError::Authentication(e) => ConnectionError::Authentication(e.clone()),
            ConnectionError::Io(e) => ConnectionError::Io(
                match e
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<TimedOutBecause>())
                {
                    Some(timeout) => io::Error::new(
                        e.kind(),
                        TimedOutBecause {
                            message: timeout.message,
                            cause: timeout.cause.clone(),
                        },
                    ),
                    None => io::Error::new(e.kind(), e.to_string()),
                },
            ),
            ConnectionError::Disconnected => ConnectionError::Disconnected,
            ConnectionError::SlowDown => ConnectionError::SlowDown,
            ConnectionError::NotFound => ConnectionError::NotFound,
            ConnectionError::Canceled => ConnectionError::Canceled,
            ConnectionError::Shutdown => ConnectionError::Shutdown,
            other => ConnectionError::Unexpected(other.to_string()),
        }
    }

    pub fn establish_retryable(&self) -> bool {
        match self {
            ConnectionError::Io(e) => {
                e.kind() == io::ErrorKind::ConnectionRefused || e.kind() == io::ErrorKind::TimedOut
            }
            ConnectionError::Authentication(AuthenticationError::Retriable(_)) => true,
            _ => false,
        }
    }
}

/// A timeout with what caused it, which `server_error` reads through.
#[derive(Debug)]
pub(crate) struct TimedOutBecause {
    pub(crate) message: &'static str,
    pub(crate) cause: std::sync::Arc<Error>,
}

impl fmt::Display for TimedOutBecause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.message, self.cause)
    }
}

impl std::error::Error for TimedOutBecause {}

impl From<io::Error> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

#[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
impl From<native_tls::Error> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: native_tls::Error) -> Self {
        ConnectionError::Tls(err)
    }
}

#[cfg(all(
    any(
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring",
    ),
    not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
))]
impl From<rustls::Error> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: rustls::Error) -> Self {
        ConnectionError::Tls(err)
    }
}

#[cfg(any(
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring",
    feature = "async-std-rustls-runtime-aws-lc-rs",
    feature = "async-std-rustls-runtime-ring",
))]
impl From<rustls::pki_types::InvalidDnsNameError> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: rustls::pki_types::InvalidDnsNameError) -> Self {
        ConnectionError::DnsName(err)
    }
}

impl From<AuthenticationError> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: AuthenticationError) -> Self {
        ConnectionError::Authentication(err)
    }
}

impl<T> From<async_channel::SendError<T>> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(_err: async_channel::SendError<T>) -> Self {
        ConnectionError::Disconnected
    }
}

impl<T> From<async_channel::TrySendError<T>> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: async_channel::TrySendError<T>) -> Self {
        match err {
            async_channel::TrySendError::Full(_) => ConnectionError::SlowDown,
            async_channel::TrySendError::Closed(_) => ConnectionError::Disconnected,
        }
    }
}

impl fmt::Display for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "{e}"),
            ConnectionError::SlowDown => write!(f, "SlowDown"),
            ConnectionError::Disconnected => write!(f, "Disconnected"),
            ConnectionError::PulsarError(e, s) => {
                write!(f, "Server error ({:?}): {}", e, s.as_deref().unwrap_or(""))
            }
            ConnectionError::Unexpected(e) => write!(f, "{e}"),
            ConnectionError::Decoding(e) => write!(f, "Error decoding message: {e}"),
            ConnectionError::Encoding(e) => write!(f, "Error encoding message: {e}"),
            ConnectionError::SocketAddr(e) => write!(f, "Error obtaining socket address: {e}"),
            ConnectionError::Tls(e) => write!(f, "Error connecting TLS stream: {e}"),
            #[cfg(any(
                feature = "tokio-rustls-runtime-aws-lc-rs",
                feature = "tokio-rustls-runtime-ring",
                feature = "async-std-rustls-runtime-aws-lc-rs",
                feature = "async-std-rustls-runtime-ring",
            ))]
            ConnectionError::DnsName(e) => write!(f, "Error resolving hostname: {e}"),
            ConnectionError::Authentication(e) => write!(f, "Authentication error: {e}"),
            ConnectionError::UnexpectedResponse(e) => {
                write!(f, "Unexpected response from pulsar: {e}")
            }
            ConnectionError::NotFound => write!(f, "error looking up URL"),
            ConnectionError::Canceled => write!(f, "canceled request"),
            ConnectionError::Shutdown => write!(f, "The connection was shut down"),
        }
    }
}

impl std::error::Error for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectionError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ConsumerError {
    Connection(ConnectionError),
    MissingPayload(String),
    Io(io::Error),
    ChannelFull,
    Closed,
    BuildError,
    /// An error a consumer hands the application, with the topic or
    /// partition it came from. [`Error::without_topic`] gives the error alone.
    Topic {
        topic: String,
        error: Box<Error>,
    },
}

impl From<ConnectionError> for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        ConsumerError::Connection(err)
    }
}

impl From<io::Error> for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: io::Error) -> Self {
        ConsumerError::Io(err)
    }
}

impl From<futures::channel::mpsc::SendError> for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: futures::channel::mpsc::SendError) -> Self {
        if err.is_full() {
            ConsumerError::ChannelFull
        } else {
            ConsumerError::Closed
        }
    }
}

impl fmt::Display for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConsumerError::Connection(e) => write!(f, "Connection error: {e}"),
            ConsumerError::MissingPayload(s) => write!(f, "Missing payload: {s}"),
            ConsumerError::Io(s) => write!(f, "Decompression error: {s}"),
            ConsumerError::ChannelFull => write!(
                f,
                "cannot send message to the consumer engine: the channel is full"
            ),
            ConsumerError::Closed => write!(
                f,
                "cannot send message to the consumer engine: the channel is closed"
            ),
            ConsumerError::BuildError => write!(f, "Error while building the consumer."),
            // the inner error's own "consumer error" prefix would repeat
            ConsumerError::Topic { topic, error } => match error.as_ref() {
                Error::Consumer(inner) => write!(f, "{topic}: {inner}"),
                other => write!(f, "{topic}: {other}"),
            },
        }
    }
}

impl std::error::Error for ConsumerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsumerError::Connection(e) => Some(e),
            _ => None,
        }
    }
}

pub enum ProducerError {
    Connection(ConnectionError),
    Custom(String),
    Io(io::Error),
    PartialSend(Vec<Result<SendFuture, Error>>),
    /// Indiciates the error was part of sending a batch, and thus shared across the batch
    Batch(Arc<Error>),
    /// Indicates this producer has lost exclusive access to the topic. Client can decided whether
    /// to recreate or not
    Fenced,
    /// Indicates the producer is closed or dropped
    Closed,
}

impl From<ConnectionError> for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        ProducerError::Connection(err)
    }
}

impl From<io::Error> for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: io::Error) -> Self {
        ProducerError::Io(err)
    }
}

impl fmt::Display for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProducerError::Connection(e) => write!(f, "Connection error: {e}"),
            ProducerError::Io(e) => write!(f, "Compression error: {e}"),
            ProducerError::Custom(s) => write!(f, "Custom error: {s}"),
            ProducerError::Batch(e) => write!(f, "Batch error: {e}"),
            ProducerError::PartialSend(e) => {
                let (successes, failures) = e.iter().fold((0, 0), |(s, f), r| match r {
                    Ok(_) => (s + 1, f),
                    Err(_) => (s, f + 1),
                });
                write!(
                    f,
                    "Partial send error - {successes} successful, {failures} failed"
                )?;

                if failures > 0 {
                    let first_error = e
                        .iter()
                        .find(|r| r.is_err())
                        .unwrap()
                        .as_ref()
                        .map(drop)
                        .unwrap_err();
                    write!(f, "first error: {first_error}")?;
                }
                Ok(())
            }
            ProducerError::Fenced => write!(f, "Producer is fenced"),
            ProducerError::Closed => write!(f, "Producer is closed or dropped"),
        }
    }
}

impl fmt::Debug for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProducerError::Connection(e) => write!(f, "Connection({e:?})"),
            ProducerError::Custom(msg) => write!(f, "Custom({msg:?})"),
            ProducerError::Io(e) => write!(f, "Connection({e:?})"),
            ProducerError::Batch(e) => write!(f, "Connection({e:?})"),
            ProducerError::PartialSend(parts) => {
                write!(f, "PartialSend(")?;
                for (i, part) in parts.iter().enumerate() {
                    match part {
                        Ok(_) => write!(f, "Ok(SendFuture)")?,
                        Err(e) => write!(f, "Err({e:?})")?,
                    }
                    if i < (parts.len() - 1) {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            ProducerError::Fenced => write!(f, "Producer is fenced"),
            ProducerError::Closed => write!(f, "Producer is closed or dropped"),
        }
    }
}

impl std::error::Error for ProducerError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProducerError::Connection(e) => Some(e),
            ProducerError::Io(e) => Some(e),
            ProducerError::Batch(e) => Some(e.as_ref()),
            ProducerError::PartialSend(parts) => parts
                .iter()
                .find(|r| r.is_err())
                .map(|r| r.as_ref().map(drop).unwrap_err() as _),
            ProducerError::Custom(_) => None,
            ProducerError::Fenced => None,
            ProducerError::Closed => None,
        }
    }
}

#[derive(Debug)]
pub enum ServiceDiscoveryError {
    Connection(ConnectionError),
    Query(Option<crate::message::proto::ServerError>, Option<String>),
    NotFound,
    DnsLookupError,
    Canceled,
    Shutdown,
    Dummy,
}

impl ServiceDiscoveryError {
    /// Whether a lookup that failed with this may succeed if tried again soon.
    pub(crate) fn may_retry(&self, phase: Phase) -> bool {
        match self {
            ServiceDiscoveryError::Query(kind, _) => {
                kind.is_some_and(|kind| retriable_answer(kind, phase))
            }
            ServiceDiscoveryError::Connection(e) => e.may_retry(phase),
            _ => false,
        }
    }
}

impl From<ConnectionError> for ServiceDiscoveryError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(err: ConnectionError) -> Self {
        ServiceDiscoveryError::Connection(err)
    }
}

impl fmt::Display for ServiceDiscoveryError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServiceDiscoveryError::Connection(e) => write!(f, "Connection error: {e}"),
            ServiceDiscoveryError::Query(e, s) => {
                write!(f, "Query error ({:?}): {}", e, s.as_deref().unwrap_or(""))
            }
            ServiceDiscoveryError::NotFound => write!(f, "cannot find topic"),
            ServiceDiscoveryError::DnsLookupError => write!(f, "cannot lookup broker address"),
            ServiceDiscoveryError::Canceled => write!(f, "canceled request"),
            ServiceDiscoveryError::Shutdown => write!(f, "service discovery engine not responding"),
            ServiceDiscoveryError::Dummy => write!(f, "placeholder error"),
        }
    }
}

impl std::error::Error for ServiceDiscoveryError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServiceDiscoveryError::Connection(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AuthenticationError {
    Custom(String),
    Retriable(String),
}

impl fmt::Display for AuthenticationError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthenticationError::Custom(m) => write!(f, "{m}"),
            AuthenticationError::Retriable(m) => write!(f, "{m} (retriable)"),
        }
    }
}

impl std::error::Error for AuthenticationError {}

#[cfg(feature = "admin-api")]
#[derive(Debug)]
pub enum AdminError {
    /// The HTTP request to the Pulsar admin API failed
    Request(reqwest::Error),
    /// The Pulsar admin API returned a non-2xx HTTP status
    Http { status: u16, body: String },
    /// The Pulsar admin API returned schema JSON this client could not parse
    SchemaDecode(String),
    /// The Pulsar admin API returned an unknown schema type
    InvalidSchemaType(String),
    /// The topic string could not be parsed
    InvalidTopic(String),
    /// TLS configuration failed (e.g. certificate chain could not be parsed)
    TlsConfig(String),
}

#[cfg(feature = "admin-api")]
impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AdminError::Request(e) => write!(f, "HTTP request failed: {e}"),
            AdminError::Http { status, body } => {
                write!(f, "admin API returned HTTP {status}: {body}")
            }
            AdminError::SchemaDecode(msg) => write!(f, "failed to decode schema response: {msg}"),
            AdminError::InvalidSchemaType(schema_type) => {
                write!(
                    f,
                    "invalid schema type returned by admin API: {schema_type}"
                )
            }
            AdminError::InvalidTopic(t) => write!(f, "invalid topic URL: {t}"),
            AdminError::TlsConfig(msg) => write!(f, "TLS configuration error: {msg}"),
        }
    }
}

#[cfg(feature = "admin-api")]
impl std::error::Error for AdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AdminError::Request(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "admin-api")]
impl From<AdminError> for Error {
    fn from(err: AdminError) -> Self {
        Error::Admin(err)
    }
}

#[derive(Clone)]
pub(crate) struct SharedError {
    /// The first error of a connection, which then stays broken for every
    /// consumer and producer on it, and for the connection manager.
    error: Arc<std::sync::OnceLock<ConnectionError>>,
}

impl SharedError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new() -> SharedError {
        SharedError {
            error: Arc::new(std::sync::OnceLock::new()),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn is_set(&self) -> bool {
        self.error.get().is_some()
    }

    /// The error, left in place.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get(&self) -> Option<ConnectionError> {
        self.error.get().map(ConnectionError::duplicate)
    }

    /// Records why the connection broke; the first reason stays.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn set(&self, error: ConnectionError) {
        let _ = self.error.set(error);
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
pub(crate) fn server_error(i: i32) -> Option<ServerError> {
    match i {
        0 => Some(ServerError::UnknownError),
        1 => Some(ServerError::MetadataError),
        2 => Some(ServerError::PersistenceError),
        3 => Some(ServerError::AuthenticationError),
        4 => Some(ServerError::AuthorizationError),
        5 => Some(ServerError::ConsumerBusy),
        6 => Some(ServerError::ServiceNotReady),
        7 => Some(ServerError::ProducerBlockedQuotaExceededError),
        8 => Some(ServerError::ProducerBlockedQuotaExceededException),
        9 => Some(ServerError::ChecksumError),
        10 => Some(ServerError::UnsupportedVersionError),
        11 => Some(ServerError::TopicNotFound),
        12 => Some(ServerError::SubscriptionNotFound),
        13 => Some(ServerError::ConsumerNotFound),
        14 => Some(ServerError::TooManyRequests),
        15 => Some(ServerError::TopicTerminatedError),
        16 => Some(ServerError::ProducerBusy),
        17 => Some(ServerError::InvalidTopicName),
        18 => Some(ServerError::IncompatibleSchema),
        19 => Some(ServerError::ConsumerAssignError),
        20 => Some(ServerError::TransactionCoordinatorNotFound),
        21 => Some(ServerError::InvalidTxnStatus),
        22 => Some(ServerError::NotAllowedError),
        23 => Some(ServerError::TransactionConflict),
        24 => Some(ServerError::TransactionNotFound),
        25 => Some(ServerError::ProducerFenced),
        _ => None,
    }
}

#[cfg(test)]
mod shared_error_tests {
    use super::{ConnectionError, SharedError};

    #[test]
    fn a_connection_error_stays_for_every_reader() {
        let error = SharedError::new();
        error.set(ConnectionError::Disconnected);
        for _ in 0..2 {
            assert!(matches!(error.get(), Some(ConnectionError::Disconnected)));
            assert!(
                error.is_set(),
                "reading the error made the connection valid"
            );
        }
    }
}
