use std::{future::Future, sync::Arc, time::Instant};

use futures::channel::mpsc::{self, UnboundedReceiver};

use crate::{
    connection::Connection,
    error::{ConnectionError, ConsumerError, Phase},
    message::Message,
    BrokerAddress, ConsumerOptions, Error, Executor, ProducerOptions, Pulsar, SubType,
};

/// Runs `operation` on the broker that owns `topic` until it succeeds, fails
/// in a way no retry can fix, or the operation timeout passes. The first try
/// uses `addr`; later ones look the topic up again, as it may have moved.
/// Every look-up and connect is a single attempt: this loop alone backs off.
// Java: ConnectionHandler.grabCnx and reconnectLater, isRetriableError
pub(crate) async fn with_retries<Exe, T, F, Fut>(
    client: &Pulsar<Exe>,
    addr: BrokerAddress,
    topic: &str,
    what: &str,
    mut operation: F,
) -> Result<(Arc<Connection<Exe>>, T), Error>
where
    Exe: Executor,
    F: FnMut(Arc<Connection<Exe>>) -> Fut,
    Fut: Future<Output = Result<T, ConnectionError>>,
{
    let options = &client.operation_retry_options;
    let max_backoff = client.manager.connection_retry_options().max_backoff;
    let start = Instant::now();
    let mut addr = Some(addr);
    let mut attempt = 0;
    loop {
        let result = async {
            let addr = match addr.take() {
                Some(addr) => addr,
                None => client.lookup_topic_once(topic).await?,
            };
            let connection = client.manager.get_connection_retrying(&addr, 0).await?;
            let value = operation(connection.clone()).await?;
            Ok::<_, Error>((connection, value))
        }
        .await;
        let error = match result {
            Ok(done) => {
                if attempt > 0 {
                    info!(
                        "{what}({topic}) succeeded after {} tries over {:?}",
                        attempt + 1,
                        start.elapsed()
                    );
                }
                return Ok(done);
            }
            Err(error) => error,
        };
        let delay = error
            .may_retry(Phase::Create)
            .then(|| options.next_delay(start, attempt, max_backoff))
            .flatten();
        let Some(delay) = delay else {
            error!("{what}({topic}) failed: {error}");
            return Err(error);
        };
        warn!("{what}({topic}) failed, retrying in {delay:?}: {error}");
        client.executor.delay(delay).await;
        attempt += 1;
    }
}

/// Subscribes to `topic`, retrying as [`with_retries`] does, and grants the
/// broker its first `window` permits.
#[allow(clippy::too_many_arguments)]
pub async fn retry_subscribe_consumer<Exe: Executor>(
    client: &Pulsar<Exe>,
    addr: BrokerAddress,
    topic: &str,
    subscription: &str,
    sub_type: SubType,
    consumer_id: u64,
    consumer_name: &Option<String>,
    options: &ConsumerOptions,
    window: u32,
) -> Result<(Arc<Connection<Exe>>, UnboundedReceiver<Message>), Error> {
    let (connection, messages) =
        with_retries(client, addr, topic, "subscribe", |connection| async move {
            subscribe_once(
                &connection,
                topic,
                subscription,
                sub_type,
                consumer_id,
                consumer_name,
                options,
            )
            .await
        })
        .await?;
    send_initial_flow(&connection, topic, consumer_id, window).await?;
    Ok((connection, messages))
}

/// Sends one subscribe request on `connection`, and returns where the
/// broker's messages for this consumer arrive.
pub async fn subscribe_once<Exe: Executor>(
    connection: &Arc<Connection<Exe>>,
    topic: &str,
    subscription: &str,
    sub_type: SubType,
    consumer_id: u64,
    consumer_name: &Option<String>,
    options: &ConsumerOptions,
) -> Result<UnboundedReceiver<Message>, ConnectionError> {
    let (resolver, messages) = mpsc::unbounded();
    connection
        .sender()
        .subscribe(
            resolver,
            topic.to_owned(),
            subscription.to_owned(),
            sub_type,
            consumer_id,
            consumer_name.clone(),
            options.clone(),
        )
        .await?;
    Ok(messages)
}

/// Grants the broker its first `window` permits for a new subscription.
pub async fn send_initial_flow<Exe: Executor>(
    connection: &Arc<Connection<Exe>>,
    topic: &str,
    consumer_id: u64,
    window: u32,
) -> Result<(), Error> {
    connection
        .sender()
        .send_flow(consumer_id, window)
        .await
        .map_err(|err| {
            error!("TopicConsumer::send_flow({topic}) error: {err:?}");
            Error::Consumer(ConsumerError::Connection(err))
        })
}

/// Creates a producer on `topic`, retrying as [`with_retries`] does, and
/// returns its connection, name and schema version.
pub async fn retry_create_producer<Exe: Executor>(
    client: &Pulsar<Exe>,
    addr: BrokerAddress,
    topic: &str,
    producer_id: u64,
    producer_name: Option<String>,
    options: &ProducerOptions,
) -> Result<(Arc<Connection<Exe>>, (String, Option<Vec<u8>>)), Error> {
    with_retries(client, addr, topic, "create producer", |connection| {
        let producer_name = producer_name.clone();
        async move {
            create_producer_once(&connection, topic, producer_id, producer_name, options).await
        }
    })
    .await
}

/// Sends one create-producer request on `connection`, waiting for exclusive
/// access if the broker asks to, and returns the producer's name and schema
/// version.
pub async fn create_producer_once<Exe: Executor>(
    connection: &Arc<Connection<Exe>>,
    topic: &str,
    producer_id: u64,
    producer_name: Option<String>,
    options: &ProducerOptions,
) -> Result<(String, Option<Vec<u8>>), ConnectionError> {
    let partial_success = connection
        .sender()
        .create_producer(
            topic.to_owned(),
            producer_id,
            producer_name,
            options.clone(),
        )
        .await?;
    let mut schema_version = partial_success.schema_version;
    // If producer is not "ready", the client will avoid to timeout the request
    // for creating the producer. Instead it will wait indefinitely until it gets
    // a subsequent  `CommandProducerSuccess` with `producer_ready==true`.
    if partial_success.producer_ready == Some(false) {
        // wait until next commandproducersuccess message has been received
        trace!("TopicProducer::create({topic}) waiting for exclusive access");
        let result = connection
            .sender()
            .wait_for_exclusive_access(partial_success.request_id)
            .await;
        trace!("TopicProducer::create({topic}) received: {result:?}");
        match result {
            Ok(success) => {
                if let Some(new_sv) = success.schema_version {
                    if schema_version.as_ref() != Some(&new_sv) {
                        debug!(
                            "TopicProducer::create({topic}) schema_version updated after \
                             exclusive access wait"
                        );
                        schema_version = Some(new_sv);
                    }
                } else if schema_version.is_some() {
                    warn!(
                        "TopicProducer::create({topic}) final CommandProducerSuccess has no \
                         schema_version, keeping initial value"
                    );
                }
            }
            Err(e) => {
                error!(
                    "TopicProducer::create({topic}) wait_for_exclusive_access failed: {e:?}, \
                     proceeding with initial schema_version"
                );
            }
        }
    }
    Ok((partial_success.producer_name, schema_version))
}
