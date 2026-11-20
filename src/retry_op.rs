use std::{io::ErrorKind, sync::Arc, time::Instant};

use futures::channel::mpsc::{self, UnboundedReceiver};

use crate::{
    connection::Connection,
    error::{ConnectionError, ConsumerError},
    message::Message,
    proto, BrokerAddress, ConsumerOptions, Error, Executor, ProducerOptions, Pulsar, SubType,
};

pub async fn handle_retry_error<Exe: Executor>(
    client: &Pulsar<Exe>,
    connection: &mut Arc<Connection<Exe>>,
    addr: &mut BrokerAddress,
    topic: &str,
    operation_name: &str,
    current_retries: u32,
    err: ConnectionError,
) -> Result<(), Error> {
    let operation_retry_options = &client.operation_retry_options;
    let (kind, text) = match err {
        ConnectionError::PulsarError(Some(kind), ref text)
            if matches!(
                kind,
                proto::ServerError::ServiceNotReady
                    | proto::ServerError::ConsumerBusy
                    | proto::ServerError::ProducerBusy
                    | proto::ServerError::ProducerBlockedQuotaExceededError
                    | proto::ServerError::ProducerBlockedQuotaExceededException
            ) =>
        {
            (
                kind.as_str_name().to_owned(),
                text.as_ref()
                    .map(|text| format!(" (\"{text}\")"))
                    .unwrap_or_default(),
            )
        }
        ConnectionError::Io(ref kind)
            if matches!(
                kind.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::BrokenPipe
                    | ErrorKind::TimedOut
                    | ErrorKind::Interrupted
                    | ErrorKind::UnexpectedEof
            ) =>
        {
            (kind.kind().to_string(), "".to_owned())
        }
        err => {
            error!("{operation_name}({topic}) error: {err:?}");
            return Err(err.into());
        }
    };
    if !(operation_retry_options.allow_retry(current_retries)) {
        error!("{operation_name}({topic}) answered {kind}{text}, reached max retries");
        return Err(err.into());
    }
    error!(
        "{operation_name}({topic}) answered {kind}{text}, retrying request after {:?} (max_retries = {:?})",
        operation_retry_options.retry_delay,
        operation_retry_options.max_retries
    );
    client
        .executor
        .delay(operation_retry_options.retry_delay)
        .await;

    *addr = client.lookup_topic(topic).await?;
    *connection = client.manager.get_connection(addr).await?;
    Ok(())
}

pub async fn retry_subscribe_consumer<Exe: Executor>(
    client: &Pulsar<Exe>,
    connection: &mut Arc<Connection<Exe>>,
    mut addr: BrokerAddress,
    topic: &str,
    subscription: &str,
    sub_type: SubType,
    consumer_id: u64,
    consumer_name: &Option<String>,
    options: &ConsumerOptions,
    window: u32,
) -> Result<UnboundedReceiver<Message>, Error> {
    *connection = client.manager.get_connection(&addr).await?;
    let mut current_retries = 0u32;
    let start = Instant::now();

    let messages = loop {
        warn!(
            "Retry #{current_retries} -> connecting consumer {consumer_id} using connection {:#} to broker {:#} to topic {topic}",
            connection.id(),
            addr.url,
        );
        match subscribe_once(
            connection,
            topic,
            subscription,
            sub_type,
            consumer_id,
            consumer_name,
            options,
        )
        .await
        {
            Ok(messages) => {
                if current_retries > 0 {
                    let dur = (Instant::now() - start).as_secs();
                    info!(
                        "TopicConsumer::subscribe({topic}) success after {} retries over {dur} seconds",
                        current_retries + 1,
                    );
                }
                break messages;
            }
            Err(err) => {
                handle_retry_error(
                    client,
                    connection,
                    &mut addr,
                    topic,
                    "TopicConsumer::subscribe",
                    current_retries,
                    err,
                )
                .await?
            }
        }
        current_retries += 1;
    };
    send_initial_flow(connection, topic, consumer_id, window).await?;
    Ok(messages)
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

pub async fn retry_create_producer<Exe: Executor>(
    client: &Pulsar<Exe>,
    connection: &mut Arc<Connection<Exe>>,
    mut addr: BrokerAddress,
    topic: &String,
    producer_id: u64,
    producer_name: Option<String>,
    options: &ProducerOptions,
) -> Result<(String, Option<Vec<u8>>), Error> {
    *connection = client.manager.get_connection(&addr).await?;
    let mut current_retries = 0u32;
    let start = Instant::now();

    loop {
        warn!(
            "Retry #{current_retries} -> connecting producer {producer_id} using connection {:#} to broker {:#} to topic {topic}",
            connection.id(),
            addr.url,
        );
        match connection
            .sender()
            .create_producer(
                topic.clone(),
                producer_id,
                producer_name.clone(),
                options.clone(),
            )
            .await
        {
            Ok(partial_success) => {
                let mut schema_version = partial_success.schema_version;
                // If producer is not "ready", the client will avoid to timeout the request
                // for creating the producer. Instead it will wait indefinitely until it gets
                // a subsequent  `CommandProducerSuccess` with `producer_ready==true`.
                if let Some(producer_ready) = partial_success.producer_ready {
                    if !producer_ready {
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
                                            "TopicProducer::create({topic}) schema_version \
                                             updated after exclusive access wait"
                                        );
                                        schema_version = Some(new_sv);
                                    }
                                } else if schema_version.is_some() {
                                    warn!(
                                        "TopicProducer::create({topic}) final \
                                         CommandProducerSuccess has no schema_version, \
                                         keeping initial value"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    "TopicProducer::create({topic}) wait_for_exclusive_access \
                                     failed: {e:?}, proceeding with initial schema_version"
                                );
                            }
                        }
                    }
                }
                if current_retries > 0 {
                    let dur = (std::time::Instant::now() - start).as_secs();
                    log::info!(
                        "TopicProducer::create({topic}) success after {} retries over {dur} seconds",
                        current_retries + 1,
                    );
                }
                return Ok((partial_success.producer_name, schema_version));
            }
            Err(err) => {
                handle_retry_error(
                    client,
                    connection,
                    &mut addr,
                    topic,
                    "TopicProducer::create",
                    current_retries,
                    err,
                )
                .await?
            }
        }
        current_retries += 1;
    }
}
