use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, SinkExt, Stream, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        config::ConsumerConfig,
        data::{DeadLetterPolicy, Delivery, EngineMessage, MessageData, MessageIdDataReceiver},
        engine::ConsumerEngine,
        message::Message,
    },
    error::{ConnectionError, ConsumerError},
    message::proto::{MessageIdData, Schema},
    proto::CommandConsumerStatsResponse,
    retry_op::retry_subscribe_consumer,
    BrokerAddress, DeserializeMessage, Error, Executor, Payload, Pulsar,
};

// this is entirely public for use in reader.rs
pub struct TopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer_id: u64,
    pub(crate) config: ConsumerConfig,
    topic: String,
    messages: Pin<Box<MessageIdDataReceiver>>,
    delivery: Arc<Mutex<Delivery>>,
    executor: Arc<Exe>,
    operation_timeout: Duration,
    engine_tx: mpsc::UnboundedSender<EngineMessage<Exe>>,
    data_type: PhantomData<fn(Payload) -> T::Output>,
    pub(crate) dead_letter_policy: Option<DeadLetterPolicy>,
    pub(super) last_message_received: Option<DateTime<Utc>>,
    pub(super) messages_received: u64,
}

impl<T: DeserializeMessage, Exe: Executor> TopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(super) async fn new(
        client: Pulsar<Exe>,
        topic: String,
        addr: BrokerAddress,
        config: ConsumerConfig,
    ) -> Result<TopicConsumer<T, Exe>, Error> {
        static CONSUMER_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

        let ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay,
            options,
            dead_letter_policy,
        } = config.clone();
        let consumer_id =
            consumer_id.unwrap_or_else(|| CONSUMER_ID_GENERATOR.fetch_add(1, Ordering::SeqCst));
        // The broker never has more than this many messages out to us: the
        // permits we grant. The smaller of the two settings bounds it.
        let window = batch_size
            .unwrap_or(1000)
            .min(options.receiver_queue_size.unwrap_or(1000))
            .max(1);
        let mut connection = client.manager.get_connection(&addr).await?;

        let messages = retry_subscribe_consumer(
            &client,
            &mut connection,
            addr,
            &topic,
            &subscription,
            sub_type,
            consumer_id,
            &consumer_name,
            &options,
            window,
        )
        .await?;

        let (engine_tx, engine_rx) = mpsc::unbounded();

        if unacked_message_redelivery_delay.is_some() {
            let mut redelivery_tx = engine_tx.clone();
            let mut interval = client.executor.interval(Duration::from_millis(500));
            let res = client.executor.spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if redelivery_tx
                        .send(EngineMessage::UnackedRedelivery)
                        .await
                        .is_err()
                    {
                        // Consumer shut down - stop ticker
                        break;
                    }
                }
            }));
            if res.is_err() {
                return Err(Error::Executor);
            }
        }
        // Unbounded, because the permits already bound it.
        let (tx, rx) = mpsc::unbounded();
        let delivery = Arc::new(Mutex::new(Delivery::new(
            window,
            unacked_message_redelivery_delay.is_some(),
        )));
        let mut c = ConsumerEngine::new(
            client.clone(),
            connection.clone(),
            topic.clone(),
            subscription.clone(),
            sub_type,
            consumer_id,
            consumer_name,
            tx,
            messages,
            delivery.clone(),
            engine_rx,
            window,
            unacked_message_redelivery_delay,
            dead_letter_policy.clone(),
            options.clone(),
        );
        let engine_task = client.executor.spawn(Box::pin(async move {
            c.engine()
                .map(|res| {
                    debug!("consumer engine stopped: {:?}", res);
                })
                .await;
        }));
        if engine_task.is_err() {
            return Err(Error::Executor);
        }

        Ok(TopicConsumer {
            consumer_id,
            config,
            topic,
            messages: Box::pin(rx),
            delivery,
            executor: client.executor.clone(),
            operation_timeout: client.operation_retry_options.operation_timeout,
            engine_tx,
            data_type: PhantomData,
            dead_letter_policy,
            last_message_received: None,
            messages_received: 0,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connection(&mut self) -> Result<Arc<Connection<Exe>>, Error> {
        let (resolver, response) = oneshot::channel();
        self.engine_tx
            .send(EngineMessage::GetConnection(resolver))
            .await
            .map_err(|_| ConsumerError::Connection(ConnectionError::Disconnected))?;

        response.await.map_err(|oneshot::Canceled| {
            error!("the consumer engine dropped the request");
            ConnectionError::Disconnected.into()
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_stats(&mut self) -> Result<CommandConsumerStatsResponse, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let consumer_stats_response = conn.sender().get_consumer_stats(consumer_id).await?;
        Ok(consumer_stats_response)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        let conn = self.connection().await?;
        info!("check connection for id {}", conn.id());
        conn.sender().send_ping().await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg.ack_id(), false))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack_with_id(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg_id, false))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn acker(&self) -> mpsc::UnboundedSender<EngineMessage<Exe>> {
        self.engine_tx.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg.ack_id(), true))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack_with_id(
        &mut self,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg_id, true))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Nack(msg.message_id().clone()))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack_with_id(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        self.engine_tx.send(EngineMessage::Nack(msg_id)).await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        // The broker closes the consumer before it confirms the seek, so the
        // engine's reconnect has to find the target before the request goes.
        // Java: ConsumerImpl.java:3048 seekAsyncInternal sets seekMessageId
        // first and restores it if the seek fails.
        // Raising the epoch keeps what is buffered from before the seek from
        // reaching the application, as Java clears the queue first.
        // Java: MultiTopicsConsumerImpl.java:944 beforeSeek
        let (epoch, before) = {
            let mut delivery = self.delivery.lock().unwrap();
            let before = (
                delivery.last.take(),
                delivery.first_queued.take(),
                delivery.seek_target.clone(),
            );
            delivery.seek_target = message_id.clone();
            delivery.epoch += 1;
            (delivery.epoch, before)
        };
        // Java: ConsumerImpl.java:3043 seekAsyncInternal retries until the
        // operation timeout, so a seek survives a broker failover. A seek
        // that failed would leave the reconnect's start filter in place.
        let deadline = std::time::Instant::now() + self.operation_timeout;
        let mut delay = Duration::from_millis(100);
        let result = loop {
            // A reconnect may have taken the target meanwhile; the next
            // reconnect, after this attempt, needs it again.
            self.delivery.lock().unwrap().seek_target = message_id.clone();
            let attempt = match self.connection().await {
                Ok(connection) => connection
                    .sender()
                    .seek(consumer_id, message_id.clone(), timestamp)
                    .await
                    .map(drop),
                Err(e) => Err(ConnectionError::Unexpected(e.to_string())),
            };
            match attempt {
                Err(e) if may_succeed_later(&e) && std::time::Instant::now() < deadline => {
                    self.executor.delay(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                result => break result,
            }
        };
        if result.is_err() {
            let mut delivery = self.delivery.lock().unwrap();
            if delivery.epoch == epoch {
                delivery.epoch -= 1;
                (delivery.last, delivery.first_queued, delivery.seek_target) = before;
            } else {
                // a reconnect applied the target, but the broker never moved
                delivery.seek_target = None;
                if let Some(target) = message_id {
                    let _ = self
                        .engine_tx
                        .unbounded_send(EngineMessage::CancelSeek(target));
                }
            }
        }
        result?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .unsubscribe(consumer_id)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .close_consumer(consumer_id)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let get_last_message_id_response = conn.sender().get_last_message_id(consumer_id).await?;
        Ok(get_last_message_id_response.last_message_id)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.last_message_received
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.messages_received
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn create_message(&self, message_id: MessageIdData, payload: Payload) -> Message<T> {
        let message_id = MessageData {
            id: message_id,
            batch_size: payload.metadata.num_messages_in_batch,
        };
        Message::new(&self.topic, message_id, payload)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn get_schema(
        &mut self,
        version: Option<Vec<u8>>,
    ) -> Result<Option<Schema>, Error> {
        let conn = self.connection().await?;
        let schema_response = conn.sender().get_schema(&self.topic, version).await?;
        Ok(schema_response.schema)
    }
}

impl<T: DeserializeMessage, Exe: Executor> Stream for TopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.messages.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok((epoch, id, payload)))) => {
                    // C++: lib/ConsumerImpl.cc:1162 receiveHelper drops messages
                    // that arrived on an earlier connection.
                    let flow = {
                        let mut delivery = self.delivery.lock().unwrap();
                        delivery.queued = delivery.queued.saturating_sub(1);
                        if epoch != delivery.epoch {
                            // its slot in the window is free for the new
                            // connection now
                            if let Some(permits) = delivery.free_permit() {
                                let _ = self
                                    .engine_tx
                                    .unbounded_send(EngineMessage::Flow(delivery.epoch, permits));
                            }
                            continue;
                        }
                        delivery.last = Some(id.clone());
                        if let Some(taken) = &mut delivery.taken {
                            taken.push((id.clone(), std::time::Instant::now()));
                        }
                        // Java: ConsumerImpl.java:2253 messageProcessed
                        delivery.free_permit()
                    };
                    if let Some(permits) = flow {
                        let _ = self
                            .engine_tx
                            .unbounded_send(EngineMessage::Flow(epoch, permits));
                    }
                    self.last_message_received = Some(Utc::now());
                    self.messages_received += 1;
                    return Poll::Ready(Some(Ok(self.create_message(id, payload))));
                }
                Poll::Ready(Some(Err(e))) => {
                    error!("we are using in the single-consumer and we got an error, {e}");
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

/// Failures of a request that a retry after a reconnect can fix.
fn may_succeed_later(error: &ConnectionError) -> bool {
    match error {
        ConnectionError::Io(_) | ConnectionError::Disconnected | ConnectionError::SlowDown => true,
        ConnectionError::PulsarError(kind, _) => {
            *kind == Some(crate::message::proto::ServerError::ServiceNotReady)
        }
        _ => false,
    }
}
