use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use chrono::{DateTime, Utc};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, SinkExt, Stream,
};

use crate::{
    connection::Connection,
    consumer::{
        config::ConsumerConfig,
        data::{
            position, DeadLetterPolicy, Delivery, EngineMessage, MessageData,
            MessageIdDataReceiver, Queued, Resume, Seek,
        },
        engine::{configured_resume, ConsumerEngine},
        message::Message,
        message::{ConsumerEvent, ConsumerItem},
    },
    error::{ConnectionError, ConsumerError},
    message::proto::{MessageIdData, Schema},
    proto::{self, CommandConsumerStatsResponse},
    retry_op::retry_subscribe_consumer,
    BrokerAddress, DeserializeMessage, Error, Executor, Payload, Pulsar,
};

pub struct TopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer_id: u64,
    pub(crate) config: ConsumerConfig,
    topic: String,
    messages: Pin<Box<MessageIdDataReceiver>>,
    delivery: Arc<Mutex<Delivery>>,
    engine_tx: mpsc::UnboundedSender<EngineMessage<Exe>>,
    data_type: PhantomData<fn(Payload) -> T::Output>,
    pub(crate) dead_letter_policy: Option<DeadLetterPolicy>,
    pub(super) last_message_received: Option<DateTime<Utc>>,
    pub(super) messages_received: u64,
    /// Whether the last Failover event the application took said active.
    pub(super) active: bool,
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
            auto_ack: _,
        } = config.clone();
        let consumer_id =
            consumer_id.unwrap_or_else(|| CONSUMER_ID_GENERATOR.fetch_add(1, Ordering::SeqCst));
        // The broker never has more than this many messages out to us: the
        // permits we grant. The smaller of the two settings bounds it.
        let window = batch_size
            .unwrap_or(1000)
            .min(options.receiver_queue_size.unwrap_or(1000))
            .max(1);
        let (connection, messages) = retry_subscribe_consumer(
            &client,
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

        // Unbounded, because the permits already bound it.
        let (tx, rx) = mpsc::unbounded();
        let delivery = Arc::new(Mutex::new(Delivery {
            resume: configured_resume(&options),
            ..Delivery::new(window, unacked_message_redelivery_delay.is_some())
        }));
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
            engine_tx,
            data_type: PhantomData,
            dead_letter_policy,
            last_message_received: None,
            messages_received: 0,
            active: false,
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
        let seek = match (message_id, timestamp) {
            (Some(target), _) => Seek::Id(target),
            (None, Some(publish_time_ms)) => Seek::Time(publish_time_ms),
            (None, None) => {
                return Err(Error::Custom(
                    "a seek needs a message id or a publish time".to_string(),
                ))
            }
        };
        // The engine makes the seek and sees it through: one dropped here
        // still happens, or fails as a whole.
        let (reply, answer) = oneshot::channel();
        self.engine_tx
            .unbounded_send(EngineMessage::Seek(seek, std::time::Instant::now(), reply))
            .map_err(|_| Error::Consumer(ConsumerError::Closed))?;
        answer
            .await
            .map_err(|_| Error::Consumer(ConsumerError::Closed))?
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

    /// Whether the topic holds a message the application has not taken yet:
    /// one the broker has past the last the application took, or at the
    /// first one queued, or else past the subscription's position. Messages
    /// queued here count through the first two, so stale ones from before a
    /// seek or reconnect do not.
    // Java: ConsumerImpl.hasMessageAvailableAsync and hasMoreMessages
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn has_message_available(&mut self) -> Result<bool, Error> {
        let resume = self.delivery.lock().unwrap().resume.clone();
        let response = self.last_message_id_response().await?;
        let last = response.last_message_id;
        // The broker sends Java's longs: an empty topic answers entry -1.
        if (last.entry_id as i64) < 0 {
            return Ok(false);
        }
        Ok(match (&resume, response.consumer_mark_delete_position) {
            (Resume::After(taken), _) => position(&last) > position(taken),
            (Resume::From(first), _) => position(&last) >= position(first),
            // -1 is "before the first entry"
            (_, Some(cursor)) => {
                (last.ledger_id as i64, last.entry_id as i64)
                    > (cursor.ledger_id as i64, cursor.entry_id as i64)
            }
            (_, None) => true,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        Ok(self.last_message_id_response().await?.last_message_id)
    }

    async fn last_message_id_response(
        &mut self,
    ) -> Result<proto::CommandGetLastMessageIdResponse, Error> {
        let consumer_id = self.consumer_id;
        let connection = self.connection().await?;
        Ok(connection.sender().get_last_message_id(consumer_id).await?)
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

impl<T: DeserializeMessage, Exe: Executor> TopicConsumer<T, Exe> {
    /// The next message or Failover event.
    pub(super) fn poll_next_item(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ConsumerItem<T>, Error>>> {
        loop {
            match self.messages.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok(Queued::Active(active)))) => {
                    // The broker repeats itself, and says "not active" to
                    // consumers that never were.
                    if active == self.active {
                        continue;
                    }
                    self.active = active;
                    let topic = self.topic.clone();
                    let event = if active {
                        ConsumerEvent::BecameActive { topic }
                    } else {
                        ConsumerEvent::BecameInactive { topic }
                    };
                    return Poll::Ready(Some(Ok(ConsumerItem::Event(event))));
                }
                Poll::Ready(Some(Ok(Queued::Message(epoch, id, payload)))) => {
                    // C++: lib/ConsumerImpl.cc:1162 receiveHelper drops messages
                    // that arrived on an earlier connection.
                    let flow = {
                        let mut delivery = self.delivery.lock().unwrap();
                        delivery.queued = delivery.queued.saturating_sub(1);
                        if epoch != delivery.epoch {
                            delivery.stale += 1;
                            // its slot in the window is free for the new
                            // connection now
                            if let Some(permits) = delivery.free_permit() {
                                let _ = self
                                    .engine_tx
                                    .unbounded_send(EngineMessage::Flow(delivery.epoch, permits));
                            }
                            continue;
                        }
                        delivery.resume = Resume::After(id.clone());
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
                    let message = self.create_message(id, payload);
                    if self.config.auto_ack {
                        // If the engine is gone, nothing can be acked; what is
                        // still queued drains, and then the stream ends.
                        let _ = self
                            .engine_tx
                            .unbounded_send(EngineMessage::Ack(message.ack_id(), false));
                    }
                    return Poll::Ready(Some(Ok(ConsumerItem::Message(message))));
                }
                Poll::Ready(Some(Err(e))) => {
                    error!("consumer of {}: {e}", self.topic);
                    return Poll::Ready(Some(Err(Error::Consumer(ConsumerError::Topic {
                        topic: self.topic.clone(),
                        error: Box::new(e),
                    }))));
                }
            }
        }
    }
}
