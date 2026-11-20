use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, mpsc::UnboundedSender},
    SinkExt, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        batched_message_iterator::BatchedMessageIterator,
        data::{DeadLetterPolicy, Delivery, EngineEvent, EngineMessage},
        options::ConsumerOptions,
    },
    error::{self, ConnectionError, ConsumerError},
    message::{
        proto::{command_subscribe::SubType, MessageIdData},
        Message as RawMessage,
    },
    producer::Message,
    proto,
    proto::{BaseCommand, CommandCloseConsumer, CommandMessage},
    retry_op::{send_initial_flow, subscribe_once},
    Error, Executor, Payload, Pulsar,
};

const SYSTEM_PROPERTY_REAL_TOPIC: &str = "REAL_TOPIC";
const SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";

pub struct ConsumerEngine<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<Connection<Exe>>,
    topic: String,
    subscription: String,
    sub_type: SubType,
    id: u64,
    name: Option<String>,
    tx: mpsc::Sender<Result<(u64, MessageIdData, Payload), Error>>,
    /// What is waiting for room in the consumer's queue. Parking it here, not
    /// blocking on `tx`, keeps acks, seeks and connection requests flowing
    /// while the application is behind. No flow permits go out while it is
    /// not empty, so the broker cannot fill it beyond the permits it has.
    pending: VecDeque<Result<(u64, MessageIdData, Payload), Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    delivery: Arc<Mutex<Delivery>>,
    /// The epoch of the current connection, as last written to `delivery`.
    epoch: u64,
    /// Messages at or before this id are dropped, as the broker sends the entry
    /// a subscription restarts from again.
    start: Option<MessageIdData>,
    /// Where a non-durable subscription starts when nothing was delivered.
    configured_start: Option<MessageIdData>,
    /// Whether `Delivery::first_queued` no longer needs updating, which spares
    /// a lock per message.
    first_queued_known: bool,
    engine_rx: Option<mpsc::UnboundedReceiver<EngineMessage<Exe>>>,
    event_rx: mpsc::UnboundedReceiver<EngineEvent<Exe>>,
    event_tx: UnboundedSender<EngineEvent<Exe>>,
    batch_size: u32,
    remaining_messages: i64,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
}

impl<Exe: Executor> ConsumerEngine<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(
        client: Pulsar<Exe>,
        connection: Arc<Connection<Exe>>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        id: u64,
        name: Option<String>,
        tx: mpsc::Sender<Result<(u64, MessageIdData, Payload), Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        delivery: Arc<Mutex<Delivery>>,
        engine_rx: mpsc::UnboundedReceiver<EngineMessage<Exe>>,
        batch_size: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> ConsumerEngine<Exe> {
        let (event_tx, event_rx) = mpsc::unbounded();
        ConsumerEngine {
            client,
            connection,
            topic,
            subscription,
            sub_type,
            id,
            name,
            tx,
            pending: VecDeque::new(),
            messages_rx: Some(messages_rx),
            delivery,
            epoch: 0,
            start: None,
            configured_start: options.start_message_id.clone(),
            first_queued_known: false,
            engine_rx: Some(engine_rx),
            event_rx,
            event_tx,
            batch_size,
            remaining_messages: batch_size as i64,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            dead_letter_policy,
            options,
        }
    }

    fn register_source<E, M>(&self, mut rx: mpsc::UnboundedReceiver<E>, mapper: M) -> Result<(), ()>
    where
        E: Send + 'static,
        M: Fn(Option<E>) -> EngineEvent<Exe> + Send + Sync + 'static,
    {
        let mut event_tx = self.event_tx.clone();

        self.client.executor.spawn(Box::pin(async move {
            while let Some(msg) = rx.next().await {
                let r = event_tx.send(mapper(Some(msg))).await;
                if let Err(err) = r {
                    log::error!("Error sending event to channel - {err}");
                    if err.is_disconnected() {
                        break;
                    }
                }
            }

            let send_end_res = event_tx.send(mapper(None)).await;
            if let Err(err) = send_end_res {
                log::debug!("Error sending close event to channel - {err}");
            }

            log::trace!("rx terminated");
        }))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn engine(&mut self) -> Result<(), Error> {
        debug!("starting the consumer engine for topic {}", self.topic);

        loop {
            if !self.connection.is_valid() {
                if let Some(err) = self.connection.error() {
                    error!(
                        "Consumer: connection {} is not valid: {:?}",
                        self.connection.id(),
                        err
                    );
                    self.reconnect().await?;
                }
            }

            if let Some(messages_rx) = self.messages_rx.take() {
                let epoch = self.epoch;
                self.register_source(messages_rx, move |msg| EngineEvent::Message(epoch, msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering messages_rx source"))
                    })?;
            }

            if let Some(engine_rx) = self.engine_rx.take() {
                self.register_source(engine_rx, |msg| EngineEvent::EngineMessage(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering engine_rx source"))
                    })?;
            }

            // In the casual workflow, we use the `batch_size` as a maximum number that we could
            // send using a send flow command message to ask the broker to send us
            // messages, which is why we have a subtraction below (`batch_size` -
            // `remaining_messages`).
            //
            // In the special case of batch messages (which is defined by clients), the number of
            // messages could be greater than the given batch size and the remaining
            // messages goes negative, which is why we use an `i64` as we want to keep
            // track of the number of negative messages as the next send flow will be
            // the batch_size - minus remaining messages which allow us to retrieve the casual
            // workflow of flow command messages.
            //
            // Here is the example of it works for a batch_size at 1000 and the error case:
            //
            // ```
            // Message (1) -> (batch_size = 1000, remaing_messages = 999, no flow message trigger)
            // ... 499 messages later ...
            // Message (1) -> (batch_size = 1000, remaing_messages = 499, flow message trigger, ask broker to send => batch_size - remaining_messages = 501)
            // ... 200 messages later ...
            // BatchMessage (1024) -> (batch_size = 1000, remaining_messages = 4294967096, no flow message trigger) [underflow on remaining messages - without the patch]
            // BatchMessage (1024) -> (batch_size = 1000, remaining_messages = -1124, flow message trigger, ask broker to send =>  batch_size - remaining_messages = 2124) [no underflow on remaining messages - with the patch]
            // ```
            if self.pending.is_empty()
                && self.remaining_messages < (self.batch_size.div_ceil(2) as i64)
            {
                match self
                    .connection
                    .sender()
                    .send_flow(
                        self.id,
                        (self.batch_size as i64 - self.remaining_messages) as u32,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(ConnectionError::Disconnected) => {
                        debug!("consumer engine: consumer connection disconnected, trying to reconnect");
                        continue;
                    }
                    // we don't need to handle the SlowDown error, since send_flow waits on the
                    // channel to be not full
                    Err(e) => {
                        error!("consumer engine: we got a unrecoverable connection error, {e}");
                        return Err(e.into());
                    }
                }

                self.remaining_messages = self.batch_size as i64;
            }

            let event = if self.pending.is_empty() {
                Self::timeout(self.event_rx.next(), Duration::from_secs(1)).await
            } else {
                match self.room_or_event().await {
                    // a closed queue makes the flush fail, which ends the engine
                    futures::future::Either::Left(()) => {
                        self.flush_pending()?;
                        continue;
                    }
                    futures::future::Either::Right(event) => Ok(event),
                }
            };
            match event {
                Err(_) => {
                    // If you are reading this comment, you may have an issue where you have
                    // received a batched message that is greater that the batch
                    // size and then break the way that we send flow command message.
                    //
                    // In that case, you could increase your batch size or patch this driver by
                    // adding the following line, if you are sure that you have
                    // at least 1 incoming message per second.
                    //
                    // ```rust
                    // self.remaining_messages = 0;
                    // ```
                    debug!("consumer engine: timeout (1s)");
                }
                Ok(Some(EngineEvent::Message(epoch, _))) if epoch != self.epoch => {
                    debug!("consumer engine: dropped a frame from a previous connection");
                }
                Ok(Some(EngineEvent::Message(_, msg))) => {
                    debug!("consumer engine: received message, {:?}", msg);
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Ok(Some(EngineEvent::EngineMessage(msg))) => {
                    debug!("consumer engine: received engine message");
                    let continue_loop = self.handle_ack_opt(msg).await;
                    if !continue_loop {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    log::warn!("Event stream is terminated");
                    return Ok(());
                }
            }
        }
    }

    async fn handle_message_opt(
        &mut self,
        message_opt: Option<crate::message::Message>,
    ) -> Option<Result<(), Error>> {
        match message_opt {
            None => {
                debug!("Consumer: old message source terminated");
                None
            }
            Some(message) => {
                self.remaining_messages -= message
                    .payload
                    .as_ref()
                    .and_then(|payload| {
                        debug!(
                            "Consumer: received message payload, num_messages_in_batch = {:?}",
                            payload.metadata.num_messages_in_batch
                        );
                        payload.metadata.num_messages_in_batch
                    })
                    .unwrap_or(1) as i64;

                match self.process_message(message).await {
                    // Continue
                    Ok(true) => None,
                    // End of Topic
                    Ok(false) => Some(Ok(())),
                    Err(e) => self.park(Err(e)).err().map(Err),
                }
            }
        }
    }

    async fn handle_ack_opt(&mut self, ack_opt: Option<EngineMessage<Exe>>) -> bool {
        match ack_opt {
            None => {
                trace!("ack channel was closed");
                false
            }
            Some(EngineMessage::Ack(message_id, cumulative)) => {
                self.ack(message_id, cumulative).await;
                true
            }
            Some(EngineMessage::Nack(message_id)) => {
                if let Err(e) = self
                    .connection
                    .sender()
                    .send_redeliver_unacknowleged_messages(self.id, vec![message_id.clone()])
                    .await
                {
                    error!(
                        "could not ask for redelivery for message {:?}: {:?}",
                        message_id, e
                    );
                }
                true
            }
            Some(EngineMessage::UnackedRedelivery) => {
                let mut h = HashSet::new();
                let now = Instant::now();
                // info!("unacked messages length: {}", self.unacked_messages.len());
                for (id, t) in self.unacked_messages.iter() {
                    if *t < now {
                        h.insert(id.clone());
                    }
                }

                let ids: Vec<_> = h.iter().cloned().collect();
                if !ids.is_empty() {
                    // info!("will unack ids: {:?}", ids);
                    if let Err(e) = self
                        .connection
                        .sender()
                        .send_redeliver_unacknowleged_messages(self.id, ids)
                        .await
                    {
                        error!("could not ask for redelivery: {:?}", e);
                    } else {
                        for i in h.iter() {
                            self.unacked_messages.remove(i);
                        }
                    }
                }
                true
            }
            Some(EngineMessage::GetConnection(sender)) => {
                let _ = sender.send(self.connection.clone()).map_err(|_| {
                    error!(
                        "consumer requested the engine's connection but dropped the \
                                     channel before receiving"
                    );
                });
                true
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn ack(&mut self, message_id: MessageIdData, cumulative: bool) {
        // FIXME: this does not handle cumulative acks
        self.unacked_messages.remove(&message_id);
        let res = self
            .connection
            .sender()
            .send_ack(self.id, vec![message_id], cumulative)
            .await;
        if res.is_err() {
            error!("ack error: {:?}", res);
        }
    }

    /// Process the message. Returns `true` if there are more messages to process
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_message(&mut self, message: RawMessage) -> Result<bool, Error> {
        match message {
            RawMessage {
                command:
                    BaseCommand {
                        reached_end_of_topic: Some(_),
                        ..
                    },
                ..
            } => {
                return Ok(false);
            }
            RawMessage {
                command:
                    BaseCommand {
                        active_consumer_change: Some(active_consumer_change),
                        ..
                    },
                ..
            } => {
                // TODO: Communicate this status to the Consumer and expose it
                debug!(
                    "Active consumer change for {} - Active: {:?}",
                    self.debug_format(),
                    active_consumer_change.is_active
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        message: Some(message),
                        ..
                    },
                payload: Some(payload),
            } => {
                self.process_payload(message, payload).await?;
            }
            RawMessage {
                command: BaseCommand {
                    message: Some(_), ..
                },
                payload: None,
            } => {
                error!(
                    "Consumer {} received message without payload",
                    self.debug_format()
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        close_consumer: Some(CommandCloseConsumer { consumer_id, .. }),
                        ..
                    },
                ..
            } => {
                error!(
                    "Broker notification of closed consumer {}: {}",
                    consumer_id,
                    self.debug_format()
                );

                // A failed reconnect has told the consumer why, so the engine
                // stops here instead of relaying a second error.
                if self.reconnect().await.is_err() {
                    return Ok(false);
                }
            }
            unexpected => {
                let r#type = proto::base_command::Type::try_from(unexpected.command.r#type)
                    .map(|t| format!("{t:?}"))
                    .unwrap_or_else(|_| unexpected.command.r#type.to_string());
                warn!(
                    "Unexpected message type sent to consumer: {}. This is probably a bug!",
                    r#type
                );
            }
        }
        Ok(true)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_payload(
        &mut self,
        message: CommandMessage,
        mut payload: Payload,
    ) -> Result<(), Error> {
        let compression = match payload.metadata.compression {
            None => proto::CompressionType::None,
            Some(compression) => proto::CompressionType::try_from(compression).map_err(|err| {
                error!("unknown compression type: {}", compression);
                Error::Consumer(ConsumerError::Io(std::io::Error::other(format!(
                    "unknown compression type {compression}: {err}"
                ))))
            })?,
        };

        let payload = match compression {
            proto::CompressionType::None => payload,
            proto::CompressionType::Lz4 => {
                #[cfg(not(feature = "lz4"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a LZ4 compressed message but 'lz4' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "lz4")]
                {
                    let decompressed_payload = lz4::block::decompress(
                        &payload.data[..],
                        payload.metadata.uncompressed_size.map(|i| i as i32),
                    )
                    .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            proto::CompressionType::Zlib => {
                #[cfg(not(feature = "flate2"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a zlib compressed message but 'flate2' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "flate2")]
                {
                    use std::io::Read;

                    use flate2::read::ZlibDecoder;

                    let mut d = ZlibDecoder::new(&payload.data[..]);
                    let mut decompressed_payload = Vec::new();
                    d.read_to_end(&mut decompressed_payload)
                        .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            proto::CompressionType::Zstd => {
                #[cfg(not(feature = "zstd"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a zstd compressed message but 'zstd' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "zstd")]
                {
                    let decompressed_payload =
                        zstd::decode_all(&payload.data[..]).map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            proto::CompressionType::Snappy => {
                #[cfg(not(feature = "snap"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a Snappy compressed message but 'snap' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "snap")]
                {
                    use std::io::Read;

                    let mut decompressed_payload = Vec::new();
                    let mut decoder = snap::read::FrameDecoder::new(&payload.data[..]);
                    decoder
                        .read_to_end(&mut decompressed_payload)
                        .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
        };

        let payloads = if payload.metadata.num_messages_in_batch.is_some() {
            BatchedMessageIterator::new(message.message_id, payload)?.collect()
        } else {
            vec![(message.message_id, payload)]
        };
        for (message_id, payload) in payloads {
            if let Some(start) = &self.start {
                if is_at_or_before(&message_id, start) {
                    debug!(
                        "consumer engine: dropped {message_id:?}, delivered before the reconnect"
                    );
                    continue;
                }
                // The broker delivers in order, so nothing after this matches.
                self.start = None;
            }
            match (message.redelivery_count, &self.dead_letter_policy) {
                (Some(redelivery_count), Some(dead_letter_policy))
                    if redelivery_count as usize >= dead_letter_policy.max_redeliver_count =>
                {
                    // Send message to Dead Letter Topic and ack message in original topic
                    let Payload { data, metadata } = payload;
                    let mut properties = metadata
                        .properties
                        .into_iter()
                        .map(|p| (p.key, p.value))
                        .collect::<HashMap<_, _>>();
                    properties
                        .entry(SYSTEM_PROPERTY_REAL_TOPIC.to_string())
                        .or_insert_with(|| self.topic.clone());
                    properties
                        .entry(SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID.to_string())
                        .or_insert_with(|| {
                            if let Some(batch_index) = message_id.batch_index {
                                format!(
                                    "{}:{}:{}:{}",
                                    message_id.ledger_id,
                                    message_id.entry_id,
                                    message_id.partition.unwrap_or(-1),
                                    batch_index
                                )
                            } else {
                                format!(
                                    "{}:{}:{}",
                                    message_id.ledger_id,
                                    message_id.entry_id,
                                    message_id.partition.unwrap_or(-1)
                                )
                            }
                        });
                    let message = Message {
                        payload: data,
                        properties,
                        partition_key: metadata.partition_key,
                        partition_key_b64_encoded: metadata.partition_key_b64_encoded,
                        ordering_key: metadata.ordering_key,
                        event_time: metadata.event_time,
                        ..Default::default()
                    };
                    self.client
                        .send(&dead_letter_policy.dead_letter_topic, message)
                        .await?
                        .await
                        .map_err(|e| {
                            error!("One shot cancelled {:?}", e);
                            Error::Custom("DLQ send error".to_string())
                        })?;

                    self.ack(message_id, false).await;
                }
                _ => self.send_to_consumer(message_id, payload).await?,
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_to_consumer(
        &mut self,
        message_id: MessageIdData,
        payload: Payload,
    ) -> Result<(), Error> {
        if !self.first_queued_known {
            let mut delivery = self.delivery.lock().unwrap();
            if delivery.last.is_none() && delivery.first_queued.is_none() {
                delivery.first_queued = Some(message_id.clone());
            }
            self.first_queued_known = true;
        }
        self.park(Ok((self.epoch, message_id, payload)))
    }

    /// Subscribes again after the connection broke or the broker closed the
    /// consumer, and keeps trying until it succeeds, the topic is gone, or the
    /// application dropped the consumer.
    ///
    /// Messages still buffered from the old connection are not delivered. A
    /// durable subscription gets them again from the broker. A non-durable one
    /// resumes after the last message the application received.
    // C++: lib/ConsumerImpl.cc:228 ConsumerImpl::connectionOpened
    // C++: lib/ConsumerImpl.cc:1347 ConsumerImpl::clearReceiveQueue
    // C++: lib/HandlerBase.cc:182 HandlerBase::scheduleReconnection
    // Java: pulsar-client/.../impl/ConsumerImpl.java:1035 clearReceiverQueue on reconnect
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting consumer for topic: {}", self.topic);
        {
            let mut delivery = self.delivery.lock().unwrap();
            delivery.epoch += 1;
            self.epoch = delivery.epoch;
            self.pending.retain(Result::is_err);
            let non_durable = self.options.durable == Some(false);
            if let Some(target) = delivery.seek_target.take() {
                // Java: ConsumerImpl.java:3070, after a seek the target becomes
                // the exclusive start.
                if non_durable {
                    self.options.start_message_id = Some(target.clone());
                }
                self.start = Some(target);
            } else if non_durable {
                let (start_message_id, start) = match (&delivery.last, &delivery.first_queued) {
                    (Some(last), _) => (Some(last.clone()), Some(last.clone())),
                    (None, Some(first)) => (Some(first.clone()), before_in_entry(first)),
                    (None, None) => (self.configured_start.clone(), None),
                };
                self.options.start_message_id = start_message_id;
                self.start = start;
            }
            if delivery.first_queued.is_none() {
                self.first_queued_known = false;
            }
        }
        self.unacked_messages.clear();

        // send CloseConsumer to server
        // remove our resolver from the Connection consumers BTreeMap,
        // stopping the current Message source
        if let Err(e) = self.connection.sender().close_consumer(self.id).await {
            error!(
                "could not close consumer {:?}({}) for topic {}: {:?}",
                self.name, self.id, self.topic, e
            );
        }

        let mut acks = Vec::new();
        let mut attempt = 0;
        loop {
            if self.tx.is_closed() {
                return Err(Error::Consumer(ConsumerError::Closed));
            }
            match self.resubscribe().await {
                Ok(messages) => {
                    // Acks sent while disconnected go out before the permits,
                    // so the broker does not dispatch those messages again.
                    self.take_queued_acks(&mut acks);
                    for (message_id, cumulative) in std::mem::take(&mut acks) {
                        self.ack(message_id, cumulative).await;
                    }
                    match send_initial_flow(&self.connection, &self.topic, self.id, self.batch_size)
                        .await
                    {
                        Ok(()) => {
                            self.remaining_messages = self.batch_size as i64;
                            self.messages_rx = Some(messages);
                            return Ok(());
                        }
                        Err(e) => {
                            let delay = self.client.manager.backoff(attempt);
                            attempt += 1;
                            warn!(
                                "consumer {}: flow after reconnect failed, retrying in {delay:?}: {e}",
                                self.debug_format()
                            );
                            if !self.wait_disconnected(delay, &mut acks).await {
                                return Err(Error::Consumer(ConsumerError::Closed));
                            }
                        }
                    }
                }
                // Java retries everything but a deleted topic; errors that no
                // retry can fix (permissions, schema) are reported here too.
                Err(e) if e.server_error().is_some_and(error::is_permanent) => {
                    error!(
                        "consumer {}: cannot subscribe again: {e}",
                        self.debug_format()
                    );
                    self.deliver_final(e).await;
                    return Err(Error::Consumer(ConsumerError::Closed));
                }
                Err(e) => {
                    let delay = self.client.manager.backoff(attempt);
                    attempt += 1;
                    warn!(
                        "consumer {}: reconnect failed, retrying in {delay:?}: {e}",
                        self.debug_format()
                    );
                    if !self.wait_disconnected(delay, &mut acks).await {
                        return Err(Error::Consumer(ConsumerError::Closed));
                    }
                }
            }
        }
    }

    /// One attempt: look the topic up, connect to its broker, subscribe.
    async fn resubscribe(&mut self) -> Result<mpsc::UnboundedReceiver<RawMessage>, Error> {
        let broker_address = self.client.lookup_topic(&self.topic).await?;
        self.connection = self.client.manager.get_connection(&broker_address).await?;
        let messages = subscribe_once(
            &self.connection,
            &self.topic,
            &self.subscription,
            self.sub_type,
            self.id,
            &self.name,
            &self.options,
        )
        .await?;
        Ok(messages)
    }

    /// Waits out `delay` between reconnect attempts while still answering the
    /// consumer: acks are kept for the new connection, and a connection request
    /// gets the old one, so a close or seek fails fast instead of hanging.
    /// Returns false once the application has dropped the consumer.
    async fn wait_disconnected(
        &mut self,
        delay: Duration,
        acks: &mut Vec<(MessageIdData, bool)>,
    ) -> bool {
        let mut timer = Box::pin(self.client.executor.delay(delay));
        loop {
            // The redelivery ticker keeps the engine channel open after the
            // consumer is dropped, so the message channel tells instead.
            if self.tx.is_closed() {
                return false;
            }
            let event = match futures::future::select(&mut timer, self.event_rx.next()).await {
                futures::future::Either::Left(_) => return true,
                futures::future::Either::Right((event, _)) => event,
            };
            match event {
                Some(EngineEvent::EngineMessage(Some(EngineMessage::Ack(id, cumulative)))) => {
                    acks.push((id, cumulative));
                }
                Some(EngineEvent::EngineMessage(Some(EngineMessage::GetConnection(sender)))) => {
                    let _ = sender.send(self.connection.clone());
                }
                // the broker redelivers what is unacked once the consumer is back
                Some(EngineEvent::EngineMessage(Some(
                    EngineMessage::Nack(_) | EngineMessage::UnackedRedelivery,
                ))) => {}
                // frames from the old connection
                Some(EngineEvent::Message(..)) => {}
                Some(EngineEvent::EngineMessage(None)) | None => return false,
            }
        }
    }

    /// Queues `item` for the consumer and hands over what fits.
    fn park(&mut self, item: Result<(u64, MessageIdData, Payload), Error>) -> Result<(), Error> {
        self.pending.push_back(item);
        self.flush_pending()
    }

    /// Hands parked messages to the consumer while its queue has room. Fails
    /// once the application has dropped the consumer.
    fn flush_pending(&mut self) -> Result<(), Error> {
        while let Some(item) = self.pending.pop_front() {
            // the redelivery clock starts once the application can have it
            let tracked = match (&item, self.unacked_message_redelivery_delay) {
                (Ok((_, id, _)), Some(delay)) => Some((id.clone(), delay)),
                _ => None,
            };
            match self.tx.try_send(item) {
                Ok(()) => {
                    if let Some((id, delay)) = tracked {
                        self.unacked_messages.insert(id, Instant::now() + delay);
                    }
                }
                Err(e) if e.is_full() => {
                    self.pending.push_front(e.into_inner());
                    return Ok(());
                }
                Err(_) => {
                    error!("consumer {}: the consumer is gone", self.debug_format());
                    return Err(Error::Consumer(ConsumerError::Closed));
                }
            }
        }
        // a burst can leave the queue large; do not keep that memory around
        if self.pending.capacity() > 64 {
            self.pending.shrink_to(64);
        }
        Ok(())
    }

    /// Takes acks already queued behind the reconnect and answers connection
    /// requests, without waiting for more.
    fn take_queued_acks(&mut self, acks: &mut Vec<(MessageIdData, bool)>) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                EngineEvent::EngineMessage(Some(EngineMessage::Ack(id, cumulative))) => {
                    acks.push((id, cumulative));
                }
                EngineEvent::EngineMessage(Some(EngineMessage::GetConnection(sender))) => {
                    let _ = sender.send(self.connection.clone());
                }
                // old frames, and nacks or redelivery ticks the new
                // subscription makes moot
                _ => {}
            }
        }
    }

    /// Hands the error the engine stops on to the application, still
    /// answering connection requests while the consumer's queue is full.
    async fn deliver_final(&mut self, error: Error) {
        self.pending.push_back(Err(error));
        loop {
            if self.flush_pending().is_err() || self.pending.is_empty() {
                return;
            }
            match self.room_or_event().await {
                futures::future::Either::Right(Some(EngineEvent::EngineMessage(Some(
                    EngineMessage::GetConnection(sender),
                )))) => {
                    let _ = sender.send(self.connection.clone());
                }
                futures::future::Either::Right(None)
                | futures::future::Either::Right(Some(EngineEvent::EngineMessage(None))) => return,
                _ => {}
            }
        }
    }

    /// Waits until the consumer's queue has room (left) or another event
    /// arrives (right), whichever comes first.
    async fn room_or_event(&mut self) -> futures::future::Either<(), Option<EngineEvent<Exe>>> {
        let tx = &mut self.tx;
        let room = futures::future::poll_fn(|cx| tx.poll_ready(cx));
        match futures::future::select(room, self.event_rx.next()).await {
            futures::future::Either::Left(_) => futures::future::Either::Left(()),
            futures::future::Either::Right((event, _)) => futures::future::Either::Right(event),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn debug_format(&self) -> String {
        format!(
            "[{id} - {subscription}{name}: {topic}]",
            id = self.id,
            subscription = self.subscription,
            name = self
                .name
                .as_ref()
                .map(|s| format!("({s})"))
                .unwrap_or_default(),
            topic = self.topic
        )
    }

    #[cfg(feature = "async-std")]
    async fn timeout<F: Future<Output = O>, O>(
        fut: F,
        dur: Duration,
    ) -> Result<O, async_std::future::TimeoutError> {
        use async_std::prelude::FutureExt;
        fut.timeout(dur).await
    }

    #[cfg(all(not(feature = "async-std"), feature = "tokio"))]
    async fn timeout<F: Future<Output = O>, O>(
        fut: F,
        dur: Duration,
    ) -> Result<O, tokio::time::error::Elapsed> {
        tokio::time::timeout_at(tokio::time::Instant::now() + dur, fut).await
    }
}

impl<Exe: Executor> std::ops::Drop for ConsumerEngine<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
        let _ = self.client.executor.spawn(Box::pin(async move {
            if let Err(e) = conn.sender().close_consumer(id).await {
                error!(
                    "could not close consumer {:?}({}) for topic {}: {:?}",
                    name, id, topic, e
                );
            }
        }));
    }
}

/// Whether `id` was already delivered when the subscription restarts from
/// `start`. The broker resends the whole entry `start` is in, so for a batch
/// the earlier members of that entry are dropped too.
// C++: lib/ConsumerImpl.cc:793 messageReceived, prior-entry check
// C++: lib/ConsumerImpl.cc:971 receiveIndividualMessagesFromBatch, prior-batch-index check
fn is_at_or_before(id: &MessageIdData, start: &MessageIdData) -> bool {
    if id.ledger_id != start.ledger_id {
        return false;
    }
    match id.batch_index {
        Some(index) => {
            id.entry_id < start.entry_id
                || (id.entry_id == start.entry_id && index <= start.batch_index.unwrap_or(-1))
        }
        None => id.entry_id <= start.entry_id,
    }
}

/// The filter that keeps `first` and drops what precedes it in its entry,
/// since the broker resends the whole entry. Nothing precedes a message that
/// is not in a batch, or that opens one.
// C++: lib/ConsumerImpl.cc:1371 clearReceiveQueue, previous message in the batch
fn before_in_entry(first: &MessageIdData) -> Option<MessageIdData> {
    let index = first.batch_index.filter(|&index| index > 0)?;
    Some(MessageIdData {
        batch_index: Some(index - 1),
        ..first.clone()
    })
}
