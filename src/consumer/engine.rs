use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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
        data::{
            position, DeadLetterPolicy, Delivery, EngineEvent, EngineMessage, Queued, Resume, Seek,
            SeekReply,
        },
        gate::{self, Gate, Told},
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
    /// Unbounded: the permits bound what the broker sends, and the consumer
    /// grants them as the application takes messages.
    tx: mpsc::UnboundedSender<Result<Queued, Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    delivery: Arc<Mutex<Delivery>>,
    /// The epoch of the current connection, as last written to `delivery`.
    epoch: u64,
    /// Messages at or before this id are dropped, as the broker sends the entry
    /// a subscription restarts from again.
    start: Option<MessageIdData>,
    /// When the first subscribe succeeded, which the configured rollback
    /// counts from.
    subscribed_at: Instant,
    /// After a rollback to a seek's publish time, which the protocol rounds
    /// to whole seconds: messages published before it are dropped, until the
    /// first one at or after it.
    // Java: the broker alone seeks by time; this keeps a reconnect as exact
    time_floor: Option<u64>,
    /// Seeks that came while the consumer was reconnecting, to make once it
    /// is back.
    deferred_seeks: VecDeque<(Seek, Instant, SeekReply)>,
    /// Whether `Delivery::resume` is past waiting for the first queued message
    /// on this connection, which spares a lock per message.
    first_queued_known: bool,
    /// Whether the broker's messages on this connection go to the
    /// application.
    gate: Gate,
    engine_rx: Option<mpsc::UnboundedReceiver<EngineMessage<Exe>>>,
    event_rx: mpsc::UnboundedReceiver<EngineEvent<Exe>>,
    event_tx: UnboundedSender<EngineEvent<Exe>>,
    /// Permits granted on each (re)subscribe.
    window: u32,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    /// Members acked so far of batch entries not yet acked as a whole, by
    /// (ledger, entry).
    batch_acks: BTreeMap<(u64, u64), BatchAcks>,
    /// The size of each batch entry a durable subscription delivered and has
    /// not acked whole, for acks by an id that lacks it.
    batch_sizes: BTreeMap<(u64, u64), i32>,
    /// Permits of the frame being processed that are accounted for, by a
    /// delivery or a refund, so a failure refunds only the rest.
    frame_accounted: u32,
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
        tx: mpsc::UnboundedSender<Result<Queued, Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        delivery: Arc<Mutex<Delivery>>,
        engine_rx: mpsc::UnboundedReceiver<EngineMessage<Exe>>,
        window: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> ConsumerEngine<Exe> {
        let (event_tx, event_rx) = mpsc::unbounded();
        let (start, first_queued_known) = {
            let delivery = delivery.lock().unwrap();
            (
                start_filter(&delivery.resume),
                matches!(delivery.resume, Resume::From(_) | Resume::After(_)),
            )
        };
        let mut engine = ConsumerEngine {
            client,
            connection,
            topic,
            subscription,
            sub_type,
            id,
            name,
            tx,
            messages_rx: Some(messages_rx),
            delivery,
            epoch: 0,
            start,
            subscribed_at: Instant::now(),
            deferred_seeks: VecDeque::new(),
            time_floor: None,
            first_queued_known,
            gate: Gate::Unguarded,
            engine_rx: Some(engine_rx),
            event_rx,
            event_tx,
            window,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            batch_acks: BTreeMap::new(),
            batch_sizes: BTreeMap::new(),
            frame_accounted: 0,
            dead_letter_policy,
            options,
        };
        engine.gate = engine.new_gate();
        engine
    }

    /// The gate of this consumer on its current connection.
    fn new_gate(&self) -> Gate {
        Gate::new(
            self.sub_type,
            &self.options,
            &self.topic,
            self.connection.sender().protocol_version(),
        )
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
        self.start_ticker()?;

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

            if let Some((seek, started, reply)) = self.deferred_seeks.pop_front() {
                if !self.answer_seek(seek, started, reply).await {
                    return Ok(());
                }
                continue;
            }

            if let Some(engine_rx) = self.engine_rx.take() {
                self.register_source(engine_rx, |msg| EngineEvent::EngineMessage(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering engine_rx source"))
                    })?;
            }

            match self.event_rx.next().await {
                Some(EngineEvent::Message(epoch, _)) if epoch != self.epoch => {
                    debug!(
                        "consumer {}: dropped a frame from a previous connection",
                        self.debug_format()
                    );
                }
                Some(EngineEvent::Message(_, msg)) => {
                    trace!(
                        "consumer {}: received message, {:?}",
                        self.debug_format(),
                        msg
                    );
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Some(EngineEvent::EngineMessage(msg)) => {
                    trace!("consumer {}: received engine message", self.debug_format());
                    let continue_loop = self.handle_ack_opt(msg).await;
                    if !continue_loop {
                        return Ok(());
                    }
                }
                // wakes the loop, whose top checks the connection
                Some(EngineEvent::Tick) => self.redeliver_if_quiet().await,
                Some(EngineEvent::RedeliveryTick) => self.redeliver_unacked().await,
                None => {
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
                let charged = gate::charged(&message);
                self.frame_accounted = 0;
                match self.process_message(message, charged).await {
                    // Continue
                    Ok(true) => None,
                    // End of Topic
                    Ok(false) => Some(Ok(())),
                    Err(e) => {
                        // the broker charged permits the application never
                        // gets messages for
                        let rest = charged.saturating_sub(self.frame_accounted);
                        self.refund_permits(rest).await;
                        self.deliver(Err(e)).err().map(Err)
                    }
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
            // The broker takes redelivery requests from the active consumer
            // only, and sends everything unacked when it names one.
            Some(EngineMessage::Nack(_)) if self.gate.is_closed() => true,
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
            Some(EngineMessage::Flow(epoch, permits)) => {
                // a flow for an earlier connection is moot: the new
                // subscription got a fresh window
                if epoch == self.epoch {
                    self.send_flow(permits).await;
                }
                true
            }
            Some(EngineMessage::Seek(seek, started, reply)) => {
                self.answer_seek(seek, started, reply).await
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
        self.track_taken();
        let tracked = MessageIdData {
            batch_size: None,
            ..message_id.clone()
        };
        if cumulative {
            let acked = position(&tracked);
            self.unacked_messages.retain(|id, _| position(id) > acked);
        } else {
            self.unacked_messages.remove(&tracked);
        }
        let Some(target) = self.ack_target(message_id, cumulative) else {
            return;
        };
        let res = self
            .connection
            .sender()
            .send_ack(self.id, vec![target], cumulative)
            .await;
        if res.is_err() {
            error!("ack error: {:?}", res);
        }
    }

    /// What to send the broker for an ack of `id`. An ack of a batch member
    /// without an `ack_set` would acknowledge the whole entry. With
    /// batch-index acks, a partly acked batch is sent with its `ack_set`.
    /// Without them, an individual ack waits until every member is acked,
    /// and a cumulative ack in the middle of a batch covers only the entry
    /// before it; the rest of that batch then comes again after a restart.
    /// An id without a batch size is sent as it is.
    // Java: PersistentAcknowledgmentsGroupingTracker.addAcknowledgment,
    // doImmediateBatchIndexAck, MessageIdAdvUtils.acknowledge and prevMessageId
    fn ack_target(&mut self, id: MessageIdData, cumulative: bool) -> Option<MessageIdData> {
        let key = (id.ledger_id, id.entry_id);
        if cumulative {
            // entries before this one are covered; this one keeps its marks
            self.batch_acks = self.batch_acks.split_off(&key);
            self.batch_sizes = self.batch_sizes.split_off(&key);
        }
        // An id from Message::message_id has no batch size; acking its entry
        // whole would ack members not taken yet.
        let size = id
            .batch_size
            .or_else(|| self.batch_sizes.get(&key).copied());
        let (Some(index), Some(size)) = (id.batch_index, size) else {
            return Some(id);
        };
        let acks = batch_acks_of(&mut self.batch_acks, key, size);
        if cumulative {
            acks.mark_up_to(index);
        } else {
            acks.mark(index);
        }
        if acks.is_complete() {
            self.batch_acks.remove(&key);
            self.batch_sizes.remove(&key);
            return Some(whole_entry(id));
        }
        if self.options.batch_index_ack != Some(false) {
            return Some(MessageIdData {
                ack_set: acks.ack_set(),
                ..whole_entry(id)
            });
        }
        if !cumulative {
            return None;
        }
        // Entry 0 has none before it in this ledger; Java sends (ledger, -1),
        // which covers every earlier ledger.
        Some(MessageIdData {
            entry_id: id.entry_id.wrapping_sub(1),
            ..whole_entry(id)
        })
    }

    /// Process the message, on which the broker spent `charged` permits.
    /// Returns `true` if there are more messages to process
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_message(&mut self, message: RawMessage, charged: u32) -> Result<bool, Error> {
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
                debug!(
                    "Active consumer change for {} - Active: {:?}",
                    self.debug_format(),
                    active_consumer_change.is_active
                );
                self.told(active_consumer_change.is_active.unwrap_or(false))
                    .await?;
            }
            RawMessage {
                command:
                    BaseCommand {
                        message: Some(message),
                        ..
                    },
                ..
            } if self.gate.is_closed() => {
                self.gate.withhold(charged);
                self.delivery.lock().unwrap().withheld += u64::from(charged);
                debug!(
                    "consumer engine: withheld {} {}:{}",
                    self.topic, message.message_id.ledger_id, message.message_id.entry_id
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
                self.gate.passed();
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
                self.refund_permits(1).await;
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

        let batch_size = payload.metadata.num_messages_in_batch;
        let entry = (message.message_id.ledger_id, message.message_id.entry_id);
        let payloads = if batch_size.is_some() {
            BatchedMessageIterator::new(message.message_id, payload)?.collect()
        } else {
            vec![(message.message_id, payload)]
        };
        if let Some(size) = batch_size.filter(|_| self.options.durable != Some(false)) {
            // bounded: a consumer that never acks keeps the newest
            if self.batch_sizes.len() >= MAX_BATCH_SIZES {
                self.batch_sizes.pop_first();
            }
            self.batch_sizes.insert(entry, size);
        }
        let acked_before = !message.ack_set.is_empty();
        if let Some(size) = batch_size.filter(|_| acked_before) {
            batch_acks_of(&mut self.batch_acks, entry, size).keep_only(&message.ack_set);
        }
        for (message_id, payload) in payloads {
            if let Some(floor) = self.time_floor {
                if payload.metadata.publish_time < floor {
                    debug!("consumer engine: dropped {message_id:?}, published before the seek");
                    self.refund_permit().await;
                    continue;
                }
                self.time_floor = None;
            }
            // The broker charged no permit for a member it reports acked.
            if acked_before
                && message_id
                    .batch_index
                    .is_some_and(|index| !is_set(&message.ack_set, index))
            {
                debug!("consumer engine: skipped {message_id:?}, acked before");
                continue;
            }
            if let Some(start) = &self.start {
                if is_at_or_before(&message_id, start) {
                    debug!(
                        "consumer engine: dropped {message_id:?}, delivered before the reconnect"
                    );
                    // the application never sees it, so it never acks it
                    if let (Some(index), Some(size)) = (message_id.batch_index, batch_size) {
                        batch_acks_of(&mut self.batch_acks, entry, size).mark(index);
                    }
                    self.refund_permit().await;
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

                    // with the batch size, so only this member counts as acked
                    self.ack(
                        MessageIdData {
                            batch_size,
                            ..message_id
                        },
                        false,
                    )
                    .await;
                    self.refund_permit().await;
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
            // A message from before a seek is stale, and resumes nothing.
            if delivery.epoch == self.epoch {
                if matches!(delivery.resume, Resume::Configured | Resume::Time(_)) {
                    delivery.resume = Resume::From(message_id.clone());
                }
                self.first_queued_known = true;
            }
        }
        self.deliver(Ok(Queued::Message(self.epoch, message_id, payload)))
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
        self.reconnect_announcing(true).await
    }

    /// `announce`: tell a Failover application it may have lost the
    /// partition. Not after its own seek: the broker hands the partition back
    /// unless another consumer has it, and says so either way, while a
    /// BecameInactive would make an application that seeks on activation
    /// seek for ever.
    async fn reconnect_announcing(&mut self, announce: bool) -> Result<(), Error> {
        debug!("consumer {}: reconnecting", self.debug_format());
        let (mut options, rollback) = {
            let mut delivery = self.delivery.lock().unwrap();
            delivery.epoch += 1;
            self.epoch = delivery.epoch;
            delivery.reconnecting();
            if let Some(taken) = &mut delivery.taken {
                taken.clear();
            }
            // Java: ConsumerImpl.java:3070, after a seek the target becomes
            // the start; after a seek by time nothing is filtered.
            if let Some(seek) = delivery.pending_seek.take() {
                self.start = start_filter(&seek.resume());
            }
            let mut options = self.options.clone();
            let rollback = if self.options.durable == Some(false) {
                // The broker applies a rollback on every subscribe, so the
                // configured one goes only until delivery moved.
                let (start_message_id, start, rollback) = match &delivery.resume {
                    Resume::Configured => (
                        self.options.start_message_id.clone(),
                        None,
                        self.options
                            .start_rollback
                            .map(|rollback| Rollback::FromFirstSubscribe {
                                at: self.subscribed_at,
                                rollback,
                            }),
                    ),
                    // The broker keeps the subscription across its own seek;
                    // a later reconnect may find it gone.
                    // Newer brokers retire a non-durable subscription on its own
                    // seek, so the rollback goes again; the floor drops what its
                    // rounding to whole seconds lets in.
                    Resume::Time(publish_time_ms) => {
                        self.time_floor = Some(*publish_time_ms);
                        (None, None, Some(Rollback::ToPublishTime(*publish_time_ms)))
                    }
                    Resume::From(id) | Resume::After(id) => {
                        (Some(id.clone()), start_filter(&delivery.resume), None)
                    }
                };
                options.start_message_id = start_message_id;
                self.start = start;
                rollback
            } else {
                None
            };
            self.first_queued_known = matches!(delivery.resume, Resume::From(_) | Resume::After(_));
            (options, rollback)
        };
        // Another consumer may receive the partition until this one is back;
        // the broker says so again if it is still ours.
        if announce {
            let _ = self.announce(false);
        }
        self.unacked_messages.clear();
        // Java ties batch ack state to the delivered ids, which a reconnect
        // delivers anew.
        self.batch_acks.clear();
        self.batch_sizes.clear();

        // Close the consumer on the old connection, without waiting on a broken
        // one: a frozen broker would hold the close for the whole operation
        // timeout, and frames from it are stale by their epoch anyway.
        if self.connection.is_valid() {
            if let Err(e) = self.connection.sender().close_consumer(self.id).await {
                error!(
                    "could not close consumer {:?}({}) for topic {}: {:?}",
                    self.name, self.id, self.topic, e
                );
            }
        } else {
            // A ping timeout may have flagged a stalled socket that still
            // works: the broker should drop the old consumer, or an Exclusive
            // one would refuse the new subscription as busy.
            let connection = self.connection.clone();
            let id = self.id;
            let _ = self.client.executor.spawn(Box::pin(async move {
                let _ = connection.sender().close_consumer(id).await;
            }));
        }

        let mut acks = Vec::new();
        let mut attempt = 0;
        let mut reported = false;
        loop {
            if self.tx.is_closed() {
                return Err(Error::Consumer(ConsumerError::Closed));
            }
            options.start_rollback = rollback.map(Rollback::now);
            match self.resubscribe(&options).await {
                Ok(messages) => {
                    // Acks sent while disconnected go out before the permits,
                    // so the broker does not dispatch those messages again.
                    let closed = self.take_queued_acks(&mut acks);
                    for (message_id, cumulative) in std::mem::take(&mut acks) {
                        self.ack(message_id, cumulative).await;
                    }
                    if closed {
                        return Err(Error::Consumer(ConsumerError::Closed));
                    }
                    let free = {
                        let mut delivery = self.delivery.lock().unwrap();
                        delivery.grant(self.window)
                    };
                    let flow = if free > 0 {
                        send_initial_flow(&self.connection, &self.topic, self.id, free).await
                    } else {
                        Ok(())
                    };
                    match flow {
                        Ok(()) => {
                            self.messages_rx = Some(messages);
                            self.gate = self.new_gate();
                            debug!(
                                "consumer {}: subscribed on connection {}, {:?}",
                                self.debug_format(),
                                self.connection.id(),
                                self.gate
                            );
                            return Ok(());
                        }
                        Err(e) => {
                            // A new epoch: flows the application sent for this
                            // grant are moot, and the next attempt grants anew.
                            {
                                let mut delivery = self.delivery.lock().unwrap();
                                delivery.epoch += 1;
                                self.epoch = delivery.epoch;
                                delivery.reconnecting();
                            }
                            let delay = self
                                .client
                                .manager
                                .connection_retry_options()
                                .backoff(attempt);
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
                // Java retries everything but a deleted topic.
                Err(e) if e.server_error().is_some_and(error::ends_after_creation) => {
                    error!(
                        "consumer {}: cannot subscribe again: {e}",
                        self.debug_format()
                    );
                    let _ = self.deliver(Err(e));
                    return Err(Error::Consumer(ConsumerError::Closed));
                }
                Err(e) => {
                    let delay = self
                        .client
                        .manager
                        .connection_retry_options()
                        .backoff(attempt);
                    attempt += 1;
                    warn!(
                        "consumer {}: reconnect failed, retrying in {delay:?}: {e}",
                        self.debug_format()
                    );
                    // A refused credential, or a refusal by authorization or
                    // policy, needs someone to act: the application hears it
                    // once an outage, and the consumer keeps trying.
                    if !reported && e.needs_attention() {
                        reported = true;
                        let _ = self.deliver(Err(e));
                    }
                    if !self.wait_disconnected(delay, &mut acks).await {
                        return Err(Error::Consumer(ConsumerError::Closed));
                    }
                }
            }
        }
    }

    /// One attempt: look the topic up, connect to its broker, subscribe.
    async fn resubscribe(
        &mut self,
        options: &ConsumerOptions,
    ) -> Result<mpsc::UnboundedReceiver<RawMessage>, Error> {
        let broker_address = self.client.lookup_topic_once(&self.topic).await?;
        // One attempt: if the broker does not answer, the next attempt looks
        // the topic up again, which finds it once ownership has moved.
        self.connection = self
            .client
            .manager
            .get_connection_retrying(&broker_address, 0)
            .await?;
        let messages = subscribe_once(
            &self.connection,
            &self.topic,
            &self.subscription,
            self.sub_type,
            self.id,
            &self.name,
            options,
        )
        .await?;
        Ok(messages)
    }

    /// Waits out `delay` between reconnect attempts while still answering the
    /// consumer: acks are kept for the new connection, a connection request
    /// gets the old one, so a close fails fast instead of hanging, and a seek
    /// waits for the reconnect. Returns false once the application has
    /// dropped the consumer.
    async fn wait_disconnected(
        &mut self,
        delay: Duration,
        acks: &mut Vec<(MessageIdData, bool)>,
    ) -> bool {
        let mut timer = Box::pin(self.client.executor.delay(delay));
        loop {
            // the application dropped the consumer
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
                Some(EngineEvent::EngineMessage(Some(EngineMessage::Seek(
                    seek,
                    started,
                    reply,
                )))) => {
                    self.deferred_seeks.push_back((seek, started, reply));
                }
                // the broker redelivers what is unacked once the consumer is
                // back, and the new subscription gets a fresh window
                Some(EngineEvent::EngineMessage(Some(_))) => {}
                // frames from the old connection, and ticks
                Some(
                    EngineEvent::Message(..) | EngineEvent::Tick | EngineEvent::RedeliveryTick,
                ) => {}
                Some(EngineEvent::EngineMessage(None)) | None => return false,
            }
        }
    }

    /// Takes acks already queued behind the reconnect and answers connection
    /// requests, without waiting for more.
    /// Returns true if the application dropped the consumer meanwhile.
    fn take_queued_acks(&mut self, acks: &mut Vec<(MessageIdData, bool)>) -> bool {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                EngineEvent::EngineMessage(Some(EngineMessage::Ack(id, cumulative))) => {
                    acks.push((id, cumulative));
                }
                EngineEvent::EngineMessage(Some(EngineMessage::GetConnection(sender))) => {
                    let _ = sender.send(self.connection.clone());
                }
                EngineEvent::EngineMessage(Some(EngineMessage::Seek(seek, started, reply))) => {
                    self.deferred_seeks.push_back((seek, started, reply));
                }
                // the application dropped the consumer
                EngineEvent::EngineMessage(None) => return true,
                // old frames, and nacks, ticks and flows the new subscription
                // makes moot
                _ => {}
            }
        }
        false
    }

    /// Gives back the permit of a message the application never gets, so the
    /// window does not shrink.
    // Java: ConsumerImpl.java:1796 refunds what the broker charged for a
    // dropped entry
    async fn refund_permit(&mut self) {
        self.refund_permits(1).await;
    }

    async fn refund_permits(&mut self, permits: u32) {
        self.frame_accounted += permits;
        if permits == 0 {
            return;
        }
        let flow = self.delivery.lock().unwrap().free_permits(permits);
        if let Some(permits) = flow {
            self.send_flow(permits).await;
        }
    }

    fn start_ticker(&mut self) -> Result<(), Error> {
        self.tick_every(Duration::from_secs(1), || EngineEvent::Tick)?;
        // Through the engine's own events, so the engine channel still closes
        // when the application drops the consumer.
        if let Some(delay) = self.unacked_message_redelivery_delay {
            self.tick_every(delay.min(Duration::from_millis(500)), || {
                EngineEvent::RedeliveryTick
            })?;
        }
        Ok(())
    }

    /// Sends the engine `event` every `period`, until the engine is gone.
    fn tick_every(
        &mut self,
        period: Duration,
        event: impl Fn() -> EngineEvent<Exe> + Send + 'static,
    ) -> Result<(), Error> {
        let mut tick_tx = self.event_tx.clone();
        let mut interval = self.client.executor.interval(period);
        self.client
            .executor
            .spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if tick_tx.send(event()).await.is_err() {
                        break;
                    }
                }
            }))
            .map_err(|_| Error::Executor)
    }

    /// Asks the broker to send again what the application took and has not
    /// acked within the redelivery delay.
    async fn redeliver_unacked(&mut self) {
        self.track_taken();
        if self.gate.is_closed() {
            return;
        }
        let now = Instant::now();
        let ids: Vec<_> = self
            .unacked_messages
            .iter()
            .filter(|(_, due)| **due < now)
            .map(|(id, _)| id.clone())
            .collect();
        if ids.is_empty() {
            return;
        }
        match self
            .connection
            .sender()
            .send_redeliver_unacknowleged_messages(self.id, ids.clone())
            .await
        {
            Ok(_) => {
                for id in &ids {
                    self.unacked_messages.remove(id);
                }
            }
            Err(e) => error!("could not ask for redelivery: {:?}", e),
        }
    }

    /// Moves the subscription for a seek and answers the application, which
    /// may have stopped waiting: a seek is seen through either way. False if
    /// the consumer cannot go on.
    async fn answer_seek(&mut self, seek: Seek, started: Instant, reply: SeekReply) -> bool {
        match self.seek(seek, started).await {
            Ok(result) => {
                let _ = reply.send(result);
                true
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                false
            }
        }
    }

    /// Moves the subscription to `seek`, made at `started`. The outer error
    /// ends the consumer; the inner one is the seek's.
    ///
    /// The broker closes the consumer before it confirms, and the subscription
    /// starts again at the target. A request lost with its connection, or
    /// that found the broker not ready, goes again once the consumer is back,
    /// within the operation timeout; for a reader, subscribing again at the
    /// target is the seek. A seek the broker refuses, or that runs out of
    /// time, leaves the subscription where it was.
    // Java: ConsumerImpl.java:3043 seekAsyncInternal
    async fn seek(&mut self, seek: Seek, started: Instant) -> Result<Result<(), Error>, Error> {
        let resume = seek.resume();
        let (message_id, timestamp) = match &seek {
            Seek::At(target) | Seek::After(target) => (Some(target.clone()), None),
            Seek::Time(publish_time_ms) => (None, Some(*publish_time_ms)),
        };
        // it waited for a reconnect that took all its time
        if started.elapsed() >= self.client.operation_retry_options.operation_timeout {
            return Ok(Err(seek_timed_out()));
        }
        let max_backoff = self.client.manager.connection_retry_options().max_backoff;
        let mut before = None;
        let mut attempt = 0;
        loop {
            let stale = {
                // What is buffered from before the seek must not reach the
                // application, as Java clears its queue first.
                // Java: MultiTopicsConsumerImpl.java:944 beforeSeek
                let mut delivery = self.delivery.lock().unwrap();
                before.get_or_insert_with(|| delivery.resume.clone());
                delivery.epoch += 1;
                delivery.resume = resume.clone();
                delivery.stale
            };
            let answer = self
                .connection
                .sender()
                .seek(self.id, message_id.clone(), timestamp)
                .await;
            let error = match answer {
                Ok(_) => {
                    // the broker moved, and closed the consumer
                    self.delivery.lock().unwrap().pending_seek = Some(seek);
                    self.reconnect_announcing(false).await?;
                    return Ok(Ok(()));
                }
                Err(e) => e,
            };
            let delay = lost_or_not_ready(&error)
                .then(|| {
                    self.client
                        .operation_retry_options
                        .next_delay(started, attempt, max_backoff)
                })
                .flatten();
            let Some(delay) = delay else {
                // refused, or out of time: back where it was
                let before = before.take().expect("set on the first attempt");
                self.undo_seek(before, stale).await?;
                return Ok(Err(error.into()));
            };
            warn!(
                "consumer {}: seek failed, trying again in {delay:?}: {error}",
                self.debug_format()
            );
            // The broker may not have moved, so nothing is filtered; a reader
            // subscribes at the resume point, the target.
            self.reconnect().await?;
            if self.options.durable == Some(false) {
                return Ok(Ok(()));
            }
            // A durable cursor moves only by a seek, which the reconnect may
            // have outlasted the time for.
            if started.elapsed() >= self.client.operation_retry_options.operation_timeout {
                self.delivery.lock().unwrap().resume =
                    before.take().expect("set on the first attempt");
                return Ok(Err(seek_timed_out()));
            }
            self.client.executor.delay(delay).await;
            attempt += 1;
        }
    }

    /// Puts the resume point back after a seek that did not happen. The
    /// epoch goes back too if the application skipped nothing as stale
    /// meanwhile; otherwise a new subscription delivers that again.
    async fn undo_seek(&mut self, before: Resume, stale: u64) -> Result<(), Error> {
        let untouched = {
            let mut delivery = self.delivery.lock().unwrap();
            delivery.resume = before;
            let untouched = delivery.stale == stale;
            if untouched {
                delivery.epoch = self.epoch;
            }
            untouched
        };
        if !untouched {
            self.reconnect().await?;
        }
        Ok(())
    }

    /// Starts the redelivery clock for messages the application has taken
    /// since the last call.
    fn track_taken(&mut self) {
        let Some(delay) = self.unacked_message_redelivery_delay else {
            return;
        };
        let taken = self
            .delivery
            .lock()
            .unwrap()
            .taken
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        for (message_id, at) in taken {
            self.unacked_messages.insert(message_id, at + delay);
        }
    }

    /// The broker said whether this consumer is the `active` one of its
    /// Failover subscription.
    async fn told(&mut self, active: bool) -> Result<(), Error> {
        match self.gate.told(active) {
            Told::Opened { withheld } => {
                debug!(
                    "consumer {}: gate opened, {withheld} permits withheld",
                    self.debug_format()
                );
                self.announce(true)?;
                self.refund_permits(withheld).await;
            }
            // The broker rewound to the subscription's position: what this
            // consumer was handed since it last heard so comes again, and
            // if another consumer was active in between, the broker may
            // have sent this one entries past those it sent the other. The
            // application hears that it lost the partition and has it back.
            Told::ActiveAgain { handed: true } => {
                self.announce(false)?;
                self.announce(true)?;
            }
            Told::ActiveAgain { handed: false } => {}
            Told::Other => self.announce(active)?,
        }
        Ok(())
    }

    /// Asks the broker to send from the subscription's position again, once
    /// the gate says enough ticks passed without a message.
    async fn redeliver_if_quiet(&mut self) {
        if !self.gate.tick() {
            return;
        }
        let asked = self
            .connection
            .sender()
            .send_redeliver_unacknowleged_messages(self.id, Vec::new())
            .await;
        if let Err(e) = asked {
            warn!(
                "consumer {}: could not ask for redelivery: {e}",
                self.debug_format()
            );
        }
    }

    /// Tells the application that this Failover consumer receives the
    /// partition from now on, or may not any more. The consumer drops what
    /// repeats the last thing the application heard.
    // Java: ConsumerImpl.java activeConsumerChanged
    fn announce(&mut self, active: bool) -> Result<(), Error> {
        if self.sub_type != SubType::Failover {
            return Ok(());
        }
        self.deliver(Ok(Queued::Active(active)))
    }

    fn deliver(&mut self, item: Result<Queued, Error>) -> Result<(), Error> {
        if let Ok(Queued::Message(..)) = item {
            self.frame_accounted += 1;
            self.delivery.lock().unwrap().queued += 1;
        }
        self.tx.unbounded_send(item).map_err(|_| {
            error!("consumer {}: the consumer is gone", self.debug_format());
            Error::Consumer(ConsumerError::Closed)
        })
    }

    /// A broken connection is noticed at the top of the loop, and the
    /// reconnect grants a fresh window, so a failed flow needs no retry.
    async fn send_flow(&mut self, permits: u32) {
        if let Err(e) = self.connection.sender().send_flow(self.id, permits).await {
            warn!("consumer {}: flow failed: {e}", self.debug_format());
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

/// The members of one batch entry not acked yet: a set bit is a member
/// still unacked, as in the protocol's `ack_set`.
struct BatchAcks {
    unacked: Vec<u64>,
}

impl BatchAcks {
    fn new(size: i32) -> Self {
        let size = size.max(1) as usize;
        let mut unacked = vec![u64::MAX; size.div_ceil(64)];
        if !size.is_multiple_of(64) {
            *unacked.last_mut().expect("at least one word") = (1u64 << (size % 64)) - 1;
        }
        BatchAcks { unacked }
    }

    fn mark(&mut self, member: i32) {
        if let Some((word, bit)) = bit_of(member) {
            if let Some(word) = self.unacked.get_mut(word) {
                *word &= !bit;
            }
        }
    }

    fn mark_up_to(&mut self, member: i32) {
        let Ok(member) = usize::try_from(member) else {
            return;
        };
        let full_words = (member + 1) / 64;
        for word in self.unacked.iter_mut().take(full_words) {
            *word = 0;
        }
        let rest = (member + 1) % 64;
        if rest != 0 {
            if let Some(word) = self.unacked.get_mut(full_words) {
                *word &= !((1u64 << rest) - 1);
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.unacked.iter().all(|&word| word == 0)
    }

    /// Forgets the members the broker's `ack_set` marks acked.
    fn keep_only(&mut self, ack_set: &[i64]) {
        for (index, word) in self.unacked.iter_mut().enumerate() {
            *word &= ack_set.get(index).map_or(0, |&set| set as u64);
        }
    }

    /// The members still unacked, as the protocol's `ack_set`.
    fn ack_set(&self) -> Vec<i64> {
        self.unacked.iter().map(|&word| word as i64).collect()
    }
}

/// The acks kept for the batch entry `key` of `size` members.
fn batch_acks_of(
    batch_acks: &mut BTreeMap<(u64, u64), BatchAcks>,
    key: (u64, u64),
    size: i32,
) -> &mut BatchAcks {
    batch_acks
        .entry(key)
        .or_insert_with(|| BatchAcks::new(size))
}

/// The word and the bit of `index` in a protocol bit set such as `ack_set`.
// Java: BitSet.valueOf(long[])
fn bit_of(index: i32) -> Option<(usize, u64)> {
    let index = usize::try_from(index).ok()?;
    Some((index / 64, 1 << (index % 64)))
}

/// Whether `index` is set in a protocol bit set such as `ack_set`.
fn is_set(bits: &[i64], index: i32) -> bool {
    bit_of(index)
        .is_some_and(|(word, bit)| bits.get(word).is_some_and(|&word| word as u64 & bit != 0))
}

/// The id of the entry `id` is in, without its place in a batch.
fn whole_entry(id: MessageIdData) -> MessageIdData {
    MessageIdData {
        batch_index: None,
        batch_size: None,
        ack_set: Vec::new(),
        ..id
    }
}

/// Where a non-durable subscription starts again by time. The protocol
/// carries a rollback from the broker's now, so it is measured anew for every
/// subscribe: time spent reconnecting must not move the start later.
#[derive(Clone, Copy, Debug)]
enum Rollback {
    /// The configured rollback, from the first subscribe.
    FromFirstSubscribe { at: Instant, rollback: Duration },
    /// A seek by publish time, in milliseconds since the epoch.
    ToPublishTime(u64),
}

impl Rollback {
    /// The rollback to ask for at `now`, which is `since_epoch` on the wall
    /// clock.
    fn at(self, now: Instant, since_epoch: Duration) -> Duration {
        match self {
            // A second early: the broker measured the first one from when the
            // request reached it, and measures this one the same way.
            Rollback::FromFirstSubscribe { at, rollback } => {
                rollback + now.saturating_duration_since(at) + Duration::from_secs(1)
            }
            // at least a millisecond, so a rollback to it is never empty
            Rollback::ToPublishTime(publish_time_ms) => since_epoch
                .saturating_sub(Duration::from_millis(publish_time_ms))
                .max(Duration::from_millis(1)),
        }
    }

    fn now(self) -> Duration {
        let since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        self.at(Instant::now(), since_epoch)
    }
}

/// Whether `id` is the start message or comes before it in the start's own
/// entry, which the broker sends whole. Other entries are not the filter's:
/// an operator may move a cursor back after a seek, and the broker sends what
/// it means to.
// Java: ConsumerImpl.isSameEntry and isPriorBatchIndex
// C++: lib/ConsumerImpl.cc:971 receiveIndividualMessagesFromBatch, prior-batch-index check
fn is_at_or_before(id: &MessageIdData, start: &MessageIdData) -> bool {
    id.ledger_id == start.ledger_id
        && id.entry_id == start.entry_id
        && id.batch_index.unwrap_or(-1) <= start.batch_index.unwrap_or(-1)
}

/// Where a non-durable subscription configured to start at a message id
/// resumes: after that message, or at it when inclusive, as if a seek had
/// put it there.
// Java: ConsumerImpl.isPriorEntryIndex and isPriorBatchIndex, resetIncludeHead
pub(crate) fn configured_resume(options: &ConsumerOptions) -> Resume {
    match (&options.start_message_id, options.durable) {
        (Some(start), Some(false)) if options.start_inclusive => Resume::From(start.clone()),
        (Some(start), Some(false)) => Resume::After(start.clone()),
        _ => Resume::Configured,
    }
}

/// What a subscription that resumes at `resume` drops of what the broker
/// sends first: it starts from the whole entry of a message id.
fn start_filter(resume: &Resume) -> Option<MessageIdData> {
    match resume {
        Resume::From(first) => before_in_entry(first),
        Resume::After(last) => Some(last.clone()),
        Resume::Configured | Resume::Time(_) => None,
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

/// How many batch sizes a durable subscription keeps for acks by id.
const MAX_BATCH_SIZES: usize = 10_000;

/// The error of a seek that waited for a reconnect past its time.
fn seek_timed_out() -> Error {
    Error::Consumer(ConsumerError::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "the seek ran out of time while the consumer reconnected",
    )))
}

/// Whether a request went with its connection or found the broker not
/// ready, so that it may go again.
fn lost_or_not_ready(error: &ConnectionError) -> bool {
    match error {
        ConnectionError::Io(_) | ConnectionError::Disconnected | ConnectionError::SlowDown => true,
        ConnectionError::PulsarError(kind, _) => {
            *kind == Some(crate::message::proto::ServerError::ServiceNotReady)
        }
        _ => false,
    }
}

#[cfg(test)]
mod rollback_tests {
    use std::time::{Duration, Instant};

    use super::Rollback;

    #[test]
    fn a_configured_rollback_keeps_its_start_however_late_the_subscribe() {
        let first = Instant::now();
        let rollback = Rollback::FromFirstSubscribe {
            at: first,
            rollback: Duration::from_secs(120),
        };
        // a minute of reconnecting later, the window still starts where the
        // first subscribe put it, a little earlier at most
        let asked = rollback.at(first + Duration::from_secs(60), Duration::ZERO);
        assert!(
            (Duration::from_secs(180)..=Duration::from_secs(182)).contains(&asked),
            "{asked:?}"
        );
    }

    #[test]
    fn a_time_seek_keeps_its_start_however_late_the_subscribe() {
        let sought = 1_000_000;
        let rollback = Rollback::ToPublishTime(sought);
        for late in [5, 65] {
            let now = Duration::from_millis(sought) + Duration::from_secs(late);
            assert_eq!(rollback.at(Instant::now(), now), Duration::from_secs(late));
        }
    }
}

#[cfg(test)]
mod start_filter_tests {
    use super::is_at_or_before;
    use crate::message::proto::MessageIdData;

    fn id(entry_id: u64, batch_index: Option<i32>) -> MessageIdData {
        MessageIdData {
            ledger_id: 1,
            entry_id,
            batch_index,
            ..Default::default()
        }
    }

    #[test]
    fn only_the_start_entry_up_to_the_start_is_dropped() {
        // a batch member: the members up to it
        let start = id(5, Some(2));
        assert!(is_at_or_before(&id(5, Some(0)), &start));
        assert!(is_at_or_before(&id(5, Some(2)), &start));
        assert!(!is_at_or_before(&id(5, Some(3)), &start));
        // other entries, earlier ones too, are not the filter's
        assert!(!is_at_or_before(&id(4, Some(9)), &start));
        assert!(!is_at_or_before(&id(4, None), &start));
        // a whole entry: that entry's message, but no member of a batch there
        let start = id(5, None);
        assert!(is_at_or_before(&id(5, None), &start));
        assert!(!is_at_or_before(&id(5, Some(0)), &start));
    }
}
