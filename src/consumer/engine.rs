use std::{
    collections::{BTreeMap, HashMap, HashSet},
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
        data::{DeadLetterPolicy, Delivery, EngineEvent, EngineMessage, Queued, Resume, Seek},
        options::ConsumerOptions,
    },
    error::{self, ConsumerError},
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
    /// Whether `Delivery::resume` is past waiting for the first queued message
    /// on this connection, which spares a lock per message.
    first_queued_known: bool,
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
        ConsumerEngine {
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
            start: None,
            first_queued_known: false,
            engine_rx: Some(engine_rx),
            event_rx,
            event_tx,
            window,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            batch_acks: BTreeMap::new(),
            frame_accounted: 0,
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

            if let Some(engine_rx) = self.engine_rx.take() {
                self.register_source(engine_rx, |msg| EngineEvent::EngineMessage(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering engine_rx source"))
                    })?;
            }

            match self.event_rx.next().await {
                Some(EngineEvent::Message(epoch, _)) if epoch != self.epoch => {
                    debug!("consumer engine: dropped a frame from a previous connection");
                }
                Some(EngineEvent::Message(_, msg)) => {
                    debug!("consumer engine: received message, {:?}", msg);
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Some(EngineEvent::EngineMessage(msg)) => {
                    debug!("consumer engine: received engine message");
                    let continue_loop = self.handle_ack_opt(msg).await;
                    if !continue_loop {
                        return Ok(());
                    }
                }
                // only wakes the loop, whose top checks the connection
                Some(EngineEvent::Tick) => {}
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
                let charged = message
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.metadata.num_messages_in_batch)
                    .unwrap_or(1)
                    .max(1) as u32;
                self.frame_accounted = 0;
                match self.process_message(message).await {
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
                self.track_taken();
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
            Some(EngineMessage::Flow(epoch, permits)) => {
                // a flow for an earlier connection is moot: the new
                // subscription got a fresh window
                if epoch == self.epoch {
                    self.send_flow(permits).await;
                }
                true
            }
            Some(EngineMessage::CancelSeek(target)) => {
                self.cancel_seek(&target);
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
    /// would acknowledge the whole entry, so an individual ack waits until
    /// every member is acked, and a cumulative ack in the middle of a batch
    /// covers only the entry before it; the rest of that batch then comes
    /// again after a restart. An id without a batch size is sent as it is.
    // Java: PersistentAcknowledgmentsGroupingTracker.java:248 (cumulative),
    // MessageIdAdvUtils.acknowledge and prevMessageId
    fn ack_target(&mut self, id: MessageIdData, cumulative: bool) -> Option<MessageIdData> {
        let key = (id.ledger_id, id.entry_id);
        if cumulative {
            // entries before this one are covered; this one keeps its marks
            while let Some(entry) = self.batch_acks.first_entry() {
                if *entry.key() >= key {
                    break;
                }
                entry.remove();
            }
        }
        let (Some(index), Some(size)) = (id.batch_index, id.batch_size) else {
            return Some(id);
        };
        let acks = self
            .batch_acks
            .entry(key)
            .or_insert_with(|| BatchAcks::new(size));
        if cumulative {
            acks.mark_up_to(index);
        } else {
            acks.mark(index);
        }
        if acks.is_complete() {
            self.batch_acks.remove(&key);
            return Some(whole_entry(id));
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
                debug!(
                    "Active consumer change for {} - Active: {:?}",
                    self.debug_format(),
                    active_consumer_change.is_active
                );
                self.announce(active_consumer_change.is_active.unwrap_or(false))?;
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
        let payloads = if batch_size.is_some() {
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
                    // the application never sees it, so it never acks it
                    if let (Some(index), Some(size)) = (message_id.batch_index, batch_size) {
                        self.batch_acks
                            .entry((message_id.ledger_id, message_id.entry_id))
                            .or_insert_with(|| BatchAcks::new(size))
                            .mark(index);
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
        debug!("reconnecting consumer for topic: {}", self.topic);
        let options = {
            let mut delivery = self.delivery.lock().unwrap();
            delivery.epoch += 1;
            self.epoch = delivery.epoch;
            delivery.freed = 0;
            if let Some(taken) = &mut delivery.taken {
                taken.clear();
            }
            // Java: ConsumerImpl.java:3070, after a seek the target becomes
            // the exclusive start; after a seek by time nothing is filtered.
            let seek = delivery.pending_seek.take();
            match &seek {
                Some(Seek::Id(target)) => self.start = Some(target.clone()),
                Some(Seek::Time(_)) => self.start = None,
                None => {}
            }
            let mut options = self.options.clone();
            if self.options.durable == Some(false) {
                // The broker applies a rollback on every subscribe, so the
                // configured one goes only until delivery moved.
                let (start_message_id, start, rollback) = match &delivery.resume {
                    Resume::Configured => (
                        self.options.start_message_id.clone(),
                        None,
                        self.options.start_rollback,
                    ),
                    // The broker keeps the subscription across its own seek;
                    // a later reconnect may find it gone.
                    Resume::Time(publish_time_ms) => {
                        (None, None, seek.is_none().then(|| since(*publish_time_ms)))
                    }
                    Resume::From(first) => (Some(first.clone()), before_in_entry(first), None),
                    Resume::After(last) => (Some(last.clone()), Some(last.clone()), None),
                };
                options.start_message_id = start_message_id;
                options.start_rollback = rollback;
                self.start = start;
            }
            self.first_queued_known = matches!(delivery.resume, Resume::From(_) | Resume::After(_));
            options
        };
        // Another consumer may receive the partition until this one is back,
        // even after a seek, since the broker reconnects every consumer of the
        // partition; the broker says so again if it is still ours.
        let _ = self.announce(false);
        self.unacked_messages.clear();
        // Java ties batch ack state to the delivered ids, which a reconnect
        // delivers anew.
        self.batch_acks.clear();

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
            match self.resubscribe(&options).await {
                Ok(messages) => {
                    // Acks sent while disconnected go out before the permits,
                    // so the broker does not dispatch those messages again.
                    self.take_queued_acks(&mut acks);
                    for (message_id, cumulative) in std::mem::take(&mut acks) {
                        self.ack(message_id, cumulative).await;
                    }
                    // what stale messages still queued leave of the window
                    let free = self
                        .window
                        .saturating_sub(self.delivery.lock().unwrap().queued);
                    let flow = if free > 0 {
                        send_initial_flow(&self.connection, &self.topic, self.id, free).await
                    } else {
                        Ok(())
                    };
                    match flow {
                        Ok(()) => {
                            self.messages_rx = Some(messages);
                            return Ok(());
                        }
                        Err(e) => {
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
                // Java retries everything but a deleted topic; errors that no
                // retry can fix (permissions, schema) are reported here too.
                Err(e) if e.server_error().is_some_and(error::is_permanent) => {
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
                Some(EngineEvent::EngineMessage(Some(EngineMessage::CancelSeek(target)))) => {
                    self.cancel_seek(&target);
                }
                // the broker redelivers what is unacked once the consumer is
                // back, and the new subscription gets a fresh window
                Some(EngineEvent::EngineMessage(Some(_))) => {}
                // frames from the old connection, and ticks
                Some(EngineEvent::Message(..) | EngineEvent::Tick) => {}
                Some(EngineEvent::EngineMessage(None)) | None => return false,
            }
        }
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
                EngineEvent::EngineMessage(Some(EngineMessage::CancelSeek(target))) => {
                    self.cancel_seek(&target);
                }
                // old frames, and nacks, redelivery ticks and flows the new
                // subscription makes moot
                _ => {}
            }
        }
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

    fn cancel_seek(&mut self, target: &MessageIdData) {
        if self.start.as_ref() == Some(target) {
            self.start = None;
        }
    }

    fn start_ticker(&mut self) -> Result<(), Error> {
        let mut tick_tx = self.event_tx.clone();
        let mut interval = self.client.executor.interval(Duration::from_secs(1));
        self.client
            .executor
            .spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if tick_tx.send(EngineEvent::Tick).await.is_err() {
                        break;
                    }
                }
            }))
            .map_err(|_| Error::Executor)
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
        let Ok(member) = usize::try_from(member) else {
            return;
        };
        if let Some(word) = self.unacked.get_mut(member / 64) {
            *word &= !(1u64 << (member % 64));
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

/// How long ago `publish_time_ms` was, and at least a millisecond, so a
/// rollback to it is never empty.
fn since(publish_time_ms: u64) -> Duration {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.saturating_sub(Duration::from_millis(publish_time_ms))
        .max(Duration::from_millis(1))
}

/// Where `id` sits in delivery order.
fn position(id: &MessageIdData) -> (u64, u64, i32) {
    (id.ledger_id, id.entry_id, id.batch_index.unwrap_or(-1))
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
