use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt::Debug,
    future::Future,
    iter,
    pin::Pin,
    task::{Context, Poll},
};

use chrono::{DateTime, Utc};
use futures::{
    future::{join_all, try_join_all},
    Stream,
};
use regex::Regex;

use crate::{
    consumer::{
        config::ConsumerConfig,
        full_topic_name,
        initial_position::InitialPosition,
        message::{ConsumerEvent, ConsumerItem, Message},
        topic::TopicConsumer,
    },
    error::{ConnectionError, ConsumerError},
    message::proto::{MessageIdData, Schema},
    proto,
    proto::CommandConsumerStatsResponse,
    service_discovery::partition_name,
    DeserializeMessage, Error, Executor, Pulsar,
};

/// A refresh under way; it logs what fails and keeps what it found.
type Refresh<T, Exe> = Pin<Box<dyn Future<Output = Refreshed<T, Exe>> + Send + Sync>>;

/// A consumer that can subscribe on multiple topics, from a regex matching
/// topic names
pub struct MultiTopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(super) namespace: String,
    pub(super) topic_regex: Option<Regex>,
    pub(super) pulsar: Pulsar<Exe>,
    pub(super) consumers: BTreeMap<String, Pin<Box<TopicConsumer<T, Exe>>>>,
    /// The partition count of each partitioned topic the application named,
    /// as last found. A refresh subscribes the partitions past it.
    // Java: MultiTopicsConsumerImpl.java partitionedTopics
    pub(super) partitioned: BTreeMap<String, u32>,
    pub(super) existing_topics: VecDeque<String>,
    /// Every topic and partition ever subscribed, ended ones included, by
    /// full name, so a refresh never subscribes one again.
    pub(super) subscribed: BTreeSet<String>,
    /// The stream ended, and stays ended.
    pub(super) ended: bool,
    pub(super) new_consumers: Option<Refresh<T, Exe>>,
    /// When the refresh under way started.
    pub(super) refresh_started: std::time::Instant,
    pub(super) refresh: Pin<Box<dyn Stream<Item = ()> + Send + Sync>>,
    pub(super) config: ConsumerConfig,
    // Stats on disconnected consumers to keep metrics correct
    pub(super) disc_messages_received: u64,
    pub(super) disc_last_message_received: Option<DateTime<Utc>>,
    /// Events for partitions dropped after an error: the application saw
    /// them active, and nothing will say otherwise now.
    pub(super) pending_events: VecDeque<ConsumerEvent>,
}

impl<T: DeserializeMessage, Exe: Executor> MultiTopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn existing_topics(&self) -> Vec<String> {
        self.existing_topics.iter().map(|s| s.to_string()).collect()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_stats(&mut self) -> Result<Vec<CommandConsumerStatsResponse>, Error> {
        let resposnes = try_join_all(self.consumers.values_mut().map(|c| c.get_stats())).await?;
        Ok(resposnes)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumers
            .values()
            .filter_map(|c| c.last_message_received)
            .chain(self.disc_last_message_received)
            .max()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.consumers
            .values()
            .map(|c| c.messages_received)
            .chain(iter::once(self.disc_messages_received))
            .sum()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connections(&mut self) -> Result<(), Error> {
        self.pulsar
            .manager
            .get_base_connection()
            .await?
            .sender()
            .send_ping()
            .await?;

        for consumer in self.consumers.values_mut() {
            consumer.connection().await?.sender().send_ping().await?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        for consumer in self.consumers.values_mut() {
            consumer.unsubscribe().await?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        for consumer in self.consumers.values_mut() {
            consumer.close().await?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(super) fn add_refreshed(&mut self, refreshed: Refreshed<T, Exe>) {
        for consumer in refreshed.consumers {
            let topic = consumer.topic().to_owned();
            // a refresh that replaced a stuck one may find it again
            if self.subscribed.insert(full_topic_name(&topic)) {
                self.consumers.insert(topic.clone(), Box::pin(consumer));
                self.existing_topics.push_back(topic);
            }
        }
        self.partitioned.extend(refreshed.partitioned);
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn remove_consumers(&mut self, topics: &[String]) {
        self.existing_topics.retain(|t| !topics.contains(t));
        for topic in topics {
            if let Some(consumer) = self.consumers.remove(topic) {
                if consumer.active {
                    self.pending_events
                        .push_back(ConsumerEvent::BecameInactive {
                            topic: topic.clone(),
                        });
                }
                self.disc_messages_received += consumer.messages_received;
                self.disc_last_message_received = self
                    .disc_last_message_received
                    .into_iter()
                    .chain(consumer.last_message_received)
                    .max();
            }
        }
    }

    /// Looks for partitions added to the partitioned topics, and for topics
    /// matching the regex, and subscribes them. Nothing subscribed before is
    /// subscribed again, an ended topic included: it ended for good, and the
    /// application got its error.
    // Java: MultiTopicsConsumerImpl.java:1560 subscribeIncreasedTopicPartitions
    pub(super) fn refresh(&self) -> Refresh<T, Exe> {
        let pulsar = self.pulsar.clone();
        let config = self.config.clone();
        let partitioned = self.partitioned.clone();
        let subscribed = self.subscribed.clone();
        let regex = self
            .topic_regex
            .clone()
            .map(|regex| (regex, self.namespace.clone()));

        Box::pin(async move {
            let mut refreshed = Refreshed {
                consumers: Vec::new(),
                partitioned: BTreeMap::new(),
            };
            let added = join_all(partitioned.into_iter().map(|(topic, known)| {
                let (pulsar, config, subscribed) = (&pulsar, &config, &subscribed);
                async move {
                    let added = added_partitions(pulsar, &topic, known, config, subscribed).await;
                    (topic, added)
                }
            }))
            .await;
            for (topic, added) in added {
                match added {
                    Ok((count, consumers)) => {
                        if !consumers.is_empty() {
                            info!("subscribed {} added partitions of {topic}", consumers.len());
                        }
                        refreshed.consumers.extend(consumers);
                        refreshed.partitioned.insert(topic, count);
                    }
                    Err(e) => warn!("could not look for added partitions of {topic}: {e}"),
                }
            }

            if let Some((regex, namespace)) = regex {
                let found: BTreeSet<String> = refreshed
                    .consumers
                    .iter()
                    .map(|c| full_topic_name(&c.topic()))
                    .collect();
                let skip = |topic: &str| {
                    let topic = full_topic_name(topic);
                    subscribed.contains(&topic) || found.contains(&topic)
                };
                match matching_topics(&pulsar, &regex, namespace, &config, skip).await {
                    // the others are tried again at the next refresh
                    Ok(subscribes) => {
                        for subscribe in subscribes {
                            match subscribe {
                                Ok(consumer) => refreshed.consumers.push(consumer),
                                Err(e) => {
                                    warn!("could not subscribe a topic matching {regex}: {e}")
                                }
                            }
                        }
                    }
                    Err(e) => warn!("could not look for topics matching {regex}: {e}"),
                }
            }
            refreshed
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.ack(msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack_with_id(
        &mut self,
        topic: &str,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(topic) {
            c.ack_with_id(msg_id).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {topic}")).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.cumulative_ack(msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack_with_id(
        &mut self,
        topic: &str,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(topic) {
            c.cumulative_ack_with_id(msg_id).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {topic}")).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.nack(msg).await?;
            Ok(())
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack_with_id(
        &mut self,
        topic: &str,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(topic) {
            c.nack_with_id(msg_id).await?;
            Ok(())
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {topic}")).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn get_schema(
        &mut self,
        topic: &str,
        version: Option<Vec<u8>>,
    ) -> Result<Option<Schema>, Error> {
        if let Some(c) = self.consumers.get_mut(topic) {
            c.get_schema(version).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {topic}")).into())
        }
    }
}

/// The consumers of the partitions of `topic` past `known` that nothing
/// subscribed yet, and the partition count to remember: the new one once every
/// partition is subscribed, `known` until then, so a failed one is tried again.
async fn added_partitions<T: DeserializeMessage, Exe: Executor>(
    pulsar: &Pulsar<Exe>,
    topic: &str,
    known: u32,
    config: &ConsumerConfig,
    subscribed: &BTreeSet<String>,
) -> Result<(u32, Vec<TopicConsumer<T, Exe>>), Error> {
    let count = pulsar.lookup_partitioned_topic_number(topic).await?;
    if count <= known {
        if count < known {
            warn!("{topic} has {count} partitions, fewer than {known}; ignored");
        }
        return Ok((known, Vec::new()));
    }
    // Everything in a partition added after the consumer started is newer
    // than its start, and the broker copies only durable subscriptions to it,
    // so a reader takes all of it.
    let mut config = config.clone();
    if config.options.durable == Some(false) {
        config.options.start_message_id = None;
        config.options.start_rollback = None;
        config.options.initial_position = InitialPosition::Earliest;
    }
    let partitions = (known..count)
        .map(|index| partition_name(topic, index))
        .filter(|partition| !subscribed.contains(&full_topic_name(partition)));
    let subscribes = join_all(partitions.map(|partition| {
        let (pulsar, config) = (pulsar.clone(), config.clone());
        async move {
            let addr = pulsar.lookup_topic(partition.clone()).await?;
            TopicConsumer::new(pulsar, partition, addr, config).await
        }
    }))
    .await;
    let mut consumers = Vec::new();
    let mut complete = true;
    for subscribe in subscribes {
        match subscribe {
            Ok(consumer) => consumers.push(consumer),
            Err(e) => {
                warn!("could not subscribe an added partition of {topic}: {e}");
                complete = false;
            }
        }
    }
    Ok((if complete { count } else { known }, consumers))
}

/// A subscribe to each topic matching `regex` in `namespace`, but those `skip`
/// names; an error when the namespace cannot be listed.
pub(super) async fn matching_topics<T: DeserializeMessage, Exe: Executor>(
    pulsar: &Pulsar<Exe>,
    regex: &Regex,
    namespace: String,
    config: &ConsumerConfig,
    skip: impl Fn(&str) -> bool,
) -> Result<Vec<Result<TopicConsumer<T, Exe>, Error>>, Error> {
    let names: Vec<String> = pulsar
        .get_topics_of_namespace(namespace, proto::command_get_topics_of_namespace::Mode::All)
        .await?
        .into_iter()
        .filter(|topic| regex.is_match(topic) && !skip(topic))
        .collect();
    trace!("new topics matching {regex}: {names:?}");
    let skip = &skip;
    let subscribes = join_all(names.into_iter().map(|name| async move {
        let topics = match pulsar.lookup_partitioned_topic(name).await {
            Ok(topics) => topics,
            Err(e) => return vec![Err(e)],
        };
        join_all(
            topics
                .into_iter()
                .filter(|(topic, _)| !skip(topic))
                .map(|(topic, addr)| {
                    TopicConsumer::new(pulsar.clone(), topic, addr, config.clone())
                }),
        )
        .await
    }))
    .await;
    Ok(subscribes.into_iter().flatten().collect())
}

/// What a refresh found: consumers of added partitions or matching topics,
/// and the partition counts it saw.
pub(super) struct Refreshed<T: DeserializeMessage, Exe: Executor> {
    pub(super) consumers: Vec<TopicConsumer<T, Exe>>,
    pub(super) partitioned: BTreeMap<String, u32>,
}

impl<T: DeserializeMessage, Exe: Executor> Debug for MultiTopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MultiTopicConsumer({:?}, {:?})",
            self.namespace, self.topic_regex
        )
    }
}

impl<T: DeserializeMessage, Exe: Executor> MultiTopicConsumer<T, Exe> {
    /// The next message or Failover event of any topic or partition.
    pub(super) fn poll_next_item(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ConsumerItem<T>, Error>>> {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(Some(Ok(ConsumerItem::Event(event))));
        }
        if self.ended {
            return Poll::Ready(None);
        }
        if let Some(mut new_consumers) = self.new_consumers.take() {
            match new_consumers.as_mut().poll(cx) {
                Poll::Ready(refreshed) => self.add_refreshed(refreshed),
                Poll::Pending => self.new_consumers = Some(new_consumers),
            }
        }

        if let Poll::Ready(Some(_)) = self.refresh.as_mut().poll_next(cx) {
            // A refresh under way is not replaced, so a slow one can finish,
            // unless it has hung for several operation timeouts: a subscribe
            // that the broker keeps refusing retries for ever.
            let stuck = self.refresh_started.elapsed()
                > self.pulsar.operation_retry_options.operation_timeout * 4;
            if self.new_consumers.is_none() || stuck {
                self.refresh_started = std::time::Instant::now();
                self.new_consumers = Some(self.refresh());
            }
            return self.poll_next_item(cx);
        }

        let mut topics_to_remove = Vec::new();
        let mut result = None;
        for _ in 0..self.existing_topics.len() {
            if result.is_some() {
                break;
            }
            let topic = self.existing_topics.pop_front().unwrap();
            if let Some(item) = self
                .consumers
                .get_mut(&topic)
                .map(|c| c.as_mut().get_mut().poll_next_item(cx))
            {
                match item {
                    Poll::Pending => {}
                    Poll::Ready(Some(Ok(item))) => result = Some(Ok(item)),
                    // The engine is gone, after its error: the partition ended.
                    Poll::Ready(None) => {
                        error!("consumer of {topic} ended");
                        topics_to_remove.push(topic.clone());
                    }
                    // The engine carries on; the application decides.
                    Poll::Ready(Some(Err(e))) => result = Some(Err(e)),
                }
            } else {
                eprintln!("BUG: Missing consumer for topic {}", topic);
            }
            self.existing_topics.push_back(topic);
        }
        self.remove_consumers(&topics_to_remove);
        if let Some(result) = result {
            return Poll::Ready(Some(result));
        }
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(Some(Ok(ConsumerItem::Event(event))));
        }
        // Every topic and partition ended, no refresh is adding any, and only
        // a regex could find more.
        if self.consumers.is_empty() && self.new_consumers.is_none() && self.topic_regex.is_none() {
            self.ended = true;
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}
