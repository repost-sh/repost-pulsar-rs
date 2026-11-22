use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use futures::{future::try_join_all, StreamExt};
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;

use crate::{
    consumer::{
        config::ConsumerConfig,
        data::DeadLetterPolicy,
        full_topic_name, multi,
        multi::{MultiTopicConsumer, Woken},
        options::ConsumerOptions,
        topic::TopicConsumer,
        InnerConsumer,
    },
    message::proto::command_subscribe::SubType,
    reader::Reader,
    BrokerAddress, Consumer, DeserializeMessage, Error, Executor, Pulsar,
};

/// Builder structure for consumers
///
/// This is the main way to create a [Consumer] or a [Reader]
#[derive(Clone)]
pub struct ConsumerBuilder<Exe: Executor> {
    pulsar: Pulsar<Exe>,
    topics: Option<Vec<String>>,
    topic_regex: Option<Regex>,
    subscription: Option<String>,
    subscription_type: Option<SubType>,
    consumer_id: Option<u64>,
    consumer_name: Option<String>,
    batch_size: Option<u32>,
    unacked_message_resend_delay: Option<Duration>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    consumer_options: Option<ConsumerOptions>,
    namespace: Option<String>,
    topic_refresh: Option<Duration>,
}

impl<Exe: Executor> ConsumerBuilder<Exe> {
    /// Creates a new [ConsumerBuilder] from an existing client instance
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(pulsar: &Pulsar<Exe>) -> Self {
        ConsumerBuilder {
            pulsar: pulsar.clone(),
            topics: None,
            topic_regex: None,
            subscription: None,
            subscription_type: None,
            consumer_id: None,
            consumer_name: None,
            batch_size: None,
            // TODO what should this default to? None seems incorrect..
            unacked_message_resend_delay: None,
            dead_letter_policy: None,
            consumer_options: None,
            namespace: None,
            topic_refresh: None,
        }
    }

    /// sets the consumer's topic or add one to the list of topics
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> ConsumerBuilder<Exe> {
        match &mut self.topics {
            Some(topics) => topics.push(topic.into()),
            None => self.topics = Some(vec![topic.into()]),
        }
        self
    }

    /// adds a list of topics to the future consumer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topics<S: AsRef<str>, I: IntoIterator<Item = S>>(
        mut self,
        topics: I,
    ) -> ConsumerBuilder<Exe> {
        let new_topics = topics.into_iter().map(|t| t.as_ref().into());
        match &mut self.topics {
            Some(topics) => {
                topics.extend(new_topics);
            }
            None => self.topics = Some(new_topics.collect()),
        }
        self
    }

    /// sets up a consumer that will listen on all topics matching the regular
    /// expression
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic_regex(mut self, regex: Regex) -> ConsumerBuilder<Exe> {
        self.topic_regex = Some(regex);
        self
    }

    /// sets the subscription's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_subscription<S: Into<String>>(mut self, subscription: S) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    /// sets the kind of subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }

    /// Tenant/Namespace to be used when matching against a regex. For other
    /// consumers, specify namespace using the
    /// `<persistent|non-persistent://<tenant>/<namespace>/<topic>`
    /// topic format.
    /// Defaults to `public/default` if not specifid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_lookup_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// How often a consumer of several topics or partitions looks for partitions
    /// added to its partitioned topics and for new topics matching its regex
    /// (default 30 s)
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic_refresh(mut self, refresh_interval: Duration) -> Self {
        self.topic_refresh = Some(refresh_interval);
        self
    }

    /// sets the consumer id for this consumer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_consumer_id(mut self, consumer_id: u64) -> Self {
        self.consumer_id = Some(consumer_id);
        self
    }

    /// sets the consumer's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_consumer_name<S: Into<String>>(mut self, consumer_name: S) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// sets the most messages the broker may have out to each topic or
    /// partition consumer before the application takes some
    ///
    /// The receiver queue size bounds the same number; the smaller of the two
    /// applies. Permits go back to the broker as the application takes
    /// messages, half the window at a time.
    ///
    /// default value: 1000
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// sets consumer options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_options(mut self, options: ConsumerOptions) -> Self {
        self.consumer_options = Some(options);
        self
    }

    /// sets the dead letter policy
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_dead_letter_policy(mut self, dead_letter_policy: DeadLetterPolicy) -> Self {
        self.dead_letter_policy = Some(dead_letter_policy);
        self
    }

    /// The time after which a message is dropped without being acknowledged or nacked
    /// that the message is resent. If `None`, messages will only be resent when a
    /// consumer disconnects with pending unacknowledged messages.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_unacked_message_resend_delay(mut self, delay: Option<Duration>) -> Self {
        self.unacked_message_resend_delay = delay;
        self
    }

    // Checks the builder for inconsistencies
    // returns a config and a list of topics with associated brokers
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn validate(self) -> Result<Validated, Error> {
        let ConsumerBuilder {
            pulsar,
            topics,
            topic_regex,
            subscription,
            subscription_type,
            consumer_id,
            mut consumer_name,
            batch_size,
            unacked_message_resend_delay,
            consumer_options,
            dead_letter_policy,
            namespace: _,
            topic_refresh: _,
        } = self;

        if consumer_name.is_none() {
            let s: String = (0..8)
                .map(|_| rand::thread_rng().sample(Alphanumeric))
                .map(|c| c as char)
                .collect();
            consumer_name = Some(format!("consumer_{s}"));
        }

        if topics.is_none() && topic_regex.is_none() {
            return Err(Error::Custom(
                "Cannot create consumer with no topics and no topic regex".into(),
            ));
        }

        let pulsar = &pulsar;
        let lookups = try_join_all(topics.into_iter().flatten().map(|topic| async move {
            let (count, partitions) = pulsar.lookup_partitions(&topic).await?;
            Ok::<_, Error>((topic, count, partitions))
        }))
        .await?;
        let mut partitioned = BTreeMap::new();
        let mut topics = Vec::new();
        for (topic, count, partitions) in lookups {
            if count > 0 {
                partitioned.insert(topic, count);
            }
            topics.extend(partitions);
        }

        if topics.is_empty() && topic_regex.is_none() {
            return Err(Error::Custom(
                "Unable to create consumer - topic not found".to_string(),
            ));
        }

        let consumer_id = match (consumer_id, topics.len()) {
            (Some(consumer_id), 1) => Some(consumer_id),
            (Some(_), _) => {
                warn!(
                    "Cannot specify consumer id for connecting to partitioned topics or multiple \
                     topics"
                );
                None
            }
            _ => None,
        };
        let subscription = subscription.unwrap_or_else(|| {
            let s: String = (0..8)
                .map(|_| rand::thread_rng().sample(Alphanumeric))
                .map(|c| c as char)
                .collect();
            let subscription = format!("sub_{s}");
            warn!(
                "Subscription not specified. Using new subscription `{}`.",
                subscription
            );
            subscription
        });
        let sub_type = subscription_type.unwrap_or_else(|| {
            warn!("Subscription Type not specified. Defaulting to `Shared`.");
            SubType::Shared
        });

        let config = ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay: unacked_message_resend_delay,
            options: consumer_options.unwrap_or_default(),
            dead_letter_policy,
            auto_ack: false,
        };
        Ok(Validated {
            config,
            topics,
            partitioned,
        })
    }

    /// creates a [Consumer] from this builder
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build<T: DeserializeMessage>(self) -> Result<Consumer<T, Exe>, Error> {
        // Java: ConsumerBuilderImpl has no rollback; the broker would reset a
        // durable cursor on every subscribe.
        if self
            .consumer_options
            .as_ref()
            .is_some_and(|o| o.start_rollback.is_some())
        {
            return Err(Error::Custom(
                "a start rollback is for readers: into_reader".to_string(),
            ));
        }
        // would this clone() consume too much memory?
        let validated = self.clone().validate().await?;
        self.assemble(validated).await
    }

    /// A consumer of the validated topics: one topic consumer each, behind a
    /// multi consumer when there are several, or partitions that may grow.
    async fn assemble<T: DeserializeMessage>(
        self,
        validated: Validated,
    ) -> Result<Consumer<T, Exe>, Error> {
        let Validated {
            config,
            topics: joined_topics,
            partitioned,
        } = validated;
        let consumers = try_join_all(joined_topics.into_iter().map(|(topic, addr)| {
            TopicConsumer::new(self.pulsar.clone(), topic, addr, config.clone())
        }))
        .await?;

        // Java: PulsarClientImpl.java:1165, a partitioned topic gets a multi
        // consumer even with one partition, so that it can grow.
        let single = consumers.len() == 1 && partitioned.is_empty() && self.topic_regex.is_none();
        let consumer = if single {
            let consumer = consumers.into_iter().next().unwrap();
            InnerConsumer::Single(consumer)
        } else {
            let consumers: BTreeMap<_, _> = consumers
                .into_iter()
                .map(|c| (c.topic(), Box::pin(c)))
                .collect();
            let topic_refresh = self
                .topic_refresh
                .unwrap_or_else(|| Duration::from_secs(30));
            // plain topics alone have nothing to find
            let refresh: std::pin::Pin<Box<dyn futures::Stream<Item = ()> + Send + Sync>> =
                if partitioned.is_empty() && self.topic_regex.is_none() {
                    Box::pin(futures::stream::pending())
                } else {
                    Box::pin(self.pulsar.executor.interval(topic_refresh).map(drop))
                };
            let subscribed = consumers.keys().map(|t| full_topic_name(t)).collect();
            let mut consumer = MultiTopicConsumer {
                namespace: self
                    .namespace
                    .unwrap_or_else(|| "public/default".to_string()),
                topic_regex: self.topic_regex,
                pulsar: self.pulsar,
                woken: Woken::new(consumers.keys().cloned()),
                consumers,
                partitioned,
                subscribed,
                ended: false,
                new_consumers: None,
                refresh_started: std::time::Instant::now(),
                refresh,
                config,
                disc_last_message_received: None,
                disc_messages_received: 0,
                disc_messages_withheld: 0,
                pending_events: VecDeque::new(),
            };
            // At build, a namespace that cannot be listed or a topic that
            // cannot be subscribed is an error, as before.
            if let Some(regex) = consumer.topic_regex.clone() {
                let subscribed = consumer.subscribed.clone();
                let consumers = multi::matching_topics(
                    &consumer.pulsar,
                    &regex,
                    consumer.namespace.clone(),
                    &consumer.config,
                    |topic| subscribed.contains(&full_topic_name(topic)),
                )
                .await?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
                consumer.add_refreshed(multi::Refreshed {
                    consumers,
                    partitioned: BTreeMap::new(),
                });
            }
            InnerConsumer::Multi(consumer)
        };
        Ok(Consumer { inner: consumer })
    }

    /// creates a [Reader] from this builder: of one topic, or of every
    /// partition of a partitioned topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn into_reader<T: DeserializeMessage>(self) -> Result<Reader<T, Exe>, Error> {
        let topic = match (&self.topics, &self.topic_regex) {
            (Some(topics), None) if topics.len() == 1 => topics[0].clone(),
            _ => {
                return Err(Error::Custom(
                    "a reader reads one topic, which may be partitioned".to_string(),
                ))
            }
        };
        // Java: ReaderBuilderImpl.java:90 rejects both; the broker would
        // silently prefer the rollback.
        if self
            .consumer_options
            .as_ref()
            .is_some_and(|o| o.start_rollback.is_some() && o.start_message_id.is_some())
        {
            return Err(Error::Custom(
                "a reader starts at a message id or a rollback, not both".to_string(),
            ));
        }
        // would this clone() consume too much memory?
        let mut validated = self.clone().validate().await?;
        validated.config.auto_ack = true;

        // Internally, the reader interface is implemented as a consumer using an exclusive,
        // non-durable subscription
        validated.config.options.durable = Some(false);

        // the validate() function defaults sub_type to SubType::Shared,
        // but a reader's subscription is exclusive
        warn!("Subscription Type for a reader is `Exclusive`. Resetting.");
        validated.config.sub_type = SubType::Exclusive;

        // An id belongs to one partition; Java hands it to every one.
        if validated.config.options.start_message_id.is_some()
            && validated
                .partitioned
                .values()
                .any(|&partitions| partitions > 1)
        {
            return Err(Error::Custom(
                "a message id starts one partition: start a partitioned reader at the \
                 earliest, the latest or a rollback, and seek_to a partition"
                    .to_string(),
            ));
        }
        let consumer = self.assemble(validated).await?;
        Ok(Reader { consumer, topic })
    }
}

/// What `validate` found: the configuration, every topic and partition to
/// subscribe with its broker, and the partition count of each partitioned
/// topic the application named.
struct Validated {
    config: ConsumerConfig,
    topics: Vec<(String, BrokerAddress)>,
    partitioned: BTreeMap<String, u32>,
}
