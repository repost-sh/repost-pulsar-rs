//! Publishes numbered messages and checks what a consumer delivered: nothing
//! lost, nothing twice, and in order, per topic or partition.

use std::{collections::BTreeMap, ops::RangeInclusive, time::Duration};

/// Long enough for a broker to fail over and a consumer to reconnect.
pub const DEADLINE: Duration = Duration::from_secs(90);

use futures::{Stream, StreamExt, TryStreamExt};
use pulsar::{
    consumer::{InitialPosition, Message},
    producer::ProducerOptions,
    reader::Reader,
    Consumer, ConsumerOptions, Error, Pulsar, SubType, TokioExecutor,
};

/// An Exclusive, durable consumer that starts at the earliest message.
pub async fn earliest_consumer(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    subscription: &str,
) -> Consumer<String, TokioExecutor> {
    earliest_consumer_with(pulsar, topic, subscription, ConsumerOptions::default()).await
}

/// [`earliest_consumer`] with other options as well.
pub async fn earliest_consumer_with(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    subscription: &str,
    options: ConsumerOptions,
) -> Consumer<String, TokioExecutor> {
    pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription(subscription)
        .with_subscription_type(SubType::Exclusive)
        .with_options(options.with_initial_position(InitialPosition::Earliest))
        .build()
        .await
        .expect("consumer")
}

/// A reader that starts at the earliest message.
pub async fn earliest_reader(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    subscription: &str,
) -> Reader<String, TokioExecutor> {
    let options = ConsumerOptions::default().with_initial_position(InitialPosition::Earliest);
    reader_with(pulsar, topic, subscription, options).await
}

/// A reader of `topic` with `options`.
pub async fn reader_with(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    subscription: &str,
    options: ConsumerOptions,
) -> Reader<String, TokioExecutor> {
    pulsar
        .reader()
        .with_topic(topic)
        .with_subscription(subscription)
        .with_options(options)
        .into_reader()
        .await
        .expect("reader")
}

/// Publishes `before` on each topic, then `after`, and returns a publish time
/// in milliseconds that falls between the two.
pub async fn publish_around_a_time(
    pulsar: &Pulsar<TokioExecutor>,
    topics: &[String],
    before: RangeInclusive<u64>,
    after: RangeInclusive<u64>,
) -> u64 {
    for topic in topics {
        publish(pulsar, topic, before.clone()).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let middle = chrono::Utc::now().timestamp_millis() as u64;
    tokio::time::sleep(Duration::from_millis(200)).await;
    for topic in topics {
        publish(pulsar, topic, after.clone()).await;
    }
    middle
}

pub async fn publish(pulsar: &Pulsar<TokioExecutor>, topic: &str, numbers: RangeInclusive<u64>) {
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .build()
        .await
        .expect("producer");
    for number in numbers {
        producer
            .send_non_blocking(number.to_string())
            .await
            .expect("send")
            .await
            .expect("receipt");
    }
}

/// Publishes `numbers` in batches of ten, one broker entry per batch.
pub async fn publish_batched(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    numbers: RangeInclusive<u64>,
) {
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .with_options(ProducerOptions {
            batch_size: Some(10),
            batch_timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .build()
        .await
        .expect("producer");
    let mut receipts = Vec::new();
    for number in numbers {
        receipts.push(
            producer
                .send_non_blocking(number.to_string())
                .await
                .expect("send"),
        );
    }
    producer.send_batch().await.expect("flush");
    for receipt in receipts {
        receipt.await.expect("receipt");
    }
}

/// The numbers delivered on each topic or partition, in delivery order.
#[derive(Debug, Default)]
pub struct Delivered {
    by_topic: BTreeMap<String, Vec<u64>>,
}

impl Delivered {
    pub fn record(&mut self, message: &Message<String>) {
        let number = message
            .deserialize()
            .expect("utf-8 payload")
            .parse()
            .expect("numbered payload");
        self.by_topic
            .entry(message.topic.clone())
            .or_default()
            .push(number);
    }

    /// How many different numbers `topic` delivered.
    pub fn distinct_on(&self, topic: &str) -> usize {
        let mut numbers = self.by_topic.get(topic).cloned().unwrap_or_default();
        numbers.sort_unstable();
        numbers.dedup();
        numbers.len()
    }

    pub fn distinct(&self) -> usize {
        self.by_topic
            .keys()
            .map(|topic| self.distinct_on(topic))
            .sum()
    }

    /// Asserts that `topic` delivered every number in `expected` exactly once,
    /// in ascending order, and nothing else.
    pub fn assert_exactly(&self, topic: &str, expected: RangeInclusive<u64>) {
        let numbers = self.by_topic.get(topic).map(Vec::as_slice).unwrap_or(&[]);
        assert_numbers(topic, numbers, expected);
    }

    /// Like [`Self::assert_exactly`], but numbers below `expected` may come
    /// back. After a broker crash, the broker redelivers messages acked in the
    /// last second, because it saves an ack position at most once a second
    /// (`managedLedgerDefaultMarkDeleteRateLimit`).
    pub fn assert_exactly_ignoring_earlier(&self, topic: &str, expected: RangeInclusive<u64>) {
        let numbers: Vec<u64> = self
            .by_topic
            .get(topic)
            .into_iter()
            .flatten()
            .copied()
            .filter(|n| n >= expected.start())
            .collect();
        assert_numbers(topic, &numbers, expected);
    }
}

fn assert_numbers(topic: &str, numbers: &[u64], expected: RangeInclusive<u64>) {
    let mut counts = BTreeMap::new();
    for &number in numbers {
        *counts.entry(number).or_insert(0u32) += 1;
    }
    let missing: Vec<u64> = expected
        .clone()
        .filter(|n| !counts.contains_key(n))
        .collect();
    let twice: Vec<u64> = counts
        .iter()
        .filter(|(_, &c)| c > 1)
        .map(|(&n, _)| n)
        .collect();
    let unexpected: Vec<u64> = counts
        .keys()
        .filter(|n| !expected.contains(n))
        .copied()
        .collect();
    let reordered = numbers.windows(2).find(|w| w[1] <= w[0]);
    assert!(
        missing.is_empty() && twice.is_empty() && unexpected.is_empty() && reordered.is_none(),
        "{topic}: missing {missing:?}, delivered more than once {twice:?}, \
         not published {unexpected:?}, first out of order {reordered:?}, \
         delivered {} in all",
        numbers.len()
    );
}

/// Takes the next `count` messages, records them, and returns them.
pub async fn take<S>(
    stream: &mut S,
    delivered: &mut Delivered,
    count: usize,
) -> Vec<Message<String>>
where
    S: Stream<Item = Result<Message<String>, Error>> + Unpin,
{
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        let message = stream.try_next().await.expect("receive").expect("message");
        delivered.record(&message);
        messages.push(message);
    }
    messages
}

/// Reads until `distinct` different messages have arrived and then nothing
/// more for three seconds, so a duplicate that arrives late is still recorded.
/// Gives up after `deadline`, or when the stream ends or fails.
pub async fn collect<S>(
    stream: &mut S,
    delivered: &mut Delivered,
    distinct: usize,
    deadline: Duration,
) where
    S: Stream<Item = Result<Message<String>, Error>> + Unpin,
{
    let quiet = Duration::from_secs(3);
    let give_up = tokio::time::Instant::now() + deadline;
    while tokio::time::Instant::now() < give_up {
        match tokio::time::timeout(quiet, stream.next()).await {
            Ok(Some(Ok(message))) => delivered.record(&message),
            Ok(Some(Err(e))) => panic!("consumer failed: {e}"),
            Ok(None) => return,
            Err(_) if delivered.distinct() >= distinct => return,
            Err(_) => {}
        }
    }
}
