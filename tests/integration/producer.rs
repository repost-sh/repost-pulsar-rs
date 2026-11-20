//! Producing to a partitioned topic: a key picks the partition the Java
//! client would pick, and partitions added while the producer runs get
//! messages too.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use futures::StreamExt;
use pulsar::{
    consumer::InitialPosition, producer::ProducerOptions, ConsumerOptions, Pulsar, TokioExecutor,
};

use crate::{
    cluster::{exclusive, partition, topic},
    delivery::{earliest_consumer, take, Delivered, DEADLINE},
};

/// Publishes one message per key, carrying the origin and the key.
async fn rust_produce(pulsar: &Pulsar<TokioExecutor>, topic: &str, keys: &[&str]) {
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .build()
        .await
        .expect("producer");
    for key in keys {
        producer
            .create_message()
            .with_content(format!("rust:{key}"))
            .with_key(*key)
            .send_non_blocking()
            .await
            .expect("send")
            .await
            .expect("receipt");
    }
}

#[tokio::test]
async fn keyed_messages_land_where_the_java_client_puts_them() {
    let cluster = exclusive().await;
    let topic = topic("producer-keys");
    cluster.create_partitioned_topic(&topic, 4).await;
    let pulsar = cluster.client().await;
    // one Java start-up per key; the unit tests pin the hash itself
    let keys = ["alpha", "bravo", "charlie", "日本語🎉"];
    let mut consumer = earliest_consumer(&pulsar, &topic, "keys").await;

    rust_produce(&pulsar, &topic, &keys).await;
    for key in keys {
        cluster
            .java_produce(&topic, key, &format!("java:{key}"))
            .await;
    }

    // every message of a key, from either client, on one partition
    let mut partitions: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for _ in 0..keys.len() * 2 {
        let message = tokio::time::timeout(DEADLINE, consumer.next())
            .await
            .expect("a message within the deadline")
            .expect("message")
            .expect("message");
        let body = message.deserialize().expect("utf-8");
        let (_, key) = body.split_once(':').unwrap();
        partitions
            .entry(key.to_string())
            .or_default()
            .insert(message.topic.clone());
    }
    for (key, topics) in &partitions {
        assert_eq!(topics.len(), 1, "{key} went to {topics:?}");
    }
}

/// A producer that looks for added partitions every second.
async fn refreshing_producer(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
) -> pulsar::Producer<TokioExecutor> {
    pulsar
        .producer()
        .with_topic(topic)
        .with_options(ProducerOptions {
            partition_refresh: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .build()
        .await
        .expect("producer")
}

/// Grows `topic` from `from` partitions to one more while a producer sends
/// keyless messages, until one lands on the new partition.
async fn grow_and_produce(from: u32, name: &str) {
    let cluster = exclusive().await;
    let topic = topic(name);
    cluster.create_partitioned_topic(&topic, from).await;
    let pulsar = cluster.client().await;
    let mut producer = refreshing_producer(&pulsar, &topic).await;
    let mut consumer: pulsar::Consumer<String, TokioExecutor> = pulsar
        .consumer()
        .with_topic(&topic)
        .with_subscription("grow")
        .with_topic_refresh(Duration::from_secs(1))
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .build()
        .await
        .expect("consumer");

    cluster.update_partitions(&topic, from + 1).await;
    let added = partition(&topic, from);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut sent = 0u64;
    loop {
        sent += 1;
        producer
            .send_non_blocking(sent.to_string())
            .await
            .expect("send")
            .await
            .expect("receipt");
        let message = take(&mut consumer, &mut Delivered::default(), 1)
            .await
            .remove(0);
        if message.topic == added {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "nothing reached {added} after {sent} messages"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_producer_writes_to_partitions_added_while_it_runs() {
    grow_and_produce(2, "producer-grows").await;
}

#[tokio::test]
async fn a_producer_of_a_one_partition_topic_grows_too() {
    grow_and_produce(1, "producer-grows-from-one").await;
}
