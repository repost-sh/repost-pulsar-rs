//! A reader over a partitioned topic: one non-durable reader that reads every
//! partition, from a chosen start.

use std::time::Duration;

use pulsar::{proto::MessageIdData, reader::Reader, ConsumerOptions, Pulsar, TokioExecutor};

use crate::{
    cluster::{exclusive, partition, topic},
    delivery::{collect, earliest_reader, publish, take, Delivered, DEADLINE},
};

#[tokio::test]
async fn a_reader_reads_every_partition_of_a_topic() {
    let cluster = exclusive().await;
    let topic = topic("reader-partitioned");
    cluster.create_partitioned_topic(&topic, 3).await;
    let pulsar = cluster.client().await;
    for index in 0..3 {
        publish(&pulsar, &partition(&topic, index), 1..=5).await;
    }

    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 15, DEADLINE).await;
    for index in 0..3 {
        delivered.assert_exactly(&partition(&topic, index), 1..=5);
    }
}

/// A reader that starts `rollback` back in time.
async fn rollback_reader(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    rollback: Duration,
) -> Reader<String, TokioExecutor> {
    pulsar
        .reader()
        .with_topic(topic)
        .with_subscription("reader")
        .with_options(ConsumerOptions::default().with_start_rollback(rollback))
        .into_reader()
        .await
        .expect("reader")
}

#[tokio::test]
async fn a_reader_with_a_rollback_starts_that_far_back() {
    let cluster = exclusive().await;
    let topic = topic("reader-rollback");
    cluster.create_partitioned_topic(&topic, 2).await;
    let partitions = [partition(&topic, 0), partition(&topic, 1)];
    let pulsar = cluster.client().await;
    for partition in &partitions {
        publish(&pulsar, partition, 1..=5).await;
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    for partition in &partitions {
        publish(&pulsar, partition, 6..=10).await;
    }

    // Four seconds back covers the second five on each partition only, with
    // room for a slow publish.
    let mut reader = rollback_reader(&pulsar, &topic, Duration::from_secs(4)).await;

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 10, DEADLINE).await;
    for partition in &partitions {
        delivered.assert_exactly(partition, 6..=10);
    }
}

#[tokio::test]
async fn a_lagging_reader_with_a_rollback_loses_nothing_on_a_reconnect() {
    let cluster = exclusive().await;
    let topic = topic("reader-rollback-lag");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=10).await;
    let mut reader = rollback_reader(&pulsar, &topic, Duration::from_secs(4)).await;
    let mut delivered = Delivered::default();
    take(&mut reader, &mut delivered, 5).await;

    // 6 to 10 fall out of the rollback window while the reader is behind.
    tokio::time::sleep(Duration::from_secs(6)).await;
    cluster.unload(&topic).await;
    // Messages published after the unload prove the reader came back.
    publish(&pulsar, &topic, 11..=12).await;

    collect(&mut reader, &mut delivered, 12, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=12);
}

#[tokio::test]
async fn a_reader_with_a_rollback_keeps_its_seek() {
    let cluster = exclusive().await;
    let topic = topic("reader-rollback-seek");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let before = chrono::Utc::now().timestamp_millis() as u64;
    publish(&pulsar, &topic, 1..=10).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    // All ten are older than the rollback, so the reader starts after them.
    let mut reader = rollback_reader(&pulsar, &topic, Duration::from_secs(2)).await;

    reader.seek(None, Some(before)).await.expect("seek");

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 10, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=10);
}

#[tokio::test]
async fn a_rollback_is_only_for_readers_without_a_start_id() {
    let cluster = exclusive().await;
    let topic = topic("reader-rollback-invalid");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let rollback = ConsumerOptions::default().with_start_rollback(Duration::from_secs(60));

    let consumer = pulsar
        .consumer()
        .with_topic(&topic)
        .with_subscription("durable")
        .with_options(rollback.clone())
        .build::<String>()
        .await;
    assert!(consumer.is_err(), "a durable consumer took a rollback");

    let reader = pulsar
        .reader()
        .with_topic(&topic)
        .with_subscription("reader")
        .with_options(rollback.starting_on_message(MessageIdData::default()))
        .into_reader::<String>()
        .await;
    assert!(reader.is_err(), "a reader took a rollback and a start id");
}

#[tokio::test]
async fn a_reader_keeps_its_seek_across_a_later_reconnect() {
    let cluster = exclusive().await;
    let topic = topic("reader-seek-reconnect");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=10).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let taken = take(&mut reader, &mut Delivered::default(), 10).await;

    // Seeking to the last message leaves nothing to deliver, so nothing moves
    // the reader before the topic is unloaded.
    reader
        .seek(Some(taken[9].message_id().clone()), None)
        .await
        .expect("seek");
    cluster.unload(&topic).await;
    cluster
        .wait_subscription(&topic, "reader", |s| !s.consumers.is_empty())
        .await;
    publish(&pulsar, &topic, 11..=12).await;

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 2, DEADLINE).await;
    delivered.assert_exactly(&topic, 11..=12);
}

#[tokio::test]
async fn a_reader_keeps_its_time_seek_across_a_later_reconnect() {
    let cluster = exclusive().await;
    let topic = topic("reader-time-seek-reconnect");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=10).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    take(&mut reader, &mut Delivered::default(), 10).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Nothing is published after this time, so nothing moves the reader
    // before the unload makes the broker create its subscription again.
    let now = chrono::Utc::now().timestamp_millis() as u64;
    reader.seek(None, Some(now)).await.expect("seek");
    // the seek's own reconnect first: this test is about a later one
    tokio::time::sleep(Duration::from_secs(1)).await;
    cluster.unload(&topic).await;
    cluster
        .wait_subscription(&topic, "reader", |s| !s.consumers.is_empty())
        .await;
    publish(&pulsar, &topic, 11..=12).await;

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 2, DEADLINE).await;
    delivered.assert_exactly(&topic, 11..=12);
}
