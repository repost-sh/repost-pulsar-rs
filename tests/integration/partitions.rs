//! A consumer over several topics or partitions: it finds partitions added
//! while it runs, and a partition that fails does not stop the rest.

use std::time::Duration;

use futures::{Stream, StreamExt};
use pulsar::{
    consumer::{InitialPosition, Message},
    Consumer, ConsumerOptions, Error, Pulsar, TokioExecutor,
};

use crate::{
    cluster::{exclusive, partition, topic, topic_in, Cluster, STRICT_NAMESPACE},
    delivery::{collect, earliest_consumer, earliest_reader, publish, take, Delivered, DEADLINE},
};

const REFRESH: Duration = Duration::from_secs(1);

/// A durable consumer at the latest message that looks for partitions every
/// second.
async fn refreshing_consumer(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
) -> Consumer<String, TokioExecutor> {
    pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription("durable")
        .with_topic_refresh(REFRESH)
        .build()
        .await
        .expect("consumer")
}

/// Grows `topic` from `from` partitions to one more while `stream` reads it,
/// and publishes to the new partition at once: what `stream` has not found
/// yet must still arrive.
async fn grow_and_read<S>(
    cluster: &Cluster,
    pulsar: &Pulsar<TokioExecutor>,
    stream: &mut S,
    topic: &str,
    from: u32,
) where
    S: Stream<Item = Result<Message<String>, Error>> + Unpin,
{
    publish(pulsar, &partition(topic, 0), 1..=3).await;
    take(stream, &mut Delivered::default(), 3).await;

    cluster.update_partitions(topic, from + 1).await;
    let added = partition(topic, from);
    publish(pulsar, &added, 1..=5).await;

    let mut delivered = Delivered::default();
    collect(stream, &mut delivered, 5, DEADLINE).await;
    delivered.assert_exactly(&added, 1..=5);
}

// The broker copies a durable subscription to an added partition at its
// start, so a consumer at the latest message misses nothing there.
#[tokio::test]
async fn a_consumer_reads_partitions_added_while_it_runs() {
    let cluster = exclusive().await;
    let topic = topic("partitions-added");
    cluster.create_partitioned_topic(&topic, 2).await;
    let pulsar = cluster.client().await;
    let mut consumer = refreshing_consumer(&pulsar, &topic).await;
    grow_and_read(&cluster, &pulsar, &mut consumer, &topic, 2).await;
}

#[tokio::test]
async fn a_topic_with_one_partition_grows_too() {
    let cluster = exclusive().await;
    let topic = topic("partitions-from-one");
    cluster.create_partitioned_topic(&topic, 1).await;
    let pulsar = cluster.client().await;
    let mut consumer = refreshing_consumer(&pulsar, &topic).await;
    grow_and_read(&cluster, &pulsar, &mut consumer, &topic, 1).await;
}

// A reader's subscription is not copied, so the reader starts an added
// partition at its beginning.
#[tokio::test]
async fn a_reader_reads_partitions_added_while_it_runs() {
    let cluster = exclusive().await;
    let topic = topic("partitions-added-reader");
    cluster.create_partitioned_topic(&topic, 2).await;
    let pulsar = cluster.client().await;
    let mut reader = pulsar
        .reader()
        .with_topic(&topic)
        .with_subscription("reader")
        .with_topic_refresh(REFRESH)
        .into_reader::<String>()
        .await
        .expect("reader");
    grow_and_read(&cluster, &pulsar, &mut reader, &topic, 2).await;
}

#[tokio::test]
async fn a_failed_topic_reports_its_error_and_the_rest_carry_on() {
    let cluster = exclusive().await;
    cluster.ensure_strict_namespace().await;
    let (gone, kept) = (
        topic_in(STRICT_NAMESPACE, "partitions-gone"),
        topic_in(STRICT_NAMESPACE, "partitions-kept"),
    );
    cluster.create_topic(&gone).await;
    cluster.create_topic(&kept).await;
    let pulsar = cluster.client().await;
    let mut consumer: Consumer<String, TokioExecutor> = pulsar
        .consumer()
        .with_topics([&gone, &kept])
        .with_subscription("durable")
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .build()
        .await
        .expect("consumer");
    publish(&pulsar, &gone, 1..=2).await;
    publish(&pulsar, &kept, 1..=2).await;
    take(&mut consumer, &mut Delivered::default(), 4).await;

    cluster.delete_topic(&gone).await;

    let failure = tokio::time::timeout(DEADLINE, consumer.next())
        .await
        .expect("an error within the deadline");
    match failure {
        Some(Err(e)) => assert!(e.to_string().contains(&gone), "{e}"),
        other => panic!("expected an error, got {other:?}"),
    }
    publish(&pulsar, &kept, 3..=4).await;
    let mut delivered = Delivered::default();
    collect(&mut consumer, &mut delivered, 2, DEADLINE).await;
    delivered.assert_exactly(&kept, 3..=4);
}

#[tokio::test]
async fn a_consumer_whose_partitions_all_ended_ends() {
    let cluster = exclusive().await;
    cluster.ensure_strict_namespace().await;
    let topic = topic_in(STRICT_NAMESPACE, "partitions-all-gone");
    cluster.create_partitioned_topic(&topic, 1).await;
    let pulsar = cluster.client().await;
    let mut consumer = refreshing_consumer(&pulsar, &topic).await;
    publish(&pulsar, &partition(&topic, 0), 1..=1).await;
    take(&mut consumer, &mut Delivered::default(), 1).await;

    cluster.delete_partitioned_topic(&topic).await;

    let failure = tokio::time::timeout(DEADLINE, consumer.next())
        .await
        .expect("an error within the deadline");
    assert!(
        matches!(failure, Some(Err(_))),
        "expected an error, got {failure:?}"
    );
    let end = tokio::time::timeout(DEADLINE, consumer.next())
        .await
        .expect("the stream ends within the deadline");
    assert!(end.is_none(), "expected the end of the stream, got {end:?}");
}

#[tokio::test]
async fn a_reader_of_one_partition_seeks_an_id_and_names_its_last_id() {
    let cluster = exclusive().await;
    let topic = topic("partitions-one-reader");
    cluster.create_partitioned_topic(&topic, 1).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &partition(&topic, 0), 1..=5).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let taken = take(&mut reader, &mut Delivered::default(), 5).await;

    let last = reader.get_last_message_id().await.expect("last id");
    assert_eq!(
        (last.ledger_id, last.entry_id),
        (
            taken[4].message_id().ledger_id,
            taken[4].message_id().entry_id
        )
    );
    reader
        .seek(Some(taken[1].message_id().clone()), None)
        .await
        .expect("seek");
    let mut after = Delivered::default();
    collect(&mut reader, &mut after, 3, DEADLINE).await;
    after.assert_exactly(&partition(&topic, 0), 3..=5);
}

#[tokio::test]
async fn a_regex_consumer_reports_a_namespace_it_cannot_list() {
    let cluster = exclusive().await;
    let pulsar = cluster.client().await;
    let result = pulsar
        .consumer()
        .with_topic_regex(regex::Regex::new("persistent://public/rprs-missing/.*").unwrap())
        .with_lookup_namespace("public/rprs-missing")
        .with_subscription("durable")
        .build::<String>()
        .await;
    assert!(result.is_err(), "built a consumer of a missing namespace");
}

// Acks name a partition the way seek_to does: by the name the application
// built the consumer with, or by its full name.
#[tokio::test]
async fn a_partition_is_found_by_its_short_or_full_name() {
    let cluster = exclusive().await;
    let full = topic("partitions-names");
    let short = full.trim_start_matches("persistent://public/default/");
    cluster.create_partitioned_topic(&full, 2).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &partition(&full, 1), 1..=1).await;
    let mut consumer = earliest_consumer(&pulsar, short, "names").await;
    let message = take(&mut consumer, &mut Delivered::default(), 1)
        .await
        .remove(0);

    consumer
        .ack_with_id(&partition(&full, 1), message.message_id().clone())
        .await
        .expect("ack by the full name");
    cluster
        .wait_subscription(&partition(&full, 1), "names", |s| s.msg_backlog == 0)
        .await;
}
