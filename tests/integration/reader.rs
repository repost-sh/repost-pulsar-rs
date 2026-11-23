//! A reader over a partitioned topic: one non-durable reader that reads every
//! partition, from a chosen start.

use std::{ops::RangeInclusive, time::Duration};

use pulsar::{
    consumer::InitialPosition,
    proto::{command_subscribe::SubType, MessageIdData},
    reader::Reader,
    ConsumerOptions, Pulsar, TokioExecutor,
};

use crate::{
    cluster::{exclusive, partition, topic, within, Cluster},
    delivery::{
        available, collect, earliest_reader, next, publish, publish_batched, reader_with, take,
        Delivered, DEADLINE,
    },
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
    let options = ConsumerOptions::default().with_start_rollback(rollback);
    reader_with(pulsar, topic, "reader", options).await
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

/// A reader of `topic` from `start`, with or without it.
async fn reader_from(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    start: MessageIdData,
    inclusive: bool,
) -> Reader<String, TokioExecutor> {
    let options = ConsumerOptions::default()
        .starting_on_message(start)
        .with_start_inclusive(inclusive);
    reader_with(pulsar, topic, "from", options).await
}

/// Reads `topic` from `start`, without it and then with it.
async fn assert_reads_from(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    start: &MessageIdData,
    after: RangeInclusive<u64>,
    at: RangeInclusive<u64>,
) {
    for (inclusive, expected) in [(false, after), (true, at)] {
        let mut reader = reader_from(pulsar, topic, start.clone(), inclusive).await;
        let mut delivered = Delivered::default();
        let count = expected.clone().count();
        collect(&mut reader, &mut delivered, count, DEADLINE).await;
        delivered.assert_exactly(topic, expected);
    }
}

/// The ids of `count` messages of `topic`, read from its start.
async fn ids(pulsar: &Pulsar<TokioExecutor>, topic: &str, count: usize) -> Vec<MessageIdData> {
    let mut reader = earliest_reader(pulsar, topic, "ids").await;
    let taken = take(&mut reader, &mut Delivered::default(), count).await;
    taken.iter().map(|m| m.message_id().clone()).collect()
}

// Java: a reader's start message is excluded unless startMessageIdInclusive.
#[tokio::test]
async fn a_reader_starts_after_or_at_its_start_message() {
    let cluster = exclusive().await;
    let topic = topic("reader-start-id");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=5).await;
    let ids = ids(&pulsar, &topic, 5).await;

    assert_reads_from(&pulsar, &topic, &ids[2], 4..=5, 3..=5).await;
}

#[tokio::test]
async fn a_reader_starts_after_or_at_a_member_of_a_batch() {
    let cluster = exclusive().await;
    let topic = topic("reader-start-batch");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;
    let ids = ids(&pulsar, &topic, 10).await;
    assert!(ids[3].batch_index.is_some(), "the producer did not batch");

    assert_reads_from(&pulsar, &topic, &ids[3], 5..=10, 4..=10).await;
}

#[tokio::test]
async fn a_reader_knows_whether_a_message_is_left() {
    let cluster = exclusive().await;
    let topic = topic("reader-available");
    cluster.create_partitioned_topic(&topic, 2).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &partition(&topic, 0), 1..=2).await;
    publish(&pulsar, &partition(&topic, 1), 1..=1).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;

    assert!(available(&mut reader).await, "three unread messages");
    take(&mut reader, &mut Delivered::default(), 3).await;
    assert!(!available(&mut reader).await, "everything was read");
    publish(&pulsar, &partition(&topic, 1), 2..=2).await;
    assert!(available(&mut reader).await, "a new message");
}

#[tokio::test]
async fn a_reader_at_the_latest_message_has_none_until_one_comes() {
    let cluster = exclusive().await;
    let topic = topic("reader-available-latest");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=3).await;
    let mut reader = reader_with(&pulsar, &topic, "latest", ConsumerOptions::default()).await;

    assert!(!available(&mut reader).await, "nothing after the start");
    publish(&pulsar, &topic, 4..=4).await;
    assert!(available(&mut reader).await, "a new message");
}

#[tokio::test]
async fn a_reader_inside_a_batch_has_the_rest_of_it() {
    let cluster = exclusive().await;
    let topic = topic("reader-available-batch");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;

    take(&mut reader, &mut Delivered::default(), 4).await;
    assert!(available(&mut reader).await, "six left in the batch");
    take(&mut reader, &mut Delivered::default(), 6).await;
    assert!(!available(&mut reader).await, "the batch was read");
}

#[tokio::test]
async fn a_reader_from_the_last_message_knows_whether_it_is_left() {
    let cluster = exclusive().await;
    let topic = topic("reader-available-from");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=5).await;
    let ids = ids(&pulsar, &topic, 5).await;

    let mut after = reader_from(&pulsar, &topic, ids[4].clone(), false).await;
    assert!(
        !available(&mut after).await,
        "nothing after the last message"
    );
    drop(after);

    let mut at = reader_from(&pulsar, &topic, ids[4].clone(), true).await;
    assert!(available(&mut at).await, "the last message itself");
    take(&mut at, &mut Delivered::default(), 1).await;
    assert!(!available(&mut at).await, "the last message was read");
}

#[tokio::test]
async fn messages_queued_before_a_seek_to_the_end_are_not_available() {
    let cluster = exclusive().await;
    let topic = topic("reader-available-seek");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=10).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let taken = take(&mut reader, &mut Delivered::default(), 1).await;
    let last = ids(&pulsar, &topic, 10).await.pop().expect("ten ids");
    // the other nine wait in the reader's queue
    cluster
        .wait_subscription(&topic, "reader", |s| s.msg_out_counter >= 10)
        .await;
    assert_eq!(taken.len(), 1);

    tokio::time::timeout(DEADLINE, reader.seek_to(&topic, last))
        .await
        .expect("a seek within the deadline")
        .expect("seek");
    assert!(!available(&mut reader).await, "only stale messages queued");
}

// A message id belongs to one partition; Java hands it to every partition of
// a reader, where it means nothing. A partitioned reader starts from the
// earliest, the latest or a rollback, and seeks a partition by id.
#[tokio::test]
async fn a_partitioned_reader_refuses_a_start_message_id() {
    let cluster = exclusive().await;
    let topic = topic("reader-partitioned-start");
    cluster.create_partitioned_topic(&topic, 2).await;
    let pulsar = cluster.client().await;
    let start = MessageIdData {
        ledger_id: 1,
        entry_id: 0,
        ..Default::default()
    };
    let reader = pulsar
        .reader()
        .with_topic(&topic)
        .with_options(ConsumerOptions::default().starting_on_message(start))
        .into_reader::<String>()
        .await;
    assert!(reader.is_err(), "a partitioned reader took a start id");
}

// A seek by time lands on the first message published at or after it. The
// protocol's rollback counts whole seconds, so after the seek's reconnect the
// client drops what that lets in.
#[tokio::test]
async fn a_reader_seeking_by_time_gets_nothing_published_before() {
    let cluster = exclusive().await;
    let topic = topic("reader-time-seek-exact");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    for number in 1..=10 {
        publish(&pulsar, &topic, number..=number).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let taken = take(&mut reader, &mut Delivered::default(), 10).await;

    let target = taken[5].metadata().publish_time;
    reader.seek(None, Some(target)).await.expect("seek");
    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 5, DEADLINE).await;
    delivered.assert_exactly(&topic, 6..=10);
}

// A reader's subscription is exclusive whatever type the builder was given:
// a second reader under the same name is refused.
#[tokio::test]
async fn a_reader_subscribes_exclusive_whatever_type_it_was_given() {
    let cluster = exclusive().await;
    let topic = topic("reader-exclusive");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let reader = |sub_type| {
        pulsar
            .reader()
            .with_topic(&topic)
            .with_subscription("reader")
            .with_subscription_type(sub_type)
            .into_reader::<String>()
    };

    let first = reader(SubType::Shared).await.expect("reader");
    assert_eq!(first.sub_type(), SubType::Exclusive);
    assert!(
        reader(SubType::Exclusive).await.is_err(),
        "a second reader took the same exclusive subscription"
    );
}

#[tokio::test]
async fn a_reader_tells_which_partitions_hold_a_message_left() {
    let cluster = exclusive().await;
    let topic = topic("reader-available-partitions");
    cluster.create_partitioned_topic(&topic, 2).await;
    let partitions = [partition(&topic, 0), partition(&topic, 1)];
    let pulsar = cluster.client().await;
    publish(&pulsar, &partitions[0], 1..=2).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let left = |first: bool, second: bool| {
        [
            (partitions[0].clone(), first),
            (partitions[1].clone(), second),
        ]
        .into_iter()
        .collect()
    };

    assert_eq!(partitions_left(&reader).await, left(true, false));
    take(&mut reader, &mut Delivered::default(), 2).await;
    assert_eq!(partitions_left(&reader).await, left(false, false));
    publish(&pulsar, &partitions[1], 1..=1).await;
    assert_eq!(partitions_left(&reader).await, left(false, true));
}

/// Which partitions hold a message `reader` has not read, answered within
/// [`DEADLINE`].
async fn partitions_left(
    reader: &Reader<String, TokioExecutor>,
) -> std::collections::BTreeMap<String, bool> {
    within(
        DEADLINE,
        "the partitions' answers",
        reader.messages_available(),
    )
    .await
    .expect("messages_available")
}

// The answer borrows nothing of the reader: while a frozen broker keeps it
// waiting, the application reads the partitions of the other broker on.
#[tokio::test]
async fn a_frozen_broker_holds_the_answer_and_not_the_other_partitions() {
    let mut cluster = exclusive().await;
    let (topic, frozen) = topic_on_both_brokers(&cluster).await;
    let (stuck, live) = (partition(&topic, 0), partition(&topic, 1));
    let pulsar = cluster.client_via(1 - frozen).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    publish(&pulsar, &live, 1..=1).await;
    take(&mut reader, &mut Delivered::default(), 1).await;

    cluster.pause_broker(frozen).await;
    let mut answer = Box::pin(reader.messages_available());
    publish(&pulsar, &live, 2..=3).await;
    let mut delivered = Delivered::default();
    for _ in 0..2 {
        tokio::select! {
            _ = next(&mut reader, &mut delivered) => {}
            early = &mut answer => panic!("the frozen broker answered: {early:?}"),
        }
    }
    delivered.assert_exactly(&live, 2..=3);

    // back before its session expires, the broker answers what it was asked
    cluster.resume_broker(frozen).await;
    let left = within(DEADLINE, "the answer once the broker runs on", answer)
        .await
        .expect("messages_available");
    assert_eq!(left.get(&stuck), Some(&false), "{left:?}");
    assert_eq!(left.get(&live), Some(&false), "{left:?}");
}

/// A topic of two partitions, the first owned by the broker returned and the
/// second by the other one. Two partitions in one bundle move together, so
/// this tries new names until a pair splits.
async fn topic_on_both_brokers(cluster: &Cluster) -> (String, usize) {
    for _ in 0..16 {
        let topic = topic("reader-two-brokers");
        cluster.create_partitioned_topic(&topic, 2).await;
        let (first, second) = (partition(&topic, 0), partition(&topic, 1));
        let owner = cluster.owner(&first).await;
        cluster.move_to(&second, 1 - owner).await;
        if cluster.owner(&first).await == owner {
            return (topic, owner);
        }
    }
    panic!("no topic with its two partitions on different brokers");
}

// A reader acknowledges cumulatively, once a second, so the broker's backlog
// of its subscription follows what it read: an entry counts until the reader
// has taken all of it.
#[tokio::test]
async fn a_readers_backlog_follows_what_it_read() {
    let cluster = exclusive().await;
    let topic = topic("reader-acks");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=6).await;
    publish_batched(&pulsar, &topic, 7..=26).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let backlog = |entries: u64| {
        let (cluster, topic) = (&cluster, &topic);
        async move {
            cluster
                .wait_subscription(topic, "reader", |s| s.msg_backlog == entries)
                .await;
        }
    };

    // six single messages and two batches of ten
    backlog(8).await;
    take(&mut reader, &mut Delivered::default(), 4).await;
    backlog(4).await;
    // into the second batch: the first one is read whole
    take(&mut reader, &mut Delivered::default(), 15).await;
    backlog(1).await;
    take(&mut reader, &mut Delivered::default(), 7).await;
    backlog(0).await;
}

// The ack a reader owes for what it read is dropped by a seek: sent after
// it, the ack would move the subscription past the seek's target. With a
// window of two the broker still holds most of what the seek brings back
// when the next tick comes.
#[tokio::test]
async fn a_seek_right_after_reading_is_not_passed_by_the_readers_ack() {
    let cluster = exclusive().await;
    let topic = topic("reader-acks-seek");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=10).await;
    let options = ConsumerOptions::default()
        .with_initial_position(InitialPosition::Earliest)
        .with_receiver_queue_size(2);
    let mut reader = reader_with(&pulsar, &topic, "reader", options).await;
    let taken = take(&mut reader, &mut Delivered::default(), 10).await;

    reader
        .seek(Some(taken[2].message_id().clone()), None)
        .await
        .expect("seek");
    // past the engine's next tick, with nothing taken meanwhile
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 7, DEADLINE).await;
    delivered.assert_exactly(&topic, 4..=10);
}
