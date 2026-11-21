//! Seeking to a message so that the message itself comes next: how an
//! application that keeps its own position finds out, when it restarts
//! there, whether the topic still holds that message.

use std::{future::Future, ops::RangeInclusive, time::Duration};

use futures::Stream;
use pulsar::{consumer::Message, proto::MessageIdData, Consumer, Error, TokioExecutor};

use crate::{
    cluster::{exclusive, partition, topic, Cluster},
    delivery::{
        available, earliest_consumer, earliest_reader, next, number, publish, publish_batched,
        take, Delivered, DEADLINE,
    },
};

/// Where a message sits: its ledger, its entry, and its place in a batch.
fn place(id: &MessageIdData) -> (u64, u64, Option<i32>) {
    (id.ledger_id, id.entry_id, id.batch_index)
}

/// Waits for a seek to succeed, within [`DEADLINE`].
async fn await_seek(seek: impl Future<Output = Result<(), Error>>) {
    tokio::time::timeout(DEADLINE, seek)
        .await
        .expect("an answer within the deadline")
        .expect("seek");
}

/// Asserts that `stream`, just sought, delivers `first` next and then the
/// rest of `expected`, each once and in order. Returns the ids delivered.
async fn assert_delivers_from<S>(
    stream: &mut S,
    topic: &str,
    first: &MessageIdData,
    expected: RangeInclusive<u64>,
) -> Vec<MessageIdData>
where
    S: Stream<Item = Result<Message<String>, Error>> + Unpin,
{
    let mut after = Delivered::default();
    let head = next(stream, &mut after).await;
    assert_eq!(
        (number(&head), place(head.message_id())),
        (*expected.start(), place(first)),
        "the first message after the seek"
    );
    let count = expected.clone().count();
    let rest = tokio::time::timeout(DEADLINE, take(stream, &mut after, count - 1))
        .await
        .expect("the rest within the deadline");
    after.assert_exactly(topic, expected);
    std::iter::once(&head)
        .chain(&rest)
        .map(|message| message.message_id().clone())
        .collect()
}

/// Asserts that nothing more arrives for three seconds, such as a duplicate.
async fn assert_quiet<S>(stream: &mut S)
where
    S: Stream<Item = Result<Message<String>, Error>> + Unpin,
{
    let late = tokio::time::timeout(
        Duration::from_secs(3),
        take(stream, &mut Delivered::default(), 1),
    )
    .await;
    if let Ok(late) = late {
        panic!(
            "{} arrived after the last expected message",
            number(&late[0])
        );
    }
}

/// A topic, a durable consumer of it, and the messages the consumer took.
type Subscribed = (
    String,
    Consumer<String, TokioExecutor>,
    Vec<Message<String>>,
);

/// A topic of 1 to 30 in batches of ten, one entry each, and a durable
/// consumer that took all 30 and acked none.
async fn batched_and_taken(cluster: &Cluster, prefix: &str) -> Subscribed {
    let topic = topic(prefix);
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=30).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 30).await;
    (topic, consumer, taken)
}

/// A topic whose first ledger holds 1 to 5 and whose second holds 6 to 8,
/// and a durable consumer that took all eight. It acked none, so its
/// subscription keeps the first ledger.
async fn two_ledgers(cluster: &Cluster, prefix: &str) -> Subscribed {
    let topic = topic(prefix);
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;
    // An unload closes the ledger, so the next message opens another.
    cluster.unload(&topic).await;
    publish(&pulsar, &topic, 6..=8).await;
    let taken = take(&mut consumer, &mut Delivered::default(), 8).await;
    let (last, following) = (taken[4].message_id(), taken[5].message_id());
    assert!(
        following.ledger_id > last.ledger_id && following.entry_id == 0,
        "no new ledger after {last:?}: {following:?}"
    );
    (topic, consumer, taken)
}

#[tokio::test]
async fn an_inclusive_seek_delivers_its_target_next() {
    let cluster = exclusive().await;
    let topic = topic("seek-at");
    cluster.create_partitioned_topic(&topic, 2).await;
    let first = partition(&topic, 0);
    let pulsar = cluster.client().await;
    publish(&pulsar, &first, 1..=10).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 10).await;
    let target = taken[4].message_id().clone();
    assert_eq!(target.batch_index, None, "the producer batched");

    await_seek(consumer.seek_to_inclusive(&first, target.clone())).await;
    assert_delivers_from(&mut consumer, &first, &target, 5..=10).await;
    assert_quiet(&mut consumer).await;
}

#[tokio::test]
async fn an_inclusive_seek_into_a_batch_starts_at_that_member() {
    let cluster = exclusive().await;
    let (topic, mut consumer, taken) = batched_and_taken(&cluster, "seek-at-member").await;

    for (number, member) in [(11, 0), (15, 4), (20, 9)] {
        let target = taken[number as usize - 1].message_id().clone();
        assert_eq!(target.batch_index, Some(member), "batches of ten");
        await_seek(consumer.seek_to_inclusive(&topic, target.clone())).await;
        assert_delivers_from(&mut consumer, &topic, &target, number..=30).await;
        // before the next seek, which discards what is buffered
        assert_quiet(&mut consumer).await;
    }
}

// What a consumer that acks its position and later restarts at it does. The
// broker forgets the members acked in an entry when it seeks there.
#[tokio::test]
async fn an_inclusive_seek_delivers_a_target_acked_inside_its_batch() {
    let cluster = exclusive().await;
    let (topic, mut consumer, taken) = batched_and_taken(&cluster, "seek-at-acked").await;
    consumer.cumulative_ack(&taken[14]).await.expect("ack");
    // the first entry, and half of the second
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_backlog == 2)
        .await;
    let target = taken[14].message_id().clone();

    await_seek(consumer.seek_to_inclusive(&topic, target.clone())).await;
    assert_delivers_from(&mut consumer, &topic, &target, 15..=30).await;
    assert_quiet(&mut consumer).await;
}

// The seek skips the members before its target without acking them, so when
// the consumer subscribes again the broker sends the target's entry whole.
#[tokio::test]
async fn a_reconnect_after_an_inclusive_seek_delivers_the_members_before_the_target() {
    let cluster = exclusive().await;
    let (topic, mut consumer, taken) = batched_and_taken(&cluster, "seek-at-resend").await;
    let target = taken[14].message_id().clone();
    await_seek(consumer.seek_to_inclusive(&topic, target.clone())).await;
    assert_delivers_from(&mut consumer, &topic, &target, 15..=16).await;

    cluster.unload(&topic).await;
    // back on the broker: nothing buffered from before the unload is left
    cluster
        .wait_subscription(&topic, "durable", |s| !s.consumers.is_empty())
        .await;

    assert_delivers_from(&mut consumer, &topic, taken[10].message_id(), 11..=30).await;
    assert_quiet(&mut consumer).await;
}

#[tokio::test]
async fn an_inclusive_seek_to_the_last_entry_of_a_ledger_continues_in_the_next() {
    let cluster = exclusive().await;
    let (topic, mut consumer, taken) = two_ledgers(&cluster, "seek-at-ledger-end").await;
    let (target, following) = (taken[4].message_id().clone(), taken[5].message_id().clone());
    let ledgers = cluster.ledgers(&topic).await;
    let closed = ledgers
        .iter()
        .find(|ledger| ledger.ledger_id == target.ledger_id)
        .expect("the target's ledger");
    assert_eq!(
        closed.entries,
        target.entry_id + 1,
        "the target is not the last entry of its ledger: {ledgers:?}"
    );

    await_seek(consumer.seek_to_inclusive(&topic, target.clone())).await;
    let ids = assert_delivers_from(&mut consumer, &topic, &target, 5..=8).await;
    assert_eq!(place(&ids[1]), place(&following));
    assert_quiet(&mut consumer).await;
}

// The broker moves a seek into a ledger it deleted to the first message it
// still has, and reports success: the message delivered next is the only
// sign that the target is gone.
#[tokio::test]
async fn an_inclusive_seek_into_a_deleted_ledger_delivers_what_follows_it() {
    let cluster = exclusive().await;
    let (topic, mut consumer, taken) = two_ledgers(&cluster, "seek-at-deleted").await;
    // The broker deletes a closed ledger once every subscription has acked
    // its last entry and retention, none on this cluster, lets it. It goes
    // by the ack position it saved, and it saves at most one a second: this
    // ack, the first since the unload loaded the topic again, is saved at
    // once.
    consumer.cumulative_ack(&taken[4]).await.expect("ack");
    cluster
        .wait_ledger_gone(&topic, taken[4].message_id().ledger_id)
        .await;

    // a message in the middle of the deleted ledger, and its last, the one
    // acked
    for target in [&taken[2], &taken[4]] {
        await_seek(consumer.seek_to_inclusive(&topic, target.message_id().clone())).await;
        assert_delivers_from(&mut consumer, &topic, taken[5].message_id(), 6..=8).await;
        assert_quiet(&mut consumer).await;
    }
}

// With nothing after a deleted target, nothing arrives until a publish, so
// waiting for the first message tells the consumer nothing.
#[tokio::test]
async fn an_inclusive_seek_into_a_deleted_ledger_with_nothing_after_it_waits_for_a_publish() {
    let cluster = exclusive().await;
    let topic = topic("seek-at-deleted-end");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;
    // An unload closes the ledger, and loading the topic again opens an
    // empty one.
    cluster.unload(&topic).await;
    let taken = take(&mut consumer, &mut Delivered::default(), 5).await;
    consumer.cumulative_ack(&taken[4]).await.expect("ack");
    cluster
        .wait_ledger_gone(&topic, taken[4].message_id().ledger_id)
        .await;

    await_seek(consumer.seek_to_inclusive(&topic, taken[2].message_id().clone())).await;
    assert_quiet(&mut consumer).await;
    publish(&pulsar, &topic, 6..=7).await;
    let mut after = Delivered::default();
    tokio::time::timeout(DEADLINE, take(&mut consumer, &mut after, 2))
        .await
        .expect("the publish within the deadline");
    after.assert_exactly(&topic, 6..=7);
}

#[tokio::test]
async fn an_inclusive_seek_made_during_a_reconnect_delivers_its_target_next() {
    let cluster = exclusive().await;
    let (topic, mut consumer, taken) = batched_and_taken(&cluster, "seek-at-reconnect").await;

    for number in 13..=15 {
        let target = taken[number as usize - 1].message_id().clone();
        // The broker closes the consumer, and the seek is made while it
        // subscribes again.
        cluster.unload(&topic).await;
        await_seek(consumer.seek_to_inclusive(&topic, target.clone())).await;
        assert_delivers_from(&mut consumer, &topic, &target, number..=30).await;
        // before the next seek, which discards what is buffered
        assert_quiet(&mut consumer).await;
    }
}

#[tokio::test]
async fn a_reader_seeking_inclusively_reads_its_target_next() {
    let cluster = exclusive().await;
    let topic = topic("seek-at-reader");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=5).await;
    publish_batched(&pulsar, &topic, 6..=25).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    let taken = take(&mut reader, &mut Delivered::default(), 25).await;

    for (number, member) in [(3, None), (13, Some(7))] {
        let target = taken[number as usize - 1].message_id().clone();
        assert_eq!(target.batch_index, member);
        await_seek(reader.seek_to_inclusive(&topic, target.clone())).await;
        assert_delivers_from(&mut reader, &topic, &target, number..=25).await;
        assert_quiet(&mut reader).await;
    }

    let last = taken[24].message_id().clone();
    await_seek(reader.seek_to_inclusive(&topic, last.clone())).await;
    assert!(available(&mut reader).await, "the target itself");
    assert_delivers_from(&mut reader, &topic, &last, 25..=25).await;
    assert!(!available(&mut reader).await, "the target was read");
    assert_quiet(&mut reader).await;
}
