//! Acking messages that the producer sent as one batch entry: an ack for
//! some of the batch must not acknowledge the rest of it, and acking all of
//! it must acknowledge the entry.

use std::time::Duration;

use pulsar::{Consumer, ConsumerOptions, Pulsar, TokioExecutor};

use crate::{
    cluster::{exclusive, topic},
    delivery::{
        collect, earliest_consumer, earliest_consumer_with, publish_batched, take, Delivered,
        DEADLINE,
    },
};

/// A consumer of `topic` from its start that keeps a partly acked batch to
/// itself, as clients without batch-index acks do.
async fn without_batch_index_acks(
    pulsar: &Pulsar<TokioExecutor>,
    topic: &str,
    subscription: &str,
) -> Consumer<String, TokioExecutor> {
    let options = ConsumerOptions::default().with_batch_index_ack(false);
    earliest_consumer_with(pulsar, topic, subscription, options).await
}

#[tokio::test]
async fn a_cumulative_ack_inside_a_batch_loses_none_of_the_rest() {
    let cluster = exclusive().await;
    let topic = topic("batch-cumulative");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=20).await;

    let mut first = without_batch_index_acks(&pulsar, &topic, "batched").await;
    // Message 15 is the fifth of the second entry.
    let taken = take(&mut first, &mut Delivered::default(), 15).await;
    assert!(
        taken[14].message_id().batch_index.is_some(),
        "the producer did not batch"
    );
    first.cumulative_ack(&taken[14]).await.expect("ack");
    // Java acks only the entry before a partly acked batch, which leaves one
    // entry in the backlog (the broker counts entries, not messages).
    cluster
        .wait_subscription(&topic, "batched", |s| s.msg_backlog == 1)
        .await;
    drop(first);

    let mut second = earliest_consumer(&pulsar, &topic, "batched").await;
    let mut delivered = Delivered::default();
    collect(&mut second, &mut delivered, 10, DEADLINE).await;
    // 11 to 15 come back with the rest of their entry; nothing is lost.
    delivered.assert_exactly(&topic, 11..=20);
}

#[tokio::test]
async fn individually_acking_part_of_a_batch_loses_none_of_the_rest() {
    let cluster = exclusive().await;
    let topic = topic("batch-individual");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;

    let mut first = without_batch_index_acks(&pulsar, &topic, "batched").await;
    let taken = take(&mut first, &mut Delivered::default(), 10).await;
    for message in &taken[..3] {
        first.ack(message).await.expect("ack");
    }
    // Nothing is sent for a partly acked batch, so there is no broker state
    // to wait for; give a wrongly sent ack time to land.
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(first);

    let mut second = earliest_consumer(&pulsar, &topic, "batched").await;
    let mut delivered = Delivered::default();
    collect(&mut second, &mut delivered, 10, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=10);
}

#[tokio::test]
async fn acking_every_member_of_a_batch_acks_the_entry() {
    let cluster = exclusive().await;
    let topic = topic("batch-complete");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=20).await;

    let mut consumer = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 20).await;
    for message in &taken[..10] {
        consumer.ack(message).await.expect("ack");
    }
    consumer.cumulative_ack(&taken[19]).await.expect("ack");

    cluster
        .wait_subscription(&topic, "batched", |s| s.msg_backlog == 0)
        .await;
}

#[tokio::test]
async fn individual_then_cumulative_acks_complete_a_batch() {
    let cluster = exclusive().await;
    let topic = topic("batch-mixed");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=20).await;

    let mut consumer = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 20).await;
    // The last five of the second entry first, then everything up to the
    // fifth of it: together that is all of both entries.
    for message in &taken[15..] {
        consumer.ack(message).await.expect("ack");
    }
    consumer.cumulative_ack(&taken[14]).await.expect("ack");

    cluster
        .wait_subscription(&topic, "batched", |s| s.msg_backlog == 0)
        .await;
}

#[tokio::test]
async fn after_a_seek_into_a_batch_acking_the_rest_acks_the_entry() {
    let cluster = exclusive().await;
    let topic = topic("batch-seek");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;

    let mut consumer = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 10).await;
    consumer
        .seek_to(&topic, taken[4].message_id().clone())
        .await
        .expect("seek");

    // Delivery restarts after 5; members 1 to 5 of the entry are skipped.
    let mut delivered = Delivered::default();
    for message in take(&mut consumer, &mut delivered, 5).await {
        consumer.ack(&message).await.expect("ack");
    }
    delivered.assert_exactly(&topic, 6..=10);
    cluster
        .wait_subscription(&topic, "batched", |s| s.msg_backlog == 0)
        .await;
}

// Java: ConsumerImpl.receiveIndividualMessagesFromBatch skips the members the
// broker's ack_set marks acked.
#[tokio::test]
async fn a_cumulative_ack_inside_a_batch_holds_across_a_restart() {
    let cluster = exclusive().await;
    let topic = topic("batch-index-cumulative");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=20).await;

    let mut first = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut first, &mut Delivered::default(), 15).await;
    first.cumulative_ack(&taken[14]).await.expect("ack");
    cluster
        .wait_subscription(&topic, "batched", |s| s.msg_backlog == 1)
        .await;
    drop(first);

    let mut second = earliest_consumer(&pulsar, &topic, "batched").await;
    let mut delivered = Delivered::default();
    collect(&mut second, &mut delivered, 5, DEADLINE).await;
    delivered.assert_exactly(&topic, 16..=20);
}

#[tokio::test]
async fn individually_acked_members_of_a_batch_stay_acked_across_a_restart() {
    let cluster = exclusive().await;
    let topic = topic("batch-index-individual");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;

    let mut first = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut first, &mut Delivered::default(), 10).await;
    for message in &taken[7..] {
        first.ack(message).await.expect("ack");
    }
    // the broker shows no state for a partly acked entry to wait for
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(first);

    // Even without batch-index acks of its own, a consumer skips the members
    // acked before, and completes the entry once it acks the rest.
    let mut second = without_batch_index_acks(&pulsar, &topic, "batched").await;
    let mut delivered = Delivered::default();
    let again = take(&mut second, &mut delivered, 7).await;
    // and nothing more after them
    collect(&mut second, &mut delivered, 7, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=7);

    for message in &again {
        second.ack(message).await.expect("ack");
    }
    cluster
        .wait_subscription(&topic, "batched", |s| s.msg_backlog == 0)
        .await;
}

// An ack by the id a message gives for acking covers that member only.
#[tokio::test]
async fn acking_members_of_a_batch_by_id_holds_across_a_restart() {
    let cluster = exclusive().await;
    let topic = topic("batch-ack-by-id");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;

    let mut first = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut first, &mut Delivered::default(), 10).await;
    for message in &taken[7..] {
        first
            .ack_with_id(&topic, message.ack_id())
            .await
            .expect("ack");
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(first);

    let mut second = earliest_consumer(&pulsar, &topic, "batched").await;
    let mut delivered = Delivered::default();
    collect(&mut second, &mut delivered, 7, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=7);
}

// A cumulative ack by the id a message reports, without its batch size, must
// not ack the members after it.
#[tokio::test]
async fn a_cumulative_ack_by_message_id_inside_a_batch_holds_across_a_restart() {
    let cluster = exclusive().await;
    let topic = topic("batch-cumulative-by-id");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish_batched(&pulsar, &topic, 1..=10).await;

    let mut first = earliest_consumer(&pulsar, &topic, "batched").await;
    let taken = take(&mut first, &mut Delivered::default(), 10).await;
    first
        .cumulative_ack_with_id(&topic, taken[3].message_id().clone())
        .await
        .expect("ack");
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(first);

    let mut second = earliest_consumer(&pulsar, &topic, "batched").await;
    let mut delivered = Delivered::default();
    collect(&mut second, &mut delivered, 6, DEADLINE).await;
    delivered.assert_exactly(&topic, 5..=10);
}
