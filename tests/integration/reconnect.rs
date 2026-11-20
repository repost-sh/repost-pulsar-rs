//! What a consumer delivers when the broker moves its topic: after an unload,
//! a rolling restart or a crash, every message arrives once and in order.

use std::time::Duration;

use futures::StreamExt;
use pulsar::{consumer::InitialPosition, reader::Reader, ConsumerOptions, TokioExecutor};

use crate::{
    cluster::{exclusive, topic, topic_in, STRICT_NAMESPACE},
    delivery::{collect, earliest_consumer, publish, take, Delivered},
};

const DEADLINE: Duration = Duration::from_secs(90);

#[tokio::test]
async fn an_unloaded_consumer_does_not_deliver_its_stale_buffer_again() {
    let cluster = exclusive().await;
    let topic = topic("reconnect-durable");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=20).await;

    let mut delivered = Delivered::default();
    for message in take(&mut consumer, &mut delivered, 5).await {
        consumer.ack(&message).await.expect("ack");
    }
    // The first five are acked on the broker; the other fifteen sit in the
    // client's receive queue, so the unload catches them there.
    cluster
        .wait_subscription(&topic, "durable", |s| {
            s.msg_backlog == 15 && s.msg_out_counter == 20
        })
        .await;
    cluster.unload(&topic).await;
    // Messages published after the unload prove the consumer came back.
    publish(&pulsar, &topic, 21..=25).await;

    collect(&mut consumer, &mut delivered, 25, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=25);
}

#[tokio::test]
async fn an_unloaded_reader_resumes_after_its_last_delivered_message() {
    let cluster = exclusive().await;
    let topic = topic("reconnect-reader");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=20).await;
    let mut reader: Reader<String, TokioExecutor> = pulsar
        .reader()
        .with_topic(&topic)
        .with_subscription("reader")
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .into_reader()
        .await
        .expect("reader");

    let mut delivered = Delivered::default();
    take(&mut reader, &mut delivered, 5).await;
    cluster
        .wait_subscription(&topic, "reader", |s| s.msg_out_counter == 20)
        .await;
    cluster.unload(&topic).await;
    publish(&pulsar, &topic, 21..=25).await;

    collect(&mut reader, &mut delivered, 25, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=25);
}

#[tokio::test]
async fn an_unloaded_reader_at_latest_skips_nothing_published_after_it_started() {
    let cluster = exclusive().await;
    let topic = topic("reconnect-latest");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    // A one-message receive queue and a two-message flow window keep most of
    // the topic on the broker, so the unload lands before it was dispatched.
    let mut reader: Reader<String, TokioExecutor> = pulsar
        .reader()
        .with_topic(&topic)
        .with_subscription("latest")
        .with_batch_size(2)
        .with_options(ConsumerOptions::default().with_receiver_queue_size(1))
        .into_reader()
        .await
        .expect("reader");
    publish(&pulsar, &topic, 1..=20).await;

    let mut delivered = Delivered::default();
    take(&mut reader, &mut delivered, 3).await;
    cluster.unload(&topic).await;

    collect(&mut reader, &mut delivered, 20, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=20);
}

#[tokio::test]
async fn a_consumer_follows_its_topic_through_a_rolling_restart() {
    let mut cluster = exclusive().await;
    let topic = topic("reconnect-stop");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_via(survivor).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;

    let mut delivered = Delivered::default();
    for message in take(&mut consumer, &mut delivered, 5).await {
        consumer.ack(&message).await.expect("ack");
    }
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_backlog == 0)
        .await;

    cluster.stop_broker(owner).await;
    publish(&pulsar, &topic, 6..=10).await;

    collect(&mut consumer, &mut delivered, 10, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=10);
}

#[tokio::test]
async fn a_consumer_follows_its_topic_when_the_broker_dies() {
    let mut cluster = exclusive().await;
    let topic = topic("reconnect-kill");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_via(survivor).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;

    let mut delivered = Delivered::default();
    for message in take(&mut consumer, &mut delivered, 5).await {
        consumer.ack(&message).await.expect("ack");
    }

    cluster.kill_broker(owner).await;
    cluster.wait_owner(&topic, survivor).await;
    publish(&cluster.client_via(survivor).await, &topic, 6..=10).await;

    collect(&mut consumer, &mut delivered, 10, DEADLINE).await;
    delivered.assert_exactly_ignoring_earlier(&topic, 6..=10);
}

#[tokio::test]
async fn an_unloaded_reader_keeps_what_it_had_not_handed_out_yet() {
    let cluster = exclusive().await;
    let topic = topic("reconnect-unread");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut reader: Reader<String, TokioExecutor> = pulsar
        .reader()
        .with_topic(&topic)
        .with_subscription("unread")
        .into_reader()
        .await
        .expect("reader");
    publish(&pulsar, &topic, 1..=10).await;
    // All ten sit in the reader's queue and none has been read when the
    // unload lands.
    cluster
        .wait_subscription(&topic, "unread", |s| s.msg_out_counter == 10)
        .await;
    cluster.unload(&topic).await;
    publish(&pulsar, &topic, 11..=15).await;

    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 15, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=15);
}

#[tokio::test]
async fn a_reader_that_seeks_back_reads_again_from_there() {
    let cluster = exclusive().await;
    let topic = topic("reconnect-seek");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=10).await;
    let mut reader: Reader<String, TokioExecutor> = pulsar
        .reader()
        .with_topic(&topic)
        .with_subscription("seek")
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .into_reader()
        .await
        .expect("reader");
    let first_pass = take(&mut reader, &mut Delivered::default(), 10).await;

    reader
        .seek(Some(first_pass[4].message_id().clone()), None)
        .await
        .expect("seek");
    publish(&pulsar, &topic, 11..=12).await;

    // As in the Java client, the target is the exclusive start: delivery
    // restarts with the message after it.
    let mut delivered = Delivered::default();
    collect(&mut reader, &mut delivered, 7, DEADLINE).await;
    delivered.assert_exactly(&topic, 6..=12);
}

#[tokio::test]
async fn a_consumer_reports_a_deleted_topic_and_ends() {
    let cluster = exclusive().await;
    cluster.ensure_strict_namespace().await;
    let topic = topic_in(STRICT_NAMESPACE, "reconnect-deleted");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = crate::delivery::earliest_consumer(&pulsar, &topic, "durable").await;

    cluster.delete_topic(&topic).await;

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
