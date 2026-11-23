//! What a consumer delivers when the broker moves its topic: after an unload,
//! a rolling restart or a crash, every message arrives once and in order.

use std::time::Duration;

use futures::StreamExt;
use pulsar::{reader::Reader, ConsumerOptions, TokioExecutor};

use crate::{
    cluster::{exclusive, topic, topic_in, STRICT_NAMESPACE},
    delivery::{collect, earliest_consumer, earliest_reader, publish, take, Delivered, DEADLINE},
};

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
    // The broker may send 1 to 5 again: it saves ack positions at most once a
    // second, and an unload right after the acks can lose the last of them.
    // What the client buffered, 6 to 20, must come once.
    delivered.assert_exactly_ignoring_earlier(&topic, 6..=25);
}

#[tokio::test]
async fn an_unloaded_reader_resumes_after_its_last_delivered_message() {
    let cluster = exclusive().await;
    let topic = topic("reconnect-reader");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=20).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;

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
    let mut reader = earliest_reader(&pulsar, &topic, "seek").await;
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
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;

    cluster.delete_topic(&topic).await;

    let failure = tokio::time::timeout(DEADLINE, consumer.next())
        .await
        .expect("an error within the deadline");
    assert!(
        matches!(&failure, Some(Err(e)) if e.is_final()),
        "expected the error that ends the consumer, got {failure:?}"
    );
    let end = tokio::time::timeout(DEADLINE, consumer.next())
        .await
        .expect("the stream ends within the deadline");
    assert!(end.is_none(), "expected the end of the stream, got {end:?}");
}

#[tokio::test]
async fn a_backed_up_consumer_still_answers_after_its_topic_is_deleted() {
    let cluster = exclusive().await;
    cluster.ensure_strict_namespace().await;
    let topic = topic_in(STRICT_NAMESPACE, "reconnect-deleted-full");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = crate::delivery::earliest_consumer_with(
        &pulsar,
        &topic,
        "durable",
        ConsumerOptions::default().with_receiver_queue_size(3),
    )
    .await;
    publish(&pulsar, &topic, 1..=10).await;
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter >= 3)
        .await;

    cluster.delete_topic(&topic).await;

    let answer = tokio::time::timeout(DEADLINE, consumer.check_connection()).await;
    assert!(
        answer.is_ok(),
        "check_connection hung after the topic was deleted"
    );
}

#[tokio::test]
async fn a_consumer_follows_its_topic_when_the_broker_freezes() {
    let mut cluster = exclusive().await;
    let topic = topic("reconnect-freeze");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_with_keepalive(survivor).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;
    let mut delivered = Delivered::default();
    for message in take(&mut consumer, &mut delivered, 5).await {
        consumer.ack(&message).await.expect("ack");
    }

    // The application only waits for messages, so no event wakes the
    // consumer: it has to notice the dead connection by itself.
    cluster.pause_broker(owner).await;
    cluster.wait_owner(&topic, survivor).await;
    publish(&cluster.client_via(survivor).await, &topic, 6..=10).await;

    collect(&mut consumer, &mut delivered, 10, DEADLINE).await;
    delivered.assert_exactly_ignoring_earlier(&topic, 6..=10);
}

// The window bounds what the broker sends ahead across a reconnect too: the
// stale messages the application skips while its consumer is away must not
// be granted twice.
#[tokio::test]
async fn a_reconnect_grants_no_more_than_the_window() {
    let mut cluster = exclusive().await;
    let topic = topic("flow-reconnect");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_via(survivor).await;
    publish(&pulsar, &topic, 1..=3000).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    // the default window of 1000 fills the receive queue
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter >= 1000)
        .await;
    take(&mut consumer, &mut Delivered::default(), 100).await;

    // The topic stays unowned for the broker's session timeout, ten seconds:
    // meanwhile the application skips the 900 stale messages.
    cluster.kill_broker(owner).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    take(&mut consumer, &mut Delivered::default(), 100).await;
    cluster.wait_owner(&topic, survivor).await;

    // 1000 granted at the reconnect; the 100 taken since free too few for a
    // flow of their own. A broker granted more would have sent more by now.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let sent = cluster
        .wait_subscription(&topic, "durable", |_| true)
        .await
        .msg_out_counter;
    assert!(sent <= 1000, "{sent} sent since the reconnect");
}

// A seek made while the consumer is away fails on the dead connection; given
// up on before the topic comes back, it must not leave the consumer dropping
// everything after the reconnect.
#[tokio::test]
async fn a_seek_given_up_during_an_outage_leaves_the_consumer_working() {
    let mut cluster = exclusive().await;
    let topic = topic("seek-outage");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_via(survivor).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;
    let mut delivered = Delivered::default();
    let taken = take(&mut consumer, &mut delivered, 5).await;
    for message in &taken {
        consumer.ack(message).await.expect("ack");
    }

    // the topic stays unowned for ten seconds
    cluster.kill_broker(owner).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let seek = consumer.seek_to(&topic, taken[2].message_id().clone());
    let given_up = tokio::time::timeout(Duration::from_secs(2), seek).await;
    assert!(given_up.is_err(), "the seek ended during the outage");

    cluster.wait_owner(&topic, survivor).await;
    publish(&cluster.client_via(survivor).await, &topic, 6..=10).await;
    collect(&mut consumer, &mut delivered, 10, DEADLINE).await;
    delivered.assert_exactly_ignoring_earlier(&topic, 6..=10);
}

// The engine of a consumer with a redelivery delay keeps a timer of its own;
// dropping the consumer must still close it on the broker.
#[tokio::test]
async fn a_dropped_consumer_with_a_redelivery_delay_lets_its_subscription_go() {
    let cluster = exclusive().await;
    let topic = topic("drop-redelivery");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let consumer = pulsar
        .consumer()
        .with_topic(&topic)
        .with_subscription("held")
        .with_subscription_type(pulsar::SubType::Exclusive)
        .with_unacked_message_resend_delay(Some(Duration::from_secs(5)))
        .build::<String>()
        .await
        .expect("consumer");
    cluster
        .wait_subscription(&topic, "held", |s| s.consumers.len() == 1)
        .await;

    drop(consumer);
    cluster
        .wait_subscription(&topic, "held", |s| s.consumers.is_empty())
        .await;
}

// A seek made while the consumer is away waits for it within the operation
// timeout only; one that would land after it fails instead of moving a
// consumer that went on.
#[tokio::test]
async fn a_seek_that_outlives_its_timeout_during_an_outage_fails() {
    let mut cluster = exclusive().await;
    let topic = topic("seek-outage-timeout");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    // an operation timeout of three seconds
    let pulsar = cluster.client_with_keepalive(survivor).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=5).await;
    // none acked, so the broker sends all five again after the crash
    let taken = take(&mut consumer, &mut Delivered::default(), 5).await;

    // the topic stays unowned for ten seconds
    cluster.kill_broker(owner).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let seek = tokio::time::timeout(
        DEADLINE,
        consumer.seek_to(&topic, taken[2].message_id().clone()),
    )
    .await
    .expect("an answer within the deadline");
    assert!(seek.is_err(), "the seek waited past its time and went");

    cluster.wait_owner(&topic, survivor).await;
    publish(&cluster.client_via(survivor).await, &topic, 6..=10).await;
    let mut again = Delivered::default();
    collect(&mut consumer, &mut again, 10, DEADLINE).await;
    // a seek gone through late would leave out 1 to 3
    again.assert_exactly(&topic, 1..=10);
}

// A refusal that a policy change lifts, here a subscription type turned off,
// must not end the consumer: Java retries everything but a deleted topic.
#[tokio::test]
async fn a_consumer_refused_on_reconnect_comes_back_once_allowed() {
    let cluster = exclusive().await;
    let namespace = "public/subscription-types";
    cluster.allow_subscription_types(namespace, &[]).await;
    let topic = topic_in(namespace, "refused");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=1).await;
    take(&mut consumer, &mut Delivered::default(), 1).await;

    // the Exclusive subscription is refused once the topic moves
    cluster
        .allow_subscription_types(namespace, &["Shared"])
        .await;
    cluster.unload(&topic).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.allow_subscription_types(namespace, &[]).await;
    publish(&pulsar, &topic, 2..=3).await;

    let mut delivered = Delivered::default();
    let give_up = tokio::time::Instant::now() + DEADLINE;
    while delivered.distinct() < 3 && tokio::time::Instant::now() < give_up {
        match tokio::time::timeout(Duration::from_secs(3), consumer.next()).await {
            Ok(Some(Ok(message))) => delivered.record(&message),
            // the refusal, told once
            Ok(Some(Err(e))) => assert!(!e.is_final(), "{e:?}"),
            Err(_) => {}
            Ok(None) => panic!("the consumer ended on a refusal"),
        }
    }
    delivered.assert_exactly_ignoring_earlier(&topic, 2..=3);
}

// A frozen broker leaves its connections open: the reconnect must not wait
// out the operation timeout on one it already knows is broken.
#[tokio::test]
async fn a_consumer_leaves_a_frozen_broker_without_waiting_for_it() {
    let mut cluster = exclusive().await;
    let topic = topic("reconnect-frozen-default");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    // A keepalive of one second notices the freeze, and short connects and
    // backoffs come back fast once the topic moved; the operation timeout,
    // which a close on the broken connection would wait out, stays at 30 s.
    let pulsar = cluster
        .client_with(
            survivor,
            pulsar::ConnectionRetryOptions {
                keep_alive: Duration::from_secs(1),
                connection_timeout: Duration::from_secs(2),
                max_backoff: Duration::from_secs(2),
                ..Default::default()
            },
            Default::default(),
        )
        .await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &topic, 1..=1).await;
    take(&mut consumer, &mut Delivered::default(), 1).await;

    cluster.pause_broker(owner).await;
    cluster.wait_owner(&topic, survivor).await;
    publish(&cluster.client_via(survivor).await, &topic, 2..=2).await;
    let published = tokio::time::Instant::now();
    take(&mut consumer, &mut Delivered::default(), 1).await;
    assert!(
        published.elapsed() < Duration::from_secs(10),
        "the consumer came back {:?} after the topic did",
        published.elapsed()
    );
}
