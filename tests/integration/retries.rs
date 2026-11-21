//! Creating consumers and producers through broker trouble: what a retry can
//! fix is retried until the operation timeout, what it cannot fails at once,
//! and a broken connection stays broken for everyone on it.

use std::time::Duration;

use pulsar::SubType;

use crate::{
    cluster::{exclusive, topic},
    delivery::{collect, earliest_consumer, publish, take, Delivered, DEADLINE},
};

#[tokio::test]
async fn a_second_exclusive_consumer_is_refused_at_once() {
    let cluster = exclusive().await;
    let topic = topic("retries-busy");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let build = || {
        pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription("exclusive")
            .with_subscription_type(SubType::Exclusive)
            .build::<String>()
    };
    let _first = build().await.expect("first consumer");

    // Java: ConsumerBusy is not retriable; retrying it holds build() forever.
    let second = tokio::time::timeout(Duration::from_secs(10), build())
        .await
        .expect("an answer within ten seconds");
    assert!(second.is_err(), "a second exclusive consumer subscribed");
}

#[tokio::test]
async fn a_consumer_built_as_its_broker_dies_gets_through() {
    let mut cluster = exclusive().await;
    let topic = topic("retries-dead-owner");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_via(survivor).await;

    // The topic stays assigned to the dead broker until its metadata session
    // expires, so the first attempts find a broker that is gone.
    cluster.kill_broker(owner).await;
    // earliest_consumer panics if the build fails, which this test is about
    let mut consumer =
        tokio::time::timeout(DEADLINE, earliest_consumer(&pulsar, &topic, "durable"))
            .await
            .expect("built within the deadline");
    publish(&pulsar, &topic, 1..=3).await;
    let mut delivered = Delivered::default();
    collect(&mut consumer, &mut delivered, 3, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=3);
}

#[tokio::test]
async fn consumers_sharing_a_connection_all_notice_it_broke() {
    let mut cluster = exclusive().await;
    let first = topic("retries-shared");
    cluster.create_topic(&first).await;
    let owner = cluster.owner(&first).await;
    // a second topic on the same broker, so both consumers share a connection
    let second = topic("retries-shared");
    cluster.create_topic(&second).await;
    cluster.move_to(&second, owner).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_with_keepalive(survivor).await;
    let mut consumers = Vec::new();
    for topic in [&first, &second] {
        let mut consumer = earliest_consumer(&pulsar, topic, "durable").await;
        publish(&pulsar, topic, 1..=2).await;
        for message in take(&mut consumer, &mut Delivered::default(), 2).await {
            consumer.ack(&message).await.expect("ack");
        }
        consumers.push(consumer);
    }

    // Both notice the dead connection by themselves, not only the first.
    cluster.pause_broker(owner).await;
    for topic in [&first, &second] {
        cluster.wait_owner(topic, survivor).await;
        publish(&cluster.client_via(survivor).await, topic, 3..=4).await;
    }
    for (consumer, topic) in consumers.iter_mut().zip([&first, &second]) {
        let mut delivered = Delivered::default();
        collect(consumer, &mut delivered, 2, DEADLINE).await;
        delivered.assert_exactly_ignoring_earlier(topic, 3..=4);
    }
}

#[tokio::test]
async fn a_client_connects_anew_to_a_broker_that_came_back() {
    let mut cluster = exclusive().await;
    let topic_a = topic("retries-back");
    cluster.create_topic(&topic_a).await;
    let owner = cluster.owner(&topic_a).await;
    let survivor = 1 - owner;
    let pulsar = cluster.client_with_keepalive(survivor).await;
    let mut consumer = earliest_consumer(&pulsar, &topic_a, "durable").await;
    publish(&pulsar, &topic_a, 1..=1).await;
    take(&mut consumer, &mut Delivered::default(), 1).await;

    // The consumer notices its connection to the frozen broker died, and
    // moves on; the broker then comes back as a new process.
    cluster.pause_broker(owner).await;
    cluster.wait_owner(&topic_a, survivor).await;
    cluster.start_broker(owner).await;

    // A topic the returned broker owns, read through the same client: the
    // client must not hand the new consumer the connection that died.
    let topic_b = topic("retries-back");
    cluster.create_topic(&topic_b).await;
    cluster.move_to(&topic_b, owner).await;
    let mut consumer_b =
        tokio::time::timeout(DEADLINE, earliest_consumer(&pulsar, &topic_b, "durable"))
            .await
            .expect("built within the deadline");
    publish(&pulsar, &topic_b, 1..=2).await;
    let mut delivered = Delivered::default();
    collect(&mut consumer_b, &mut delivered, 2, DEADLINE).await;
    delivered.assert_exactly(&topic_b, 1..=2);
}

// A connect given up on halfway, as an application timeout around a build
// does, must not leave the next one waiting on it for ever.
#[tokio::test]
async fn a_connect_given_up_on_does_not_block_the_next() {
    let mut cluster = exclusive().await;
    let topic = topic("retries-abandoned-connect");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;
    let pulsar = cluster.client_via(1 - owner).await;

    // the owner takes the connection and never answers on it
    cluster.pause_broker(owner).await;
    let first = tokio::time::timeout(
        Duration::from_secs(2),
        earliest_consumer(&pulsar, &topic, "durable"),
    )
    .await;
    assert!(first.is_err(), "built against a frozen broker");

    // back before its session expires, it still owns the topic
    cluster.resume_broker(owner).await;
    let second =
        tokio::time::timeout(DEADLINE, earliest_consumer(&pulsar, &topic, "durable")).await;
    assert!(
        second.is_ok(),
        "the next build waited on the abandoned connect"
    );
}
