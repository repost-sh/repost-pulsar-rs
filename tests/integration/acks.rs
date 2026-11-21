//! Flow control and acknowledgements: a consumer holds no more than its
//! receive queue, and acks reach the broker while the application is behind.

use std::time::Duration;

use pulsar::{consumer::InitialPosition, Consumer, ConsumerOptions, SubType, TokioExecutor};

use crate::{
    cluster::{exclusive, topic},
    delivery::{earliest_consumer_with, publish, take, Delivered},
};

#[tokio::test]
async fn acks_reach_the_broker_while_the_receive_queue_is_full() {
    let cluster = exclusive().await;
    let topic = topic("acks-full-queue");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer_with(
        &pulsar,
        &topic,
        "durable",
        ConsumerOptions::default().with_receiver_queue_size(3),
    )
    .await;
    publish(&pulsar, &topic, 1..=20).await;

    // After three, the queue fills up again behind them and the application
    // reads no further.
    for message in take(&mut consumer, &mut Delivered::default(), 3).await {
        consumer.ack(&message).await.expect("ack");
    }

    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_backlog == 17)
        .await;
}

#[tokio::test]
async fn messages_the_application_has_not_read_are_not_redelivered() {
    let cluster = exclusive().await;
    let topic = topic("acks-redelivery");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer: Consumer<String, TokioExecutor> = pulsar
        .consumer()
        .with_topic(&topic)
        .with_subscription("shared")
        .with_subscription_type(SubType::Shared)
        .with_unacked_message_resend_delay(Some(Duration::from_secs(2)))
        .with_options(
            ConsumerOptions::default()
                .with_initial_position(InitialPosition::Earliest)
                .with_receiver_queue_size(3),
        )
        .build()
        .await
        .expect("consumer");
    publish(&pulsar, &topic, 1..=30).await;

    // Each message is acked well within the two seconds once the application
    // has it, but most wait longer than that before it gets to them.
    let mut delivered = Delivered::default();
    for _ in 1..=30 {
        let message = take(&mut consumer, &mut delivered, 1).await.remove(0);
        tokio::time::sleep(Duration::from_millis(200)).await;
        consumer.ack(&message).await.expect("ack");
    }
    let late = tokio::time::timeout(
        Duration::from_secs(3),
        take(&mut consumer, &mut delivered, 1),
    )
    .await;
    assert!(
        late.is_err(),
        "a redelivery arrived after all 30 were acked"
    );
    delivered.assert_exactly(&topic, 1..=30);
}

#[tokio::test]
async fn a_consumer_holds_no_more_than_its_receive_queue() {
    let cluster = exclusive().await;
    let topic = topic("acks-window");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer_with(
        &pulsar,
        &topic,
        "durable",
        ConsumerOptions::default().with_receiver_queue_size(3),
    )
    .await;
    publish(&pulsar, &topic, 1..=50).await;

    // The broker dispatches what the permits allow and no more while the
    // application reads nothing.
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter >= 3)
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter == 3)
        .await;

    // Reading returns permits, and everything arrives.
    let mut delivered = Delivered::default();
    take(&mut consumer, &mut delivered, 50).await;
    delivered.assert_exactly(&topic, 1..=50);
}

#[tokio::test]
async fn a_slow_consumer_holds_one_window_across_reconnects() {
    let cluster = exclusive().await;
    let topic = topic("acks-flapping");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer_with(
        &pulsar,
        &topic,
        "durable",
        ConsumerOptions::default().with_receiver_queue_size(3),
    )
    .await;
    publish(&pulsar, &topic, 1..=50).await;
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter == 3)
        .await;

    // The application reads nothing while the topic moves five times. The
    // three messages it holds already fill the window, so the broker must
    // not send more after any of the reconnects.
    for _ in 0..5 {
        cluster.unload(&topic).await;
        cluster
            .wait_subscription(&topic, "durable", |s| !s.consumers.is_empty())
            .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter == 0)
        .await;

    let mut delivered = Delivered::default();
    take(&mut consumer, &mut delivered, 50).await;
    delivered.assert_exactly(&topic, 1..=50);
}

#[tokio::test]
async fn messages_taken_and_not_acked_come_again_after_the_delay() {
    let cluster = exclusive().await;
    let topic = topic("acks-redelivered");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let mut consumer: Consumer<String, TokioExecutor> = pulsar
        .consumer()
        .with_topic(&topic)
        .with_subscription("shared")
        .with_subscription_type(SubType::Shared)
        .with_unacked_message_resend_delay(Some(Duration::from_secs(2)))
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .build()
        .await
        .expect("consumer");
    publish(&pulsar, &topic, 1..=3).await;

    let mut first = Delivered::default();
    take(&mut consumer, &mut first, 3).await;
    // none acked: the three come again once the delay has passed
    let mut again = Delivered::default();
    let redelivered =
        tokio::time::timeout(Duration::from_secs(10), take(&mut consumer, &mut again, 3)).await;
    assert!(redelivered.is_ok(), "no redelivery within ten seconds");
    again.assert_exactly(&topic, 1..=3);
}
