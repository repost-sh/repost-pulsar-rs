//! Acknowledgements reach the broker, including while the application is
//! behind and the consumer's receive queue is full.

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
