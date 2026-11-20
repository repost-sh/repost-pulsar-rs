//! Credentials that expire or are rejected: a client renews its token on the
//! connection it has, and a rejected one is reported while the client keeps
//! trying, so it recovers once the token is valid again.

use std::time::Duration;

use futures::StreamExt;
use pulsar::{message::proto::ServerError, producer::SendFuture, Error, Producer, TokioExecutor};

use crate::{
    auth_broker::{auth_broker, MintedTokens},
    cluster::topic,
    delivery::{collect, earliest_consumer, publish, take, Delivered, DEADLINE},
};

#[tokio::test]
async fn a_consumer_reads_on_through_token_refreshes() {
    let broker = auth_broker().await;
    let tokens = MintedTokens::new(Duration::from_secs(4));
    let pulsar = broker.client(tokens.clone()).await;
    let topic = topic("auth-refresh");
    let mut consumer = earliest_consumer(&pulsar, &topic, "refresh").await;
    publish(&pulsar, &topic, 1..=1).await;
    let mut delivered = Delivered::default();
    take(&mut consumer, &mut delivered, 1).await;
    let connected = broker.connected_since(&topic, "refresh").await;
    assert!(connected.is_some(), "no consumer on the broker");

    // three token lifetimes, a message a second
    for number in 2..=12 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        publish(&pulsar, &topic, number..=number).await;
        take(&mut consumer, &mut delivered, 1).await;
    }

    delivered.assert_exactly(&topic, 1..=12);
    assert!(
        tokens.minted() > 3,
        "the broker never asked for a fresh token"
    );
    assert_eq!(
        broker.connected_since(&topic, "refresh").await,
        connected,
        "the consumer had to connect again"
    );
}

#[tokio::test]
async fn a_consumer_with_a_rejected_token_reports_it_and_recovers() {
    let broker = auth_broker().await;
    let tokens = MintedTokens::new(Duration::from_secs(3));
    let pulsar = broker.client(tokens.clone()).await;
    let topic = topic("auth-rejected");
    let mut consumer = earliest_consumer(&pulsar, &topic, "rejected").await;
    publish(&pulsar, &topic, 1..=1).await;
    take(&mut consumer, &mut Delivered::default(), 1).await;

    // The next refresh hands the broker a token it rejects, so it closes the
    // connection, and connecting again is refused too.
    tokens.set_valid(false);
    let failure = tokio::time::timeout(DEADLINE, consumer.next())
        .await
        .expect("an error within the deadline");
    match failure {
        Some(Err(e)) => assert_eq!(
            e.server_error(),
            Some(ServerError::AuthenticationError),
            "{e}"
        ),
        other => panic!("expected the authentication error, got {other:?}"),
    }

    tokens.set_valid(true);
    let publisher = broker
        .client(MintedTokens::new(Duration::from_secs(3600)))
        .await;
    publish(&publisher, &topic, 2..=3).await;
    let mut delivered = Delivered::default();
    collect(&mut consumer, &mut delivered, 2, DEADLINE).await;
    delivered.assert_exactly_ignoring_earlier(&topic, 2..=3);
}

async fn send(producer: &mut Producer<TokioExecutor>, body: &str) -> Result<(), Error> {
    let receipt: SendFuture = producer.send_non_blocking(body.to_string()).await?;
    receipt.await.map(drop)
}

#[tokio::test]
async fn a_producer_with_a_rejected_token_reports_it_and_recovers() {
    let broker = auth_broker().await;
    let tokens = MintedTokens::new(Duration::from_secs(3));
    let pulsar = broker.client(tokens.clone()).await;
    let topic = topic("auth-rejected-producer");
    let mut producer = pulsar
        .producer()
        .with_topic(&topic)
        .build()
        .await
        .expect("producer");
    send(&mut producer, "1").await.expect("send");

    tokens.set_valid(false);
    // wait for the broker to close the connection over the rejected refresh
    tokio::time::sleep(Duration::from_secs(6)).await;
    match send(&mut producer, "2").await {
        Err(e) => assert_eq!(
            e.server_error(),
            Some(ServerError::AuthenticationError),
            "{e}"
        ),
        Ok(()) => panic!("sent with a rejected token"),
    }

    tokens.set_valid(true);
    let recovered = tokio::time::timeout(DEADLINE, async {
        while send(&mut producer, "3").await.is_err() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;
    assert!(recovered.is_ok(), "the producer never recovered");
}
