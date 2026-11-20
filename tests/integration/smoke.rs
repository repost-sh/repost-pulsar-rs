//! Proves the cluster helpers do what the fault-injection tests rely on.

use futures::TryStreamExt;
use pulsar::{consumer::InitialPosition, Consumer, ConsumerOptions, SubType};

use crate::cluster::{exclusive, topic, Cluster};

#[tokio::test]
async fn a_topic_round_trips() {
    let cluster = exclusive().await;

    assert_eq!(
        round_trip(&cluster, &topic("smoke"), "hello").await,
        "hello"
    );
}

#[tokio::test]
async fn a_killed_broker_hands_its_topics_to_the_other() {
    let mut cluster = exclusive().await;
    let topic = topic("smoke");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;

    cluster.kill_broker(owner).await;
    cluster.wait_owner(&topic, 1 - owner).await;
    assert_eq!(round_trip(&cluster, &topic, "moved").await, "moved");
}

#[tokio::test]
async fn a_stopped_broker_unloads_its_topics_before_exiting() {
    let mut cluster = exclusive().await;
    let topic = topic("smoke");
    cluster.create_topic(&topic).await;
    let owner = cluster.owner(&topic).await;

    cluster.stop_broker(owner).await;
    cluster.wait_owner(&topic, 1 - owner).await;
    assert_eq!(
        round_trip(&cluster, &topic, "handed over").await,
        "handed over"
    );
}

#[tokio::test]
async fn unload_and_partition_updates_take_effect() {
    let cluster = exclusive().await;
    let topic = topic("smoke-partitioned");

    cluster.create_partitioned_topic(&topic, 2).await;
    cluster.update_partitions(&topic, 3).await;
    let partitions = cluster
        .client()
        .await
        .lookup_partitioned_topic_number(&topic)
        .await
        .expect("partitioned metadata");
    assert_eq!(partitions, 3);

    let partition = format!("{topic}-partition-2");
    cluster.unload(&partition).await;
    assert_eq!(
        round_trip(&cluster, &partition, "after unload").await,
        "after unload"
    );
}

async fn round_trip(cluster: &Cluster, topic: &str, payload: &str) -> String {
    let pulsar = cluster.client().await;
    let mut consumer: Consumer<String, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription("smoke")
        .with_subscription_type(SubType::Exclusive)
        .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest))
        .build()
        .await
        .expect("consumer");
    pulsar
        .send(topic, payload)
        .await
        .expect("send")
        .await
        .expect("receipt");

    let message = tokio::time::timeout(std::time::Duration::from_secs(30), consumer.try_next())
        .await
        .expect("message within 30 s")
        .expect("consumer stream")
        .expect("one message");
    consumer.ack(&message).await.expect("ack");
    message.deserialize().expect("utf-8 payload")
}
