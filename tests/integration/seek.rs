//! Seeking a consumer on a partitioned topic: one partition at a time, or all
//! of them to a publish time, without disturbing the rest.

use pulsar::ConsumerOptions;

use crate::{
    cluster::{exclusive, partition, topic},
    delivery::{
        collect, earliest_consumer, earliest_consumer_with, earliest_reader, publish,
        publish_around_a_time, take, Delivered, DEADLINE,
    },
};

#[tokio::test]
async fn seeking_one_partition_leaves_the_other_alone() {
    let cluster = exclusive().await;
    let topic = topic("seek-one");
    cluster.create_partitioned_topic(&topic, 2).await;
    let (first, second) = (partition(&topic, 0), partition(&topic, 1));
    let pulsar = cluster.client().await;
    // A three-message receive queue keeps both partitions backed up, so the
    // seek goes out while messages from before it are still buffered.
    let mut consumer = earliest_consumer_with(
        &pulsar,
        &topic,
        "durable",
        ConsumerOptions::default().with_receiver_queue_size(3),
    )
    .await;
    publish(&pulsar, &first, 1..=20).await;
    publish(&pulsar, &second, 1..=10).await;

    let mut all = Delivered::default();
    let fifth = loop {
        let message = take(&mut consumer, &mut all, 1).await.remove(0);
        if message.topic == first && message.deserialize().unwrap() == "5" {
            break message.message_id().clone();
        }
    };

    tokio::time::timeout(DEADLINE, consumer.seek_to(&first, fifth))
        .await
        .expect("seek within the deadline")
        .expect("seek");
    publish(&pulsar, &first, 21..=22).await;
    publish(&pulsar, &second, 11..=12).await;

    // The first partition restarts after 5; the second carries on as if
    // nothing happened.
    let mut after = Delivered::default();
    while after.distinct() < 17 || all.distinct_on(&second) < 12 {
        let message =
            tokio::time::timeout(DEADLINE, take(&mut consumer, &mut Delivered::default(), 1))
                .await
                .expect("message within the deadline")
                .remove(0);
        if message.topic == first {
            after.record(&message);
        } else {
            all.record(&message);
        }
    }
    after.assert_exactly(&first, 6..=22);
    all.assert_exactly(&second, 1..=12);
}

#[tokio::test]
async fn seeking_to_a_time_restarts_every_partition_there() {
    let cluster = exclusive().await;
    let topic = topic("seek-time");
    cluster.create_partitioned_topic(&topic, 2).await;
    let partitions = [partition(&topic, 0), partition(&topic, 1)];
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let middle = publish_around_a_time(&pulsar, &partitions, 1..=5, 6..=10).await;

    for message in take(&mut consumer, &mut Delivered::default(), 20).await {
        consumer.ack(&message).await.expect("ack");
    }

    tokio::time::timeout(DEADLINE, consumer.seek_to_time(middle))
        .await
        .expect("seek within the deadline")
        .expect("seek");

    let mut after = Delivered::default();
    collect(&mut consumer, &mut after, 10, DEADLINE).await;
    for partition in &partitions {
        after.assert_exactly(partition, 6..=10);
    }
}

#[tokio::test]
async fn a_reader_seeking_to_a_time_restarts_there() {
    let cluster = exclusive().await;
    let topic = topic("seek-reader-time");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let middle = publish_around_a_time(&pulsar, std::slice::from_ref(&topic), 1..=5, 6..=10).await;
    let mut reader = earliest_reader(&pulsar, &topic, "reader").await;
    take(&mut reader, &mut Delivered::default(), 10).await;

    reader.seek(None, Some(middle)).await.expect("seek");

    let mut after = Delivered::default();
    collect(&mut reader, &mut after, 5, DEADLINE).await;
    after.assert_exactly(&topic, 6..=10);
}

#[tokio::test]
async fn last_message_ids_name_their_partition() {
    let cluster = exclusive().await;
    let topic = topic("seek-last");
    cluster.create_partitioned_topic(&topic, 3).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    for index in 0..3 {
        publish(&pulsar, &partition(&topic, index), 1..=u64::from(index + 1)).await;
    }

    let mut last_delivered = std::collections::BTreeMap::new();
    for message in take(&mut consumer, &mut Delivered::default(), 6).await {
        last_delivered.insert(message.topic.clone(), message.message_id().clone());
    }
    let last = consumer.last_message_ids().await.expect("last message ids");

    assert_eq!(
        last.keys().collect::<Vec<_>>(),
        last_delivered.keys().collect::<Vec<_>>()
    );
    for (partition, delivered) in &last_delivered {
        let id = &last[partition];
        assert_eq!(
            (id.ledger_id, id.entry_id),
            (delivered.ledger_id, delivered.entry_id),
            "{partition}"
        );
    }
}

#[tokio::test]
async fn seeking_a_topic_the_consumer_does_not_read_fails() {
    let cluster = exclusive().await;
    let topic = topic("seek-unknown");
    cluster.create_partitioned_topic(&topic, 2).await;
    let pulsar = cluster.client().await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    publish(&pulsar, &partition(&topic, 0), 1..=1).await;
    let message = take(&mut consumer, &mut Delivered::default(), 1).await;

    let result = consumer
        .seek_to(&partition(&topic, 7), message[0].message_id().clone())
        .await;
    assert!(result.is_err(), "seek on an unknown partition: {result:?}");
}
