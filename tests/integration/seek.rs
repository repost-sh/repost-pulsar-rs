//! Seeking a consumer on a partitioned topic: one partition at a time, or all
//! of them to a publish time, without disturbing the rest.

use std::time::Duration;

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

#[tokio::test]
async fn a_time_seek_after_an_id_seek_restarts_at_the_time() {
    let cluster = exclusive().await;
    let topic = topic("seek-id-then-time");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    let before = chrono::Utc::now().timestamp_millis() as u64;
    publish(&pulsar, &topic, 1..=10).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 10).await;

    // Seeking to the last message leaves nothing to deliver after it.
    consumer
        .seek_to(&topic, taken[9].message_id().clone())
        .await
        .expect("seek to the last");
    consumer
        .seek_to_time(before)
        .await
        .expect("seek to the time");

    let mut after = Delivered::default();
    collect(&mut consumer, &mut after, 10, DEADLINE).await;
    after.assert_exactly(&topic, 1..=10);
}

// Threads of their own for the engine and the test, so the application can
// read between the seek's answer and the engine handling the broker's close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_message_from_before_a_seek_arrives_after_it() {
    let cluster = exclusive().await;
    let topic = topic("seek-floor");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=300).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 10).await;
    // the rest of the backlog fills the receive queue
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter >= 300)
        .await;

    // Many seeks back, each with a full queue of messages from before it:
    // the very next message must be the one after the target, every time.
    for round in 0..20 {
        let target = round % 5;
        let next = tokio::time::timeout(DEADLINE, async {
            consumer
                .seek_to(&topic, taken[target].message_id().clone())
                .await
                .expect("seek");
            take(&mut consumer, &mut Delivered::default(), 1).await
        })
        .await
        .expect("a message within the deadline");
        let number: u64 = next[0].deserialize().unwrap().parse().unwrap();
        assert_eq!(number, target as u64 + 2, "round {round}");
    }
}

#[tokio::test]
async fn a_seek_cancelled_after_it_went_out_still_takes_effect() {
    let cluster = exclusive().await;
    let topic = topic("seek-cancelled");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=20).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 5).await;
    cluster
        .wait_subscription(&topic, "durable", |s| s.msg_out_counter >= 20)
        .await;

    // The first poll asks the engine for the connection; the engine, on this
    // same thread, answers once the test yields; the second poll sends the
    // request and waits for the broker. Dropped there, the broker moves
    // anyway, and the client must follow: nothing from before the seek,
    // nothing lost after it.
    {
        let mut seek = Box::pin(consumer.seek_to(&topic, taken[0].message_id().clone()));
        assert!(
            futures::poll!(seek.as_mut()).is_pending(),
            "seek done at once"
        );
        tokio::task::yield_now().await;
        assert!(
            futures::poll!(seek.as_mut()).is_pending(),
            "seek done in two polls"
        );
    }

    let mut after = Delivered::default();
    collect(&mut consumer, &mut after, 19, DEADLINE).await;
    after.assert_exactly(&topic, 2..=20);
}

// After a seek, only the target's own entry is the client's to drop; an
// operator who then moves the cursor back gets the replay.
#[tokio::test]
async fn a_cursor_reset_after_a_seek_is_delivered() {
    let cluster = exclusive().await;
    let topic = topic("seek-then-reset");
    cluster.create_topic(&topic).await;
    let pulsar = cluster.client().await;
    publish(&pulsar, &topic, 1..=5).await;
    let mut consumer = earliest_consumer(&pulsar, &topic, "durable").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 5).await;
    // caught up: nothing after the target arrives to end the seek's filter
    consumer
        .seek_to(&topic, taken[4].message_id().clone())
        .await
        .expect("seek");
    tokio::time::sleep(Duration::from_secs(2)).await;

    cluster.reset_cursor(&topic, "durable", 0).await;
    let mut delivered = Delivered::default();
    collect(&mut consumer, &mut delivered, 5, DEADLINE).await;
    delivered.assert_exactly(&topic, 1..=5);
}
