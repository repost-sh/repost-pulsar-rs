//! Failover events: which consumer of a Failover subscription the broker
//! sends each partition to, in order with that partition's messages.

use std::time::Duration;

use pulsar::{Consumer, ConsumerEvent, ConsumerItem, TokioExecutor};

use crate::{
    cluster::{exclusive, partition, topic},
    delivery::{failover_consumer, number, publish, take, Delivered, DEADLINE},
};

/// One thing a consumer handed the application.
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Event { topic: String, active: bool },
    Message { topic: String, number: u64 },
}

/// What `consumer` hands over until `done` holds of it, and then until two
/// quiet seconds pass.
async fn gather(
    consumer: &mut Consumer<String, TokioExecutor>,
    done: impl Fn(&[Seen]) -> bool,
) -> Vec<Seen> {
    let mut seen = Vec::new();
    let give_up = tokio::time::Instant::now() + DEADLINE;
    loop {
        let quiet = if done(&seen) {
            Duration::from_secs(2)
        } else {
            give_up.saturating_duration_since(tokio::time::Instant::now())
        };
        match tokio::time::timeout(quiet, consumer.next_item()).await {
            Ok(Some(Ok(ConsumerItem::Event(event)))) => seen.push(match event {
                ConsumerEvent::BecameActive { topic } => Seen::Event {
                    topic,
                    active: true,
                },
                ConsumerEvent::BecameInactive { topic } => Seen::Event {
                    topic,
                    active: false,
                },
            }),
            Ok(Some(Ok(ConsumerItem::Message(message)))) => seen.push(Seen::Message {
                topic: message.topic.clone(),
                number: number(&message),
            }),
            Ok(Some(Err(e))) => panic!("consumer failed: {e}"),
            Ok(None) => panic!("consumer ended"),
            Err(_) if done(&seen) => return seen,
            Err(_) => panic!("gave up waiting; saw {seen:?}"),
        }
    }
}

/// The events for `topic`, as active or not, in order.
fn events(seen: &[Seen], topic: &str) -> Vec<bool> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Event { topic: t, active } if t == topic => Some(*active),
            _ => None,
        })
        .collect()
}

/// Whether the last BecameActive for `topic` comes before its first message.
fn active_before_messages(seen: &[Seen], topic: &str) -> bool {
    let active = seen
        .iter()
        .rposition(|s| matches!(s, Seen::Event { topic: t, active: true } if t == topic));
    let message = seen
        .iter()
        .position(|s| matches!(s, Seen::Message { topic: t, .. } if t == topic));
    matches!((active, message), (Some(a), Some(m)) if a < m)
}

fn messages(seen: &[Seen], topic: &str) -> Vec<u64> {
    seen.iter()
        .filter_map(|s| match s {
            Seen::Message { topic: t, number } if t == topic => Some(*number),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn failover_consumers_hear_which_partitions_they_own() {
    let cluster = exclusive().await;
    let topic = topic("events-owners");
    cluster.create_partitioned_topic(&topic, 2).await;
    let (first, second) = (partition(&topic, 0), partition(&topic, 1));
    let pulsar = cluster.client().await;

    let mut a = failover_consumer(&pulsar, &topic, "a").await;
    let seen = gather(&mut a, |s| {
        events(s, &first) == [true] && events(s, &second) == [true]
    })
    .await;
    assert_eq!(
        (events(&seen, &first), events(&seen, &second)),
        (vec![true], vec![true]),
        "{seen:?}"
    );

    // The broker sorts consumers by name: partition 1 moves to b.
    let mut b = failover_consumer(&pulsar, &topic, "b").await;
    let seen = gather(&mut a, |s| events(s, &second) == [false]).await;
    assert_eq!(
        (events(&seen, &first), events(&seen, &second)),
        (vec![], vec![false]),
        "a: {seen:?}"
    );
    let seen = gather(&mut b, |s| events(s, &second) == [true]).await;
    // b never had partition 0, so it hears nothing about it.
    assert_eq!(
        (events(&seen, &first), events(&seen, &second)),
        (vec![], vec![true]),
        "b: {seen:?}"
    );

    publish(&pulsar, &second, 1..=5).await;
    let seen = gather(&mut b, |s| messages(s, &second).len() == 5).await;
    assert_eq!(messages(&seen, &second), (1..=5).collect::<Vec<_>>());

    // b goes without acking; a gets partition 1 back, and its messages
    // after the event that says so.
    b.close().await.expect("close");
    drop(b);
    let seen = gather(&mut a, |s| messages(s, &second).len() == 5).await;
    assert_eq!(events(&seen, &second), [true], "{seen:?}");
    assert!(active_before_messages(&seen, &second), "{seen:?}");
    assert_eq!(messages(&seen, &second), (1..=5).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_seek_keeps_ownership_the_broker_keeps() {
    let cluster = exclusive().await;
    let topic = topic("events-seek");
    cluster.create_partitioned_topic(&topic, 2).await;
    let first = partition(&topic, 0);
    let pulsar = cluster.client().await;
    publish(&pulsar, &first, 1..=3).await;
    let mut consumer = failover_consumer(&pulsar, &topic, "a").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 3).await;

    // The broker reconnects the active consumer when it seeks; alone, this
    // one gets the partition back, and hears nothing it did not already know.
    consumer
        .seek_to(&first, taken[0].message_id().clone())
        .await
        .expect("seek");

    let seen = gather(&mut consumer, |s| messages(s, &first) == [2, 3]).await;
    assert_eq!(events(&seen, &first), Vec::<bool>::new(), "{seen:?}");
}

// A consumer that seeks whenever it gets a partition, as an indexer seeking
// to its checkpoint does, must not hear of the partition again for its own
// seek, or it seeks for ever.
#[tokio::test]
async fn a_consumer_that_seeks_on_activation_settles() {
    let cluster = exclusive().await;
    let topic = topic("events-seek-loop");
    cluster.create_partitioned_topic(&topic, 2).await;
    let first = partition(&topic, 0);
    let pulsar = cluster.client().await;
    publish(&pulsar, &first, 1..=3).await;
    let mut consumer = failover_consumer(&pulsar, &topic, "a").await;
    let taken = take(&mut consumer, &mut Delivered::default(), 3).await;
    let checkpoint = taken[0].message_id().clone();
    consumer
        .seek_to(&first, checkpoint.clone())
        .await
        .expect("seek");

    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < until {
        let item = tokio::time::timeout(Duration::from_secs(1), consumer.next_item()).await;
        if let Ok(Some(Ok(ConsumerItem::Event(ConsumerEvent::BecameActive { topic })))) = item {
            assert_ne!(
                topic, first,
                "the consumer got its partition back: it would seek again"
            );
        }
    }
}

#[tokio::test]
async fn a_reconnect_ends_ownership_until_the_broker_confirms_it() {
    let cluster = exclusive().await;
    let topic = topic("events-reconnect");
    cluster.create_partitioned_topic(&topic, 2).await;
    let (first, second) = (partition(&topic, 0), partition(&topic, 1));
    let pulsar = cluster.client().await;
    let mut consumer = failover_consumer(&pulsar, &topic, "a").await;
    gather(&mut consumer, |s| {
        events(s, &first) == [true] && events(s, &second) == [true]
    })
    .await;

    // Another consumer may own the partition while this one is away.
    cluster.unload(&first).await;

    let seen = gather(&mut consumer, |s| events(s, &first).len() == 2).await;
    assert_eq!(
        (events(&seen, &first), events(&seen, &second)),
        (vec![false, true], vec![]),
        "{seen:?}"
    );
}
