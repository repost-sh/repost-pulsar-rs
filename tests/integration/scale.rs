//! What reading a topic of many partitions costs the reader. Realtime reads
//! every partition of its topics, so a message on one partition must not cost
//! a pass over all of them.

use std::time::{Duration, Instant};

use pulsar::{Pulsar, TokioExecutor};

use crate::{
    cluster::{exclusive, partition, topic},
    delivery::{earliest_reader, publish, take, Delivered},
};

const MESSAGES: usize = 1000;
const PARTITIONS: u32 = 200;

/// How long a reader of `topic` takes to hand over `MESSAGES` messages the
/// broker already sent it, at best of three readers. The test runs on one
/// thread, so this is the client's own work.
async fn drain(pulsar: &Pulsar<TokioExecutor>, topic: &str) -> Duration {
    let mut best = Duration::MAX;
    for round in 0..3 {
        let mut reader = earliest_reader(pulsar, topic, &format!("scale-{round}")).await;
        // every partition's queue fills
        tokio::time::sleep(Duration::from_secs(5)).await;
        let start = Instant::now();
        take(&mut reader, &mut Delivered::default(), MESSAGES).await;
        best = best.min(start.elapsed());
    }
    best
}

fn report(name: &str, took: Duration) -> f64 {
    let per_message = took.as_secs_f64() * 1e6 / MESSAGES as f64;
    println!("{name}: {took:?} for {MESSAGES} messages, {per_message:.1} us each");
    per_message
}

#[tokio::test]
#[ignore = "a measurement: cargo test --test integration scale -- --ignored --nocapture"]
async fn reading_many_partitions() {
    let cluster = exclusive().await;
    let pulsar = cluster.client().await;

    let single = topic("scale-single");
    cluster.create_topic(&single).await;
    publish(&pulsar, &single, 1..=MESSAGES as u64).await;
    let baseline = report("1 partition", drain(&pulsar, &single).await);

    let spread = topic("scale-spread");
    cluster.create_partitioned_topic(&spread, PARTITIONS).await;
    for index in 0..PARTITIONS {
        publish(
            &pulsar,
            &partition(&spread, index),
            1..=(MESSAGES / PARTITIONS as usize) as u64,
        )
        .await;
    }
    let dense = report("200 partitions, all busy", drain(&pulsar, &spread).await);

    let one = topic("scale-one-busy");
    cluster.create_partitioned_topic(&one, PARTITIONS).await;
    publish(&pulsar, &partition(&one, 0), 1..=MESSAGES as u64).await;
    let sparse = report("200 partitions, one busy", drain(&pulsar, &one).await);

    println!(
        "against 1 partition: all busy {:.1}x, one busy {:.1}x",
        dense / baseline,
        sparse / baseline
    );
    // Before polling went by wakes, a pass over every partition per
    // message made this about 40x.
    assert!(
        sparse < baseline * 5.0,
        "a message on one partition of 200 costs a pass over all of them"
    );
}
