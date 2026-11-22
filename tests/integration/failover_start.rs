//! Failover consumers that start a partition which has no checkpoint yet at
//! its first message, as Quickwit's pipelines do when they start together
//! on a topic that already holds messages.
//!
//! Such a partition is where an entry the broker sends out of turn does the
//! most harm: with no checkpoint, nothing tells the application that the
//! message it was handed is not the partition's first one, and once it
//! acknowledges from there, the messages before are gone. The client
//! guarantee under test is that the first message a consumer is handed
//! after it seeks a partition to the start is that partition's first
//! message, while consumers join and leave, messages keep arriving and
//! partitions start new ledgers.
//!
//! The broker sends out of turn what a publish wakes a waiting read for, or
//! what a dispatch under way at a change of owner reads on for, so a
//! backlog alone does not show it. `failover_seek.rs` cites the broker
//! code. With `failover_seek.rs` running alongside on the same brokers, 7
//! of 342 seeks in 30 runs of this test started past the partition's first
//! message without the client withholding such entries, and none of 342
//! with it. On brokers that run nothing else none of 350 seeks did, even
//! without it, so a pass here alone says little. The client cannot
//! withhold the entries of a read the broker had under way for the consumer
//! when it rewound, which arrive after the notification; a run that meets
//! that case fails here.
//!
//! Each step is written to stderr as it happens, with the time in the
//! brokers' clock, and each wait on the cluster or a consumer is bounded, so
//! a run that stops says where.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use pulsar::{
    proto::MessageIdData, Consumer, ConsumerEvent, ConsumerItem, Producer, Pulsar, TokioExecutor,
};
use tokio::{sync::oneshot, time::Instant};

use crate::{
    cluster::{eventually, exclusive, index, partition, step, topic, within, Cluster},
    delivery::{expected_owners, failover_consumer, number, publish_with, Member, DEADLINE},
};

const PARTITIONS: u32 = 4;
/// Entries published to each partition before each unload, which closes the
/// ledger: the backlog the consumers find spans this many ledgers.
const LEDGERS: [u64; 3] = [30, 30, 30];
const NAMES: [&str; 4] = ["a", "b", "c", "d"];
/// Times a consumer leaves and joins again while the others start.
const REJOINS: usize = 8;
/// Longer than the broker waits before it announces a change of owner.
const ANNOUNCED: Duration = Duration::from_millis(1500);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the group must then stay as it is.
const QUIET: Duration = Duration::from_secs(3);

/// The first message a consumer was handed after a seek to the start.
struct First {
    at: Duration,
    consumer: &'static str,
    partition: u32,
    number: u64,
    ledger: u64,
    entry: u64,
}

#[derive(Default)]
struct Group {
    firsts: Vec<First>,
    /// The partitions each consumer sought to the start and has no message
    /// from yet.
    awaiting: BTreeMap<&'static str, BTreeSet<String>>,
    /// The consumers whose last event for a partition made them active.
    active: BTreeMap<String, BTreeSet<&'static str>>,
    events: usize,
    /// Messages the client withheld from the consumers that have left.
    withheld: u64,
}

async fn join(
    cluster: &Cluster,
    topic: &str,
    name: &'static str,
    group: &Arc<Mutex<Group>>,
    started: Instant,
) -> Member {
    let pulsar = within(DEADLINE, format!("{name}: client"), cluster.client()).await;
    let consumer = within(
        DEADLINE,
        format!("{name}: subscribe"),
        failover_consumer(&pulsar, topic, name),
    )
    .await;
    step(format!("{name}: built"));
    let group = group.clone();
    Member::spawn(|stop| pipeline(name, consumer, group, stop, started))
}

async fn leave(name: &str, member: Member) {
    step(format!("{name}: leaves"));
    member.stop(name).await;
    step(format!("{name}: left"));
}

/// Publishes rounds of ten numbers above `last` to every partition until
/// `stop`. A read that waits at the end of a partition wakes with each of
/// them, for whichever consumer the broker then holds active.
async fn keep_publishing(
    pulsar: Pulsar<TokioExecutor>,
    partitions: Vec<String>,
    mut last: u64,
    stop: Arc<AtomicBool>,
) {
    let mut producers = Vec::new();
    for partition in &partitions {
        producers.push(producer(&pulsar, partition).await);
    }
    step("publisher: started");
    while !stop.load(Ordering::Relaxed) {
        for (partition, producer) in partitions.iter().zip(&mut producers) {
            within(
                DEADLINE,
                format!("publisher: {} to {}", last + 10, index(partition)),
                publish_with(producer, last + 1..=last + 10),
            )
            .await;
        }
        last += 10;
    }
    step(format!("publisher: stopped at {last}"));
}

async fn producer(pulsar: &Pulsar<TokioExecutor>, partition: &str) -> Producer<TokioExecutor> {
    within(
        DEADLINE,
        format!("producer of {}", index(partition)),
        crate::delivery::producer(pulsar, partition),
    )
    .await
}

/// The id a seek takes for the start of a topic.
// Java: MessageId.earliest
fn earliest() -> MessageIdData {
    MessageIdData {
        ledger_id: u64::MAX,
        entry_id: u64::MAX,
        ..Default::default()
    }
}

/// Seeks `topic` to its start. The broker refuses a seek while another one
/// moves the same subscription, so this asks until it does not.
async fn seek_to_start(name: &str, consumer: &mut Consumer<String, TokioExecutor>, topic: &str) {
    let partition = index(topic);
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        step(format!("{name}: seeks p{partition}"));
        let sent = Instant::now();
        let answer = within(
            DEADLINE,
            format!("{name}: seek of p{partition}"),
            consumer.seek_to_inclusive(topic, earliest()),
        )
        .await;
        let took = sent.elapsed();
        let Err(error) = answer else {
            step(format!("{name}: seek of p{partition} done in {took:?}"));
            return;
        };
        step(format!(
            "{name}: seek of p{partition} refused in {took:?}: {error}"
        ));
        assert!(Instant::now() < deadline, "seek of {topic}: {error}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Drives `consumer` until `stop`: it seeks each partition it gains to the
/// start, as a pipeline without checkpoint does, and notes the first message
/// it is handed after each such seek. It acknowledges nothing.
async fn pipeline(
    name: &'static str,
    mut consumer: Consumer<String, TokioExecutor>,
    group: Arc<Mutex<Group>>,
    mut stop: oneshot::Receiver<()>,
    started: Instant,
) {
    loop {
        let item = tokio::select! {
            _ = &mut stop => break,
            item = consumer.next_item() => item.expect("consumer ended").expect("consumer failed"),
        };
        match item {
            ConsumerItem::Event(ConsumerEvent::BecameActive { topic }) => {
                step(format!("{name}: BecameActive p{}", index(&topic)));
                {
                    let mut group = group.lock().unwrap();
                    group.events += 1;
                    group.active.entry(topic.clone()).or_default().insert(name);
                    group
                        .awaiting
                        .entry(name)
                        .or_default()
                        .insert(topic.clone());
                }
                seek_to_start(name, &mut consumer, &topic).await;
            }
            ConsumerItem::Event(ConsumerEvent::BecameInactive { topic }) => {
                step(format!("{name}: BecameInactive p{}", index(&topic)));
                let mut group = group.lock().unwrap();
                group.events += 1;
                group.active.entry(topic.clone()).or_default().remove(name);
                group.awaiting.entry(name).or_default().remove(&topic);
            }
            ConsumerItem::Message(message) => {
                let mut group = group.lock().unwrap();
                if !group
                    .awaiting
                    .entry(name)
                    .or_default()
                    .remove(&message.topic)
                {
                    continue;
                }
                let id = message.message_id();
                step(format!(
                    "{name}: p{} first message {} ({}:{})",
                    index(&message.topic),
                    number(&message),
                    id.ledger_id,
                    id.entry_id
                ));
                group.firsts.push(First {
                    at: started.elapsed(),
                    consumer: name,
                    partition: index(&message.topic),
                    number: number(&message),
                    ledger: id.ledger_id,
                    entry: id.entry_id,
                });
            }
        }
    }
    let withheld = consumer.messages_withheld();
    within(DEADLINE, format!("{name}: close"), consumer.close())
        .await
        .expect("close");
    let mut group = group.lock().unwrap();
    group.withheld += withheld;
    group.awaiting.remove(name);
    for owners in group.active.values_mut() {
        owners.remove(name);
    }
}

#[tokio::test]
async fn a_partition_without_checkpoint_starts_at_its_first_message() {
    let cluster = exclusive().await;
    let topic = topic("failover-start");
    cluster.create_partitioned_topic(&topic, PARTITIONS).await;
    // the subscription holds the backlog, which the consumers start on
    cluster.create_subscription(&topic, "failover").await;
    step(format!("setup: created {topic} and its subscription"));
    let partitions: Vec<String> = (0..PARTITIONS).map(|i| partition(&topic, i)).collect();
    let pulsar = within(DEADLINE, "setup: client", cluster.client()).await;
    let mut published = 0;
    for (ledger, entries) in LEDGERS.into_iter().enumerate() {
        for partition in &partitions {
            let mut producer = producer(&pulsar, partition).await;
            for round in (published..published + entries).step_by(10) {
                within(
                    DEADLINE,
                    format!("setup: publish {} to {}", round + 10, index(partition)),
                    publish_with(&mut producer, round + 1..=round + 10),
                )
                .await;
            }
            step(format!(
                "setup: published to {} up to {}",
                index(partition),
                published + entries
            ));
        }
        published += entries;
        for partition in &partitions {
            cluster.unload(partition).await;
            step(format!(
                "setup: unloaded {} after ledger {ledger}",
                index(partition)
            ));
        }
    }
    for partition in &partitions {
        // right after an unload, the stats can still be those of the topic
        // being closed, whose last ledger is open and counts no entries
        let seen = std::cell::RefCell::new(Vec::new());
        let filled = eventually(SETTLE_TIMEOUT, || async {
            *seen.borrow_mut() = cluster.ledgers(partition).await;
            seen.borrow()
                .iter()
                .map(|ledger| ledger.entries)
                .take(LEDGERS.len())
                .eq(LEDGERS)
        })
        .await;
        let ledgers = seen.into_inner();
        assert!(
            filled,
            "{partition} holds one ledger per round of publishing: {ledgers:?}"
        );
        step(format!(
            "setup: {} holds ledgers {ledgers:?}",
            index(partition)
        ));
    }

    let stop_publishing = Arc::new(AtomicBool::new(false));
    let publisher = tokio::spawn(keep_publishing(
        cluster.client().await,
        partitions.clone(),
        published,
        stop_publishing.clone(),
    ));
    let group = Arc::new(Mutex::new(Group::default()));
    let started = Instant::now();
    let joined = futures::future::join_all(
        NAMES
            .iter()
            .map(|name| join(&cluster, &topic, name, &group, started)),
    )
    .await;
    let mut members: BTreeMap<&'static str, Member> = NAMES.into_iter().zip(joined).collect();
    for round in 0..REJOINS {
        tokio::time::sleep(Duration::from_millis(150 + 170 * (round as u64 % 4))).await;
        let name = NAMES[round % NAMES.len()];
        step(format!("rejoin round {round}"));
        leave(name, members.remove(name).expect("a member")).await;
        members.insert(name, join(&cluster, &topic, name, &group, started).await);
        // every other round a partition starts a new ledger as well, whose
        // first entry is then among those the broker sends out of turn
        if round % 2 == 1 {
            let partition = &partitions[round / 2 % partitions.len()];
            cluster.unload(partition).await;
            step(format!(
                "rejoin round {round}: unloaded {}",
                index(partition)
            ));
        }
    }
    // then changes far enough apart for the broker to announce each one, so
    // that every consumer that gains a partition seeks it
    for name in NAMES {
        step(format!("announced change: {name}"));
        leave(name, members.remove(name).expect("a member")).await;
        tokio::time::sleep(ANNOUNCED).await;
        members.insert(name, join(&cluster, &topic, name, &group, started).await);
        tokio::time::sleep(ANNOUNCED).await;
    }
    step("settling");

    // each partition on the consumer the broker picks among the four by
    // name, which has its first message, and no event for a while
    let expected = expected_owners(&partitions, &NAMES);
    let settled = eventually(SETTLE_TIMEOUT, || async {
        let events = {
            let group = group.lock().unwrap();
            if group.active != expected || group.awaiting.values().any(|topics| !topics.is_empty())
            {
                return false;
            }
            group.events
        };
        tokio::time::sleep(QUIET).await;
        group.lock().unwrap().events == events
    })
    .await;
    step(format!("settled: {settled}"));
    stop_publishing.store(true, Ordering::Relaxed);
    within(DEADLINE, "publisher: stop", publisher)
        .await
        .expect("publisher");
    for (name, member) in members {
        leave(name, member).await;
    }

    let group = group.lock().unwrap();
    let mut report = String::new();
    for first in &group.firsts {
        writeln!(
            report,
            "{:>8.3}s {} p{}: first message {} ({}:{})",
            first.at.as_secs_f64(),
            first.consumer,
            first.partition,
            first.number,
            first.ledger,
            first.entry
        )
        .unwrap();
    }
    let wrong = group
        .firsts
        .iter()
        .filter(|first| first.number != 1)
        .count();
    println!(
        "{report}first messages after a seek to the start: {}, of which not the partition's first: \
         {wrong}\nmessages the client withheld: {}",
        group.firsts.len(),
        group.withheld,
    );
    assert!(
        settled,
        "not {expected:?} with a first message each within {SETTLE_TIMEOUT:?}, but {:?}, \
         awaiting {:?}",
        group.active, group.awaiting
    );
    assert_eq!(
        wrong, 0,
        "a consumer started a partition past its first message"
    );
}
