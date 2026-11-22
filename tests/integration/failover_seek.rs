//! Failover consumers that seek each partition they gain to a shared
//! checkpoint, as Quickwit's Pulsar source does with one consumer per
//! pipeline.
//!
//! The client guarantee under test is that `seek_to`, with `BecameActive`
//! and `BecameInactive`, lets an application restart each partition at its
//! own checkpoint, settle, and miss nothing. Settling is not a given. A
//! seek closes the partition's active consumer, and if the broker gave the
//! partition to another consumer meanwhile, that one would seek in turn,
//! and the partition would move for ever.
//!
//! Missing nothing is not a given either. In apache/pulsar `v5.0.1` the
//! broker sends a consumer entries ahead of the checkpoint on two paths. A
//! seek closes the active consumer without picking another one, and the
//! dispatcher keeps reading for the closed one
//! (`AbstractDispatcherSingleActiveConsumer.java:267-289` and `:342-349`,
//! `PersistentSubscription.java:994`). A read under way then writes its
//! next position over the reset one (`OpReadEntry.java:249-251`), and its
//! entries go out under the consumer id that the client uses again when it
//! subscribes anew (`PersistentDispatcherSingleActiveConsumer.java:224-246`).
//! And when the active consumer changes, Failover holds back the rewind and
//! the new owner's notification for a second (same file, `:139-152`), while
//! every dispatch that completes starts a read for whoever is active now,
//! past the entries the last owner was sent (`:265`): only a flow request
//! waits for the rewind (`:284`).
//!
//! Both reach a consumer before the broker has told it, since it last
//! subscribed, that it is the active one, and the client drops what arrives
//! then: the broker sends it again from the checkpoint with its
//! notification. So here one group processes every message above the
//! checkpoint and must still process each number once and in order
//! ([`Rule::Above`]). Without that rule in the client it missed messages in
//! 3 of 32 runs, and with it in none of 64.
//!
//! The other group keeps checking that each message directly follows the
//! checkpoint, and seeks again when one does not ([`Rule::Follows`]), which
//! an application that must not skip messages still does: a consumer that
//! stays connected while its partition goes to another consumer and back
//! within the broker's delay is told nothing in between, and the entries of
//! a read the broker started for the new owner and had under way when it
//! rewound arrive after the notification, with the cursor moved past them
//! (the task at `:139-152` neither cancels nor disowns that read). The
//! client can tell neither from a regular delivery, so [`Rule::Above`]
//! fails on the second if a run meets it. The report counts the entries
//! [`Rule::Follows`] caught.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Write as _},
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use pulsar::{
    consumer::Message, proto::MessageIdData, Consumer, ConsumerEvent, ConsumerItem, Pulsar,
    TokioExecutor,
};
use tokio::{sync::oneshot, time::Instant};

use crate::{
    cluster::{eventually, exclusive, index, partition, topic, Cluster},
    delivery::{
        batching_producer, expected_owners, failover_consumer, number, producer, publish_with,
        Delivered, Member, DEADLINE,
    },
};

/// How long a change of members may take to reach the broker's layout.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a reached layout must then hold without an event.
const QUIET: Duration = Duration::from_secs(10);
/// A consumer acks cumulatively after every this many messages it processes.
const ACK_EVERY: u64 = 25;
/// How many times in a row a partition may be sought again after a gap
/// before its consumer gives up, as the source then fails.
const RESEEKS_IN_A_ROW: u32 = 3;

/// What a consumer does with a message above its partition's checkpoint.
#[derive(Clone, Copy, PartialEq)]
enum Rule {
    /// Processes it only if it directly follows the checkpoint, and seeks
    /// the partition to the checkpoint again if it does not.
    Follows,
    /// Processes it, whatever lies between the two.
    Above,
}

/// What the consumers of a group share, as Quickwit's pipelines share the
/// metastore checkpoint, and what the test sees each of them do.
struct Group {
    rule: Rule,
    started: Instant,
    tracks: BTreeMap<String, Track>,
    processed: Delivered,
    /// The consumers whose last event for a partition made them active.
    active: BTreeMap<String, BTreeSet<&'static str>>,
    log: Vec<Step>,
    /// Processed messages in a later ledger than the checkpoint before them.
    ledger_rollovers: u64,
    /// Why a consumer gave up on a partition, if one did.
    gave_up: Option<String>,
    /// Messages the client withheld from the consumers that have left.
    withheld: u64,
}

/// What the group did with one partition.
#[derive(Default)]
struct Track {
    /// The last message processed: its `ack_id`, which a restart seeks to,
    /// and where it is.
    checkpoint: Option<(MessageIdData, Mark)>,
    /// Re-seeks after gaps since the last message processed.
    reseeks_in_a_row: u32,
    /// Who processed the numbers of the partition.
    processed_by: Runs,
    /// Who dropped numbers of the partition as at or below the checkpoint.
    dropped_by: Runs,
}

impl Track {
    /// What `rule` makes of a message at `received`.
    fn judge(&self, received: &Position, rule: Rule) -> Verdict {
        let Some((_, last)) = &self.checkpoint else {
            return Verdict::Process;
        };
        if *received <= last.position {
            Verdict::Redelivered
        } else if rule == Rule::Above || follows(&last.position, received) {
            Verdict::Process
        } else {
            Verdict::Gap
        }
    }
}

enum Verdict {
    /// What the rule processes: above the checkpoint and, under
    /// [`Rule::Follows`], right after it; or the partition has none yet.
    Process,
    /// At or below the checkpoint, so processed already.
    Redelivered,
    /// Above the checkpoint, with messages between the two, under
    /// [`Rule::Follows`].
    Gap,
}

/// What a consumer does with a message once the group judged it.
enum Offer {
    /// Processed: count it towards the next ack.
    Processed,
    /// Dropped as processed already.
    Dropped,
    /// Dropped after a gap: seek the partition to this checkpoint again.
    Reseek(MessageIdData),
    /// Dropped after a gap with no re-seek left: stop, as the source fails.
    GiveUp,
}

/// Where a message sits in its partition, from its `ack_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Position {
    ledger: u64,
    entry: u64,
    /// `None` for an entry of one message outside a batch.
    batch: Option<BatchMember>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BatchMember {
    index: i32,
    size: i32,
}

impl Position {
    fn of(id: &MessageIdData) -> Self {
        let batch = id.batch_index.filter(|&index| index >= 0).map(|index| {
            let size = id
                .batch_size
                .expect("a batch member's ack_id carries its batch size");
            BatchMember { index, size }
        });
        Self {
            ledger: id.ledger_id,
            entry: id.entry_id,
            batch,
        }
    }

    fn opens_entry(&self) -> bool {
        self.batch.is_none_or(|member| member.index == 0)
    }

    fn closes_entry(&self) -> bool {
        self.batch
            .is_none_or(|member| member.index + 1 == member.size)
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ledger, self.entry)?;
        match self.batch {
            Some(member) => write!(f, ":{}/{}", member.index, member.size),
            None => Ok(()),
        }
    }
}

/// Whether `next` directly follows `previous` in their partition.
// Ledger ids are not consecutive within a topic, and a message id does not
// say whether its entry closed its ledger: so entry 0 of any later ledger
// follows the last member of an entry, and a ledger's lost tail goes unseen.
fn follows(previous: &Position, next: &Position) -> bool {
    if (next.ledger, next.entry) == (previous.ledger, previous.entry) {
        return previous
            .batch
            .zip(next.batch)
            .is_some_and(|(before, after)| after.index == before.index + 1);
    }
    if !previous.closes_entry() || !next.opens_entry() {
        return false;
    }
    let next_entry = next.ledger == previous.ledger && next.entry == previous.entry + 1;
    let next_ledger = next.ledger > previous.ledger && next.entry == 0;
    next_entry || next_ledger
}

/// A message as the log names it: its number, and where it is.
#[derive(Debug, Clone, Copy)]
struct Mark {
    number: u64,
    position: Position,
}

impl fmt::Display for Mark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.number, self.position)
    }
}

/// Numbers of one partition in the order they came, as runs of consecutive
/// numbers that one consumer had.
#[derive(Default)]
struct Runs(Vec<(&'static str, RangeInclusive<u64>)>);

impl Runs {
    fn push(&mut self, consumer: &'static str, number: u64) {
        match self.0.last_mut() {
            Some((had, numbers)) if *had == consumer && *numbers.end() + 1 == number => {
                *numbers = *numbers.start()..=number;
            }
            _ => self.0.push((consumer, number..=number)),
        }
    }

    /// How many numbers `consumer` had.
    fn count(&self, consumer: &str) -> u64 {
        self.0
            .iter()
            .filter(|(had, _)| *had == consumer)
            .map(|(_, numbers)| numbers.end() - numbers.start() + 1)
            .sum()
    }
}

impl fmt::Display for Runs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (consumer, numbers) in &self.0 {
            write!(f, " {consumer} {numbers:?}")?;
        }
        Ok(())
    }
}

struct Step {
    at: Duration,
    consumer: &'static str,
    what: What,
}

enum What {
    Joining,
    Leaving,
    Active(u32),
    Inactive(u32),
    Seeking {
        partition: u32,
        after: Mark,
    },
    /// A message that does not follow the checkpoint, and which re-seek in a
    /// row it led to.
    Gap {
        partition: u32,
        after: Mark,
        received: Mark,
        reseek: u32,
    },
}

impl Step {
    fn is_event(&self) -> bool {
        matches!(self.what, What::Active(_) | What::Inactive(_))
    }
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:>8.3}s {} ", self.at.as_secs_f64(), self.consumer)?;
        match self.what {
            What::Joining => write!(f, "joins"),
            What::Leaving => write!(f, "leaves"),
            What::Active(partition) => write!(f, "BecameActive p{partition}"),
            What::Inactive(partition) => write!(f, "BecameInactive p{partition}"),
            What::Seeking { partition, after } => write!(f, "seeks p{partition} to after {after}"),
            What::Gap {
                partition,
                after,
                received,
                reseek,
            } => write!(
                f,
                "gap on p{partition}: expected {}, the message after {}; received {received}; \
                 re-seek {reseek} of {RESEEKS_IN_A_ROW}",
                after.number + 1,
                after.position
            ),
        }
    }
}

impl Group {
    fn new(rule: Rule) -> Self {
        Self {
            rule,
            started: Instant::now(),
            tracks: BTreeMap::new(),
            processed: Delivered::default(),
            active: BTreeMap::new(),
            log: Vec::new(),
            ledger_rollovers: 0,
            gave_up: None,
            withheld: 0,
        }
    }

    fn note(&mut self, consumer: &'static str, what: What) {
        let at = self.started.elapsed();
        self.log.push(Step { at, consumer, what });
    }

    /// Marks `consumer` active on `partition`, and returns the checkpoint it
    /// restarts the partition at, if there is one.
    fn became_active(&mut self, consumer: &'static str, partition: &str) -> Option<MessageIdData> {
        self.active
            .entry(partition.to_owned())
            .or_default()
            .insert(consumer);
        self.note(consumer, What::Active(index(partition)));
        let (checkpoint, after) = self.tracks.get(partition)?.checkpoint.clone()?;
        let partition = index(partition);
        self.note(consumer, What::Seeking { partition, after });
        Some(checkpoint)
    }

    fn became_inactive(&mut self, consumer: &'static str, partition: &str) {
        self.active
            .entry(partition.to_owned())
            .or_default()
            .remove(consumer);
        self.note(consumer, What::Inactive(index(partition)));
    }

    /// Takes a closed consumer out of every partition's owners.
    fn forget(&mut self, consumer: &'static str) {
        for owners in self.active.values_mut() {
            owners.remove(consumer);
        }
    }

    fn checkpointed(&self, partitions: &[String]) -> bool {
        partitions.iter().all(|partition| {
            self.tracks
                .get(partition)
                .is_some_and(|track| track.checkpoint.is_some())
        })
    }

    /// Processes `message` if the group's rule says so, and moves the
    /// partition's checkpoint to it in the same step.
    fn offer(&mut self, consumer: &'static str, message: &Message<String>) -> Offer {
        let id = message.ack_id();
        let received = Mark {
            number: number(message),
            position: Position::of(&id),
        };
        let partition = index(&message.topic);
        let at = self.started.elapsed();
        let track = self.tracks.entry(message.topic.clone()).or_default();
        match track.judge(&received.position, self.rule) {
            Verdict::Process => {}
            Verdict::Redelivered => {
                track.dropped_by.push(consumer, received.number);
                return Offer::Dropped;
            }
            Verdict::Gap => {
                let (checkpoint, after) = track.checkpoint.clone().expect("a gap has a checkpoint");
                if track.reseeks_in_a_row == RESEEKS_IN_A_ROW {
                    self.gave_up = Some(format!(
                        "{consumer} gave up on p{partition}: {RESEEKS_IN_A_ROW} re-seeks in a row \
                         to after {after} brought back {received}, not the message after it"
                    ));
                    return Offer::GiveUp;
                }
                track.reseeks_in_a_row += 1;
                let what = What::Gap {
                    partition,
                    after,
                    received,
                    reseek: track.reseeks_in_a_row,
                };
                self.log.push(Step { at, consumer, what });
                return Offer::Reseek(checkpoint);
            }
        }
        let rolled_over = track
            .checkpoint
            .as_ref()
            .is_some_and(|(_, last)| last.position.ledger != received.position.ledger);
        if rolled_over {
            self.ledger_rollovers += 1;
        }
        track.reseeks_in_a_row = 0;
        track.processed_by.push(consumer, received.number);
        track.checkpoint = Some((id, received));
        self.processed.record(message);
        Offer::Processed
    }

    /// Fails the test, with the report, if a consumer gave up on a partition.
    fn assert_going(&self, changes: &[Change]) {
        if let Some(why) = &self.gave_up {
            panic!("{why}\n{}", self.report(changes));
        }
    }

    fn report(&self, changes: &[Change]) -> String {
        let mut report = String::new();
        for step in &self.log {
            writeln!(report, "{step}").unwrap();
        }
        writeln!(report, "consumer events seeks processed dropped").unwrap();
        let consumers: BTreeSet<&str> = self.log.iter().map(|step| step.consumer).collect();
        for name in consumers {
            let steps = || self.log.iter().filter(move |step| step.consumer == name);
            let events = steps().filter(|step| step.is_event()).count();
            let seeks = steps()
                .filter(|step| matches!(step.what, What::Seeking { .. }))
                .count();
            let processed: u64 = self
                .tracks
                .values()
                .map(|track| track.processed_by.count(name))
                .sum();
            let dropped: u64 = self
                .tracks
                .values()
                .map(|track| track.dropped_by.count(name))
                .sum();
            writeln!(
                report,
                "{name:>8} {events:>6} {seeks:>5} {processed:>9} {dropped:>7}"
            )
            .unwrap();
        }
        for (partition, track) in &self.tracks {
            let partition = index(partition);
            writeln!(report, "p{partition} processed{}", track.processed_by).unwrap();
            if !track.dropped_by.0.is_empty() {
                writeln!(report, "p{partition} dropped{}", track.dropped_by).unwrap();
            }
        }
        let repaired = self
            .log
            .iter()
            .filter(|step| matches!(step.what, What::Gap { .. }))
            .count();
        writeln!(report, "stale entries repaired by a re-seek: {repaired}").unwrap();
        writeln!(report, "messages the client withheld: {}", self.withheld).unwrap();
        writeln!(report, "ledger rollovers: {}", self.ledger_rollovers).unwrap();
        for change in changes {
            writeln!(report, "{change}").unwrap();
        }
        report
    }
}

/// What one change of members led to.
struct Change {
    /// The step that changed the members.
    cause: String,
    /// From that step until each partition was active on its consumer.
    settled_after: Duration,
    events: usize,
    seeks: usize,
    /// Events in the [`QUIET`] after that.
    quiet_events: usize,
}

impl fmt::Display for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: layout reached after {:.3}s, {} events and {} seeks; {} events in the next {QUIET:?}",
            self.cause.trim_start(),
            self.settled_after.as_secs_f64(),
            self.events,
            self.seeks,
            self.quiet_events
        )
    }
}

/// Stops driving the consumer `name` and closes it.
async fn leave(name: &'static str, member: Member, group: &Mutex<Group>) {
    group.lock().unwrap().note(name, What::Leaving);
    member.stop(name).await;
}

/// Subscribes `name` to the group with a client of its own, as each Quickwit
/// pipeline has, and drives it in its own task.
async fn join(
    cluster: &Cluster,
    topic: &str,
    name: &'static str,
    group: &Arc<Mutex<Group>>,
) -> Member {
    group.lock().unwrap().note(name, What::Joining);
    let consumer = failover_consumer(&cluster.client().await, topic, name).await;
    let group = group.clone();
    Member::spawn(|stop| pipeline(name, consumer, group, stop))
}

/// Drives `consumer` as a Quickwit pipeline drives its Pulsar source, until
/// `stop`: it restarts each partition it gains at the group's checkpoint,
/// processes only what follows the checkpoint, restarts a partition at the
/// checkpoint again on a gap, and acks what it processed.
async fn pipeline(
    name: &'static str,
    mut consumer: Consumer<String, TokioExecutor>,
    group: Arc<Mutex<Group>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut processed = 0;
    loop {
        let item = tokio::select! {
            _ = &mut stop => break,
            item = consumer.next_item() => item.expect("consumer ended").expect("consumer failed"),
        };
        let message = match item {
            ConsumerItem::Event(ConsumerEvent::BecameActive { topic }) => {
                let checkpoint = group.lock().unwrap().became_active(name, &topic);
                if let Some(checkpoint) = checkpoint {
                    consumer.seek_to(&topic, checkpoint).await.expect("seek");
                }
                continue;
            }
            ConsumerItem::Event(ConsumerEvent::BecameInactive { topic }) => {
                group.lock().unwrap().became_inactive(name, &topic);
                continue;
            }
            ConsumerItem::Message(message) => message,
        };
        let offer = group.lock().unwrap().offer(name, &message);
        match offer {
            Offer::Processed => {}
            Offer::Dropped => continue,
            Offer::Reseek(checkpoint) => {
                consumer
                    .seek_to(&message.topic, checkpoint)
                    .await
                    .expect("seek");
                continue;
            }
            Offer::GiveUp => break,
        }
        processed += 1;
        if processed % ACK_EVERY == 0 {
            // never past the checkpoint, which is at this message or later
            consumer
                .cumulative_ack_with_id(&message.topic, message.ack_id())
                .await
                .expect("ack");
        }
    }
    let withheld = consumer.messages_withheld();
    consumer.close().await.expect("close");
    let mut group = group.lock().unwrap();
    group.withheld += withheld;
    group.forget(name);
}

/// Publishes rounds of ten numbered messages to every partition until
/// `stop`, every other round as one batch, and returns the last number.
async fn publish_rounds(
    pulsar: Pulsar<TokioExecutor>,
    partitions: Vec<String>,
    stop: Arc<AtomicBool>,
) -> u64 {
    let mut producers = Vec::new();
    for partition in &partitions {
        let single = producer(&pulsar, partition).await;
        producers.push((single, batching_producer(&pulsar, partition).await));
    }
    let mut last = 0;
    while !stop.load(Ordering::Relaxed) {
        let round = last + 1..=last + 10;
        for (single, batching) in &mut producers {
            let producer = if last % 20 == 0 { single } else { batching };
            publish_with(producer, round.clone()).await;
        }
        last += 10;
    }
    last
}

/// Waits until each partition is active on the one consumer the broker picks
/// among `members`, `members[partition % count]` in name order, then watches
/// for [`QUIET`] more. Measures from the last change of members logged.
async fn settle(
    group: &Mutex<Group>,
    partitions: &[String],
    members: &BTreeMap<&'static str, Member>,
) -> Change {
    let names: Vec<&'static str> = members.keys().copied().collect();
    let expected = expected_owners(partitions, &names);
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let (settled_at, settled) = loop {
        {
            let group = group.lock().unwrap();
            group.assert_going(&[]);
            if group.active == expected {
                break (group.started.elapsed(), group.log.len());
            }
            assert!(
                Instant::now() < deadline,
                "{names:?}: not {expected:?} within {SETTLE_TIMEOUT:?}, but {:?}\n{}",
                group.active,
                group.report(&[])
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    tokio::time::sleep(QUIET).await;

    let group = group.lock().unwrap();
    group.assert_going(&[]);
    let start = group
        .log
        .iter()
        .rposition(|step| matches!(step.what, What::Joining | What::Leaving))
        .expect("a change of members");
    let until_settled = &group.log[start..settled];
    Change {
        cause: group.log[start].to_string(),
        settled_after: settled_at - group.log[start].at,
        events: until_settled.iter().filter(|step| step.is_event()).count(),
        seeks: until_settled
            .iter()
            .filter(|step| matches!(step.what, What::Seeking { .. }))
            .count(),
        quiet_events: group.log[settled..]
            .iter()
            .filter(|step| step.is_event())
            .count(),
    }
}

#[tokio::test]
async fn consumers_that_seek_on_activation_settle() {
    seek_on_activation(Rule::Follows).await;
}

#[tokio::test]
async fn consumers_that_process_all_above_the_checkpoint_miss_nothing() {
    seek_on_activation(Rule::Above).await;
}

/// Joins three consumers one at a time under load, then removes the first,
/// and checks that ownership settles after each change and that every
/// published number is processed exactly once and in order.
async fn seek_on_activation(rule: Rule) {
    let cluster = exclusive().await;
    let topic = topic("failover-seek");
    cluster.create_partitioned_topic(&topic, 4).await;
    let partitions: Vec<String> = (0..4).map(|i| partition(&topic, i)).collect();
    let group = Arc::new(Mutex::new(Group::new(rule)));
    let stop_publishing = Arc::new(AtomicBool::new(false));
    let publishing = Instant::now();
    let publisher = tokio::spawn(publish_rounds(
        cluster.client().await,
        partitions.clone(),
        stop_publishing.clone(),
    ));

    let mut members = BTreeMap::new();
    members.insert("a", join(&cluster, &topic, "a", &group).await);
    // b and c seek a partition they gain only once it has a checkpoint
    let checkpointed = eventually(SETTLE_TIMEOUT, || async {
        group.lock().unwrap().checkpointed(&partitions)
    })
    .await;
    assert!(
        checkpointed,
        "a checkpoint on every partition within {SETTLE_TIMEOUT:?}\n{}",
        group.lock().unwrap().report(&[])
    );
    let mut changes = Vec::new();
    for name in ["b", "c"] {
        members.insert(name, join(&cluster, &topic, name, &group).await);
        changes.push(settle(&group, &partitions, &members).await);
    }
    // every partition moves
    let a = members.remove("a").expect("a joined");
    leave("a", a, &group).await;
    changes.push(settle(&group, &partitions, &members).await);

    stop_publishing.store(true, Ordering::Relaxed);
    let published = publisher.await.expect("publisher");
    let publishing = publishing.elapsed();
    // what is still missing then, the check of each partition names
    eventually(DEADLINE, || async {
        let group = group.lock().unwrap();
        group.gave_up.is_some()
            || partitions
                .iter()
                .all(|partition| group.processed.distinct_on(partition) as u64 == published)
    })
    .await;
    group.lock().unwrap().assert_going(&changes);
    for (name, member) in members {
        leave(name, member, &group).await;
    }

    let group = group.lock().unwrap();
    println!(
        "published 1..={published} on each of {} partitions in {:.1}s\n{}",
        partitions.len(),
        publishing.as_secs_f64(),
        group.report(&changes),
    );
    for change in &changes {
        assert_eq!(change.quiet_events, 0, "did not stay settled: {change}");
    }
    for partition in &partitions {
        group.processed.assert_exactly(partition, 1..=published);
    }
}
