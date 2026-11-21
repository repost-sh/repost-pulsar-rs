use std::{sync::Arc, time::Instant};

use futures::channel::{mpsc, oneshot};

use crate::{
    connection::Connection,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

/// What an engine hands its consumer, in the order the broker sent it.
pub enum Queued {
    /// A message, tagged with the epoch of the connection it arrived on.
    Message(u64, MessageIdData, Payload),
    /// What the broker says, or a reconnect implies, about whether this
    /// Failover consumer receives the partition. No reconnect makes it stale.
    Active(bool),
}

pub type MessageIdDataReceiver = mpsc::UnboundedReceiver<Result<Queued, Error>>;

pub(crate) enum EngineEvent<Exe: Executor> {
    /// A frame from the broker, tagged with the epoch of its connection.
    Message(u64, Option<RawMessage>),
    EngineMessage(Option<EngineMessage<Exe>>),
    /// Once a second, so a connection that broke without closing is noticed
    /// even when nothing else happens.
    Tick,
    /// Time to look for taken messages left unacked past the redelivery
    /// delay.
    RedeliveryTick,
}

/// Where a non-durable subscription starts again after a reconnect.
// Java: ConsumerImpl.java:1273-1304 startMessageId
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Resume {
    /// Nothing moved delivery yet: the configured start, with its rollback,
    /// which the broker would apply again on every subscribe after that.
    // Java: ConsumerImpl.java:1066 startMessageId equals the initial one
    #[default]
    Configured,
    /// After a seek to this publish time, in milliseconds. A later reconnect
    /// rolls the subscription back to it, since the broker would otherwise
    /// start a new subscription at the configured start. Java starts from the
    /// earliest message instead (ConsumerImpl.java:3125), replaying the topic.
    Time(u64),
    /// The first message queued while the application had taken none.
    From(MessageIdData),
    /// After the last message the application took, or a seek's target.
    After(MessageIdData),
}

/// Where a seek moves a consumer.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Seek {
    /// After this message.
    Id(MessageIdData),
    /// To the first message published at or after this time, in milliseconds.
    Time(u64),
}

/// What the application has taken from one topic consumer, shared between the
/// consumer and its engine.
///
/// One lock covers both fields, so a reconnect cannot start between the
/// consumer checking a message's epoch and recording it as delivered.
#[derive(Default)]
pub(crate) struct Delivery {
    /// Bumped on every reconnect. A buffered message from an earlier epoch is
    /// dropped instead of delivered: the broker sends it again on the new
    /// connection if it still owes it.
    pub epoch: u64,
    /// Where a non-durable subscription starts again after a reconnect.
    pub resume: Resume,
    /// A seek the next reconnect has not applied yet. The broker closes the
    /// consumer when it seeks, and delivery restarts there.
    pub pending_seek: Option<Seek>,
    /// Permits freed on this epoch's connection and not yet given back, by
    /// messages the application took or the engine dropped.
    pub freed: u32,
    /// How many freed permits make a flow: half the window, as in Java.
    pub refill: u32,
    /// Messages in the channel to the application, from any epoch. A
    /// reconnect grants only what this leaves of the window, so stale
    /// messages still queued do not add up across reconnects.
    pub queued: u32,
    /// A reconnect has not granted its window yet. Permits freed meanwhile
    /// are in that grant, so none flow on their own.
    pub granting: bool,
    /// Messages the application skipped as stale, ever. A seek that did not
    /// happen lowers its epoch again only if this did not move.
    pub stale: u64,
    /// Messages the application took, and when, for the redelivery timer.
    /// Only kept when a redelivery delay is set.
    pub taken: Option<Vec<(MessageIdData, Instant)>>,
}

impl Delivery {
    pub fn new(window: u32, track_taken: bool) -> Self {
        Delivery {
            refill: (window / 2).max(1),
            taken: track_taken.then(Vec::new),
            ..Delivery::default()
        }
    }

    /// Counts one freed permit, and returns how many to give back once half
    /// the window is free.
    // Java: ConsumerImpl.java:2331 increaseAvailablePermits
    pub fn free_permit(&mut self) -> Option<u32> {
        self.free_permits(1)
    }

    pub fn free_permits(&mut self, permits: u32) -> Option<u32> {
        // a reconnect's grant counts these
        if self.granting {
            return None;
        }
        self.freed += permits;
        (self.freed >= self.refill).then(|| std::mem::take(&mut self.freed))
    }

    /// A reconnect began: what the old connection freed is void.
    pub fn reconnecting(&mut self) {
        self.freed = 0;
        self.granting = true;
    }

    /// The permits a reconnect grants: the window, less the stale messages
    /// still queued. Those taken since the reconnect began are in it already.
    pub fn grant(&mut self, window: u32) -> u32 {
        self.granting = false;
        window.saturating_sub(self.queued)
    }
}

/// Where the engine answers a seek.
pub(crate) type SeekReply = oneshot::Sender<Result<(), Error>>;

pub(crate) enum EngineMessage<Exe: Executor> {
    Ack(MessageIdData, bool),
    Nack(MessageIdData),
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
    /// Permits the application has freed by taking messages delivered on the
    /// connection of this epoch.
    Flow(u64, u32),
    /// Move the subscription, and answer once it moved or could not. The
    /// seek has the operation timeout from when it was made.
    Seek(Seek, Instant, SeekReply),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageData {
    pub id: MessageIdData,
    pub batch_size: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    /// Maximum number of times that a message will be redelivered before being sent to the dead
    /// letter queue.
    pub max_redeliver_count: usize,
    /// Name of the dead topic where the failing messages will be sent.
    pub dead_letter_topic: String,
}

/// Where `id` sits in delivery order.
pub(crate) fn position(id: &MessageIdData) -> (u64, u64, i32) {
    (id.ledger_id, id.entry_id, id.batch_index.unwrap_or(-1))
}

#[cfg(test)]
mod flow_tests {
    use super::Delivery;

    #[test]
    fn permits_freed_before_a_reconnect_grants_go_with_the_grant() {
        // a window of ten, a flow per five freed
        let mut delivery = Delivery::new(10, false);
        delivery.queued = 8;
        delivery.reconnecting();
        // stale messages taken before the grant: counted in it
        for _ in 0..6 {
            delivery.queued -= 1;
            assert_eq!(delivery.free_permit(), None);
        }
        assert_eq!(delivery.grant(10), 8);
        // the two still queued, and three fresh ones, make the next flow
        for _ in 0..4 {
            assert_eq!(delivery.free_permit(), None);
        }
        assert_eq!(delivery.free_permit(), Some(5));
    }
}
