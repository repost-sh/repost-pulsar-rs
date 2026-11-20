use std::{sync::Arc, time::Instant};

use futures::channel::{mpsc, oneshot};

use crate::{
    connection::Connection,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

/// Messages on their way from the engine to the application, each tagged with
/// the epoch of the connection it arrived on.
pub type MessageIdDataReceiver =
    mpsc::UnboundedReceiver<Result<(u64, MessageIdData, Payload), Error>>;

pub enum EngineEvent<Exe: Executor> {
    /// A frame from the broker, tagged with the epoch of its connection.
    Message(u64, Option<RawMessage>),
    EngineMessage(Option<EngineMessage<Exe>>),
    /// Once a second, so a connection that broke without closing is noticed
    /// even when nothing else happens.
    Tick,
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
    /// The last message handed to the application, where a non-durable
    /// subscription resumes after a reconnect.
    pub last: Option<MessageIdData>,
    /// The first message queued for the application while it had taken none
    /// yet, where a non-durable subscription resumes if `last` is unset.
    pub first_queued: Option<MessageIdData>,
    /// The target of a seek the next reconnect has not applied yet. The broker
    /// closes the consumer when it seeks, and delivery restarts after it.
    pub seek_target: Option<MessageIdData>,
    /// Permits freed on this epoch's connection and not yet given back, by
    /// messages the application took or the engine dropped.
    pub freed: u32,
    /// How many freed permits make a flow: half the window, as in Java.
    pub refill: u32,
    /// Messages in the channel to the application, from any epoch. A
    /// reconnect grants only what this leaves of the window, so stale
    /// messages still queued do not add up across reconnects.
    pub queued: u32,
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
        self.freed += permits;
        (self.freed >= self.refill).then(|| std::mem::take(&mut self.freed))
    }
}

pub enum EngineMessage<Exe: Executor> {
    Ack(MessageIdData, bool),
    Nack(MessageIdData),
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
    /// Permits the application has freed by taking messages delivered on the
    /// connection of this epoch.
    Flow(u64, u32),
    /// A seek to this id failed after a reconnect had already applied it, so
    /// its start filter must go.
    CancelSeek(MessageIdData),
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
