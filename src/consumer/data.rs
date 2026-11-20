use std::sync::Arc;

use futures::channel::{mpsc, oneshot};

use crate::{
    connection::Connection,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

/// Messages on their way from the engine to the application, each tagged with
/// the epoch of the connection it arrived on.
pub type MessageIdDataReceiver = mpsc::Receiver<Result<(u64, MessageIdData, Payload), Error>>;

pub enum EngineEvent<Exe: Executor> {
    /// A frame from the broker, tagged with the epoch of its connection.
    Message(u64, Option<RawMessage>),
    EngineMessage(Option<EngineMessage<Exe>>),
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
}

pub enum EngineMessage<Exe: Executor> {
    Ack(MessageIdData, bool),
    Nack(MessageIdData),
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
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
