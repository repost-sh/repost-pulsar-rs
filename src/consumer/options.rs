use std::{collections::BTreeMap, time::Duration};

use crate::{
    consumer::initial_position::InitialPosition,
    message::proto::{MessageIdData, Schema},
};

/// Configuration options for consumers
#[derive(Clone, Default, Debug)]
pub struct ConsumerOptions {
    pub priority_level: Option<i32>,
    /// Signal whether the subscription should be backed by a
    /// durable cursor or not
    pub durable: Option<bool>,
    /// If specified, the subscription will position the cursor
    /// marked-delete position on the particular message id and
    /// will send messages from that point
    pub start_message_id: Option<MessageIdData>,
    /// Add optional metadata key=value to this consumer
    pub metadata: BTreeMap<String, String>,
    pub read_compacted: Option<bool>,
    pub schema: Option<Schema>,
    /// The most messages each topic or partition consumer holds for the
    /// application. The broker sends no more than this, or the batch size if
    /// that is smaller, until the application takes some (default 1000).
    pub receiver_queue_size: Option<u32>,
    /// Signal whether the subscription will initialize on latest
    /// or earliest message (default on latest)
    ///
    /// an enum can be used to initialize it:
    ///
    /// ```rust,ignore
    /// ConsumerOptions {
    ///     initial_position: InitialPosition::Earliest,
    /// }
    /// ```
    pub initial_position: InitialPosition,
    /// Starts a reader this far back in time, rounded up to whole seconds.
    /// Readers only, and not together with `start_message_id`.
    // Java: ReaderBuilder.startMessageFromRollbackDuration
    pub start_rollback: Option<Duration>,
    /// Whether a reader starting at `start_message_id` reads that message
    /// too (default false: from the one after it).
    // Java: ReaderBuilder.startMessageIdInclusive
    pub start_inclusive: bool,
    /// Whether an ack of part of a batch reaches the broker, with the
    /// batch's `ack_set`, so that a restart redelivers only the members not
    /// acked yet. `None` is on, as in Java since Pulsar 4.0 (PIP-391). A broker without
    /// `acknowledgmentAtBatchIndexLevelEnabled` ignores such acks, and then
    /// redelivers the whole batch, as it does with this off.
    // Java: ConsumerBuilder.enableBatchIndexAcknowledgment
    pub batch_index_ack: Option<bool>,
}

impl ConsumerOptions {
    /// within options, sets the priority level
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_priority_level(mut self, priority_level: i32) -> Self {
        self.priority_level = Some(priority_level);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = Some(durable);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn starting_on_message(mut self, message_id_data: MessageIdData) -> Self {
        self.start_message_id = Some(message_id_data);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn read_compacted(mut self, read_compacted: bool) -> Self {
        self.read_compacted = Some(read_compacted);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_initial_position(mut self, initial_position: InitialPosition) -> Self {
        self.initial_position = initial_position;
        self
    }

    /// Turns batch-index acks on or off; see `batch_index_ack`.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_batch_index_ack(mut self, on: bool) -> Self {
        self.batch_index_ack = Some(on);
        self
    }

    /// Makes a reader that starts at a message id read that message too.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_start_inclusive(mut self, inclusive: bool) -> Self {
        self.start_inclusive = inclusive;
        self
    }

    /// Starts a reader `rollback` back in time, rounded up to whole seconds:
    /// at the first message published since then.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_start_rollback(mut self, rollback: Duration) -> Self {
        self.start_rollback = Some(rollback);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_receiver_queue_size(mut self, size: u32) -> Self {
        // todo: support zero_queue_size consumer
        self.receiver_queue_size = Some(if size == 0 { 1000 } else { size });
        self
    }
}
