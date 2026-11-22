use std::marker::PhantomData;

use base64::Engine;

use crate::{
    consumer::data::MessageData,
    message::proto::{MessageIdData, MessageMetadata},
    DeserializeMessage, Error, Payload,
};

/// Which consumer of a Failover subscription the broker sends a topic or
/// partition to. For each partition, events alternate, starting with
/// [`ConsumerEvent::BecameActive`].
// Java: ConsumerEventListener becameActive / becameInactive
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumerEvent {
    /// This consumer receives `topic` from now on, a partition name such as
    /// `persistent://public/default/orders-partition-3`. The broker restarts
    /// it at the last acknowledged message.
    ///
    /// What an application can rely on: on a durable subscription of a
    /// persistent topic, every message of `topic` comes after this event.
    /// Before the broker dispatches to a new owner, it rewinds to the last
    /// acknowledged message and tells the consumers which of them is
    /// active. The client drops what the broker
    /// sends a consumer before it has told it so since the consumer last
    /// subscribed, and the broker sends that again once it does. Every
    /// re-subscribe, the consumer's own seek included, starts over, so after
    /// a [`ConsumerEvent::BecameInactive`] no message comes until the next
    /// `BecameActive`. [`crate::Consumer::messages_withheld`] counts what the
    /// client dropped. Other subscription types, readers, non-persistent
    /// topics and brokers before protocol version 12 get every message the
    /// broker sends, as before.
    ///
    /// An application that keeps its own checkpoint can seek the partition
    /// to it on this event, if each consumer of the subscription has its own
    /// name: a seek closes only the active consumer, and the broker picks
    /// the owner by name order, so the seeker stays active with no new
    /// event, unless the broker gives the partition to another consumer
    /// meanwhile. Equal names are untested. After such a seek the partition
    /// resumes once the broker has said again that this consumer is active,
    /// about its failover delay later
    /// (`activeConsumerFailoverDelayTimeMillis`, 1 s by default).
    ///
    /// What it must still check: two cases remain in which the broker
    /// delivers a message ahead of the last acknowledged one and the client
    /// cannot tell. A consumer that stays connected while its partition goes
    /// to another consumer and back within the failover delay hears nothing
    /// until the delay ends.
    /// What the broker sends it meanwhile can start past the entries it sent
    /// the other consumer. When the broker then says a second time that the
    /// consumer is active, the application gets a `BecameInactive` and this
    /// event again, if it was handed a message in between: those messages
    /// come again, and may have skipped some. The other case is a read the
    /// broker started for this consumer before it told it, and that was
    /// still under way when the broker rewound: its entries arrive after
    /// this event, and the broker goes on from the last of them, so the
    /// messages between the last acknowledged one and them come only after
    /// the next rewind, such as a seek. An application that must not skip
    /// messages therefore still checks that each message follows the one it
    /// processed before, and seeks again when one does not, as
    /// `tests/integration/failover_seek.rs` does. The first message after a
    /// seek to the start of a partition has nothing before it to check
    /// against: it is the partition's first message unless that read was
    /// under way, which `tests/integration/failover_start.rs` tests.
    BecameActive { topic: String },
    /// This consumer may no longer be the one that receives `topic`: the
    /// broker moved it to another consumer, the consumer reconnected for a
    /// reason other than its own seek, or the broker restarted the
    /// partition at the last acknowledged message for a consumer that was
    /// active already. Messages of `topic` handed over before this event
    /// may be redelivered, here or elsewhere.
    BecameInactive { topic: String },
}

/// What [`crate::Consumer::next_item`] hands over: a message, or a Failover
/// event, in the order the broker sent them for each partition.
#[derive(Debug)]
pub enum ConsumerItem<T> {
    Message(Message<T>),
    Event(ConsumerEvent),
}

/// a message received by a consumer
///
/// it is generic over the type it can be deserialized to
#[derive(Debug)]
pub struct Message<T> {
    /// origin topic of the message
    pub topic: String,
    /// contains the message's data and other metadata
    pub payload: Payload,
    /// contains the message's id and batch size data
    pub message_id: MessageData,
    pub(super) _phantom: PhantomData<T>,
}

impl<T> Message<T> {
    pub fn new(topic: &str, message_id: MessageData, payload: Payload) -> Self {
        Message {
            topic: topic.to_string(),
            message_id,
            payload,
            _phantom: PhantomData,
        }
    }

    /// Pulsar metadata for the message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn metadata(&self) -> &MessageMetadata {
        &self.payload.metadata
    }

    /// Get Pulsar message id for the message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn message_id(&self) -> &MessageIdData {
        &self.message_id.id
    }

    /// The id to ack this message by, with `Consumer::ack_with_id` and the
    /// like: [`message_id`](Self::message_id) with the size of its batch, so
    /// an ack covers this member of a batch only. `message_id` leaves the
    /// size out, so that it equals the ids the broker reports.
    // Java: BatchMessageIdImpl carries batchSize
    pub fn ack_id(&self) -> MessageIdData {
        MessageIdData {
            batch_size: self.message_id.batch_size,
            ..self.message_id.id.clone()
        }
    }

    /// Get message key (partition key string as stored in metadata)
    ///
    /// When [`has_base64_encoded_key`](Self::has_base64_encoded_key) is true, this is the
    /// Base64 encoding of the raw key bytes.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn key(&self) -> Option<String> {
        self.payload.metadata.partition_key.clone()
    }

    /// Whether the partition key is a Base64 encoding of raw key bytes.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn has_base64_encoded_key(&self) -> bool {
        self.payload.metadata.partition_key_b64_encoded()
    }

    /// Get the raw partition key bytes.
    ///
    /// If the key is Base64-encoded, it is decoded; otherwise the UTF-8 bytes of
    /// the key string are returned. Returns `Ok(None)` when there is no key.
    ///
    /// Returns an error when the key is marked Base64-encoded but decoding fails.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn key_bytes(&self) -> Result<Option<Vec<u8>>, Error> {
        match self.payload.metadata.partition_key.as_ref() {
            None => Ok(None),
            Some(key) if !self.has_base64_encoded_key() => Ok(Some(key.as_bytes().to_vec())),
            Some(key) => match base64::engine::general_purpose::STANDARD.decode(key) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) => Err(Error::Custom(format!(
                    "failed to decode Base64 partition key on topic {} message_id={:?}: {e}",
                    self.topic,
                    self.message_id()
                ))),
            },
        }
    }
}

impl<T: DeserializeMessage> Message<T> {
    /// directly deserialize a message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn deserialize(&self) -> T::Output {
        T::deserialize_message(&self.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::data::MessageData;
    use crate::message::proto::MessageIdData;

    fn message_with_key(partition_key: Option<String>, b64: Option<bool>) -> Message<()> {
        Message::new(
            "persistent://public/default/topic",
            MessageData {
                id: MessageIdData {
                    ledger_id: 1,
                    entry_id: 2,
                    ..Default::default()
                },
                batch_size: None,
            },
            Payload {
                metadata: MessageMetadata {
                    producer_name: "test".into(),
                    sequence_id: 0,
                    publish_time: 0,
                    partition_key,
                    partition_key_b64_encoded: b64,
                    ..Default::default()
                },
                data: vec![],
            },
        )
    }

    #[test]
    fn key_bytes_decodes_when_flag_set() {
        let raw = b"\x00\x01binary";
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        let msg = message_with_key(Some(encoded), Some(true));
        assert!(msg.has_base64_encoded_key());
        assert_eq!(msg.key_bytes().unwrap().as_deref(), Some(raw.as_slice()));
    }

    #[test]
    fn key_bytes_uses_utf8_when_flag_unset() {
        let msg = message_with_key(Some("plain-key".into()), Some(false));
        assert!(!msg.has_base64_encoded_key());
        assert_eq!(
            msg.key_bytes().unwrap().as_deref(),
            Some(b"plain-key".as_slice())
        );
    }

    #[test]
    fn key_bytes_none_without_key() {
        let msg = message_with_key(None, None);
        assert!(!msg.has_base64_encoded_key());
        assert!(msg.key_bytes().unwrap().is_none());
    }

    #[test]
    fn key_bytes_errors_on_invalid_base64() {
        let msg = message_with_key(Some("not!!valid".into()), Some(true));
        assert!(msg.has_base64_encoded_key());
        let err = msg.key_bytes().unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("persistent://public/default/topic"),
            "error should include topic: {err_msg}"
        );
        assert!(
            err_msg.contains("message_id="),
            "error should include message id: {err_msg}"
        );
    }
}
