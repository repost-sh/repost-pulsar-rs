use crate::{
    consumer::options::ConsumerOptions,
    message::{proto::command_subscribe::SubType, Message as RawMessage},
};

/// The first protocol version in which a broker tells Failover consumers
/// which of them is active.
// PulsarApi.proto: v12, "Added CommandActiveConsumerChange"
const ANNOUNCES_ACTIVE_CONSUMER: i32 = 12;

/// Ticks after an opening that withheld messages until the engine asks the
/// broker to send again: the second, so that the broker has had a whole
/// second to send on its own.
const ASK_AGAIN_AFTER: u8 = 2;

/// Whether a Failover consumer hands the application what the broker sends
/// it for its topic or partition.
///
/// Before the broker dispatches to a new active consumer, it rewinds to the
/// subscription's position and tells the consumers which of them is active.
/// What it dispatches outside that order is not at that position: what it
/// goes on sending a consumer that a seek has closed, under the id this
/// client subscribes with again, and what it reads on for the next consumer
/// while the notification is still pending. So the gate lets through only
/// what arrives while the last thing the broker said on this connection is
/// that this consumer is active. The broker sends the rest again once it
/// says so.
// Broker: PersistentDispatcherSingleActiveConsumer.scheduleReadOnActiveConsumer
// rewinds, notifies, then reads. The write listener of dispatchEntriesToConsumer
// reads on for whoever is active without waiting for that, and
// AbstractDispatcherSingleActiveConsumer.removeConsumer keeps the consumer a
// cursor reset closed as the active one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    /// The broker names no active consumer here, so every message passes.
    Unguarded,
    /// The broker has not said on this connection that this consumer is
    /// active, or has said since that it is not. `withheld` permits were
    /// spent on messages dropped meanwhile.
    Closed { withheld: u32 },
    /// The broker said this consumer is active.
    Open {
        /// Whether a message passed since the broker last said so.
        handed: bool,
        /// Ticks until the engine asks the broker to send from the
        /// subscription's position again, unless a message comes first;
        /// zero when it will not ask.
        ///
        /// Set when the gate opens after it withheld messages. The read that
        /// brought them can still be waiting at the end of the topic, and
        /// then the broker reads neither for its notification nor for a
        /// flow, and sends what the gate withheld only with the next
        /// publish.
        // Broker: scheduleReadOnActiveConsumer cancels a pending read when it
        // schedules the change, not when its delayed task runs. A read that
        // the write listener of dispatchEntriesToConsumer starts during the
        // failover delay stays parked at the end of the topic, so the task's
        // readMoreEntries skips it for havePendingRead, and
        // internalConsumerFlow ignores a flow while it is pending.
        // internalRedeliverUnacknowledgedMessages cancels it, rewinds and
        // reads.
        ask_again_in: u8,
    },
}

/// What a word from the broker about the active consumer did to a [`Gate`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Told {
    /// It opened. The broker now sends from the subscription's position,
    /// and is owed the permits of what was withheld.
    Opened { withheld: u32 },
    /// It was open already: the broker rewound to the subscription's
    /// position once more. `handed` says whether a message passed since it
    /// last said so.
    ActiveAgain { handed: bool },
    /// Anything else, which the engine passes on to the application as the
    /// broker said it.
    Other,
}

impl Gate {
    /// The gate of a subscription on a connection the broker has said
    /// nothing on yet. Only a durable Failover subscription of a persistent
    /// topic has one: there alone every broker that speaks
    /// `broker_protocol_version` names the active consumer before it
    /// dispatches to it. Brokers before 5.0 never tell the active consumer
    /// of a non-persistent topic, which has no position to rewind to.
    pub fn new(
        sub_type: SubType,
        options: &ConsumerOptions,
        topic: &str,
        broker_protocol_version: i32,
    ) -> Gate {
        let guarded = sub_type == SubType::Failover
            && options.durable != Some(false)
            && !topic.starts_with("non-persistent://")
            && broker_protocol_version >= ANNOUNCES_ACTIVE_CONSUMER;
        if guarded {
            Gate::Closed { withheld: 0 }
        } else {
            Gate::Unguarded
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, Gate::Closed { .. })
    }

    /// Counts the `permits` of a message dropped at the closed gate.
    pub fn withhold(&mut self, permits: u32) {
        if let Gate::Closed { withheld } = self {
            *withheld += permits;
        }
    }

    /// A message passed the gate to the application.
    pub fn passed(&mut self) {
        if let Gate::Open {
            handed,
            ask_again_in,
        } = self
        {
            *handed = true;
            *ask_again_in = 0;
        }
    }

    /// A tick passed. True once the broker is to be asked to send again.
    pub fn tick(&mut self) -> bool {
        let Gate::Open { ask_again_in, .. } = self else {
            return false;
        };
        if *ask_again_in == 0 {
            return false;
        }
        *ask_again_in -= 1;
        *ask_again_in == 0
    }

    /// The broker said whether this consumer is the `active` one.
    pub fn told(&mut self, active: bool) -> Told {
        match (&mut *self, active) {
            (Gate::Unguarded, _) | (Gate::Closed { .. }, false) => Told::Other,
            (Gate::Closed { withheld }, true) => {
                let withheld = *withheld;
                *self = Gate::Open {
                    handed: false,
                    ask_again_in: if withheld > 0 { ASK_AGAIN_AFTER } else { 0 },
                };
                Told::Opened { withheld }
            }
            (Gate::Open { handed, .. }, true) => Told::ActiveAgain {
                handed: std::mem::take(handed),
            },
            (Gate::Open { .. }, false) => {
                *self = Gate::Closed { withheld: 0 };
                Told::Other
            }
        }
    }
}

/// The permits the broker spent on `frame`: none for a frame without a
/// message, else one per member of its batch that the `ack_set` does not
/// mark acked.
// Broker: Consumer.sendMessages charges totalMessages less the acked members
pub(crate) fn charged(frame: &RawMessage) -> u32 {
    let Some(message) = &frame.command.message else {
        return 0;
    };
    let members = frame
        .payload
        .as_ref()
        .and_then(|payload| payload.metadata.num_messages_in_batch)
        .unwrap_or(1)
        .max(1) as u32;
    if message.ack_set.is_empty() {
        return members;
    }
    // a set bit is a member still unacked
    let unacked: u32 = message
        .ack_set
        .iter()
        .map(|word| (*word as u64).count_ones())
        .sum();
    unacked.min(members)
}

#[cfg(test)]
mod tests {
    use super::{charged, Gate, Told};
    use crate::{
        consumer::options::ConsumerOptions,
        message::{
            proto::{command_subscribe::SubType, CommandMessage, MessageMetadata},
            BaseCommand, Message as RawMessage,
        },
        Payload,
    };

    const PARTITION: &str = "persistent://public/default/orders-partition-3";

    fn failover() -> Gate {
        Gate::new(
            SubType::Failover,
            &ConsumerOptions::default(),
            PARTITION,
            19,
        )
    }

    #[test]
    fn an_opening_that_withheld_asks_again_on_the_second_quiet_tick_only() {
        let mut gate = failover();
        gate.withhold(2);
        assert_eq!(gate.told(true), Told::Opened { withheld: 2 });
        assert!(!gate.tick());
        assert!(gate.tick());
        assert!(!gate.tick());
        assert!(!gate.tick());
    }

    #[test]
    fn a_message_before_the_second_tick_cancels_the_ask() {
        let mut gate = failover();
        gate.withhold(1);
        gate.told(true);
        assert!(!gate.tick());
        gate.passed();
        assert!(!gate.tick());
        assert!(!gate.tick());
    }

    #[test]
    fn an_opening_that_withheld_nothing_never_asks() {
        let mut gate = failover();
        gate.told(true);
        assert!(!gate.tick());
        assert!(!gate.tick());
        // nor does a closed gate, whatever it withheld before
        gate.withhold(1);
        gate.told(false);
        gate.withhold(1);
        assert!(!gate.tick());
        assert!(!gate.tick());
    }

    #[test]
    fn a_second_active_says_whether_a_message_passed_since_the_last() {
        let mut gate = failover();
        gate.told(true);
        assert_eq!(gate.told(true), Told::ActiveAgain { handed: false });
        gate.passed();
        gate.passed();
        assert_eq!(gate.told(true), Told::ActiveAgain { handed: true });
        assert_eq!(gate.told(true), Told::ActiveAgain { handed: false });
        // a reopened gate starts with nothing handed
        gate.passed();
        gate.told(false);
        gate.told(true);
        assert_eq!(gate.told(true), Told::ActiveAgain { handed: false });
    }

    #[test]
    fn only_a_durable_failover_subscription_of_a_persistent_topic_has_a_gate() {
        let durable = ConsumerOptions::default();
        let reader = ConsumerOptions {
            durable: Some(false),
            ..Default::default()
        };
        for sub_type in [SubType::Exclusive, SubType::Shared, SubType::KeyShared] {
            assert_eq!(
                Gate::new(sub_type, &durable, PARTITION, 19),
                Gate::Unguarded
            );
        }
        assert_eq!(
            Gate::new(SubType::Exclusive, &reader, PARTITION, 19),
            Gate::Unguarded
        );
        assert_eq!(
            Gate::new(SubType::Failover, &reader, PARTITION, 19),
            Gate::Unguarded
        );
        assert_eq!(
            Gate::new(
                SubType::Failover,
                &durable,
                "non-persistent://public/default/orders",
                19
            ),
            Gate::Unguarded
        );
        // a broker that never names the active consumer
        assert_eq!(
            Gate::new(SubType::Failover, &durable, PARTITION, 11),
            Gate::Unguarded
        );
        assert!(Gate::new(SubType::Failover, &durable, "orders", 12).is_closed());
    }

    fn frame(ack_set: Vec<i64>, members: Option<Option<i32>>) -> RawMessage {
        RawMessage {
            command: BaseCommand {
                message: Some(CommandMessage {
                    ack_set,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: members.map(|members| Payload {
                metadata: MessageMetadata {
                    num_messages_in_batch: members,
                    ..Default::default()
                },
                data: Vec::new(),
            }),
        }
    }

    #[test]
    fn a_frame_counts_as_the_broker_charged_it() {
        assert_eq!(charged(&frame(Vec::new(), Some(None))), 1);
        assert_eq!(charged(&frame(Vec::new(), Some(Some(10)))), 10);
        assert_eq!(charged(&frame(Vec::new(), None)), 1);
        // members 0 to 6 acked before: the broker charged the other three
        assert_eq!(charged(&frame(vec![0b11_1000_0000], Some(Some(10)))), 3);
        let notification = RawMessage {
            command: BaseCommand::default(),
            payload: None,
        };
        assert_eq!(charged(&notification), 0);
    }
}
