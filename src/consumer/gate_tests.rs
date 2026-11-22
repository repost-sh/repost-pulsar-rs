//! The gate of a Failover consumer, as the application and the broker see
//! it, against a broker that sends what the test tells it to. No real broker
//! dispatches out of turn on demand.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{channel::mpsc, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::{
    connection::messages,
    consumer::{message::Message as Received, ConsumerEvent, ConsumerItem, ConsumerOptions},
    message::{
        proto::{self, base_command::Type, command_lookup_topic_response::LookupType},
        BaseCommand, Codec, Message, Payload,
    },
    Consumer, Pulsar, SubType, TokioExecutor,
};

const TOPIC: &str = "persistent://public/default/gate";
/// The permits a test consumer grants, of which it returns two at a time.
const WINDOW: u32 = 4;
const WAIT: Duration = Duration::from_secs(10);

/// What the client sent the broker for its consumer.
#[derive(Debug, PartialEq)]
enum Sent {
    Subscribe,
    Flow(u32),
    Redeliver,
    Seek,
    Close,
}

/// The consumer the client last subscribed: its id, and its connection.
type Subscribed = Arc<Mutex<Option<(u64, mpsc::UnboundedSender<Message>)>>>;

/// A broker that answers what a consumer needs to subscribe, and sends it
/// only the frames the test asks for.
struct Broker {
    url: String,
    subscribed: Subscribed,
    sent: mpsc::UnboundedReceiver<Sent>,
}

impl Broker {
    /// Starts a broker at least as new as the client.
    async fn start() -> Broker {
        Broker::speaking(i32::MAX).await
    }

    /// Starts a broker that speaks up to `protocol_version`. As a real one,
    /// it answers a connect with the older of that and the client's.
    async fn speaking(protocol_version: i32) -> Broker {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("pulsar://{}", listener.local_addr().unwrap());
        let subscribed = Subscribed::default();
        let (sent_tx, sent) = mpsc::unbounded();
        tokio::spawn({
            let (url, subscribed) = (url.clone(), subscribed.clone());
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(serve(
                        stream,
                        url.clone(),
                        protocol_version,
                        subscribed.clone(),
                        sent_tx.clone(),
                    ));
                }
            }
        });
        Broker {
            url,
            subscribed,
            sent,
        }
    }

    async fn client(&self) -> Pulsar<TokioExecutor> {
        Pulsar::builder(&self.url, TokioExecutor)
            .build()
            .await
            .unwrap()
    }

    /// Waits for the client's next subscribe.
    async fn subscribed(&mut self) {
        while self.sent().await != Sent::Subscribe {}
    }

    /// Waits for what the client sends for a seek: the seek, then the close
    /// and the subscribe that follow it, and the window.
    async fn resubscribed_after_a_seek(&mut self) {
        for expected in [Sent::Seek, Sent::Close, Sent::Subscribe, Sent::Flow(WINDOW)] {
            assert_eq!(self.sent().await, expected);
        }
    }

    /// The next thing the client sent for its consumer.
    async fn sent(&mut self) -> Sent {
        tokio::time::timeout(WAIT, self.sent.next())
            .await
            .expect("the client sends something")
            .expect("the broker runs")
    }

    /// Asserts that the client sends nothing for `duration`.
    async fn quiet_for(&mut self, duration: Duration) {
        let sent = tokio::time::timeout(duration, self.sent.next()).await;
        assert!(sent.is_err(), "the client sent {sent:?}");
    }

    /// Sends the subscribed consumer the frame `frame` makes for its id.
    fn send(&self, frame: impl FnOnce(u64) -> Message) {
        let subscribed = self.subscribed.lock().unwrap();
        let (consumer_id, connection) = subscribed.as_ref().expect("a subscribed consumer");
        connection
            .unbounded_send(frame(*consumer_id))
            .expect("the consumer's connection");
    }

    /// Dispatches entry `entry`, a batch of `members` messages if given.
    fn dispatch(&self, entry: u64, members: Option<i32>) {
        let payload = Payload {
            metadata: proto::MessageMetadata {
                producer_name: "test".to_owned(),
                num_messages_in_batch: members,
                ..Default::default()
            },
            data: entry.to_string().into_bytes(),
        };
        self.send(|consumer_id| Message {
            command: BaseCommand {
                r#type: Type::Message as i32,
                message: Some(proto::CommandMessage {
                    consumer_id,
                    message_id: proto::MessageIdData {
                        ledger_id: 1,
                        entry_id: entry,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: Some(payload),
        });
    }

    /// Tells the consumer whether it is the active one.
    fn tell(&self, active: bool) {
        self.send(|consumer_id| Message {
            command: BaseCommand {
                r#type: Type::ActiveConsumerChange as i32,
                active_consumer_change: Some(proto::CommandActiveConsumerChange {
                    consumer_id,
                    is_active: Some(active),
                }),
                ..Default::default()
            },
            payload: None,
        });
    }

    /// Closes the consumer, as a broker that unloads its topic does.
    fn close_consumer(&self) {
        // no request of the client's has this id
        self.send(|consumer_id| messages::close_consumer(consumer_id, u64::MAX));
    }
}

fn success(request_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::Success as i32,
        success: Some(proto::CommandSuccess {
            request_id,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Answers one connection of the client until it closes.
async fn serve(
    stream: TcpStream,
    url: String,
    protocol_version: i32,
    subscribed: Subscribed,
    sent: mpsc::UnboundedSender<Sent>,
) {
    let mut frames = Framed::new(stream, Codec);
    let (to_consumer, mut for_consumer) = mpsc::unbounded();
    loop {
        let received = tokio::select! {
            received = frames.next() => received,
            frame = for_consumer.next() => {
                frames.send(frame.expect("this task holds a sender")).await.unwrap();
                continue;
            }
        };
        let Some(Ok(Message { command, .. })) = received else {
            return;
        };
        let answer = if let Some(connect) = command.connect {
            BaseCommand {
                r#type: Type::Connected as i32,
                connected: Some(proto::CommandConnected {
                    server_version: "scripted".to_owned(),
                    protocol_version: connect
                        .protocol_version
                        .map(|client| client.min(protocol_version)),
                    ..Default::default()
                }),
                ..Default::default()
            }
        } else if command.ping.is_some() {
            messages::pong().command
        } else if let Some(metadata) = command.partition_metadata {
            BaseCommand {
                r#type: Type::PartitionedMetadataResponse as i32,
                partition_metadata_response: Some(proto::CommandPartitionedTopicMetadataResponse {
                    partitions: Some(0),
                    request_id: metadata.request_id,
                    response: Some(0),
                    ..Default::default()
                }),
                ..Default::default()
            }
        } else if let Some(lookup) = command.lookup_topic {
            BaseCommand {
                r#type: Type::LookupResponse as i32,
                lookup_topic_response: Some(proto::CommandLookupTopicResponse {
                    broker_service_url: Some(url.clone()),
                    response: Some(LookupType::Connect as i32),
                    request_id: lookup.request_id,
                    authoritative: Some(true),
                    proxy_through_service_url: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }
        } else if let Some(subscribe) = command.subscribe {
            *subscribed.lock().unwrap() = Some((subscribe.consumer_id, to_consumer.clone()));
            let _ = sent.unbounded_send(Sent::Subscribe);
            success(subscribe.request_id)
        } else if let Some(flow) = command.flow {
            let _ = sent.unbounded_send(Sent::Flow(flow.message_permits));
            continue;
        } else if command.redeliver_unacknowledged_messages.is_some() {
            let _ = sent.unbounded_send(Sent::Redeliver);
            continue;
        } else if let Some(seek) = command.seek {
            let _ = sent.unbounded_send(Sent::Seek);
            success(seek.request_id)
        } else if let Some(close) = command.close_consumer {
            let _ = sent.unbounded_send(Sent::Close);
            success(close.request_id)
        } else {
            continue;
        };
        frames
            .send(Message {
                command: answer,
                payload: None,
            })
            .await
            .unwrap();
    }
}

async fn consumer(
    broker: &Broker,
    topic: &str,
    sub_type: SubType,
) -> Consumer<String, TokioExecutor> {
    broker
        .client()
        .await
        .consumer()
        .with_topic(topic)
        .with_subscription("gate")
        .with_subscription_type(sub_type)
        .with_options(ConsumerOptions {
            receiver_queue_size: Some(WINDOW),
            ..Default::default()
        })
        .build()
        .await
        .unwrap()
}

/// A Failover consumer, once it subscribed and granted its window.
async fn failover(broker: &mut Broker) -> Consumer<String, TokioExecutor> {
    let consumer = consumer(broker, TOPIC, SubType::Failover).await;
    assert_eq!(broker.sent().await, Sent::Subscribe);
    assert_eq!(broker.sent().await, Sent::Flow(WINDOW));
    consumer
}

async fn next(consumer: &mut Consumer<String, TokioExecutor>) -> ConsumerItem<String> {
    tokio::time::timeout(WAIT, consumer.next_item())
        .await
        .expect("an item")
        .expect("the consumer goes on")
        .expect("no error")
}

async fn next_message(consumer: &mut Consumer<String, TokioExecutor>) -> Received<String> {
    match next(consumer).await {
        ConsumerItem::Message(message) => message,
        ConsumerItem::Event(event) => panic!("a message comes next, not {event:?}"),
    }
}

/// The entry of the message that comes next.
async fn next_entry(consumer: &mut Consumer<String, TokioExecutor>) -> u64 {
    next_message(consumer).await.message_id().entry_id
}

async fn next_event(consumer: &mut Consumer<String, TokioExecutor>) -> ConsumerEvent {
    match next(consumer).await {
        ConsumerItem::Event(event) => event,
        ConsumerItem::Message(message) => panic!(
            "an event comes next, not entry {}",
            message.message_id().entry_id
        ),
    }
}

fn active() -> ConsumerEvent {
    ConsumerEvent::BecameActive {
        topic: TOPIC.to_owned(),
    }
}

fn inactive() -> ConsumerEvent {
    ConsumerEvent::BecameInactive {
        topic: TOPIC.to_owned(),
    }
}

#[tokio::test]
async fn what_arrives_before_the_broker_says_active_is_dropped_and_its_permits_returned() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    // a batch of three and a single message: the whole window
    broker.dispatch(1, Some(3));
    broker.dispatch(2, None);
    broker.tell(true);
    broker.dispatch(3, None);

    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 3);
    assert_eq!(consumer.messages_withheld(), 4);
    // nothing went back while the gate was closed, and then all of it
    assert_eq!(broker.sent().await, Sent::Flow(4));
}

#[tokio::test]
async fn a_broker_that_stays_quiet_after_the_gate_opened_is_asked_to_send_again() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    // the read that brought this entry now waits at the end of the topic
    broker.dispatch(1, None);
    broker.dispatch(2, None);
    broker.tell(true);
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(broker.sent().await, Sent::Flow(2));
    assert_eq!(broker.sent().await, Sent::Redeliver);

    // once, and not after the broker says active with nothing withheld
    broker.dispatch(1, None);
    assert_eq!(next_entry(&mut consumer).await, 1);
    broker.tell(false);
    broker.tell(true);
    assert_eq!(next_event(&mut consumer).await, inactive());
    assert_eq!(next_event(&mut consumer).await, active());
    broker.quiet_for(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn a_broker_that_sends_after_the_gate_opened_is_not_asked_again() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    broker.dispatch(1, None);
    broker.dispatch(2, None);
    broker.tell(true);
    broker.dispatch(1, None);
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 1);
    assert_eq!(broker.sent().await, Sent::Flow(2));
    broker.quiet_for(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn the_gate_closes_when_the_broker_says_not_active() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    broker.tell(true);
    broker.dispatch(1, None);
    broker.tell(false);
    broker.dispatch(2, None);
    broker.tell(true);
    broker.dispatch(3, None);

    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 1);
    assert_eq!(next_event(&mut consumer).await, inactive());
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 3);
    assert_eq!(consumer.messages_withheld(), 1);
}

#[tokio::test]
async fn a_second_active_after_messages_tells_the_application_it_lost_the_partition_and_has_it_back(
) {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    // twice with nothing in between: the application has nothing to redo
    broker.tell(true);
    broker.tell(true);
    broker.dispatch(1, None);
    // the broker rewound, as its delayed task does after the partition went
    // to another consumer and came back
    broker.tell(true);
    broker.dispatch(1, None);

    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 1);
    assert_eq!(next_event(&mut consumer).await, inactive());
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 1);
    assert_eq!(consumer.messages_withheld(), 0);
}

#[tokio::test]
async fn a_consumer_the_broker_said_is_not_active_asks_for_no_redelivery() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    broker.tell(true);
    broker.dispatch(1, None);
    broker.tell(false);
    assert_eq!(next_event(&mut consumer).await, active());
    let message = next_message(&mut consumer).await;
    assert_eq!(next_event(&mut consumer).await, inactive());
    consumer.nack(&message).await.unwrap();

    broker.tell(true);
    assert_eq!(next_event(&mut consumer).await, active());
    consumer.nack(&message).await.unwrap();
    // the second one only
    assert_eq!(broker.sent().await, Sent::Redeliver);
    broker.quiet_for(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn the_redelivery_timer_waits_while_the_broker_says_not_active() {
    let mut broker = Broker::start().await;
    let mut consumer: Consumer<String, TokioExecutor> = broker
        .client()
        .await
        .consumer()
        .with_topic(TOPIC)
        .with_subscription("gate")
        .with_subscription_type(SubType::Failover)
        .with_unacked_message_resend_delay(Some(Duration::from_millis(200)))
        .build()
        .await
        .unwrap();
    broker.subscribed().await;
    assert!(matches!(broker.sent().await, Sent::Flow(_)));

    broker.tell(true);
    broker.dispatch(1, None);
    broker.tell(false);
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 1);
    assert_eq!(next_event(&mut consumer).await, inactive());
    // well past the delay of the message taken and not acked
    broker.quiet_for(Duration::from_millis(1500)).await;

    broker.tell(true);
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(broker.sent().await, Sent::Redeliver);
}

#[tokio::test]
async fn the_gate_closes_when_the_consumer_reconnects() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    broker.tell(true);
    broker.dispatch(1, None);
    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 1);

    broker.close_consumer();
    assert_eq!(next_event(&mut consumer).await, inactive());
    assert_eq!(broker.sent().await, Sent::Close);
    assert_eq!(broker.sent().await, Sent::Subscribe);
    assert_eq!(broker.sent().await, Sent::Flow(WINDOW));
    // the broker said active on the connection before this one only
    broker.dispatch(2, None);
    broker.tell(true);
    broker.dispatch(3, None);

    assert_eq!(next_event(&mut consumer).await, active());
    assert_eq!(next_entry(&mut consumer).await, 3);
    assert_eq!(consumer.messages_withheld(), 1);
}

#[tokio::test]
async fn the_gate_closes_for_the_consumers_own_seek_and_opens_without_an_event() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;

    broker.tell(true);
    broker.dispatch(1, None);
    assert_eq!(next_event(&mut consumer).await, active());
    let first = next_message(&mut consumer).await.message_id().clone();

    consumer.seek_to(TOPIC, first).await.unwrap();
    broker.resubscribed_after_a_seek().await;
    // an entry of a read that was under way when the seek moved the cursor
    broker.dispatch(7, None);
    broker.tell(true);
    broker.dispatch(2, None);

    assert_eq!(next_entry(&mut consumer).await, 2);
    assert_eq!(consumer.messages_withheld(), 1);
}

/// What an application that seeks each partition it gains did.
#[derive(Debug, PartialEq)]
enum Did {
    Sought,
    Took(u64),
    Lost,
}

async fn next_did(did: &mut mpsc::UnboundedReceiver<Did>) -> Did {
    tokio::time::timeout(WAIT, did.next())
        .await
        .expect("the application acts")
        .expect("the application runs")
}

#[tokio::test]
async fn an_application_that_seeks_on_every_activation_does_not_loop() {
    let mut broker = Broker::start().await;
    let mut consumer = failover(&mut broker).await;
    let (did_tx, mut did) = mpsc::unbounded();
    tokio::spawn(async move {
        while let Some(Ok(item)) = consumer.next_item().await {
            let done = match item {
                ConsumerItem::Event(ConsumerEvent::BecameActive { topic }) => {
                    let checkpoint = proto::MessageIdData {
                        ledger_id: 1,
                        entry_id: 0,
                        ..Default::default()
                    };
                    consumer.seek_to(&topic, checkpoint).await.unwrap();
                    Did::Sought
                }
                ConsumerItem::Event(ConsumerEvent::BecameInactive { .. }) => Did::Lost,
                ConsumerItem::Message(message) => Did::Took(message.message_id().entry_id),
            };
            if did_tx.unbounded_send(done).is_err() {
                return;
            }
        }
    });

    broker.tell(true);
    broker.dispatch(1, None);
    assert_eq!(next_did(&mut did).await, Did::Sought);
    for expected in [Sent::Seek, Sent::Close, Sent::Subscribe] {
        assert_eq!(broker.sent().await, expected);
    }
    // entry 1 races the application's seek: queued before it, it holds back
    // its permit until the application drops it as stale
    let flow = broker.sent().await;
    assert!(
        [Sent::Flow(WINDOW), Sent::Flow(WINDOW - 1)].contains(&flow),
        "{flow:?}"
    );

    // on the new connection, once with nothing before: no event, so no seek
    broker.tell(true);
    broker.dispatch(1, None);
    assert_eq!(next_did(&mut did).await, Did::Took(1));
    if flow == Sent::Flow(WINDOW - 1) {
        // the stale entry's permit and this one's make half the window
        assert_eq!(broker.sent().await, Sent::Flow(WINDOW / 2));
    }
    // the broker rewound for this consumer after the partition went to
    // another and back: it seeks once more
    broker.tell(true);
    assert_eq!(next_did(&mut did).await, Did::Lost);
    assert_eq!(next_did(&mut did).await, Did::Sought);
    broker.resubscribed_after_a_seek().await;

    broker.tell(true);
    broker.dispatch(1, None);
    assert_eq!(next_did(&mut did).await, Did::Took(1));
    broker.quiet_for(Duration::from_secs(3)).await;
}

#[tokio::test]
async fn subscriptions_the_broker_names_no_active_consumer_for_have_no_gate() {
    let mut broker = Broker::start().await;
    for sub_type in [SubType::Exclusive, SubType::Shared, SubType::KeyShared] {
        let mut consumer = consumer(&broker, TOPIC, sub_type).await;
        broker.subscribed().await;
        broker.dispatch(1, None);
        assert_eq!(next_entry(&mut consumer).await, 1, "{sub_type:?}");
        assert_eq!(consumer.messages_withheld(), 0);
    }

    let mut reader = broker
        .client()
        .await
        .reader()
        .with_topic(TOPIC)
        .into_reader::<String>()
        .await
        .unwrap();
    broker.subscribed().await;
    broker.dispatch(1, None);
    let message = tokio::time::timeout(WAIT, reader.next())
        .await
        .expect("a message")
        .expect("the reader goes on")
        .expect("no error");
    assert_eq!(message.message_id().entry_id, 1);

    // brokers before 5.0 never tell the active consumer of such a topic
    let topic = "non-persistent://public/default/gate";
    let mut consumer = consumer(&broker, topic, SubType::Failover).await;
    broker.subscribed().await;
    broker.dispatch(1, None);
    assert_eq!(next_entry(&mut consumer).await, 1);
}

#[tokio::test]
async fn a_broker_too_old_to_name_the_active_consumer_has_no_gate() {
    let mut broker = Broker::speaking(11).await;
    let mut consumer = failover(&mut broker).await;
    broker.dispatch(1, None);
    assert_eq!(next_entry(&mut consumer).await, 1);
    assert_eq!(consumer.messages_withheld(), 0);
}
