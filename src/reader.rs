use std::{collections::BTreeMap, pin::Pin};

use chrono::{DateTime, Utc};
use futures::{
    task::{Context, Poll},
    Future, Stream,
};
use url::Url;

use crate::{
    client::DeserializeMessage,
    consumer::{Consumer, ConsumerOptions, DeadLetterPolicy, Message},
    error::Error,
    executor::Executor,
    message::proto::{command_subscribe::SubType, MessageIdData},
};

/// Reads a topic, or every partition of a partitioned topic, from a start
/// position, through a non-durable subscription. It acknowledges each message
/// as the application takes it.
///
/// On a partitioned topic, partitions added later are found at each topic
/// refresh (30 s by default) and read from their beginning. A partition whose
/// consumer fails for good hands over its error and is dropped; the others
/// carry on.
pub struct Reader<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer: Consumer<T, Exe>,
    /// The topic the reader was built for, partitioned or not.
    pub(crate) topic: String,
}

impl<T: DeserializeMessage + 'static, Exe: Executor> Unpin for Reader<T, Exe> {}

impl<T: DeserializeMessage + 'static, Exe: Executor> Stream for Reader<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().consumer).poll_next(cx)
    }
}

impl<T: DeserializeMessage, Exe: Executor> Reader<T, Exe> {
    /// test that the connections to the Pulsar brokers are still valid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        self.consumer.check_connection().await
    }

    /// returns the topic this reader was built for
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    /// returns the broker URLs this reader is connected to, one per broker
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connections(&mut self) -> Result<Vec<Url>, Error> {
        self.consumer.connections().await
    }

    /// returns the consumer's configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ConsumerOptions {
        self.consumer.options()
    }

    /// returns the consumer's dead letter policy options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn dead_letter_policy(&self) -> Option<&DeadLetterPolicy> {
        self.consumer.dead_letter_policy()
    }

    /// returns the readers's subscription name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn subscription(&self) -> &str {
        self.consumer.subscription()
    }

    /// returns the reader's subscription type
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn sub_type(&self) -> SubType {
        self.consumer.sub_type()
    }

    /// returns the reader's batch size
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn batch_size(&self) -> Option<u32> {
        self.consumer.batch_size()
    }

    /// returns the reader's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader_name(&self) -> Option<&str> {
        self.consumer.consumer_name()
    }

    /// returns the reader's consumer ids, one per partition
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader_id(&self) -> Vec<u64> {
        self.consumer.consumer_id()
    }

    /// Restarts after `message_id`, or at the first message published at or
    /// after `timestamp`. A reader over a partitioned topic takes only a
    /// timestamp here; [`Reader::seek_to`] moves one partition to an id.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        if let Some(consumer) = self.consumer.single_mut() {
            return consumer.seek(message_id, timestamp).await;
        }
        match (message_id, timestamp) {
            (None, Some(timestamp)) => self.consumer.seek_to_time(timestamp).await,
            _ => Err(Error::Custom(
                "a reader over a partitioned topic seeks an id with seek_to".to_string(),
            )),
        }
    }

    /// Restarts one topic or partition after `message_id`, as
    /// [`Consumer::seek_to`] does.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek_to(&mut self, topic: &str, message_id: MessageIdData) -> Result<(), Error> {
        self.consumer.seek_to(topic, message_id).await
    }

    /// Restarts one topic or partition at `message_id`, which is read next,
    /// as [`Consumer::seek_to_inclusive`] does.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek_to_inclusive(
        &mut self,
        topic: &str,
        message_id: MessageIdData,
    ) -> Result<(), Error> {
        self.consumer.seek_to_inclusive(topic, message_id).await
    }

    /// Restarts every partition at the first message published at or after
    /// `publish_time_ms`, as [`Consumer::seek_to_time`] does.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek_to_time(&mut self, publish_time_ms: u64) -> Result<(), Error> {
        self.consumer.seek_to_time(publish_time_ms).await
    }

    /// returns the date of the last message reception
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumer.last_message_received()
    }

    /// The id of the last message published on the topic. A reader over a
    /// partitioned topic has one per partition: [`Reader::last_message_ids`].
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        match self.consumer.single_mut() {
            Some(consumer) => consumer.get_last_message_id().await,
            None => Err(Error::Custom(
                "a reader over a partitioned topic has one last id per partition: \
                 last_message_ids"
                    .to_string(),
            )),
        }
    }

    /// The id of the last message published on each partition, as
    /// [`Consumer::last_message_ids`] does.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn last_message_ids(&mut self) -> Result<BTreeMap<String, MessageIdData>, Error> {
        self.consumer.last_message_ids().await
    }

    /// Whether the topic, or any of its partitions, holds a message this
    /// reader has not read yet.
    // Java: Reader.hasMessageAvailable
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn has_message_available(&mut self) -> Result<bool, Error> {
        self.consumer.has_message_available().await
    }

    /// Whether each partition holds a message this reader has not read yet,
    /// as [`Consumer::messages_available`] answers it: by a future that
    /// borrows nothing of the reader, to await while reading on.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_available(
        &self,
    ) -> impl Future<Output = Result<BTreeMap<String, bool>, Error>> + Send + 'static {
        self.consumer.messages_available()
    }

    /// returns the current number of messages received
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.consumer.messages_received()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;

    use crate::{
        consumer::{DeadLetterPolicy, InitialPosition, Message},
        producer,
        proto::MessageIdData,
        reader::Reader,
        ConsumerOptions, DeserializeMessage, Error, Executor, Payload, Pulsar, SerializeMessage,
        SubType, TokioExecutor,
    };

    #[derive(Serialize, Deserialize)]
    struct TestData {
        data: String,
    }

    impl SerializeMessage for &TestData {
        fn serialize_message(input: Self) -> Result<producer::Message, Error> {
            let payload = serde_json::to_vec(&input).map_err(|e| Error::Custom(e.to_string()))?;
            Ok(producer::Message {
                payload,
                ..Default::default()
            })
        }
    }

    impl DeserializeMessage for TestData {
        type Output = Result<TestData, serde_json::Error>;

        fn deserialize_message(payload: &Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    #[tokio::test]
    async fn reader() {
        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("test_reader_{}", rand::random::<u16>());
        let dead_letter_policy = DeadLetterPolicy {
            max_redeliver_count: 1,
            dead_letter_topic: format!("{}_dead_letter", topic),
        };
        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();
        let mut reader: Reader<TestData, _> = client
            .reader()
            .with_topic(&topic)
            .with_consumer_name("test_reader")
            .with_subscription("test_reader_subscription")
            .with_dead_letter_policy(dead_letter_policy)
            .with_options(ConsumerOptions::default())
            .into_reader()
            .await
            .unwrap();
        assert!(reader.check_connection().await.is_ok());
        assert_eq!(reader.topic(), topic);

        let urls = reader.connections().await.unwrap();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].as_str(), addr);

        let option = reader.options();
        assert_eq!(option.initial_position, InitialPosition::Latest);

        let policy = reader.dead_letter_policy().unwrap();
        assert_eq!(policy.max_redeliver_count, 1);
        assert_eq!(policy.dead_letter_topic, format!("{}_dead_letter", topic));
        assert_eq!(reader.subscription(), "test_reader_subscription");
        assert_eq!(reader.sub_type(), SubType::Exclusive);
        assert_eq!(reader.batch_size(), None);
        assert_eq!(reader.reader_name().unwrap(), "test_reader");
        let reader_id = reader.reader_id();
        assert!(reader_id.iter().all(|id| *id > 0));

        let message = TestData {
            data: "test_reader_data".to_string(),
        };
        let message_count = 10;
        let mut lastest_message_id: [u64; 2] = [0, 0];
        for index in 0..message_count {
            let receipt = client.send(&topic, &message).await.unwrap().await.unwrap();
            let message_id = receipt.message_id.unwrap();
            println!(
                "producer sends done, message_id: {}:{}",
                message_id.ledger_id, message_id.entry_id
            );
            if index == message_count - 1 {
                lastest_message_id[0] = message_id.ledger_id;
                lastest_message_id[1] = message_id.entry_id;
            }
        }

        let mut seek_message_id: Option<MessageIdData> = None;
        let messages = reader_messages(&mut reader, message_count, 5000).await;
        assert!(messages.len() <= message_count);
        for (i, data) in messages.into_iter().enumerate() {
            let value = data.deserialize().unwrap();
            assert_eq!(value.data, "test_reader_data".to_string());
            if i <= message_count / 2 {
                seek_message_id = Some(data.message_id.id.clone());
            }
        }
        let time = reader.last_message_received().unwrap();
        assert!(time <= chrono::Utc::now());

        let last_message_id_data = reader.get_last_message_id().await.unwrap();
        println!("last message id: {:?}", last_message_id_data);
        assert_eq!(last_message_id_data.ledger_id, lastest_message_id[0]);
        assert_eq!(last_message_id_data.entry_id, lastest_message_id[1]);

        let received = reader.messages_received();
        assert!(received <= message_count as u64);

        // seek to half message
        reader.seek(seek_message_id, None).await.unwrap();
        let seek_message = reader_messages(&mut reader, message_count / 2, 5000).await;
        assert!(seek_message.len() <= message_count / 2);
    }

    async fn reader_messages(
        reader: &mut Reader<TestData, impl Executor>,
        max_num_messages: usize,
        receive_timeout_ms: u64,
    ) -> Vec<Message<TestData>> {
        let mut messages = Vec::new();
        loop {
            match timeout(Duration::from_millis(receive_timeout_ms), reader.next()).await {
                Ok(Some(msg)) => {
                    let msg = msg.unwrap();
                    messages.push(msg);
                    if messages.len() >= max_num_messages {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    info!("timed out waiting for reading messages: {}", e);
                    break;
                }
            }
        }
        messages
    }
}
