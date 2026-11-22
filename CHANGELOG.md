# Changelog

## Unreleased

### Consumer

- On a durable Failover subscription of a persistent topic, no message of
  a topic or partition reaches the application before the broker has said
  on the current connection that the consumer is active, so the first one
  comes after `ConsumerEvent::BecameActive`. Every re-subscribe, a
  reconnect or the consumer's own seek, starts over. The client drops what
  the broker sends in between, without acknowledging it, and the broker
  sends it again from the last acknowledged message once it says so.
  `Consumer::messages_withheld()` counts the dropped messages.
- The time to the first message is unchanged: about one failover delay
  (`activeConsumerFailoverDelayTimeMillis`, 1 s by default) after a
  subscribe, a seek or a reconnect. After its own seek the application
  still gets no event.
- When the broker says a second time that the consumer is active, and the
  consumer has been handed messages since the first, the application gets
  `BecameInactive` and then `BecameActive`: the broker rewound to the last
  acknowledged message, so those messages come again.
- A nack, or the unacked-message timer, sends no redelivery request while
  the broker has not said that the consumer is active. If the client
  withheld messages and the broker then sends nothing for one to two
  seconds, the client asks it once to send the unacknowledged messages
  again.
- Exclusive, Shared and Key_Shared subscriptions, non-durable
  subscriptions, readers, non-persistent topics and brokers that speak a
  protocol before version 12 are unchanged.
- At the `debug` level the consumer engine names the consumer in its
  lines, and logs each subscribe with its connection, each withheld entry
  with its topic and position, and how many permits it had withheld when
  the broker says the consumer is active. The lines for each received
  frame and each request from the application moved to `trace`.
- An application that must not skip messages still checks that each
  message follows the one before. The `BecameActive` docs name the two
  cases the client cannot tell from a regular delivery.

## 7.1.0

### Consumer

- Failover consumers that each seek a partition to their own checkpoint on
  `BecameActive` settle, when each has its own name. In 72 of 72 runs on
  Pulsar 5.0.1, with three consumers, four partitions and one membership
  change at a time, ownership reached the broker's layout about a second
  after each change and no further event came for 10 s. This replaces the
  7.0.0 advice against seeking on activation. Each such seek stalls the
  partition for the broker's failover delay. Around a change of owner the
  broker can deliver a message ahead of the last acknowledged one; the
  `BecameActive` docs say how an application detects it.
- `Consumer::seek_to_inclusive(topic, id)` and `Reader::seek_to_inclusive`
  restart a topic or partition at `id`, where `seek_to` restarts after it.
  The next message is `id` itself, and the members of its batch before it
  are not delivered. The seek holds under failures as `seek_to` does. When
  the broker no longer holds `id`, because its ledger was deleted, the seek
  succeeds and the next message is the first one the topic holds after
  `id`, or nothing arrives until the next publish. On a durable
  subscription, a reconnect after `id` has arrived and before its batch is
  acked delivers the unacked members of that batch again, the ones before
  `id` included.

## 7.0.0

This fork of pulsar-rs brings the consumer and producer machinery that a
partitioned topic needs up to the Java and C++ clients: reconnects without
duplicates or gaps, per-partition seeks, flow control, batch acks, Failover
events, partition discovery, Java key routing, and one retry rule. An
integration suite on a two-broker cluster tests them
(`cargo test --features cluster-tests --test integration`, Docker needed);
what no test from outside can force is unit-tested or named in the commit
messages.

### Breaking changes

Upgrading needs code changes, and some behaviour changes without one:

- **Keys route like the Java client.** A keyed message now goes to
  `JavaStringHash(key) % partitions`, sign-masked, by default and with
  `RoutingPolicy::Single` too. Before, the default ignored keys and sent every
  message round-robin, and `RoutingPolicy::RoundRobin` used an unmasked
  Murmur3. Keys land on other partitions after the upgrade, so a rolling
  deploy mixes placements for a while. `ProducerOptions::hashing_scheme` picks
  Java's Murmur3, which is sign-masked: it matches the old `RoundRobin`
  placement only when the partition count is a power of two. To keep the old
  placement exactly, route with `RoutingPolicy::Custom` and the deprecated
  `RoutingPolicy::compute_partition_index_for_key`.
- **A reader started at a message id starts after it**, as in Java. It used
  to deliver that message too; `ConsumerOptions::with_start_inclusive(true)`
  keeps it.
- **Batch-index acks are on by default**, as in Java and the broker since
  Pulsar 4.0 (PIP-391). An ack of part of a batch now reaches the broker.
  `ConsumerOptions::with_batch_index_ack(false)` turns it off. A broker
  without `acknowledgmentAtBatchIndexLevelEnabled` ignores these acks.
- **Seeks name a partition.** `Consumer::seek` and
  `Consumer::get_last_message_id` are replaced by `seek_to(topic, id)`, which
  moves one topic or partition, `seek_to_time(publish_time_ms)`, which moves
  them all, and `last_message_ids()`, keyed by topic or partition. A seek with
  neither an id nor a time is an error.
- **A seek happens once called.** The consumer's engine makes it and sees it
  through: dropping the future does not cancel it. Seeks run in order, and
  one made while the consumer reconnects waits for it, within the operation
  timeout. A seek made after the consumer's engine stopped fails with
  `ConsumerError::Closed`.
- **An error on a consumer's stream is not its end.** Keep polling: the
  engine carries on. Once a consumer or producer exists, only a deleted
  topic ends it (and, for a producer, a terminated topic or a fenced
  producer), as in Java. An authorization or policy refusal is retried and
  told once per outage: brokers also give it when their authorization check
  fails, a metadata timeout say.
- **Every error on a consumer's stream comes as `ConsumerError::Topic`**,
  single topics included; `Error::without_topic` and `server_error` look
  inside.
- **A partitioned topic of one partition gets a multi consumer** and producer,
  so it can grow. Ended partitions are not rebuilt.
- **Retries.** `OperationRetryOptions::retry_delay` is now the first step of a
  backoff (default 100 ms, was a flat 5 s), and `max_retries: None` means as
  many as fit in the operation timeout, no longer forever. A build may take up
  to about twice the operation timeout (lookups, then subscribe or create).
- **Reader.** `Reader::connections` returns `Vec<Url>`, `Reader::reader_id`
  returns `Vec<u64>`, `reader::State` is gone, and `TopicConsumer` is no
  longer a `Stream`.
- **A partitioned reader refuses a start message id**: an id belongs to one
  partition, and Java hands it to every one. Start at the earliest, the
  latest or a rollback, and `seek_to` a partition.
- **Ack a batched message by `Message::ack_id()`** with `ack_with_id` and the
  like: it carries the batch size, so the ack covers that message only.
  `message_id()` leaves it out, so it equals the ids the broker reports. A
  durable consumer finds the size of the batches it delivered (the last
  10,000), so an ack by `message_id()` covers that message too; for any
  other entry it covers the whole entry, as in Java.
- **Engine internals in `consumer::data` changed**: `EngineMessage` (also
  re-exported as `consumer::EngineMessage`) and `EngineEvent` are no longer
  public, `MessageIdDataReceiver` carries `Queued` items, and `Queued` is
  public. None of them is taken or returned by a public function.
- **New public fields** on `ConsumerOptions` (`start_rollback`,
  `start_inclusive`, `batch_index_ack`) and `ProducerOptions`
  (`partition_refresh`, `hashing_scheme`): struct literals need
  `..Default::default()`.

### Consumer

- Reconnects after an unload, a rolling restart, a crash or a frozen broker
  deliver every message once and in order: buffered messages from the old
  connection are dropped, non-durable subscriptions resume after the last
  message taken, and a time-based start stays where it was.
- Flow permits are granted as the application takes messages, half the window
  at a time, as in Java; a reconnect grants only what stale messages leave of
  the window.
- Acking part of a batch no longer acks the whole entry.
- Failover subscriptions report `ConsumerEvent::BecameActive` and
  `BecameInactive` per partition, through `Consumer::next_item`. A
  consumer's own seek tells it nothing new unless the broker gives the
  partition to another consumer meanwhile; don't seek on every activation,
  as a group of consumers doing so can hand a partition back and forth. (Withdrawn in 7.1.0.)
- Partitioned readers, `ConsumerOptions::with_start_rollback`, and
  `has_message_available` on consumers and readers.
- Partitions added to a topic are found while the consumer runs.
- Seeks hold up under failures: a lost connection sends the seek again once
  the consumer is back, and a refused one leaves the subscription where it
  was, with nothing filtered. After a seek, only the target's own entry is
  filtered, as in Java, so a cursor an operator moves back is delivered. A
  reader's seek by time holds across reconnects, also on brokers that retire
  the subscription on a seek, and stays exact: the client drops what the
  protocol's whole-second rollback lets in from before the target.
- A dropped consumer closes on the broker even with a redelivery delay set.
- Acks and nacks by id accept a partition's short or full name, as `seek_to`
  does.
- A consumer of many partitions polls only the partitions that woke: one busy
  partition of 200 costs 0.6 us a message instead of 21 us.

### Producer

- A partitioned producer finds partitions added while it runs
  (`ProducerOptions::partition_refresh`, 60 s by default).
- Keyless messages route round-robin; with batching on, a producer stays on a
  partition for ten batching delays so its batches fill, as in Java.
- A send that times out says why: the error carries the last failure.

### Connections, auth and retries

- One retry rule, after Java's `isRetriableError`, for subscribe, producer
  creation and lookups, with a backoff from `retry_delay` to `max_backoff`
  that spreads retries at every step.
- A keepalive ping unanswered within the keepalive interval (at most the
  operation timeout) marks its connection broken, as Java's keepalive does,
  and a reconnect does not wait on a broken connection to close: a frozen
  broker is left within a keepalive interval, not after two operation
  timeouts.
- A connect given up on halfway, by an application timeout around a build
  say, no longer leaves the next one waiting for ever, and concurrent pings
  on one connection no longer cancel each other.
- A refused credential is retried with whatever the provider hands out next,
  and the consumer's stream reports it once per outage.
- Protocol version 19, with token refresh on the broker's challenge.
