use std::sync::Arc;

use murmur3::murmur3_32;

use crate::producer::Message;

/// How a producer of a partitioned topic picks the partition of a message.
#[derive(Clone, Default)]
pub enum RoutingPolicy {
    /// A message with a key goes to the partition its key hashes to, the
    /// others to each partition in turn. The default, as in Java.
    #[default]
    RoundRobin,
    Single,
    Custom(Arc<dyn CustomRoutingPolicy>),
}

impl RoutingPolicy {
    /// The partition this crate used to pick for a key before 7.0: Murmur3
    /// without Java's sign mask. Only for moving off it; producers now route
    /// keys with [`HashingScheme`].
    #[deprecated(note = "keys now route with HashingScheme, as in the Java client")]
    pub fn compute_partition_index_for_key(key: &str, partition_count: usize) -> usize {
        let hash = murmur3_32(&mut key.as_bytes(), 0).unwrap_or(0);
        (hash % partition_count.max(1) as u32) as usize
    }
}

/// How a key is hashed to a partition. Every client that publishes the same
/// keys to a topic needs the same scheme to keep each key on one partition.
// Java: org.apache.pulsar.client.api.HashingScheme
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HashingScheme {
    /// Java's `String.hashCode`, the default of the Java and Go clients.
    #[default]
    JavaStringHash,
    /// Murmur3 32-bit with seed 0.
    Murmur3_32Hash,
}

impl HashingScheme {
    /// The partition of `key` among `partition_count`.
    // Java: RoundRobinPartitionMessageRouterImpl.choosePartition, JavaStringHash
    // and Murmur3Hash32 mask the sign bit before the modulo.
    pub fn partition(self, key: &str, partition_count: usize) -> usize {
        let hash = match self {
            HashingScheme::JavaStringHash => key.encode_utf16().fold(0i32, |hash, unit| {
                hash.wrapping_mul(31).wrapping_add(unit as i32)
            }),
            HashingScheme::Murmur3_32Hash => murmur3_32(&mut key.as_bytes(), 0).unwrap_or(0) as i32,
        } & i32::MAX;
        // no partitions: index 0, which the caller cannot use either way
        hash as usize % partition_count.max(1)
    }
}

pub trait CustomRoutingPolicy: Send + Sync {
    fn route(&self, message: &Message, num_producers: usize) -> usize;
}

#[cfg(test)]
mod tests {
    use crate::routing_policy::HashingScheme;

    #[test]
    fn keys_hash_like_java() {
        // Java: "hello".hashCode() == 99162322; the emoji makes a surrogate
        // pair, and the hash goes negative before the sign bit is masked.
        for (key, hash) in [
            ("hello", 99162322),
            ("", 0),
            ("key-7", 101943369),
            ("日本語🎉", 1290440356),
        ] {
            assert_eq!(
                HashingScheme::JavaStringHash.partition(key, i32::MAX as usize),
                hash,
                "{key}"
            );
        }
        // Murmur3 x86 32-bit, seed 0, over UTF-8, sign bit masked, as Java's
        // Murmur3_32Hash: tail bytes are taken unsigned (both non-ASCII keys
        // end in a tail byte above 0x7f). Values from an independent
        // implementation of the reference algorithm.
        for (key, hash) in [
            ("hello", 613153351),
            ("é", 269551495),
            ("日本語", 631534231),
        ] {
            assert_eq!(
                HashingScheme::Murmur3_32Hash.partition(key, i32::MAX as usize),
                hash,
                "{key}"
            );
        }
    }
}
