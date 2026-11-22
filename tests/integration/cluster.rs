//! A two-broker Apache Pulsar cluster for fault-injection tests.
//!
//! The containers are reused across test runs, so a red-green loop does not pay
//! the boot time again. Remove them, which also resets the cluster, with:
//!
//! ```sh
//! docker rm -f $(docker ps -aq --filter label=rprs.cluster=test)
//! docker network rm rprs-test
//! ```
//!
//! Both brokers live in the network namespace of one holder container, which
//! publishes their ports one to one. Each broker advertises `127.0.0.1`, an
//! address that reaches either broker from the test process and from the other
//! broker alike. Killing a broker leaves the holder, and so the namespace, up.

use std::{ffi::OsStr, future::Future, time::Duration};

use pulsar::{Pulsar, TokioExecutor};
use serde::Deserialize;
use tokio::{
    process::Command,
    sync::{Mutex, MutexGuard, OnceCell},
    time::Instant,
};

const IMAGE: &str = "apachepulsar/pulsar:5.0.1";
pub(crate) const NETWORK: &str = "rprs-test";
const LABEL: &str = "rprs.cluster=test";
const CLUSTER_NAME: &str = "test";
const NAMESPACE: &str = "public/default";
/// A namespace where the broker never creates a topic on its own, as on
/// staging, so a deleted topic stays deleted.
pub const STRICT_NAMESPACE: &str = "public/strict";
const ZOOKEEPER: &str = "rprs-zk";
const BOOKIE: &str = "rprs-bookie";
const HOLDER: &str = "rprs-brokers-net";
const BROKERS: [&str; 2] = ["rprs-broker-1", "rprs-broker-2"];
pub(crate) const BOOT_TIMEOUT: Duration = Duration::from_secs(240);
/// Longer than a broker takes to stop gracefully.
const DOCKER_TIMEOUT: Duration = Duration::from_secs(120);
/// The brokers' metadata session lasts 10 s, so a dead broker's topics move
/// well within this.
const FAILOVER_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Cluster {
    /// Brokers this harness stopped and has not started again.
    down: [bool; 2],
    /// Brokers this harness froze. Their metadata session expires meanwhile,
    /// so they come back as a new process rather than unpaused.
    frozen: [bool; 2],
}

static CLUSTER: OnceCell<Mutex<Cluster>> = OnceCell::const_new();

/// Gives a test sole use of the cluster, with both brokers running.
///
/// Tests stop brokers and unload topics, so they cannot share the cluster with
/// a concurrent test. A broker that a panicked test left stopped is started
/// again here, before the next test runs.
pub async fn exclusive() -> MutexGuard<'static, Cluster> {
    let mut cluster = CLUSTER
        .get_or_init(|| async { Mutex::new(boot().await) })
        .await
        .lock()
        .await;
    for broker in 0..BROKERS.len() {
        if cluster.down[broker] {
            cluster.start_broker(broker).await;
        }
    }
    cluster
}

/// A fresh topic name in the test namespace.
pub fn topic(prefix: &str) -> String {
    topic_in(NAMESPACE, prefix)
}

/// The name of partition `index` of a partitioned topic.
pub fn partition(topic: &str, index: u32) -> String {
    format!("{topic}-partition-{index}")
}

/// The index of a partition, from the name [`partition`] gives it.
pub fn index(partition: &str) -> u32 {
    partition
        .rsplit_once("-partition-")
        .and_then(|(_, index)| index.parse().ok())
        .expect("a partition name")
}

/// A fresh topic name in `namespace`.
pub fn topic_in(namespace: &str, prefix: &str) -> String {
    format!("persistent://{namespace}/{prefix}-{}", uuid::Uuid::new_v4())
}

fn service_url(broker: usize) -> String {
    format!("pulsar://127.0.0.1:{}", 6650 + broker)
}

fn web_url(broker: usize) -> String {
    format!("http://127.0.0.1:{}", 8080 + broker)
}

pub(crate) struct Container<'a> {
    pub(crate) name: &'a str,
    pub(crate) network: &'a str,
    pub(crate) ports: &'a [usize],
    pub(crate) env: &'a [(&'a str, &'a str)],
    pub(crate) script: &'a str,
}

/// Creates the test network if it is missing.
pub(crate) async fn ensure_network() {
    if !docker_ok(&["network", "inspect", NETWORK]).await {
        docker(&["network", "create", NETWORK]).await;
    }
}

async fn boot() -> Cluster {
    let _ = env_logger::builder().is_test(true).try_init();
    step("cluster: boot");
    ensure_network().await;
    let label_filter = format!("label={LABEL}");
    let existing = docker(&[
        "ps",
        "-a",
        "--filter",
        &label_filter,
        "--format",
        "{{.Names}}",
    ])
    .await;
    let exists = |name: &str| existing.lines().any(|line| line == name);
    let metadata_store = format!("zk:{ZOOKEEPER}:2181");

    up(
        exists(ZOOKEEPER),
        Container {
            name: ZOOKEEPER,
            network: NETWORK,
            ports: &[],
            env: &[("PULSAR_MEM", "-Xms64m -Xmx256m")],
            script: "bin/apply-config-from-env.py conf/zookeeper.conf \
                     && bin/generate-zookeeper-config.sh conf/zookeeper.conf \
                     && exec bin/pulsar zookeeper",
        },
    )
    .await;
    let ready = eventually(BOOT_TIMEOUT, || async {
        docker_ok(&["exec", ZOOKEEPER, "bin/pulsar-zookeeper-ruok.sh"]).await
    })
    .await;
    assert!(
        ready,
        "{ZOOKEEPER} not ready within {BOOT_TIMEOUT:?}:\n{}",
        docker_logs(ZOOKEEPER).await
    );
    // Init formats BookKeeper's metadata, so it runs only while the cluster is
    // not registered yet, which also covers a first boot that died halfway.
    let cluster_node = format!("/admin/clusters/{CLUSTER_NAME}");
    let shell = [
        "exec",
        ZOOKEEPER,
        "bin/pulsar",
        "zookeeper-shell",
        "-server",
        "localhost:2181",
    ];
    // The shell retries an unreachable server for ever, and any failure other
    // than a missing node must not format a live cluster.
    let stat = docker_output(&[&shell[..], &["stat", &cluster_node]].concat())
        .await
        .expect("run docker");
    let registered = stat.status.success();
    if !registered {
        let stderr = String::from_utf8_lossy(&stat.stderr);
        assert!(
            stderr.contains("Node does not exist"),
            "stat {cluster_node}: {stderr}"
        );
    }
    step(format!("cluster: registered {registered}"));
    if !registered {
        docker(&[
            "run",
            "--rm",
            "--network",
            NETWORK,
            IMAGE,
            "bin/pulsar",
            "initialize-cluster-metadata",
            "-c",
            CLUSTER_NAME,
            "-md",
            &metadata_store,
            "-cms",
            &metadata_store,
            "-uw",
            &web_url(0),
            "-ub",
            &service_url(0),
            "-bn",
            "8",
        ])
        .await;
    }

    let bookie_metadata = format!("metadata-store:{metadata_store}");
    up(
        exists(BOOKIE),
        Container {
            name: BOOKIE,
            network: NETWORK,
            ports: &[],
            env: &[
                (
                    "BOOKIE_MEM",
                    "-Xms128m -Xmx256m -XX:MaxDirectMemorySize=256m",
                ),
                ("PULSAR_PREFIX_metadataServiceUri", &bookie_metadata),
                ("PULSAR_PREFIX_advertisedAddress", BOOKIE),
                ("PULSAR_PREFIX_dbStorage_writeCacheMaxSizeMb", "32"),
                ("PULSAR_PREFIX_dbStorage_readAheadCacheMaxSizeMb", "32"),
            ],
            script: "bin/apply-config-from-env.py conf/bookkeeper.conf && exec bin/pulsar bookie",
        },
    )
    .await;

    up(
        exists(HOLDER),
        Container {
            name: HOLDER,
            network: NETWORK,
            ports: &[6650, 6651, 8080, 8081],
            env: &[],
            script: "exec sleep infinity",
        },
    )
    .await;

    let holder_network = format!("container:{HOLDER}");
    for (broker, &name) in BROKERS.iter().enumerate() {
        let service_port = (6650 + broker).to_string();
        let web_port = (8080 + broker).to_string();
        up(
            exists(name),
            Container {
                name,
                network: &holder_network,
                ports: &[],
                env: &[
                    (
                        "PULSAR_MEM",
                        "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=256m",
                    ),
                    ("PULSAR_PREFIX_metadataStoreUrl", &metadata_store),
                    (
                        "PULSAR_PREFIX_configurationMetadataStoreUrl",
                        &metadata_store,
                    ),
                    ("PULSAR_PREFIX_metadataStoreSessionTimeoutMillis", "10000"),
                    ("PULSAR_PREFIX_clusterName", CLUSTER_NAME),
                    ("PULSAR_PREFIX_advertisedAddress", "127.0.0.1"),
                    ("PULSAR_PREFIX_brokerServicePort", &service_port),
                    ("PULSAR_PREFIX_webServicePort", &web_port),
                    ("PULSAR_PREFIX_managedLedgerDefaultEnsembleSize", "1"),
                    ("PULSAR_PREFIX_managedLedgerDefaultWriteQuorum", "1"),
                    ("PULSAR_PREFIX_managedLedgerDefaultAckQuorum", "1"),
                    ("PULSAR_PREFIX_loadBalancerSheddingEnabled", "false"),
                    ("PULSAR_PREFIX_loadBalancerAutoBundleSplitEnabled", "false"),
                    ("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false"),
                ],
                script: "bin/apply-config-from-env.py conf/broker.conf && exec bin/pulsar broker",
            },
        )
        .await;
    }

    step("cluster: containers up");
    for broker in 0..BROKERS.len() {
        wait_healthy(broker).await;
    }
    step("cluster: brokers healthy");
    Cluster {
        down: [false; 2],
        frozen: [false; 2],
    }
}

/// Starts an existing container as it is, or creates it.
///
/// Docker's network DNS resolves containers by name, so each name doubles as
/// the address the other containers use.
pub(crate) async fn up(exists: bool, container: Container<'_>) {
    if exists {
        // a test that froze a broker and then failed leaves it paused
        docker_ok(&["unpause", container.name]).await;
        docker(&["start", container.name]).await;
        return;
    }
    let mut args: Vec<String> = ["run", "-d", "--name", container.name]
        .into_iter()
        .chain(["--network", container.network, "--label", LABEL])
        .map(String::from)
        .collect();
    for port in container.ports {
        args.extend(["-p".to_string(), format!("{port}:{port}")]);
    }
    for (key, value) in container.env {
        args.extend(["-e".to_string(), format!("{key}={value}")]);
    }
    args.extend([IMAGE, "bash", "-c", container.script].map(String::from));
    docker(&args).await;
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Lookup {
    broker_url: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TopicStats {
    pub(crate) subscriptions: std::collections::BTreeMap<String, SubscriptionStats>,
}

/// What the broker knows about a subscription.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionStats {
    /// Messages not yet acknowledged.
    pub msg_backlog: u64,
    /// Messages dispatched to the subscription's consumers, buffered or not,
    /// since the topic was last loaded.
    pub msg_out_counter: u64,
    #[serde(default)]
    pub consumers: Vec<ConsumerStats>,
}

/// What the broker knows about one consumer.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerStats {
    /// When this consumer last subscribed.
    #[serde(default)]
    pub connected_since: String,
}

#[derive(Debug, Deserialize)]
struct InternalStats {
    ledgers: Vec<Ledger>,
}

/// A ledger of a topic, as the broker lists it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ledger {
    pub ledger_id: u64,
    /// Zero while the ledger is the one written to.
    pub entries: u64,
}

impl Cluster {
    /// A client bootstrapped through a broker that is up.
    pub async fn client(&self) -> Pulsar<TokioExecutor> {
        let broker = self.running().next().expect("a running broker");
        self.client_via(broker).await
    }

    /// Like [`Cluster::client_via`], with a fast keepalive so a frozen broker
    /// is noticed within seconds.
    pub async fn client_with_keepalive(&self, broker: usize) -> Pulsar<TokioExecutor> {
        self.client_with(
            broker,
            pulsar::ConnectionRetryOptions {
                keep_alive: Duration::from_secs(1),
                ..Default::default()
            },
            pulsar::OperationRetryOptions {
                operation_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .await
    }

    /// A client whose lookups go through `broker`. Fault tests bootstrap
    /// through the broker that stays up, as a production service URL always
    /// reaches a live broker.
    pub async fn client_via(&self, broker: usize) -> Pulsar<TokioExecutor> {
        Pulsar::builder(service_url(broker), TokioExecutor)
            .build()
            .await
            .expect("connect to the cluster")
    }

    pub async fn create_topic(&self, topic: &str) {
        self.admin(reqwest::Method::PUT, &path(topic), None).await;
    }

    pub async fn create_partitioned_topic(&self, topic: &str, partitions: u32) {
        self.set_partitions(reqwest::Method::PUT, topic, partitions)
            .await;
    }

    /// Creates `subscription` on `topic`, on each partition of a partitioned
    /// one, at the end of the topic. Until a topic has a subscription, the
    /// broker may delete its closed ledgers at any time.
    pub async fn create_subscription(&self, topic: &str, subscription: &str) {
        self.admin(
            reqwest::Method::PUT,
            &format!("{}/subscription/{subscription}", path(topic)),
            None,
        )
        .await;
    }

    pub async fn update_partitions(&self, topic: &str, partitions: u32) {
        self.set_partitions(reqwest::Method::POST, topic, partitions)
            .await;
    }

    async fn set_partitions(&self, method: reqwest::Method, topic: &str, partitions: u32) {
        self.admin(
            method,
            &format!("{}/partitions", path(topic)),
            Some(partitions.into()),
        )
        .await;
    }

    /// Deletes a topic, closing every producer and consumer on it.
    pub async fn delete_topic(&self, topic: &str) {
        self.admin(
            reqwest::Method::DELETE,
            &format!("{}?force=true", path(topic)),
            None,
        )
        .await;
    }

    /// Deletes a partitioned topic and its partitions, closing every producer
    /// and consumer on them.
    pub async fn delete_partitioned_topic(&self, topic: &str) {
        self.admin(
            reqwest::Method::DELETE,
            &format!("{}/partitions?force=true", path(topic)),
            None,
        )
        .await;
    }

    /// Publishes `message` with `key` through the Java client's command line,
    /// inside a running broker's container, so it routes it by its default
    /// rule.
    pub async fn java_produce(&self, topic: &str, key: &str, message: &str) {
        let broker = self.running().next().expect("a running broker");
        docker(&[
            "exec",
            BROKERS[broker],
            "bin/pulsar-client",
            "--url",
            &service_url(broker),
            "produce",
            topic,
            "--key",
            key,
            "--messages",
            message,
        ])
        .await;
    }

    /// A client bootstrapped through `broker` with its own retry options.
    pub async fn client_with(
        &self,
        broker: usize,
        connection: pulsar::ConnectionRetryOptions,
        operation: pulsar::OperationRetryOptions,
    ) -> Pulsar<TokioExecutor> {
        Pulsar::builder(service_url(broker), TokioExecutor)
            .with_connection_retry_options(connection)
            .with_operation_retry_options(operation)
            .build()
            .await
            .expect("connect to the cluster")
    }

    /// Lets subscriptions in `namespace` be only of `types`, or of any type
    /// when `types` is empty. Creates the namespace if needed.
    pub async fn allow_subscription_types(&self, namespace: &str, types: &[&str]) {
        self.ensure_namespace(namespace).await;
        let policy = format!("namespaces/{namespace}/subscriptionTypesEnabled");
        if types.is_empty() {
            self.admin(reqwest::Method::DELETE, &policy, None).await;
        } else {
            self.admin(
                reqwest::Method::POST,
                &policy,
                Some(serde_json::json!(types)),
            )
            .await;
        }
    }

    /// Moves `subscription` of `topic` back to the first message published
    /// at or after `publish_time_ms`, as an operator would.
    pub async fn reset_cursor(&self, topic: &str, subscription: &str, publish_time_ms: u64) {
        self.admin(
            reqwest::Method::POST,
            &format!(
                "{}/subscription/{subscription}/resetcursor/{publish_time_ms}",
                path(topic)
            ),
            None,
        )
        .await;
    }

    /// Creates `namespace` if it is missing.
    async fn ensure_namespace(&self, namespace: &str) {
        let path = format!("admin/v2/namespaces/{namespace}");
        let created = self
            .web(reqwest::Method::PUT, &path, None)
            .await
            .unwrap_or_else(|e| panic!("{path}: {e}"));
        let status = created.status();
        assert!(
            status.is_success() || status == reqwest::StatusCode::CONFLICT,
            "{path}: {status}"
        );
    }

    /// Creates [`STRICT_NAMESPACE`] if it is missing.
    pub async fn ensure_strict_namespace(&self) {
        self.ensure_namespace(STRICT_NAMESPACE).await;
        self.admin(
            reqwest::Method::POST,
            &format!("namespaces/{STRICT_NAMESPACE}/autoTopicCreation"),
            Some(serde_json::json!({ "allowAutoTopicCreation": false })),
        )
        .await;
    }

    /// Unloads one topic or one partition. The broker closes every producer
    /// and consumer on it, and the next lookup may pick either broker.
    pub async fn unload(&self, topic: &str) {
        self.admin(
            reqwest::Method::PUT,
            &format!("{}/unload", path(topic)),
            None,
        )
        .await;
    }

    /// The ledgers `topic` still has, oldest first.
    pub async fn ledgers(&self, topic: &str) -> Vec<Ledger> {
        let stats: InternalStats = self
            .admin(
                reqwest::Method::GET,
                &format!("{}/internalStats", path(topic)),
                None,
            )
            .await
            .json()
            .await
            .expect("internal stats");
        stats.ledgers
    }

    /// Has the broker delete now the ledgers of `topic` that no subscription
    /// and no retention holds, which it otherwise does on its own schedule.
    async fn trim(&self, topic: &str) {
        self.admin(
            reqwest::Method::POST,
            &format!("{}/trim", path(topic)),
            None,
        )
        .await;
    }

    /// Trims `topic` until the broker has deleted `ledger_id`.
    pub async fn wait_ledger_gone(&self, topic: &str, ledger_id: u64) {
        let last = std::cell::RefCell::new(Vec::new());
        let gone = eventually(FAILOVER_TIMEOUT, || async {
            self.trim(topic).await;
            let ledgers = self.ledgers(topic).await;
            let gone = ledgers.iter().all(|ledger| ledger.ledger_id != ledger_id);
            *last.borrow_mut() = ledgers;
            gone
        })
        .await;
        assert!(
            gone,
            "ledger {ledger_id} of {topic} still there after {FAILOVER_TIMEOUT:?}: {:?}",
            last.borrow()
        );
    }

    /// Index of the broker that owns `topic`, assigning an owner if none has it.
    pub async fn owner(&self, topic: &str) -> usize {
        self.try_owner(topic)
            .await
            .unwrap_or_else(|e| panic!("lookup {topic}: {e}"))
    }

    /// The owner, or why the lookup failed. During a failover the lookup can
    /// fail or point at the dead broker for a while.
    async fn try_owner(&self, topic: &str) -> Result<usize, String> {
        let path = format!("lookup/v2/topic/{}", path(topic));
        let lookup: Lookup = self
            .web(reqwest::Method::GET, &path, None)
            .await?
            .error_for_status()
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        (0..BROKERS.len())
            .find(|&b| service_url(b) == lookup.broker_url)
            .ok_or_else(|| format!("unknown broker {}", lookup.broker_url))
    }

    /// Moves `topic` to `broker`, by unloading its bundle there, and waits
    /// until it owns it.
    pub async fn move_to(&self, topic: &str, broker: usize) {
        let name = topic.trim_start_matches("persistent://");
        let namespace = name.rsplit_once('/').expect("tenant/namespace/topic").0;
        let bundle = self
            .web(
                reqwest::Method::GET,
                &format!("lookup/v2/topic/persistent/{name}/bundle"),
                None,
            )
            .await
            .and_then(|response| response.error_for_status().map_err(|e| e.to_string()))
            .unwrap_or_else(|e| panic!("bundle of {topic}: {e}"))
            .text()
            .await
            .expect("bundle range");
        let bundle = bundle.trim().trim_matches('"');
        self.admin(
            reqwest::Method::PUT,
            &format!(
                "namespaces/{namespace}/{bundle}/unload?destinationBroker=127.0.0.1:{}",
                8080 + broker
            ),
            None,
        )
        .await;
        self.wait_owner(topic, broker).await;
    }

    /// Waits until `topic` is owned by `broker`. After a kill, ownership moves
    /// only once the dead broker's metadata session expires.
    pub async fn wait_owner(&self, topic: &str, broker: usize) {
        let what = format!("{topic} owned by broker {broker}");
        let moved = eventually(FAILOVER_TIMEOUT, || async {
            self.try_owner(topic).await == Ok(broker)
        })
        .await;
        assert!(moved, "{what} not within {FAILOVER_TIMEOUT:?}");
    }

    /// Waits until the broker's view of `subscription` on `topic` satisfies
    /// `holds`, for example that every ack has landed.
    pub async fn wait_subscription(
        &self,
        topic: &str,
        subscription: &str,
        holds: impl Fn(&SubscriptionStats) -> bool,
    ) -> SubscriptionStats {
        let last = std::cell::RefCell::new(SubscriptionStats::default());
        let held = eventually(FAILOVER_TIMEOUT, || async {
            let mut stats: TopicStats = self
                .admin(
                    reqwest::Method::GET,
                    &format!("{}/stats", path(topic)),
                    None,
                )
                .await
                .json()
                .await
                .expect("topic stats");
            let current = stats.subscriptions.remove(subscription).unwrap_or_default();
            let done = holds(&current);
            *last.borrow_mut() = current;
            done
        })
        .await;
        assert!(
            held,
            "{subscription} on {topic} never reached the state: {:?}",
            last.borrow()
        );
        last.into_inner()
    }

    /// Stops a broker the way a crash does: no graceful unload of its topics.
    pub async fn kill_broker(&mut self, broker: usize) {
        self.down[broker] = true;
        docker(&["kill", BROKERS[broker]]).await;
    }

    /// Stops a broker the way a rolling restart does: it unloads its topics
    /// before it exits.
    pub async fn stop_broker(&mut self, broker: usize) {
        self.down[broker] = true;
        docker(&["stop", "--time", "90", BROKERS[broker]]).await;
    }

    /// Freezes a broker: its sockets stay open and silent, as when a host
    /// stops answering, so only a keepalive notices.
    pub async fn pause_broker(&mut self, broker: usize) {
        self.down[broker] = true;
        self.frozen[broker] = true;
        docker(&["pause", BROKERS[broker]]).await;
    }

    /// Lets a paused broker run on as it was, before its metadata session
    /// expires, so it keeps its topics.
    pub async fn resume_broker(&mut self, broker: usize) {
        docker(&["unpause", BROKERS[broker]]).await;
        self.frozen[broker] = false;
        self.down[broker] = false;
    }

    pub async fn start_broker(&mut self, broker: usize) {
        if self.frozen[broker] {
            docker_ok(&["unpause", BROKERS[broker]]).await;
            docker(&["kill", BROKERS[broker]]).await;
            self.frozen[broker] = false;
        }
        wait_healthy(broker).await;
        self.down[broker] = false;
    }

    fn running(&self) -> impl Iterator<Item = usize> + '_ {
        (0..BROKERS.len()).filter(|&b| !self.down[b])
    }

    async fn admin(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> reqwest::Response {
        let response = self
            .web(method, &format!("admin/v2/{path}"), body)
            .await
            .unwrap_or_else(|e| panic!("{path}: {e}"));
        let status = response.status();
        assert!(
            status.is_success(),
            "{path}: {status} {}",
            response.text().await.unwrap_or_default()
        );
        response
    }

    /// Sends a web request to the first running broker that answers, and
    /// follows any redirect to the owning broker.
    ///
    /// Docker's port proxy can accept a connection for a broker that is not
    /// listening and then reset it, so a send error moves on to the next one.
    async fn web(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<reqwest::Response, String> {
        let http = http();
        let mut errors = Vec::new();
        for broker in self.running() {
            let url = format!("{}/{path}", web_url(broker));
            let mut request = http.request(method.clone(), url);
            if let Some(body) = &body {
                request = request.json(&body);
            }
            match request.send().await {
                Ok(response) => return Ok(response),
                Err(e) => errors.push(format!("{}: {e}", BROKERS[broker])),
            }
        }
        Err(format!("no running broker answered: {errors:?}"))
    }
}

/// Starts the broker if it is not running and waits until it serves.
///
/// A broker started right after a kill can exit at once, because its previous
/// metadata session still holds its registration. Starting it again after
/// that session expires succeeds, so an exit leads to another start.
async fn wait_healthy(broker: usize) {
    let name = BROKERS[broker];
    let healthy = eventually(BOOT_TIMEOUT, || async {
        let running = docker(&["inspect", "-f", "{{.State.Running}}", name]).await;
        if running.trim() != "true" {
            docker(&["start", name]).await;
            return false;
        }
        http()
            .get(format!("{}/admin/v2/brokers/health", web_url(broker)))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    })
    .await;
    assert!(
        healthy,
        "{name} not healthy within {BOOT_TIMEOUT:?}:\n{}",
        docker_logs(name).await
    );
}

/// Writes `what` to stderr at once, stamped with the brokers' clock (UTC), so
/// that a run that stops shows how far it got and lines up with the brokers'
/// logs.
pub fn step(what: impl std::fmt::Display) {
    eprintln!("{} {what}", chrono::Utc::now().format("%H:%M:%S%.3fZ"));
}

/// Awaits `future`, and fails the test with `what` if it takes longer than
/// `limit`. The future goes on the heap: a test's own future lives on its
/// thread's stack.
pub async fn within<T>(
    limit: Duration,
    what: impl std::fmt::Display,
    future: impl Future<Output = T>,
) -> T {
    match tokio::time::timeout(limit, Box::pin(future)).await {
        Ok(output) => output,
        Err(_) => panic!("{what}: not within {limit:?}"),
    }
}

/// Polls `check` every half second until it holds. Returns false if it still
/// does not hold after `timeout`.
pub(crate) async fn eventually<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !check().await {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    true
}

pub(crate) fn path(topic: &str) -> String {
    topic.replace("://", "/")
}

/// One client per test thread. Each `#[tokio::test]` runs its own runtime on
/// its own thread, and a pooled connection cannot outlive its runtime.
pub(crate) fn http() -> reqwest::Client {
    thread_local! {
        static HTTP: reqwest::Client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("http client");
    }
    HTTP.with(Clone::clone)
}

/// Runs the docker CLI, killing it after a time limit: when the Docker VM
/// stalls, a call can wait for ever. `docker run` may pull the image first.
async fn docker_output<S: AsRef<OsStr>>(args: &[S]) -> std::io::Result<std::process::Output> {
    let limit = if args.first().is_some_and(|arg| arg.as_ref() == "run") {
        BOOT_TIMEOUT
    } else {
        DOCKER_TIMEOUT
    };
    let output = Command::new("docker")
        .args(args)
        .kill_on_drop(true)
        .output();
    within(limit, format!("docker {}", joined(args)), output).await
}

fn joined<S: AsRef<OsStr>>(args: &[S]) -> String {
    args.iter()
        .map(|a| a.as_ref().to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) async fn docker_ok<S: AsRef<OsStr>>(args: &[S]) -> bool {
    docker_output(args).await.is_ok_and(|o| o.status.success())
}

/// The last lines a container wrote. Pulsar logs to both streams.
pub(crate) async fn docker_logs(name: &str) -> String {
    match docker_output(&["logs", "--tail", "40", name]).await {
        Ok(o) => format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => format!("docker logs {name}: {e}"),
    }
}

/// Runs the docker CLI without blocking the test's runtime, so a client under
/// test keeps running while a broker stops. Returns stdout.
async fn docker<S: AsRef<OsStr>>(args: &[S]) -> String {
    let output = docker_output(args).await.expect("run docker");
    assert!(
        output.status.success(),
        "docker {}: {}",
        joined(args),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}
