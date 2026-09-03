# rustfs-kafka API Guide

This guide focuses on the current public APIs in `rustfs-kafka` (sync) and the most common usage patterns.

## 1. Quick Start

### 1.1 Producer

```rust,no_run
use std::time::Duration;
use rustfs_kafka::producer::{Producer, Record, RequiredAcks};

let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_client_id("app-producer".to_owned())
    .with_ack_timeout(Duration::from_secs(1))
    .with_required_acks(RequiredAcks::One)
    .create()
    .unwrap();

producer.send(&Record::from_value("my-topic", b"hello")).unwrap();
```

### 1.2 Consumer

```rust,no_run
use rustfs_kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_topic("my-topic".to_owned())
    .with_group("my-group".to_owned())
    .with_fallback_offset(FetchOffset::Earliest)
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();

for ms in consumer.poll().unwrap() {
    for m in ms.messages() {
        println!("offset={} value={:?}", m.offset, m.value);
    }
    consumer.consume_messageset(&ms).unwrap();
}
consumer.commit_consumed().unwrap();
```

### 1.3 KafkaClient (mid-level)

```rust,no_run
use rustfs_kafka::client::KafkaClient;

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();
```

## 2. Producer APIs

### 2.1 Single and batch send

- `Producer::send(&Record)` sends one record.
- `Producer::send_all(&[Record])` sends a batch synchronously.

### 2.2 Partitioner selection

- `DefaultPartitioner` (default, key-hash based)
- `RoundRobinPartitioner`
- `StickyPartitioner`
- `UniformPartitioner`

### 2.3 Batch producer

```rust,no_run
use rustfs_kafka::producer::{BatchProducer, Record};
use std::time::Duration;

let mut batch = BatchProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_batch_size(100)
    .with_linger(Duration::from_millis(5))
    .create()
    .unwrap();

batch.send(Record::from_value("my-topic", b"msg-1")).unwrap();
batch.send(Record::from_value("my-topic", b"msg-2")).unwrap();
let _confirms = batch.flush().unwrap();
```

### 2.4 Transactional producer

Use `TransactionalProducer` for exactly-once style workflows.

```rust,no_run
use rustfs_kafka::producer::{TransactionalProducer, Record};

let mut tx = TransactionalProducer::from_hosts(vec!["localhost:9092".to_owned()])
    .with_transactional_id("txn-demo".to_owned())
    .create()
    .unwrap();

tx.begin().unwrap();
tx.send(&Record::from_value("my-topic", b"in-txn")).unwrap();
tx.commit().unwrap();
```

## 3. Consumer APIs

### 3.1 Offset strategy

- `FetchOffset::Earliest`
- `FetchOffset::Latest`
- `FetchOffset::ByTime(i64)`

### 3.2 Pause/resume partitions

```rust,no_run
// consumer.pause("my-topic", &[0, 1]);
// consumer.resume("my-topic", &[1]);
```

### 3.3 Group offset APIs through `KafkaClient`

- `commit_offset`, `commit_offsets`
- `fetch_group_offsets`, `fetch_group_topic_offset`

## 4. KafkaClient Administrative APIs

### 4.1 Topic metadata and offsets

- `load_metadata_all`, `load_metadata`
- `fetch_offsets`, `list_offsets`, `fetch_topic_offsets`

### 4.2 Cluster and group inspection

```rust,no_run
use std::time::Duration;
use rustfs_kafka::client::{
    ClientQuotaEntityFilter, ConfigResource, DescribeAclsFilter, DescribeClientQuotasOptions,
    KafkaClient, KafkaPrincipal, ListTransactionsOptions, TopicPartitionFilter,
    ACL_OPERATION_READ, ACL_PATTERN_TYPE_LITERAL, ACL_PERMISSION_TYPE_ALLOW, ACL_RESOURCE_TYPE_TOPIC,
};

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);

let api_versions = client.fetch_api_versions().unwrap();
println!("broker supports {} Kafka APIs", api_versions.api_keys.len());

let cluster = client.describe_cluster().unwrap();
println!(
    "cluster={} controller={} brokers={}",
    cluster.cluster_id,
    cluster.controller_id,
    cluster.brokers.len()
);

let groups = client.list_groups().unwrap();
for group in groups.groups {
    println!(
        "group={} protocol={} state={} type={}",
        group.group_id,
        group.protocol_type,
        group.group_state,
        group.group_type
    );
}

let described = client.describe_groups(&["my-group"]).unwrap();
for group in described.groups {
    println!("group={} members={}", group.group_id, group.members.len());
}

let acl_filter = DescribeAclsFilter::any()
    .with_resource_type(ACL_RESOURCE_TYPE_TOPIC)
    .with_resource_name("my-topic")
    .with_pattern_type(ACL_PATTERN_TYPE_LITERAL)
    .with_operation(ACL_OPERATION_READ)
    .with_permission_type(ACL_PERMISSION_TYPE_ALLOW);
let acls = client.describe_acls_with_filter(&acl_filter).unwrap();
for resource in acls.resources {
    println!("acl_resource={} acls={}", resource.resource_name, resource.acls.len());
}

let configs = client
    .describe_configs(&[
        ConfigResource::topic("my-topic").with_configuration_keys(["retention.ms"]),
        ConfigResource::broker("1"),
    ])
    .unwrap();
for resource in configs.results {
    println!("resource={} configs={}", resource.resource_name, resource.configs.len());
}

let partitions = [TopicPartitionFilter::new("my-topic", [0, 1])];

let log_dirs = client.describe_log_dirs_for(&partitions).unwrap();
for log_dir in log_dirs.results {
    println!(
        "log_dir={} usable_bytes={} topics={}",
        log_dir.log_dir,
        log_dir.usable_bytes,
        log_dir.topics.len()
    );
}

let reassignments = client
    .list_partition_reassignments_for(&partitions, Duration::from_secs(10))
    .unwrap();
for topic in reassignments.topics {
    println!("topic={} reassignments={}", topic.name, topic.partitions.len());
}

let tokens = client
    .describe_delegation_tokens_for(&[KafkaPrincipal::user("alice")])
    .unwrap();
for token in tokens.tokens {
    println!("token_id={} renewers={}", token.token_id, token.renewers.len());
}

let quota_filter = DescribeClientQuotasOptions::new()
    .with_component(ClientQuotaEntityFilter::exact("user", "alice"));
let quotas = client.describe_client_quotas_with_options(&quota_filter).unwrap();
for entry in quotas.entries.unwrap_or_default() {
    println!("quota_entity_parts={} values={}", entry.entity.len(), entry.values.len());
}

let scram_credentials = client
    .describe_user_scram_credentials_for(&["alice"])
    .unwrap();
for user in scram_credentials.results {
    println!("user={} credentials={}", user.user, user.credential_infos.len());
}

let producers = client.describe_producers(&partitions).unwrap();
for topic in producers.topics {
    println!("topic={} producer_partitions={}", topic.name, topic.partitions.len());
}

let transaction_filter = ListTransactionsOptions::new()
    .with_state_filters(["Ongoing"])
    .with_duration_filter_ms(30_000)
    .with_transactional_id_pattern("rustfs-.*");
let transactions = client
    .list_transactions_with_options(&transaction_filter)
    .unwrap();
for transaction in transactions.transaction_states {
    println!(
        "transactional_id={} state={}",
        transaction.transactional_id,
        transaction.transaction_state
    );
}

let described_transactions = client.describe_transactions(&["rustfs-txn-a"]).unwrap();
for transaction in described_transactions.transaction_states {
    println!(
        "transactional_id={} partitions={}",
        transaction.transactional_id,
        transaction.topics.len()
    );
}
```

Optional variants expose the highest fields currently wired from `kafka-protocol`:

- `describe_cluster_with_options(include_authorized_operations, include_fenced_brokers)`
- `list_groups_with_filters(states_filter, types_filter)`
- `describe_groups_with_options(groups, include_authorized_operations)`
- `describe_acls_with_filter(filter)`
- `describe_configs_with_options(resources, include_synonyms, include_documentation)`
- `describe_delegation_tokens_for(owners)`
- `describe_log_dirs_for(topic_partition_filters)`
- `list_partition_reassignments_for(topic_partition_filters, timeout)`
- `describe_client_quotas_with_options(options)`
- `describe_user_scram_credentials_for(users)`
- `list_transactions_with_options(options)`

### 4.3 Topic create/delete

```rust,no_run
use std::time::Duration;
use rustfs_kafka::client::{KafkaClient, TopicConfig};

let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
client.load_metadata_all().unwrap();

let topics = vec![TopicConfig::new("demo-topic").with_partitions(3)];
let _ = client.create_topics(&topics, Duration::from_secs(10)).unwrap();
let _ = client.delete_topics(&["demo-topic"], Duration::from_secs(10)).unwrap();
```

## 5. TLS

Enable TLS with default feature `security` (rustls + aws-lc-rs):

```toml
[dependencies]
rustfs-kafka = "1.2.0"
```

By default, TLS verification uses `webpki-roots`. Use `SecurityConfig::with_ca_cert` when Kafka brokers are signed by a
private or enterprise CA.

`security-ring` switches rustls crypto provider to `ring`:

```toml
[dependencies]
rustfs-kafka = { version = "1.2.0", default-features = false, features = ["security-ring"] }
```

## 6. Metrics

Enable metrics feature:

```toml
[dependencies]
rustfs-kafka = { version = "1.2.0", features = ["metrics"] }
```

Metrics include produce/fetch/metadata refresh and connection-level counters/gauges.

## 7. Feature Flags

- `security` (default)
- `security-ring`
- `producer_timestamp`
- `metrics`
- `nightly`
- `integration_tests`

## 8. Async crate

For async wrappers built on tokio, see `crates/rustfs-kafka-async` and its README.
