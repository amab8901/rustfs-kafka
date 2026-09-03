//! Async Kafka client built on top of the tokio runtime.
//!
//! This crate provides native asynchronous Kafka clients built on tokio.
//! It exposes three primary types:
//!
//! - [`AsyncKafkaClient`]: bootstrap and connection management for async code.
//! - [`AsyncProducer`]: an async producer using non-blocking Kafka protocol I/O.
//! - [`AsyncProducerBuilder`]: async builder for configuring and creating an
//!   `AsyncProducer` without blocking the tokio scheduler.
//! - [`AsyncConsumer`]: an async consumer using non-blocking Kafka protocol I/O.
//! - [`AsyncConsumerBuilder`]: async builder for configuring and creating an
//!   `AsyncConsumer` without blocking the tokio scheduler.
//!
//! # Example
//!
//! ```no_run
//! use rustfs_kafka_async::{AsyncKafkaClient, AsyncProducer};
//! use rustfs_kafka::producer::Record;
//!
//! #[tokio::main]
//! async fn main() -> rustfs_kafka::error::Result<()> {
//!     // Create an async client from bootstrap hosts
//!     let client = AsyncKafkaClient::new(vec!["localhost:9092".to_owned()]).await?;
//!     // Create an async producer which manages a background task
//!     let mut producer = AsyncProducer::new(client).await?;
//!
//!     // Send a single message and close the producer
//!     producer.send(&Record::from_value("test-topic", &b"hello"[..])).await?;
//!     producer.close().await?;
//!     Ok(())
//! }
//! ```

mod client;
mod connection;
mod consumer;
mod metrics;
mod producer;

pub use client::AsyncKafkaClient;
pub use consumer::{AsyncConsumer, AsyncConsumerBuilder};
pub use producer::{AsyncProducer, AsyncProducerBuilder, AsyncProducerConfig};

// Re-export core types from the sync crate for convenience
pub use rustfs_kafka::client::{
    ACL_OPERATION_ALL, ACL_OPERATION_ALTER, ACL_OPERATION_ALTER_CONFIGS, ACL_OPERATION_ANY,
    ACL_OPERATION_CLUSTER_ACTION, ACL_OPERATION_CREATE, ACL_OPERATION_CREATE_TOKENS,
    ACL_OPERATION_DELETE, ACL_OPERATION_DESCRIBE, ACL_OPERATION_DESCRIBE_CONFIGS,
    ACL_OPERATION_DESCRIBE_TOKENS, ACL_OPERATION_IDEMPOTENT_WRITE, ACL_OPERATION_READ,
    ACL_OPERATION_WRITE, ACL_PATTERN_TYPE_ANY, ACL_PATTERN_TYPE_LITERAL, ACL_PATTERN_TYPE_MATCH,
    ACL_PATTERN_TYPE_PREFIXED, ACL_PERMISSION_TYPE_ALLOW, ACL_PERMISSION_TYPE_ANY,
    ACL_PERMISSION_TYPE_DENY, ACL_RESOURCE_TYPE_ANY, ACL_RESOURCE_TYPE_CLUSTER,
    ACL_RESOURCE_TYPE_DELEGATION_TOKEN, ACL_RESOURCE_TYPE_GROUP, ACL_RESOURCE_TYPE_TOPIC,
    ACL_RESOURCE_TYPE_TRANSACTIONAL_ID, ACL_RESOURCE_TYPE_USER, AclDescription, AclResource,
    ActiveProducer, ApiVersionsResponseData, BrokerApiVersion, CLIENT_QUOTA_MATCH_ANY_SPECIFIED,
    CLIENT_QUOTA_MATCH_DEFAULT, CLIENT_QUOTA_MATCH_EXACT, CONFIG_RESOURCE_TYPE_BROKER,
    CONFIG_RESOURCE_TYPE_BROKER_LOGGER, CONFIG_RESOURCE_TYPE_TOPIC, ClientQuotaEntity,
    ClientQuotaEntityFilter, ClientQuotaEntry, ClientQuotaValue, ClusterBroker, ConfigEntry,
    ConfigResource, ConfigSynonym, ConsumerGroupAssignment, ConsumerGroupDescribeResponseData,
    ConsumerGroupDescription, ConsumerGroupMemberDescription, ConsumerGroupTopicPartitions,
    DelegationTokenDescription, DescribeAclsFilter, DescribeAclsResponseData,
    DescribeClientQuotasOptions, DescribeClientQuotasResponseData, DescribeClusterResponseData,
    DescribeConfigsResponseData, DescribeConfigsResult, DescribeDelegationTokenResponseData,
    DescribeGroupsResponseData, DescribeLogDirsResponseData, DescribeProducersResponseData,
    DescribeQuorumResponseData, DescribeShareGroupOffsetsResponseData,
    DescribeTopicPartitionsOptions, DescribeTopicPartitionsResponseData,
    DescribeTransactionsResponseData, DescribeUserScramCredentialsResponseData, DescribedGroup,
    DescribedGroupMember, DescribedTopicPartition, DescribedTopicPartitionsTopic,
    DescribedTransaction, KafkaPrincipal, ListConfigResourcesResponseData, ListGroupsResponseData,
    ListPartitionReassignmentsResponseData, ListTransactionsOptions, ListTransactionsResponseData,
    ListedConfigResource, ListedGroup, ListedTransaction, LogDirDescription, LogDirPartition,
    LogDirTopic, PartitionReassignment, ProducerPartition, ProducerTopic, QuorumListener,
    QuorumNode, QuorumPartition, QuorumReplicaState, QuorumTopic, RequiredAcks,
    SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_512, SaslConfig, ScramCredentialInfo,
    SecurityConfig, ShareGroupAssignment, ShareGroupDescribeResponseData, ShareGroupDescription,
    ShareGroupMemberDescription, ShareGroupOffsetGroup, ShareGroupOffsetPartition,
    ShareGroupOffsetRequest, ShareGroupOffsetTopic, ShareGroupTopicPartitions, TlsConfig,
    TopicPartitionFilter, TopicPartitionsCursor, TopicReassignment, TransactionTopic,
    UserScramCredentialsDescription,
};
pub use rustfs_kafka::error;
pub use rustfs_kafka::producer::{AsBytes, Headers, Record};
