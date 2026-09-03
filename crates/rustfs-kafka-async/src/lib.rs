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
    ACL_RESOURCE_TYPE_TRANSACTIONAL_ID, ACL_RESOURCE_TYPE_USER, AclBinding, AclDescription,
    AclResource, ActiveProducer, AddOffsetsToTxnResponseData, AlterClientQuotaEntryResult,
    AlterClientQuotasOptions, AlterClientQuotasResponseData, AlterConfigsEntry,
    AlterConfigsOptions, AlterConfigsResource, AlterConfigsResourceResult,
    AlterConfigsResponseData, AlterPartitionReassignmentsOptions,
    AlterPartitionReassignmentsPartitionResult, AlterPartitionReassignmentsResponseData,
    AlterPartitionReassignmentsTopicResult, AlterReplicaLogDir, AlterReplicaLogDirPartitionResult,
    AlterReplicaLogDirTopic, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponseData,
    AlterShareGroupOffsetPartition, AlterShareGroupOffsetPartitionResult,
    AlterShareGroupOffsetTopic, AlterShareGroupOffsetTopicResult,
    AlterShareGroupOffsetsResponseData, AlterUserScramCredentialResult,
    AlterUserScramCredentialsOptions, AlterUserScramCredentialsResponseData,
    ApiVersionsResponseData, BrokerApiVersion, CLIENT_QUOTA_MATCH_ANY_SPECIFIED,
    CLIENT_QUOTA_MATCH_DEFAULT, CLIENT_QUOTA_MATCH_EXACT, CONFIG_OPERATION_APPEND,
    CONFIG_OPERATION_DELETE, CONFIG_OPERATION_SET, CONFIG_OPERATION_SUBTRACT,
    CONFIG_RESOURCE_TYPE_BROKER, CONFIG_RESOURCE_TYPE_BROKER_LOGGER, CONFIG_RESOURCE_TYPE_TOPIC,
    ClientQuotaAlteration, ClientQuotaAlterationOp, ClientQuotaEntity, ClientQuotaEntityFilter,
    ClientQuotaEntitySpec, ClientQuotaEntry, ClientQuotaValue, ClusterBroker, ConfigEntry,
    ConfigResource, ConfigSynonym, ConsumerGroupAssignment, ConsumerGroupDescribeResponseData,
    ConsumerGroupDescription, ConsumerGroupMemberDescription, ConsumerGroupTopicPartitions,
    CreateAclResult, CreateAclsResponseData, CreateDelegationTokenOptions,
    CreateDelegationTokenResponseData, CreatePartitionsOptions, CreatePartitionsResponseData,
    CreatePartitionsTopicResult, CreatePartitionsTopicSpec, DelegationTokenDescription,
    DeleteAclsFilterResult, DeleteAclsResponseData, DeleteGroupsResponseData,
    DeleteRecordsPartitionResult, DeleteRecordsPartitionSpec, DeleteRecordsResponseData,
    DeleteRecordsTopicResult, DeleteRecordsTopicSpec, DeleteShareGroupOffsetTopic,
    DeleteShareGroupOffsetTopicResult, DeleteShareGroupOffsetsResponseData, DeletedAcl,
    DeletedGroup, DescribeAclsFilter, DescribeAclsResponseData, DescribeClientQuotasOptions,
    DescribeClientQuotasResponseData, DescribeClusterResponseData, DescribeConfigsResponseData,
    DescribeConfigsResult, DescribeDelegationTokenResponseData, DescribeGroupsResponseData,
    DescribeLogDirsResponseData, DescribeProducersResponseData, DescribeQuorumResponseData,
    DescribeShareGroupOffsetsResponseData, DescribeTopicPartitionsOptions,
    DescribeTopicPartitionsResponseData, DescribeTransactionsResponseData,
    DescribeUserScramCredentialsResponseData, DescribedGroup, DescribedGroupMember,
    DescribedTopicPartition, DescribedTopicPartitionsTopic, DescribedTransaction,
    ELECTION_TYPE_PREFERRED, ELECTION_TYPE_UNCLEAN, ElectLeadersOptions,
    ElectLeadersPartitionResult, ElectLeadersResponseData, ElectLeadersTopicResult,
    ExpireDelegationTokenResponseData, FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE,
    FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE, FEATURE_UPGRADE_TYPE_UPGRADE, FeatureUpdate,
    IncrementalAlterConfig, IncrementalAlterConfigsOptions, IncrementalAlterConfigsResource,
    IncrementalAlterConfigsResourceResult, IncrementalAlterConfigsResponseData, KafkaPrincipal,
    LeaderEpochPartitionOffset, LeaderEpochPartitionRequest, LeaderEpochTopicOffsets,
    LeaderEpochTopicRequest, ListConfigResourcesResponseData, ListGroupsResponseData,
    ListPartitionReassignmentsResponseData, ListTransactionsOptions, ListTransactionsResponseData,
    ListedConfigResource, ListedGroup, ListedTransaction, LogDirDescription, LogDirPartition,
    LogDirTopic, OffsetDeletePartitionResult, OffsetDeleteResponseData, OffsetDeleteTopicResult,
    OffsetForLeaderEpochResponseData, PartitionReassignment, PartitionReassignmentSpec,
    PartitionReassignmentTopicSpec, ProducerPartition, ProducerTopic, QuorumListener, QuorumNode,
    QuorumPartition, QuorumReplicaState, QuorumTopic, RenewDelegationTokenResponseData,
    RequiredAcks, SCRAM_MECHANISM_SHA_256, SCRAM_MECHANISM_SHA_512, SaslConfig,
    ScramCredentialDeletion, ScramCredentialInfo, ScramCredentialUpsertion, SecurityConfig,
    ShareGroupAssignment, ShareGroupDescribeResponseData, ShareGroupDescription,
    ShareGroupMemberDescription, ShareGroupOffsetGroup, ShareGroupOffsetPartition,
    ShareGroupOffsetRequest, ShareGroupOffsetTopic, ShareGroupTopicPartitions, TlsConfig,
    TopicPartitionFilter, TopicPartitionsCursor, TopicReassignment, TransactionTopic,
    TxnOffsetCommitPartitionResult, TxnOffsetCommitResponseData, TxnOffsetCommitTopicPartition,
    TxnOffsetCommitTopicResult, UnregisterBrokerResponseData, UpdateFeaturesResponseData,
    UpdateFeaturesResult, UserScramCredentialsDescription,
};
pub use rustfs_kafka::error;
pub use rustfs_kafka::producer::{AsBytes, Headers, Record};

#[cfg(test)]
mod public_reexports_tests {
    use super::*;

    #[test]
    fn admin_mutation_reexports_are_constructible() {
        let configs =
            IncrementalAlterConfigsOptions::new([IncrementalAlterConfigsResource::topic(
                "topic-a",
                [
                    IncrementalAlterConfig::set("retention.ms", "60000"),
                    IncrementalAlterConfig::delete("cleanup.policy"),
                ],
            )])
            .with_validate_only(true);
        assert!(configs.validate_only);

        let quota = AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
            [
                ClientQuotaEntitySpec::named("user", "alice"),
                ClientQuotaEntitySpec::default_entity("client-id"),
            ],
            [
                ClientQuotaAlterationOp::set("producer_byte_rate", 1024.5),
                ClientQuotaAlterationOp::remove("consumer_byte_rate"),
            ],
        )])
        .with_validate_only(true);
        assert!(quota.validate_only);

        let create_partitions =
            CreatePartitionsOptions::new([CreatePartitionsTopicSpec::new("topic-a", 6)])
                .with_validate_only(true);
        assert_eq!(create_partitions.topics[0].count, 6);

        let reassignment =
            AlterPartitionReassignmentsOptions::new([PartitionReassignmentTopicSpec::new(
                "topic-a",
                [
                    PartitionReassignmentSpec::new(0, [1, 2]),
                    PartitionReassignmentSpec::cancel(1),
                ],
            )]);
        assert!(reassignment.topics[0].partitions[1].replicas.is_none());

        let leader_epoch =
            LeaderEpochTopicRequest::new("topic-a", [LeaderEpochPartitionRequest::new(0, -1, 7)]);
        assert_eq!(leader_epoch.partitions[0].leader_epoch, 7);

        let alter_configs = AlterConfigsOptions::new([AlterConfigsResource::topic(
            "topic-a",
            [AlterConfigsEntry::new("retention.ms", "60000")],
        )])
        .with_validate_only(true);
        assert!(alter_configs.validate_only);

        let log_dirs = AlterReplicaLogDir::new(
            "/kafka-logs-2",
            vec![AlterReplicaLogDirTopic::new("topic-a", [0, 1])],
        );
        assert_eq!(log_dirs.topics[0].partitions, vec![0, 1]);

        let token = CreateDelegationTokenOptions::new()
            .with_owner(KafkaPrincipal::user("alice"))
            .with_renewer(KafkaPrincipal::user("bob"))
            .with_max_lifetime_ms(60_000);
        assert_eq!(token.renewers[0].principal_name, "bob");

        let scram = AlterUserScramCredentialsOptions::new()
            .with_deletion(ScramCredentialDeletion::new(
                "old-user",
                SCRAM_MECHANISM_SHA_256,
            ))
            .with_upsertion(ScramCredentialUpsertion::new(
                "new-user",
                SCRAM_MECHANISM_SHA_512,
                8192,
                bytes::Bytes::from_static(b"salt"),
                bytes::Bytes::from_static(b"salted-password"),
            ));
        assert_eq!(scram.upsertions[0].iterations, 8192);

        let txn_offset =
            TxnOffsetCommitTopicPartition::new("topic-a", 0, 42).with_metadata("metadata");
        assert_eq!(txn_offset.metadata.as_deref(), Some("metadata"));

        let feature_update = FeatureUpdate::safe_downgrade("metadata.version", 20);
        assert_eq!(
            feature_update.upgrade_type,
            FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE
        );

        let share_alter = AlterShareGroupOffsetTopic::new(
            "topic-a",
            [AlterShareGroupOffsetPartition::new(0, 42)],
        );
        assert_eq!(share_alter.partitions[0].offset, 42);

        let share_delete = DeleteShareGroupOffsetTopic::new("topic-a");
        assert_eq!(share_delete.topic_name, "topic-a");
    }
}
