//! Type definitions for Kafka administration protocol helpers.

use bytes::Bytes;
use uuid::Uuid;

/// Endpoint type for broker endpoints in `DescribeCluster`.
pub const DESCRIBE_CLUSTER_ENDPOINT_BROKERS: i8 = 1;

/// Topic config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_TOPIC: i8 = 2;
/// Broker config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER: i8 = 4;
/// Broker logger config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;

/// Set a config key to a value in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_SET: i8 = 0;
/// Delete a config key in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_DELETE: i8 = 1;
/// Append a value to a list config in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_APPEND: i8 = 2;
/// Subtract a value from a list config in `IncrementalAlterConfigs`.
pub const CONFIG_OPERATION_SUBTRACT: i8 = 3;

/// Preferred replica leader election.
pub const ELECTION_TYPE_PREFERRED: i8 = 0;
/// Unclean leader election.
pub const ELECTION_TYPE_UNCLEAN: i8 = 1;

/// Match any ACL resource type.
pub const ACL_RESOURCE_TYPE_ANY: i8 = 1;
/// Topic ACL resource type.
pub const ACL_RESOURCE_TYPE_TOPIC: i8 = 2;
/// Consumer group ACL resource type.
pub const ACL_RESOURCE_TYPE_GROUP: i8 = 3;
/// Cluster ACL resource type.
pub const ACL_RESOURCE_TYPE_CLUSTER: i8 = 4;
/// Transactional ID ACL resource type.
pub const ACL_RESOURCE_TYPE_TRANSACTIONAL_ID: i8 = 5;
/// Delegation token ACL resource type.
pub const ACL_RESOURCE_TYPE_DELEGATION_TOKEN: i8 = 6;
/// User ACL resource type.
pub const ACL_RESOURCE_TYPE_USER: i8 = 7;

/// Match any ACL resource pattern type.
pub const ACL_PATTERN_TYPE_ANY: i8 = 1;
/// Match literal or prefixed ACL resource patterns.
pub const ACL_PATTERN_TYPE_MATCH: i8 = 2;
/// Literal ACL resource pattern type.
pub const ACL_PATTERN_TYPE_LITERAL: i8 = 3;
/// Prefixed ACL resource pattern type.
pub const ACL_PATTERN_TYPE_PREFIXED: i8 = 4;

/// Match any ACL operation.
pub const ACL_OPERATION_ANY: i8 = 1;
/// All ACL operations.
pub const ACL_OPERATION_ALL: i8 = 2;
/// Read ACL operation.
pub const ACL_OPERATION_READ: i8 = 3;
/// Write ACL operation.
pub const ACL_OPERATION_WRITE: i8 = 4;
/// Create ACL operation.
pub const ACL_OPERATION_CREATE: i8 = 5;
/// Delete ACL operation.
pub const ACL_OPERATION_DELETE: i8 = 6;
/// Alter ACL operation.
pub const ACL_OPERATION_ALTER: i8 = 7;
/// Describe ACL operation.
pub const ACL_OPERATION_DESCRIBE: i8 = 8;
/// Cluster action ACL operation.
pub const ACL_OPERATION_CLUSTER_ACTION: i8 = 9;
/// Describe configs ACL operation.
pub const ACL_OPERATION_DESCRIBE_CONFIGS: i8 = 10;
/// Alter configs ACL operation.
pub const ACL_OPERATION_ALTER_CONFIGS: i8 = 11;
/// Idempotent write ACL operation.
pub const ACL_OPERATION_IDEMPOTENT_WRITE: i8 = 12;
/// Create tokens ACL operation.
pub const ACL_OPERATION_CREATE_TOKENS: i8 = 13;
/// Describe tokens ACL operation.
pub const ACL_OPERATION_DESCRIBE_TOKENS: i8 = 14;

/// Match any ACL permission type.
pub const ACL_PERMISSION_TYPE_ANY: i8 = 1;
/// Deny ACL permission type.
pub const ACL_PERMISSION_TYPE_DENY: i8 = 2;
/// Allow ACL permission type.
pub const ACL_PERMISSION_TYPE_ALLOW: i8 = 3;

/// Match an exact client quota entity name.
pub const CLIENT_QUOTA_MATCH_EXACT: i8 = 0;
/// Match the default client quota entity.
pub const CLIENT_QUOTA_MATCH_DEFAULT: i8 = 1;
/// Match any specified client quota entity name.
pub const CLIENT_QUOTA_MATCH_ANY_SPECIFIED: i8 = 2;

/// Kafka SCRAM-SHA-256 mechanism code.
pub const SCRAM_MECHANISM_SHA_256: i8 = 1;
/// Kafka SCRAM-SHA-512 mechanism code.
pub const SCRAM_MECHANISM_SHA_512: i8 = 2;

/// A resource whose configs should be described.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name, such as a topic name or broker ID.
    pub resource_name: String,
    /// Configuration keys to fetch, or `None` to fetch all keys.
    pub configuration_keys: Option<Vec<String>>,
}

impl ConfigResource {
    /// Create a config resource with a raw Kafka resource type.
    #[must_use]
    pub fn new(resource_type: i8, resource_name: impl Into<String>) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configuration_keys: None,
        }
    }

    /// Create a topic config resource.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(CONFIG_RESOURCE_TYPE_TOPIC, name)
    }

    /// Create a broker config resource.
    #[must_use]
    pub fn broker(id: impl Into<String>) -> Self {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER, id)
    }

    /// Create a broker logger config resource.
    #[must_use]
    pub fn broker_logger(id: impl Into<String>) -> Self {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER_LOGGER, id)
    }

    /// Restrict the request to the supplied configuration keys.
    #[must_use]
    pub fn with_configuration_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.configuration_keys = Some(keys.into_iter().map(Into::into).collect());
        self
    }
}

/// One config operation for `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfig {
    /// Configuration key name.
    pub name: String,
    /// Raw Kafka config operation code.
    pub operation: i8,
    /// Value used by set/append/subtract operations, or `None` for delete.
    pub value: Option<String>,
}

impl IncrementalAlterConfig {
    /// Create a config operation with a raw Kafka operation code.
    #[must_use]
    pub fn new(name: impl Into<String>, operation: i8, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            operation,
            value,
        }
    }

    /// Set a config key to a value.
    #[must_use]
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_SET, Some(value.into()))
    }

    /// Delete a config key.
    #[must_use]
    pub fn delete(name: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_DELETE, None)
    }

    /// Append a value to a list config key.
    #[must_use]
    pub fn append(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_APPEND, Some(value.into()))
    }

    /// Subtract a value from a list config key.
    #[must_use]
    pub fn subtract(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, CONFIG_OPERATION_SUBTRACT, Some(value.into()))
    }
}

/// One resource updated by `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name, such as a topic name or broker ID.
    pub resource_name: String,
    /// Config operations for this resource.
    pub configs: Vec<IncrementalAlterConfig>,
}

impl IncrementalAlterConfigsResource {
    /// Create a config mutation resource with a raw Kafka resource type.
    #[must_use]
    pub fn new<I>(resource_type: i8, resource_name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configs: configs.into_iter().collect(),
        }
    }

    /// Create a topic config mutation resource.
    #[must_use]
    pub fn topic<I>(name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_TOPIC, name, configs)
    }

    /// Create a broker config mutation resource.
    #[must_use]
    pub fn broker<I>(id: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER, id, configs)
    }

    /// Create a broker logger config mutation resource.
    #[must_use]
    pub fn broker_logger<I>(id: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfig>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER_LOGGER, id, configs)
    }
}

/// Options for an `IncrementalAlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsOptions {
    /// Resources to mutate.
    pub resources: Vec<IncrementalAlterConfigsResource>,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl IncrementalAlterConfigsOptions {
    /// Create options with the supplied resources.
    #[must_use]
    pub fn new<I>(resources: I) -> Self
    where
        I: IntoIterator<Item = IncrementalAlterConfigsResource>,
    {
        Self {
            resources: resources.into_iter().collect(),
            validate_only: false,
        }
    }

    /// Validate the request without applying it.
    #[must_use]
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }
}

/// Per-resource result returned by `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResourceResult {
    /// Per-resource broker error code.
    pub error_code: i16,
    /// Optional per-resource broker error message.
    pub error_message: Option<String>,
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
}

/// Parsed response from an `IncrementalAlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-resource config mutation results.
    pub responses: Vec<IncrementalAlterConfigsResourceResult>,
}

/// A topic plus a partition list used by read-only diagnostic APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionFilter {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to inspect.
    pub partitions: Vec<i32>,
}

impl TopicPartitionFilter {
    /// Create a topic/partition filter.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Partition count expansion for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicSpec {
    /// Topic name.
    pub topic: String,
    /// Desired total partition count after expansion.
    pub count: i32,
    /// Optional explicit replica assignments for the newly created partitions.
    pub assignments: Option<Vec<Vec<i32>>>,
}

impl CreatePartitionsTopicSpec {
    /// Create a partition expansion spec without explicit broker assignments.
    #[must_use]
    pub fn new(topic: impl Into<String>, count: i32) -> Self {
        Self {
            topic: topic.into(),
            count,
            assignments: None,
        }
    }

    /// Attach explicit broker assignments for the new partitions.
    #[must_use]
    pub fn with_assignments<I, J>(mut self, assignments: I) -> Self
    where
        I: IntoIterator<Item = J>,
        J: IntoIterator<Item = i32>,
    {
        self.assignments = Some(
            assignments
                .into_iter()
                .map(|assignment| assignment.into_iter().collect())
                .collect(),
        );
        self
    }
}

/// Options for a `CreatePartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsOptions {
    /// Topic partition expansions to request.
    pub topics: Vec<CreatePartitionsTopicSpec>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl CreatePartitionsOptions {
    /// Create options with the supplied topic partition expansions.
    #[must_use]
    pub fn new<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = CreatePartitionsTopicSpec>,
    {
        Self {
            topics: topics.into_iter().collect(),
            timeout_ms: 60_000,
            validate_only: false,
        }
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Validate the request without applying it.
    #[must_use]
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }
}

/// Result of one topic in a `CreatePartitions` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-topic broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `CreatePartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic partition creation results returned by the broker.
    pub results: Vec<CreatePartitionsTopicResult>,
}

/// A partition and high-watermark offset used by `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartitionSpec {
    /// Partition index.
    pub partition_index: i32,
    /// Delete records before this offset.
    pub offset: i64,
}

impl DeleteRecordsPartitionSpec {
    /// Create a delete-records partition spec.
    #[must_use]
    pub fn new(partition_index: i32, offset: i64) -> Self {
        Self {
            partition_index,
            offset,
        }
    }
}

/// Per-topic delete-records request spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicSpec {
    /// Topic name.
    pub topic: String,
    /// Partition offsets to truncate to.
    pub partitions: Vec<DeleteRecordsPartitionSpec>,
}

impl DeleteRecordsTopicSpec {
    /// Create a delete-records topic spec.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = DeleteRecordsPartitionSpec>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Per-partition result returned by `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Partition low watermark after deletion.
    pub low_watermark: i64,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result returned by `DeleteRecords`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition-level deletion results.
    pub partitions: Vec<DeleteRecordsPartitionResult>,
}

/// Parsed response from a `DeleteRecords` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic-level deletion results returned by the broker.
    pub topics: Vec<DeleteRecordsTopicResult>,
}

/// Options for an `ElectLeaders` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersOptions {
    /// Raw Kafka election type.
    pub election_type: i8,
    /// Topic partitions to elect leaders for, or `None` for all eligible partitions.
    pub topic_partitions: Option<Vec<TopicPartitionFilter>>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

impl ElectLeadersOptions {
    /// Create options for the supplied partitions.
    #[must_use]
    pub fn new<I>(election_type: i8, topic_partitions: I) -> Self
    where
        I: IntoIterator<Item = TopicPartitionFilter>,
    {
        Self {
            election_type,
            topic_partitions: Some(topic_partitions.into_iter().collect()),
            timeout_ms: 60_000,
        }
    }

    /// Create options that ask the broker to elect leaders for all eligible partitions.
    #[must_use]
    pub fn all_partitions(election_type: i8) -> Self {
        Self {
            election_type,
            topic_partitions: None,
            timeout_ms: 60_000,
        }
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Per-partition leader election result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersPartitionResult {
    /// Partition index.
    pub partition_id: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Per-topic leader election result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partition-level election results.
    pub partition_results: Vec<ElectLeadersPartitionResult>,
}

/// Parsed response from an `ElectLeaders` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Topic-level election results returned by the broker.
    pub replica_election_results: Vec<ElectLeadersTopicResult>,
}

/// A partition reassignment entry for `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentSpec {
    /// Partition index.
    pub partition_index: i32,
    /// New replica broker IDs, or `None` to cancel an active reassignment.
    pub replicas: Option<Vec<i32>>,
}

impl PartitionReassignmentSpec {
    /// Create a partition reassignment.
    #[must_use]
    pub fn new<I>(partition_index: i32, replicas: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            partition_index,
            replicas: Some(replicas.into_iter().collect()),
        }
    }

    /// Create a partition reassignment cancellation.
    #[must_use]
    pub fn cancel(partition_index: i32) -> Self {
        Self {
            partition_index,
            replicas: None,
        }
    }
}

/// Per-topic reassignment spec for `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentTopicSpec {
    /// Topic name.
    pub topic: String,
    /// Partition reassignments for this topic.
    pub partitions: Vec<PartitionReassignmentSpec>,
}

impl PartitionReassignmentTopicSpec {
    /// Create a topic reassignment spec.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = PartitionReassignmentSpec>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Options for an `AlterPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsOptions {
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Whether replication-factor changes are allowed.
    pub allow_replication_factor_change: bool,
    /// Topic reassignments to alter.
    pub topics: Vec<PartitionReassignmentTopicSpec>,
}

impl AlterPartitionReassignmentsOptions {
    /// Create options with the supplied topic reassignments.
    #[must_use]
    pub fn new<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = PartitionReassignmentTopicSpec>,
    {
        Self {
            timeout_ms: 60_000,
            allow_replication_factor_change: true,
            topics: topics.into_iter().collect(),
        }
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Control whether replication-factor changes are allowed.
    #[must_use]
    pub fn with_allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }
}

/// Per-partition result returned by `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Per-topic result returned by `AlterPartitionReassignments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition-level reassignment results.
    pub partitions: Vec<AlterPartitionReassignmentsPartitionResult>,
}

/// Parsed response from an `AlterPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Whether replication-factor changes were allowed.
    pub allow_replication_factor_change: bool,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Topic-level reassignment results returned by the broker.
    pub responses: Vec<AlterPartitionReassignmentsTopicResult>,
}

/// Partition request for `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochPartitionRequest {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch known by the caller, or Kafka's `-1` sentinel.
    pub current_leader_epoch: i32,
    /// Leader epoch whose end offset should be looked up.
    pub leader_epoch: i32,
}

impl LeaderEpochPartitionRequest {
    /// Create a leader-epoch offset lookup partition request.
    #[must_use]
    pub fn new(partition: i32, current_leader_epoch: i32, leader_epoch: i32) -> Self {
        Self {
            partition,
            current_leader_epoch,
            leader_epoch,
        }
    }
}

/// Per-topic request for `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochTopicRequest {
    /// Topic name.
    pub topic: String,
    /// Partition epoch lookups for this topic.
    pub partitions: Vec<LeaderEpochPartitionRequest>,
}

impl LeaderEpochTopicRequest {
    /// Create a leader-epoch offset lookup topic request.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = LeaderEpochPartitionRequest>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// Per-partition offset returned by `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochPartitionOffset {
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch of the returned end offset.
    pub leader_epoch: i32,
    /// End offset for the requested leader epoch.
    pub end_offset: i64,
}

/// Per-topic result returned by `OffsetForLeaderEpoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderEpochTopicOffsets {
    /// Topic name.
    pub topic: String,
    /// Partition offsets for this topic.
    pub partitions: Vec<LeaderEpochPartitionOffset>,
}

/// Parsed response from an `OffsetForLeaderEpoch` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic-level leader-epoch offsets returned by the broker.
    pub topics: Vec<LeaderEpochTopicOffsets>,
}

/// Per-partition result returned by `OffsetDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeletePartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result returned by `OffsetDelete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition-level offset deletion results.
    pub partitions: Vec<OffsetDeletePartitionResult>,
}

/// Parsed response from an `OffsetDelete` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic-level offset deletion results returned by the broker.
    pub topics: Vec<OffsetDeleteTopicResult>,
}

/// Cursor used to page through `DescribeTopicPartitions` results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionsCursor {
    /// Topic name where the next page should start.
    pub topic_name: String,
    /// Partition index where the next page should start.
    pub partition_index: i32,
}

impl TopicPartitionsCursor {
    /// Create a topic-partitions pagination cursor.
    #[must_use]
    pub fn new(topic_name: impl Into<String>, partition_index: i32) -> Self {
        Self {
            topic_name: topic_name.into(),
            partition_index,
        }
    }
}

/// Options for a `DescribeTopicPartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsOptions {
    /// Topic names to describe. Empty lets the broker return all visible topics.
    pub topics: Vec<String>,
    /// Maximum number of partitions to include in one response page.
    pub response_partition_limit: i32,
    /// Optional cursor returned by a previous page.
    pub cursor: Option<TopicPartitionsCursor>,
}

impl DescribeTopicPartitionsOptions {
    /// Create options with a response partition limit and no topic filter.
    #[must_use]
    pub fn new(response_partition_limit: i32) -> Self {
        Self {
            topics: Vec::new(),
            response_partition_limit,
            cursor: None,
        }
    }

    /// Restrict the request to selected topic names.
    #[must_use]
    pub fn with_topics<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Continue from a broker-supplied pagination cursor.
    #[must_use]
    pub fn with_cursor(mut self, cursor: TopicPartitionsCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

/// Partition metadata returned by `DescribeTopicPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartition {
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Partition index.
    pub partition_index: i32,
    /// Current leader broker ID, or Kafka's sentinel when unknown.
    pub leader_id: i32,
    /// Current leader epoch.
    pub leader_epoch: i32,
    /// Replicas hosting this partition.
    pub replica_nodes: Vec<i32>,
    /// Replicas currently in sync with the leader.
    pub isr_nodes: Vec<i32>,
    /// Eligible leader replicas when returned by the broker.
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// Last known eligible leader replicas when returned by the broker.
    pub last_known_elr: Option<Vec<i32>>,
    /// Replicas currently offline.
    pub offline_replicas: Vec<i32>,
}

/// Topic metadata returned by `DescribeTopicPartitions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopicPartitionsTopic {
    /// Per-topic broker error code.
    pub error_code: i16,
    /// Topic name, omitted by Kafka for some error responses.
    pub name: Option<String>,
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Whether Kafka marks the topic as internal.
    pub is_internal: bool,
    /// Partition metadata returned for this topic.
    pub partitions: Vec<DescribedTopicPartition>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub topic_authorized_operations: i32,
}

/// Parsed response from a `DescribeTopicPartitions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topic partition metadata returned by the broker.
    pub topics: Vec<DescribedTopicPartitionsTopic>,
    /// Cursor for the next page, or `None` when the response is complete.
    pub next_cursor: Option<TopicPartitionsCursor>,
}

/// One configurable resource returned by `ListConfigResources`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedConfigResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
}

/// Parsed response from a `ListConfigResources` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Config resources returned by the broker.
    pub resources: Vec<ListedConfigResource>,
}

/// Endpoint for one `KRaft` quorum node returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumListener {
    /// Listener name.
    pub name: String,
    /// Listener host.
    pub host: String,
    /// Listener port.
    pub port: u16,
}

/// One `KRaft` quorum node returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumNode {
    /// Broker or controller node ID.
    pub node_id: i32,
    /// Listeners returned for this node.
    pub listeners: Vec<QuorumListener>,
}

/// Replica state returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumReplicaState {
    /// Broker or controller replica ID.
    pub replica_id: i32,
    /// Replica directory UUID as a string, or Kafka's nil UUID sentinel.
    pub replica_directory_id: String,
    /// Last known log end offset.
    pub log_end_offset: i64,
    /// Last fetch timestamp in milliseconds, or Kafka's `-1` sentinel.
    pub last_fetch_timestamp: i64,
    /// Last caught-up timestamp in milliseconds, or Kafka's `-1` sentinel.
    pub last_caught_up_timestamp: i64,
}

/// Per-partition `KRaft` quorum state returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional per-partition broker error message.
    pub error_message: Option<String>,
    /// Current leader ID, or Kafka's `-1` sentinel if unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// High watermark for the quorum partition.
    pub high_watermark: i64,
    /// Current voters in the quorum.
    pub current_voters: Vec<QuorumReplicaState>,
    /// Observers in the quorum.
    pub observers: Vec<QuorumReplicaState>,
}

/// Per-topic `KRaft` quorum state returned by `DescribeQuorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumTopic {
    /// Topic name.
    pub name: String,
    /// Partition quorum states.
    pub partitions: Vec<QuorumPartition>,
}

/// Parsed response from a `DescribeQuorum` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Quorum state grouped by topic.
    pub topics: Vec<QuorumTopic>,
    /// Quorum nodes returned by Kafka v2+.
    pub nodes: Vec<QuorumNode>,
}

/// A Kafka principal identified by type and name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaPrincipal {
    /// Principal type, such as `User`.
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

impl KafkaPrincipal {
    /// Create a principal with explicit Kafka type and name.
    #[must_use]
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// Create a Kafka user principal.
    #[must_use]
    pub fn user(name: impl Into<String>) -> Self {
        Self::new("User", name)
    }
}

/// Filters for a `DescribeAcls` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsFilter {
    /// Raw Kafka ACL resource type filter.
    pub resource_type_filter: i8,
    /// Optional resource name filter.
    pub resource_name_filter: Option<String>,
    /// Raw Kafka ACL pattern type filter.
    pub pattern_type_filter: i8,
    /// Optional principal filter.
    pub principal_filter: Option<String>,
    /// Optional host filter.
    pub host_filter: Option<String>,
    /// Raw Kafka ACL operation filter.
    pub operation: i8,
    /// Raw Kafka ACL permission type filter.
    pub permission_type: i8,
}

impl Default for DescribeAclsFilter {
    fn default() -> Self {
        Self {
            resource_type_filter: ACL_RESOURCE_TYPE_ANY,
            resource_name_filter: None,
            pattern_type_filter: ACL_PATTERN_TYPE_ANY,
            principal_filter: None,
            host_filter: None,
            operation: ACL_OPERATION_ANY,
            permission_type: ACL_PERMISSION_TYPE_ANY,
        }
    }
}

impl DescribeAclsFilter {
    /// Create a filter that matches all ACLs visible to the broker.
    #[must_use]
    pub fn any() -> Self {
        Self::default()
    }

    /// Restrict by Kafka resource type.
    #[must_use]
    pub fn with_resource_type(mut self, resource_type: i8) -> Self {
        self.resource_type_filter = resource_type;
        self
    }

    /// Restrict by resource name.
    #[must_use]
    pub fn with_resource_name(mut self, resource_name: impl Into<String>) -> Self {
        self.resource_name_filter = Some(resource_name.into());
        self
    }

    /// Restrict by Kafka resource pattern type.
    #[must_use]
    pub fn with_pattern_type(mut self, pattern_type: i8) -> Self {
        self.pattern_type_filter = pattern_type;
        self
    }

    /// Restrict by principal string, such as `User:alice`.
    #[must_use]
    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal_filter = Some(principal.into());
        self
    }

    /// Restrict by host.
    #[must_use]
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host_filter = Some(host.into());
        self
    }

    /// Restrict by Kafka ACL operation.
    #[must_use]
    pub fn with_operation(mut self, operation: i8) -> Self {
        self.operation = operation;
        self
    }

    /// Restrict by Kafka ACL permission type.
    #[must_use]
    pub fn with_permission_type(mut self, permission_type: i8) -> Self {
        self.permission_type = permission_type;
        self
    }
}

/// One ACL binding to create with `CreateAcls`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    /// Raw Kafka ACL resource type.
    pub resource_type: i8,
    /// Resource name for the ACL.
    pub resource_name: String,
    /// Raw Kafka ACL resource pattern type.
    pub resource_pattern_type: i8,
    /// ACL principal string, such as `User:alice`.
    pub principal: String,
    /// Host to which the ACL applies.
    pub host: String,
    /// Raw Kafka ACL operation code.
    pub operation: i8,
    /// Raw Kafka ACL permission type code.
    pub permission_type: i8,
}

impl AclBinding {
    /// Create an ACL binding with all Kafka ACL fields supplied explicitly.
    #[must_use]
    pub fn new(
        resource_type: i8,
        resource_name: impl Into<String>,
        resource_pattern_type: i8,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: i8,
        permission_type: i8,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            resource_pattern_type,
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }
}

/// One entity component used to filter `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntityFilter {
    /// Kafka quota entity type, for example `user`, `client-id`, or `ip`.
    pub entity_type: String,
    /// Raw Kafka match type.
    pub match_type: i8,
    /// Name to match when `match_type` is exact.
    pub match_value: Option<String>,
}

impl ClientQuotaEntityFilter {
    /// Match an exact quota entity name.
    #[must_use]
    pub fn exact(entity_type: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type: CLIENT_QUOTA_MATCH_EXACT,
            match_value: Some(value.into()),
        }
    }

    /// Match the default quota entity.
    #[must_use]
    pub fn default_entity(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type: CLIENT_QUOTA_MATCH_DEFAULT,
            match_value: None,
        }
    }

    /// Match any specified quota entity name.
    #[must_use]
    pub fn any_specified(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type: CLIENT_QUOTA_MATCH_ANY_SPECIFIED,
            match_value: None,
        }
    }
}

/// Filters for a `DescribeClientQuotas` request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeClientQuotasOptions {
    /// Entity filter components. Empty means all quota entities visible to the broker.
    pub components: Vec<ClientQuotaEntityFilter>,
    /// Whether Kafka should exclude entities with unspecified entity types.
    pub strict: bool,
}

impl DescribeClientQuotasOptions {
    /// Create options that describe all visible client quota entities.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Require strict entity matching.
    #[must_use]
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    /// Add one entity filter component.
    #[must_use]
    pub fn with_component(mut self, component: ClientQuotaEntityFilter) -> Self {
        self.components.push(component);
        self
    }

    /// Replace the entity filter components.
    #[must_use]
    pub fn with_components<I>(mut self, components: I) -> Self
    where
        I: IntoIterator<Item = ClientQuotaEntityFilter>,
    {
        self.components = components.into_iter().collect();
        self
    }
}

/// One entity component used to alter client quotas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntitySpec {
    /// Kafka quota entity type, for example `user`, `client-id`, or `ip`.
    pub entity_type: String,
    /// Entity name, or `None` for Kafka's default entity.
    pub entity_name: Option<String>,
}

impl ClientQuotaEntitySpec {
    /// Create a quota entity with a concrete entity name.
    #[must_use]
    pub fn named(entity_type: impl Into<String>, entity_name: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_name: Some(entity_name.into()),
        }
    }

    /// Create a quota entity that targets Kafka's default entity for this type.
    #[must_use]
    pub fn default_entity(entity_type: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_name: None,
        }
    }
}

/// One quota operation for `AlterClientQuotas`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlterationOp {
    /// Quota configuration key.
    pub key: String,
    /// Value to set; ignored by Kafka when `remove` is true.
    pub value: f64,
    /// Whether the quota key should be removed.
    pub remove: bool,
}

impl ClientQuotaAlterationOp {
    /// Set a quota value.
    #[must_use]
    pub fn set(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            remove: false,
        }
    }

    /// Remove a quota value.
    #[must_use]
    pub fn remove(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: 0.0,
            remove: true,
        }
    }
}

/// One quota entity alteration entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    /// Entity components that identify this quota entry.
    pub entity: Vec<ClientQuotaEntitySpec>,
    /// Quota operations to apply to this entity.
    pub ops: Vec<ClientQuotaAlterationOp>,
}

impl ClientQuotaAlteration {
    /// Create a quota alteration for an entity.
    #[must_use]
    pub fn new<I, J>(entity: I, ops: J) -> Self
    where
        I: IntoIterator<Item = ClientQuotaEntitySpec>,
        J: IntoIterator<Item = ClientQuotaAlterationOp>,
    {
        Self {
            entity: entity.into_iter().collect(),
            ops: ops.into_iter().collect(),
        }
    }
}

/// Options for an `AlterClientQuotas` request.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotasOptions {
    /// Quota entries to alter.
    pub entries: Vec<ClientQuotaAlteration>,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl AlterClientQuotasOptions {
    /// Create options with the supplied quota alterations.
    #[must_use]
    pub fn new<I>(entries: I) -> Self
    where
        I: IntoIterator<Item = ClientQuotaAlteration>,
    {
        Self {
            entries: entries.into_iter().collect(),
            validate_only: false,
        }
    }

    /// Validate the request without applying it.
    #[must_use]
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }
}

/// Filters for a `ListTransactions` request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListTransactionsOptions {
    /// Transaction states to include, or empty to include all states.
    pub state_filters: Vec<String>,
    /// Producer IDs to include, or empty to include all producer IDs.
    pub producer_id_filters: Vec<i64>,
    /// Minimum running duration in milliseconds, or `None` to include all durations.
    pub duration_filter_ms: Option<i64>,
    /// Optional transactional ID regular expression pattern.
    pub transactional_id_pattern: Option<String>,
}

impl ListTransactionsOptions {
    /// Create default options that list all transactions visible to the broker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict the request to the supplied transaction states.
    #[must_use]
    pub fn with_state_filters<I, S>(mut self, states: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.state_filters = states.into_iter().map(Into::into).collect();
        self
    }

    /// Restrict the request to the supplied producer IDs.
    #[must_use]
    pub fn with_producer_id_filters<I>(mut self, producer_ids: I) -> Self
    where
        I: IntoIterator<Item = i64>,
    {
        self.producer_id_filters = producer_ids.into_iter().collect();
        self
    }

    /// Restrict the request to transactions running longer than the supplied duration.
    #[must_use]
    pub fn with_duration_filter_ms(mut self, duration_ms: i64) -> Self {
        self.duration_filter_ms = Some(duration_ms);
        self
    }

    /// Restrict the request to transactional IDs matching the supplied pattern.
    #[must_use]
    pub fn with_transactional_id_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.transactional_id_pattern = Some(pattern.into());
        self
    }
}

/// A broker returned by `DescribeCluster`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBroker {
    /// Broker ID.
    pub broker_id: i32,
    /// Broker host name.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Optional broker rack.
    pub rack: Option<String>,
    /// Whether the broker is fenced.
    pub is_fenced: bool,
}

/// Parsed response from a `DescribeCluster` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
    /// Endpoint type described by the broker.
    pub endpoint_type: i8,
    /// Kafka cluster ID.
    pub cluster_id: String,
    /// Current controller broker ID.
    pub controller_id: i32,
    /// Brokers returned by the cluster.
    pub brokers: Vec<ClusterBroker>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub cluster_authorized_operations: i32,
}

/// A group returned by `ListGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    /// Group ID.
    pub group_id: String,
    /// Group protocol type, for example `consumer`.
    pub protocol_type: String,
    /// Group state name when returned by the broker.
    pub group_state: String,
    /// Group type name when returned by the broker.
    pub group_type: String,
}

/// Parsed response from a `ListGroups` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Groups returned by the broker.
    pub groups: Vec<ListedGroup>,
}

/// Result of one group deletion returned by `DeleteGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedGroup {
    /// Group ID.
    pub group_id: String,
    /// Broker error code for this group deletion.
    pub error_code: i16,
}

/// Parsed response from a `DeleteGroups` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-group deletion results returned by the broker.
    pub results: Vec<DeletedGroup>,
}

/// A member returned by `DescribeGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    /// Member ID assigned by the group coordinator.
    pub member_id: String,
    /// Static membership instance ID, when configured.
    pub group_instance_id: Option<String>,
    /// Client ID reported by the member.
    pub client_id: String,
    /// Client host reported by the broker.
    pub client_host: String,
    /// Opaque protocol metadata for the member.
    pub member_metadata: Bytes,
    /// Opaque assignment payload for the member.
    pub member_assignment: Bytes,
}

/// A group returned by `DescribeGroups`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Group state name.
    pub group_state: String,
    /// Group protocol type.
    pub protocol_type: String,
    /// Active protocol name/data selected by the group.
    pub protocol_data: String,
    /// Members in the group.
    pub members: Vec<DescribedGroupMember>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub authorized_operations: i32,
}

/// Parsed response from a `DescribeGroups` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described groups returned by the broker.
    pub groups: Vec<DescribedGroup>,
}

/// Topic partitions in a modern consumer group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupTopicPartitions {
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Topic name.
    pub topic_name: String,
    /// Assigned partition indexes.
    pub partitions: Vec<i32>,
}

/// Assignment returned by `ConsumerGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupAssignment {
    /// Topic partitions in the assignment.
    pub topic_partitions: Vec<ConsumerGroupTopicPartitions>,
}

/// Member state returned by `ConsumerGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMemberDescription {
    /// Member ID assigned by the group coordinator.
    pub member_id: String,
    /// Static membership instance ID, when configured.
    pub instance_id: Option<String>,
    /// Rack ID reported by the member, when configured.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID reported by the member.
    pub client_id: String,
    /// Client host reported by the broker.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Subscribed topic regex, when provided.
    pub subscribed_topic_regex: Option<String>,
    /// Current assignment.
    pub assignment: ConsumerGroupAssignment,
    /// Target assignment during rebalancing.
    pub target_assignment: ConsumerGroupAssignment,
    /// Kafka member type code, or `-1` for unknown on older response versions.
    pub member_type: i8,
}

/// Consumer group state returned by `ConsumerGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescription {
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Current group state.
    pub group_state: String,
    /// Current group epoch.
    pub group_epoch: i32,
    /// Current assignment epoch.
    pub assignment_epoch: i32,
    /// Selected assignor name.
    pub assignor_name: String,
    /// Members in the group.
    pub members: Vec<ConsumerGroupMemberDescription>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub authorized_operations: i32,
}

/// Parsed response from a `ConsumerGroupDescribe` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described consumer groups returned by the broker.
    pub groups: Vec<ConsumerGroupDescription>,
}

/// Topic partitions in a share group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupTopicPartitions {
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Topic name.
    pub topic_name: String,
    /// Assigned partition indexes.
    pub partitions: Vec<i32>,
}

/// Assignment returned by `ShareGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupAssignment {
    /// Topic partitions in the assignment.
    pub topic_partitions: Vec<ShareGroupTopicPartitions>,
}

/// Member state returned by `ShareGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberDescription {
    /// Member ID assigned by the share group coordinator.
    pub member_id: String,
    /// Rack ID reported by the member, when configured.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID reported by the member.
    pub client_id: String,
    /// Client host reported by the broker.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Current assignment.
    pub assignment: ShareGroupAssignment,
}

/// Share group state returned by `ShareGroupDescribe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescription {
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Current group state.
    pub group_state: String,
    /// Current group epoch.
    pub group_epoch: i32,
    /// Current assignment epoch.
    pub assignment_epoch: i32,
    /// Selected assignor name.
    pub assignor_name: String,
    /// Members in the group.
    pub members: Vec<ShareGroupMemberDescription>,
    /// Authorized operations bitfield, or Kafka's sentinel when not requested.
    pub authorized_operations: i32,
}

/// Parsed response from a `ShareGroupDescribe` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescribeResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described share groups returned by the broker.
    pub groups: Vec<ShareGroupDescription>,
}

/// One group included in a `DescribeShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetRequest {
    /// Share group ID.
    pub group_id: String,
    /// Topic partitions to query, or `None` to describe all share-partition offsets for the group.
    pub topics: Option<Vec<TopicPartitionFilter>>,
}

impl ShareGroupOffsetRequest {
    /// Describe all share-partition offsets for a group.
    #[must_use]
    pub fn all_partitions(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: None,
        }
    }

    /// Describe selected share-partition offsets for a group.
    #[must_use]
    pub fn with_topics<I>(group_id: impl Into<String>, topics: I) -> Self
    where
        I: IntoIterator<Item = TopicPartitionFilter>,
    {
        Self {
            group_id: group_id.into(),
            topics: Some(topics.into_iter().collect()),
        }
    }
}

/// Share group offset state for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Share-partition start offset.
    pub start_offset: i64,
    /// Partition leader epoch.
    pub leader_epoch: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional per-partition broker error message.
    pub error_message: Option<String>,
}

/// Share group offset state for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic UUID as a string.
    pub topic_id: String,
    /// Partition offset states.
    pub partitions: Vec<ShareGroupOffsetPartition>,
}

/// Share group offset state for one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetGroup {
    /// Share group ID.
    pub group_id: String,
    /// Topic offset states returned by the broker.
    pub topics: Vec<ShareGroupOffsetTopic>,
    /// Per-group broker error code.
    pub error_code: i16,
    /// Optional per-group broker error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `DescribeShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-group share offset states returned by the broker.
    pub groups: Vec<ShareGroupOffsetGroup>,
}

/// A config synonym returned by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    /// Synonym name.
    pub name: String,
    /// Synonym value, omitted by Kafka for sensitive values.
    pub value: Option<String>,
    /// Raw Kafka config source code for the synonym.
    pub source: i8,
}

/// A config entry returned by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// Config key name.
    pub name: String,
    /// Config value, omitted by Kafka for sensitive values.
    pub value: Option<String>,
    /// Whether the config is read-only.
    pub read_only: bool,
    /// Raw Kafka config source code.
    pub config_source: i8,
    /// Whether the config is sensitive.
    pub is_sensitive: bool,
    /// Config synonyms returned by the broker.
    pub synonyms: Vec<ConfigSynonym>,
    /// Raw Kafka config type code.
    pub config_type: i8,
    /// Optional broker-provided config documentation.
    pub documentation: Option<String>,
}

/// Configs returned for one resource by `DescribeConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    /// Per-resource broker error code.
    pub error_code: i16,
    /// Optional per-resource broker error message.
    pub error_message: Option<String>,
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
    /// Config entries returned for this resource.
    pub configs: Vec<ConfigEntry>,
}

/// Parsed response from a `DescribeConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-resource config results.
    pub results: Vec<DescribeConfigsResult>,
}

/// Partition storage details returned by `DescribeLogDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Size of log segments in bytes.
    pub partition_size: i64,
    /// Log end offset lag relative to the partition watermark or replica log.
    pub offset_lag: i64,
    /// Whether this is a future log created by replica movement.
    pub is_future_key: bool,
}

/// Per-topic storage details inside one log directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirTopic {
    /// Topic name.
    pub name: String,
    /// Partitions present in the log directory.
    pub partitions: Vec<LogDirPartition>,
}

/// One log directory returned by `DescribeLogDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirDescription {
    /// Per-log-directory broker error code.
    pub error_code: i16,
    /// Absolute broker log directory path.
    pub log_dir: String,
    /// Topics present in the log directory.
    pub topics: Vec<LogDirTopic>,
    /// Total bytes on the backing volume, or Kafka's `-1` sentinel before v4.
    pub total_bytes: i64,
    /// Usable bytes on the backing volume, or Kafka's `-1` sentinel before v4.
    pub usable_bytes: i64,
}

/// Parsed response from a `DescribeLogDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Log directories returned by the broker.
    pub results: Vec<LogDirDescription>,
}

/// Ongoing partition reassignment details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    /// Partition index.
    pub partition_index: i32,
    /// Current replica set.
    pub replicas: Vec<i32>,
    /// Replicas currently being added.
    pub adding_replicas: Vec<i32>,
    /// Replicas currently being removed.
    pub removing_replicas: Vec<i32>,
}

/// Ongoing reassignments for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicReassignment {
    /// Topic name.
    pub name: String,
    /// Ongoing partition reassignments.
    pub partitions: Vec<PartitionReassignment>,
}

/// Parsed response from a `ListPartitionReassignments` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Ongoing reassignments returned by the broker.
    pub topics: Vec<TopicReassignment>,
}

/// One ACL entry returned by `DescribeAcls`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclDescription {
    /// ACL principal string, such as `User:alice`.
    pub principal: String,
    /// Host to which the ACL applies.
    pub host: String,
    /// Raw Kafka ACL operation code.
    pub operation: i8,
    /// Raw Kafka ACL permission type code.
    pub permission_type: i8,
}

/// ACLs grouped by resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclResource {
    /// Raw Kafka ACL resource type code.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
    /// Raw Kafka ACL pattern type code.
    pub pattern_type: i8,
    /// ACL entries on the resource.
    pub acls: Vec<AclDescription>,
}

/// Parsed response from a `DescribeAcls` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// ACL resources returned by the broker.
    pub resources: Vec<AclResource>,
}

/// Result of one ACL creation returned by `CreateAcls`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclResult {
    /// Broker error code for this ACL creation.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `CreateAcls` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-ACL creation results returned by the broker.
    pub results: Vec<CreateAclResult>,
}

/// One ACL matched and deleted by a `DeleteAcls` filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedAcl {
    /// Per-ACL deletion error code.
    pub error_code: i16,
    /// Optional per-ACL deletion error message.
    pub error_message: Option<String>,
    /// Raw Kafka ACL resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
    /// Raw Kafka ACL resource pattern type.
    pub pattern_type: i8,
    /// ACL principal string, such as `User:alice`.
    pub principal: String,
    /// Host to which the ACL applied.
    pub host: String,
    /// Raw Kafka ACL operation code.
    pub operation: i8,
    /// Raw Kafka ACL permission type code.
    pub permission_type: i8,
}

/// Result for one `DeleteAcls` filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsFilterResult {
    /// Per-filter broker error code.
    pub error_code: i16,
    /// Optional per-filter broker error message.
    pub error_message: Option<String>,
    /// ACLs that matched the filter and were deleted or attempted.
    pub matching_acls: Vec<DeletedAcl>,
}

/// Parsed response from a `DeleteAcls` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-filter delete results returned by the broker.
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

/// One delegation token returned by `DescribeDelegationToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationTokenDescription {
    /// Owner principal.
    pub owner: KafkaPrincipal,
    /// Requester principal when returned by Kafka v3+.
    pub requester: Option<KafkaPrincipal>,
    /// Token issue timestamp in milliseconds since Unix epoch.
    pub issue_timestamp: i64,
    /// Token expiry timestamp in milliseconds since Unix epoch.
    pub expiry_timestamp: i64,
    /// Token maximum timestamp in milliseconds since Unix epoch.
    pub max_timestamp: i64,
    /// Token ID.
    pub token_id: String,
    /// Broker-provided token HMAC. Treat this value as sensitive credential material.
    pub hmac: Bytes,
    /// Principals allowed to renew this token.
    pub renewers: Vec<KafkaPrincipal>,
}

/// Parsed response from a `DescribeDelegationToken` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokenResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Delegation tokens returned by the broker.
    pub tokens: Vec<DelegationTokenDescription>,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

/// Options for creating a Kafka delegation token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenOptions {
    /// Optional owner principal; Kafka defaults this to the request principal when absent.
    pub owner: Option<KafkaPrincipal>,
    /// Principals allowed to renew the token.
    pub renewers: Vec<KafkaPrincipal>,
    /// Maximum token lifetime in milliseconds, or `-1` to use the broker default.
    pub max_lifetime_ms: i64,
}

impl Default for CreateDelegationTokenOptions {
    fn default() -> Self {
        Self {
            owner: None,
            renewers: Vec::new(),
            max_lifetime_ms: -1,
        }
    }
}

impl CreateDelegationTokenOptions {
    /// Create options using Kafka's broker-side defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the token owner principal.
    #[must_use]
    pub fn with_owner(mut self, owner: KafkaPrincipal) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Add one token renewer principal.
    #[must_use]
    pub fn with_renewer(mut self, renewer: KafkaPrincipal) -> Self {
        self.renewers.push(renewer);
        self
    }

    /// Replace the token renewer principals.
    #[must_use]
    pub fn with_renewers<I>(mut self, renewers: I) -> Self
    where
        I: IntoIterator<Item = KafkaPrincipal>,
    {
        self.renewers = renewers.into_iter().collect();
        self
    }

    /// Set the maximum token lifetime in milliseconds.
    #[must_use]
    pub fn with_max_lifetime_ms(mut self, max_lifetime_ms: i64) -> Self {
        self.max_lifetime_ms = max_lifetime_ms;
        self
    }
}

/// Parsed response from a `CreateDelegationToken` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Owner principal for the token.
    pub owner: KafkaPrincipal,
    /// Requester principal returned by Kafka v3+.
    pub requester: Option<KafkaPrincipal>,
    /// Token issue timestamp in milliseconds since Unix epoch.
    pub issue_timestamp: i64,
    /// Token expiry timestamp in milliseconds since Unix epoch.
    pub expiry_timestamp: i64,
    /// Token maximum timestamp in milliseconds since Unix epoch.
    pub max_timestamp: i64,
    /// Token ID.
    pub token_id: String,
    /// Broker-provided token HMAC. Treat this value as sensitive credential material.
    pub hmac: Bytes,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

/// Parsed response from a `RenewDelegationToken` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewDelegationTokenResponseData {
    /// Broker error code.
    pub error_code: i16,
    /// Token expiry timestamp in milliseconds since Unix epoch.
    pub expiry_timestamp_ms: i64,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

/// Parsed response from an `ExpireDelegationToken` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpireDelegationTokenResponseData {
    /// Broker error code.
    pub error_code: i16,
    /// Token expiry timestamp in milliseconds since Unix epoch.
    pub expiry_timestamp_ms: i64,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

/// One entity component returned by `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntity {
    /// Kafka quota entity type.
    pub entity_type: String,
    /// Entity name, or `None` for Kafka's default entity.
    pub entity_name: Option<String>,
}

/// One quota key/value returned by `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaValue {
    /// Quota configuration key.
    pub key: String,
    /// Quota configuration value.
    pub value: f64,
}

/// One quota entity entry returned by `DescribeClientQuotas`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaEntry {
    /// Entity components that identify this quota entry.
    pub entity: Vec<ClientQuotaEntity>,
    /// Quota values configured for the entity.
    pub values: Vec<ClientQuotaValue>,
}

/// Parsed response from a `DescribeClientQuotas` request.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// Quota entries returned by the broker.
    pub entries: Option<Vec<ClientQuotaEntry>>,
}

/// One quota entity result returned by `AlterClientQuotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotaEntryResult {
    /// Per-entry broker error code.
    pub error_code: i16,
    /// Optional per-entry broker error message.
    pub error_message: Option<String>,
    /// Entity components that identify this quota entry.
    pub entity: Vec<ClientQuotaEntity>,
}

/// Parsed response from an `AlterClientQuotas` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotasResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-entity quota alteration results returned by the broker.
    pub entries: Vec<AlterClientQuotaEntryResult>,
}

/// One SCRAM mechanism configured for a user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialInfo {
    /// Raw Kafka SCRAM mechanism code.
    pub mechanism: i8,
    /// Iteration count used for this SCRAM credential.
    pub iterations: i32,
}

/// SCRAM credential description for one user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentialsDescription {
    /// User name.
    pub user: String,
    /// Per-user broker error code.
    pub error_code: i16,
    /// Optional per-user broker error message.
    pub error_message: Option<String>,
    /// SCRAM credentials configured for this user.
    pub credential_infos: Vec<ScramCredentialInfo>,
}

/// Parsed response from a `DescribeUserScramCredentials` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Optional top-level broker error message.
    pub error_message: Option<String>,
    /// User credential descriptions returned by the broker.
    pub results: Vec<UserScramCredentialsDescription>,
}

/// SCRAM credential deletion operation for one user/mechanism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialDeletion {
    /// User name.
    pub name: String,
    /// Raw Kafka SCRAM mechanism code.
    pub mechanism: i8,
}

impl ScramCredentialDeletion {
    /// Create a SCRAM credential deletion.
    #[must_use]
    pub fn new(name: impl Into<String>, mechanism: i8) -> Self {
        Self {
            name: name.into(),
            mechanism,
        }
    }
}

/// SCRAM credential upsertion operation for one user/mechanism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialUpsertion {
    /// User name.
    pub name: String,
    /// Raw Kafka SCRAM mechanism code.
    pub mechanism: i8,
    /// SCRAM iteration count.
    pub iterations: i32,
    /// Client-generated salt bytes.
    pub salt: Bytes,
    /// Precomputed salted password bytes for the selected SCRAM mechanism.
    pub salted_password: Bytes,
}

impl ScramCredentialUpsertion {
    /// Create a SCRAM credential upsertion with precomputed salted password material.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        mechanism: i8,
        iterations: i32,
        salt: impl Into<Bytes>,
        salted_password: impl Into<Bytes>,
    ) -> Self {
        Self {
            name: name.into(),
            mechanism,
            iterations,
            salt: salt.into(),
            salted_password: salted_password.into(),
        }
    }
}

/// Options for an `AlterUserScramCredentials` request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlterUserScramCredentialsOptions {
    /// SCRAM credentials to delete.
    pub deletions: Vec<ScramCredentialDeletion>,
    /// SCRAM credentials to upsert.
    pub upsertions: Vec<ScramCredentialUpsertion>,
}

impl AlterUserScramCredentialsOptions {
    /// Create empty SCRAM credential mutation options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one SCRAM credential deletion.
    #[must_use]
    pub fn with_deletion(mut self, deletion: ScramCredentialDeletion) -> Self {
        self.deletions.push(deletion);
        self
    }

    /// Replace SCRAM credential deletions.
    #[must_use]
    pub fn with_deletions<I>(mut self, deletions: I) -> Self
    where
        I: IntoIterator<Item = ScramCredentialDeletion>,
    {
        self.deletions = deletions.into_iter().collect();
        self
    }

    /// Add one SCRAM credential upsertion.
    #[must_use]
    pub fn with_upsertion(mut self, upsertion: ScramCredentialUpsertion) -> Self {
        self.upsertions.push(upsertion);
        self
    }

    /// Replace SCRAM credential upsertions.
    #[must_use]
    pub fn with_upsertions<I>(mut self, upsertions: I) -> Self
    where
        I: IntoIterator<Item = ScramCredentialUpsertion>,
    {
        self.upsertions = upsertions.into_iter().collect();
        self
    }
}

/// Per-user result returned by `AlterUserScramCredentials`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialResult {
    /// User name.
    pub user: String,
    /// Per-user broker error code.
    pub error_code: i16,
    /// Optional per-user broker error message.
    pub error_message: Option<String>,
}

/// Parsed response from an `AlterUserScramCredentials` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-user mutation results returned by the broker.
    pub results: Vec<AlterUserScramCredentialResult>,
}

/// State for one active producer returned by `DescribeProducers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveProducer {
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// Last sequence number sent by the producer.
    pub last_sequence: i32,
    /// Last timestamp sent by the producer.
    pub last_timestamp: i64,
    /// Current epoch of the producer group coordinator.
    pub coordinator_epoch: i32,
    /// Current transaction start offset, or Kafka's sentinel when absent.
    pub current_txn_start_offset: i64,
}

/// Producer state for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional per-partition broker error message.
    pub error_message: Option<String>,
    /// Active producers returned for the partition.
    pub active_producers: Vec<ActiveProducer>,
}

/// Producer state for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerTopic {
    /// Topic name.
    pub name: String,
    /// Partition producer states.
    pub partitions: Vec<ProducerPartition>,
}

/// Parsed response from a `DescribeProducers` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topics returned by the broker.
    pub topics: Vec<ProducerTopic>,
}

/// Summary state for one transaction returned by `ListTransactions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTransaction {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID currently associated with the transaction.
    pub producer_id: i64,
    /// Current transaction state.
    pub transaction_state: String,
}

/// Parsed response from a `ListTransactions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Requested state filters unknown to the transaction coordinator.
    pub unknown_state_filters: Vec<String>,
    /// Transaction summaries returned by the broker.
    pub transaction_states: Vec<ListedTransaction>,
}

/// Topic partitions included in a described transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionTopic {
    /// Topic name.
    pub topic: String,
    /// Partition IDs included in the transaction.
    pub partitions: Vec<i32>,
}

/// Detailed state for one transaction returned by `DescribeTransactions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTransaction {
    /// Per-transaction broker error code.
    pub error_code: i16,
    /// Transactional ID.
    pub transactional_id: String,
    /// Current transaction state.
    pub transaction_state: String,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Transaction start time in milliseconds since Unix epoch.
    pub transaction_start_time_ms: i64,
    /// Producer ID currently associated with the transaction.
    pub producer_id: i64,
    /// Producer epoch currently associated with the transaction.
    pub producer_epoch: i16,
    /// Topic partitions included in the current transaction.
    pub topics: Vec<TransactionTopic>,
}

/// Parsed response from a `DescribeTransactions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Detailed transaction states returned by the broker.
    pub transaction_states: Vec<DescribedTransaction>,
}

/// Parsed response from an `AddOffsetsToTxn` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
}

/// Per-partition result in a `TxnOffsetCommit` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result in a `TxnOffsetCommit` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopicResult {
    /// Topic name.
    pub topic: String,
    /// Per-partition commit results.
    pub partitions: Vec<TxnOffsetCommitPartitionResult>,
}

/// Parsed response from a `TxnOffsetCommit` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic offset commit results.
    pub topics: Vec<TxnOffsetCommitTopicResult>,
}

/// A topic/partition/offset tuple for `TxnOffsetCommit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Offset to commit.
    pub offset: i64,
    /// Optional leader epoch.
    pub leader_epoch: Option<i32>,
    /// Optional metadata string.
    pub metadata: Option<String>,
}

impl TxnOffsetCommitTopicPartition {
    /// Create a transactional offset commit entry.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            leader_epoch: None,
            metadata: None,
        }
    }

    /// Set the committed leader epoch.
    #[must_use]
    pub fn with_leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = Some(leader_epoch);
        self
    }

    /// Set committed offset metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }
}

/// Upgrade type for `UpdateFeatures`: upgrade only (default).
pub const FEATURE_UPGRADE_TYPE_UPGRADE: i8 = 1;
/// Upgrade type for `UpdateFeatures`: safe downgrade only (lossless).
pub const FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE: i8 = 2;
/// Upgrade type for `UpdateFeatures`: unsafe downgrade (lossy).
pub const FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE: i8 = 3;

/// A feature to update via `UpdateFeatures`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    /// The feature name to update.
    pub feature: String,
    /// The new maximum version level.
    pub max_version_level: i16,
    /// The upgrade type (1=upgrade, 2=safe downgrade, 3=unsafe downgrade).
    pub upgrade_type: i8,
}

impl FeatureUpdate {
    /// Create a feature upgrade.
    #[must_use]
    pub fn upgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FEATURE_UPGRADE_TYPE_UPGRADE,
        }
    }

    /// Create a safe (lossless) downgrade.
    #[must_use]
    pub fn safe_downgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FEATURE_UPGRADE_TYPE_SAFE_DOWNGRADE,
        }
    }

    /// Create an unsafe (lossy) downgrade.
    #[must_use]
    pub fn unsafe_downgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FEATURE_UPGRADE_TYPE_UNSAFE_DOWNGRADE,
        }
    }
}

/// Per-feature result from `UpdateFeatures`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResult {
    /// The feature name.
    pub feature: String,
    /// Per-feature broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from an `UpdateFeatures` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Per-feature update results.
    pub results: Vec<UpdateFeaturesResult>,
}

/// Parsed response from an `UnregisterBroker` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// A network listener used when adding or updating a `KRaft` voter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterListener {
    /// Listener name, such as `CONTROLLER`.
    pub name: String,
    /// Listener host name.
    pub host: String,
    /// Listener port.
    pub port: u16,
}

impl RaftVoterListener {
    /// Create a `KRaft` voter listener.
    #[must_use]
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            port,
        }
    }
}

/// Options for an `AddRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRaftVoterOptions {
    /// Optional cluster ID expected by the controller quorum.
    pub cluster_id: Option<String>,
    /// Broker-side timeout in milliseconds.
    pub timeout_ms: i32,
    /// Replica ID of the voter to add.
    pub voter_id: i32,
    /// Directory ID of the voter to add.
    pub voter_directory_id: Uuid,
    /// Controller listeners for the voter.
    pub listeners: Vec<RaftVoterListener>,
}

impl AddRaftVoterOptions {
    /// Create options for adding a `KRaft` voter.
    #[must_use]
    pub fn new<I>(voter_id: i32, voter_directory_id: Uuid, listeners: I) -> Self
    where
        I: IntoIterator<Item = RaftVoterListener>,
    {
        Self {
            cluster_id: None,
            timeout_ms: 60_000,
            voter_id,
            voter_directory_id,
            listeners: listeners.into_iter().collect(),
        }
    }

    /// Set the expected cluster ID.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Clear the expected cluster ID.
    #[must_use]
    pub fn without_cluster_id(mut self) -> Self {
        self.cluster_id = None;
        self
    }

    /// Set the broker-side timeout in milliseconds.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Options for a `RemoveRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveRaftVoterOptions {
    /// Optional cluster ID expected by the controller quorum.
    pub cluster_id: Option<String>,
    /// Replica ID of the voter to remove.
    pub voter_id: i32,
    /// Directory ID of the voter to remove.
    pub voter_directory_id: Uuid,
}

impl RemoveRaftVoterOptions {
    /// Create options for removing a `KRaft` voter.
    #[must_use]
    pub fn new(voter_id: i32, voter_directory_id: Uuid) -> Self {
        Self {
            cluster_id: None,
            voter_id,
            voter_directory_id,
        }
    }

    /// Set the expected cluster ID.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Clear the expected cluster ID.
    #[must_use]
    pub fn without_cluster_id(mut self) -> Self {
        self.cluster_id = None;
        self
    }
}

/// Supported `KRaft` protocol version range for `UpdateRaftVoter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftVersionFeature {
    /// Minimum supported `KRaft` protocol version.
    pub min_supported_version: i16,
    /// Maximum supported `KRaft` protocol version.
    pub max_supported_version: i16,
}

impl RaftVersionFeature {
    /// Create a supported `KRaft` protocol version range.
    #[must_use]
    pub fn new(min_supported_version: i16, max_supported_version: i16) -> Self {
        Self {
            min_supported_version,
            max_supported_version,
        }
    }
}

/// Options for an `UpdateRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRaftVoterOptions {
    /// Optional cluster ID expected by the controller quorum.
    pub cluster_id: Option<String>,
    /// Current leader epoch, or `-1` when unknown.
    pub current_leader_epoch: i32,
    /// Replica ID of the voter to update.
    pub voter_id: i32,
    /// Directory ID of the voter to update.
    pub voter_directory_id: Uuid,
    /// Controller listeners for the voter.
    pub listeners: Vec<RaftVoterListener>,
    /// Supported `KRaft` protocol version range.
    pub raft_version_feature: RaftVersionFeature,
}

impl UpdateRaftVoterOptions {
    /// Create options for updating a `KRaft` voter.
    #[must_use]
    pub fn new<I>(
        voter_id: i32,
        voter_directory_id: Uuid,
        listeners: I,
        raft_version_feature: RaftVersionFeature,
    ) -> Self
    where
        I: IntoIterator<Item = RaftVoterListener>,
    {
        Self {
            cluster_id: None,
            current_leader_epoch: -1,
            voter_id,
            voter_directory_id,
            listeners: listeners.into_iter().collect(),
            raft_version_feature,
        }
    }

    /// Set the expected cluster ID.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Clear the expected cluster ID.
    #[must_use]
    pub fn without_cluster_id(mut self) -> Self {
        self.cluster_id = None;
        self
    }

    /// Set the current leader epoch.
    #[must_use]
    pub fn with_current_leader_epoch(mut self, current_leader_epoch: i32) -> Self {
        self.current_leader_epoch = current_leader_epoch;
        self
    }
}

/// Parsed response from `AddRaftVoter` or `RemoveRaftVoter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Current leader details returned by `UpdateRaftVoter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterCurrentLeader {
    /// Replica ID of the current leader, or `-1` when unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// Leader host name.
    pub host: String,
    /// Leader port.
    pub port: i32,
}

/// Parsed response from an `UpdateRaftVoter` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRaftVoterResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Broker error code.
    pub error_code: i16,
    /// Current leader details when Kafka returned the optional tagged field.
    pub current_leader: Option<RaftVoterCurrentLeader>,
}

/// One topic assignment for `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryTopicAssignment {
    /// Topic ID to assign.
    pub topic_id: Uuid,
    /// Partition indexes to assign to the directory.
    pub partitions: Vec<i32>,
}

impl ReplicaDirectoryTopicAssignment {
    /// Create a topic assignment.
    #[must_use]
    pub fn new<I>(topic_id: Uuid, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// One directory assignment for `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryAssignment {
    /// Directory ID.
    pub directory_id: Uuid,
    /// Topic assignments for this directory.
    pub topics: Vec<ReplicaDirectoryTopicAssignment>,
}

impl ReplicaDirectoryAssignment {
    /// Create a directory assignment.
    #[must_use]
    pub fn new<I>(directory_id: Uuid, topics: I) -> Self
    where
        I: IntoIterator<Item = ReplicaDirectoryTopicAssignment>,
    {
        Self {
            directory_id,
            topics: topics.into_iter().collect(),
        }
    }
}

/// Options for an `AssignReplicasToDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsOptions {
    /// ID of the requesting broker.
    pub broker_id: i32,
    /// Epoch of the requesting broker.
    pub broker_epoch: i64,
    /// Directory assignments to apply.
    pub directories: Vec<ReplicaDirectoryAssignment>,
}

impl AssignReplicasToDirsOptions {
    /// Create options for assigning replicas to log directories.
    #[must_use]
    pub fn new<I>(broker_id: i32, broker_epoch: i64, directories: I) -> Self
    where
        I: IntoIterator<Item = ReplicaDirectoryAssignment>,
    {
        Self {
            broker_id,
            broker_epoch,
            directories: directories.into_iter().collect(),
        }
    }
}

/// Per-partition result from `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Partition-level broker error code.
    pub error_code: i16,
}

/// Per-topic result from `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryTopicResult {
    /// Topic ID.
    pub topic_id: Uuid,
    /// Per-partition results.
    pub partitions: Vec<ReplicaDirectoryPartitionResult>,
}

/// Per-directory result from `AssignReplicasToDirs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDirectoryAssignmentResult {
    /// Directory ID.
    pub directory_id: Uuid,
    /// Per-topic results.
    pub topics: Vec<ReplicaDirectoryTopicResult>,
}

/// Parsed response from an `AssignReplicasToDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReplicasToDirsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level broker error code.
    pub error_code: i16,
    /// Per-directory assignment results.
    pub directories: Vec<ReplicaDirectoryAssignmentResult>,
}

/// A partition offset to alter for a share group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The new offset value.
    pub offset: i64,
}

impl AlterShareGroupOffsetPartition {
    /// Create a partition offset spec.
    #[must_use]
    pub fn new(partition_index: i32, offset: i64) -> Self {
        Self {
            partition_index,
            offset,
        }
    }
}

/// A topic with partition offsets for `AlterShareGroupOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partition offsets to alter.
    pub partitions: Vec<AlterShareGroupOffsetPartition>,
}

impl AlterShareGroupOffsetTopic {
    /// Create a topic with partition offsets.
    #[must_use]
    pub fn new<I>(topic_name: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = AlterShareGroupOffsetPartition>,
    {
        Self {
            topic_name: topic_name.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// A partition result in `AlterShareGroupOffsets` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// A topic result in `AlterShareGroupOffsets` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition results.
    pub partitions: Vec<AlterShareGroupOffsetPartitionResult>,
}

/// Parsed response from an `AlterShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Per-topic results.
    pub responses: Vec<AlterShareGroupOffsetTopicResult>,
}

/// A topic whose share-group offsets are deleted by `DeleteShareGroupOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetTopic {
    /// Topic name.
    pub topic_name: String,
}

impl DeleteShareGroupOffsetTopic {
    /// Create a topic whose share-group offsets should be deleted.
    #[must_use]
    pub fn new(topic_name: impl Into<String>) -> Self {
        Self {
            topic_name: topic_name.into(),
        }
    }
}

/// A topic result in `DeleteShareGroupOffsets` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-topic broker error code.
    pub error_code: i16,
    /// Optional broker-provided error message.
    pub error_message: Option<String>,
}

/// Parsed response from a `DeleteShareGroupOffsets` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: i16,
    /// Optional top-level error message.
    pub error_message: Option<String>,
    /// Per-topic results.
    pub responses: Vec<DeleteShareGroupOffsetTopicResult>,
}

/// A config key-value pair for the legacy `AlterConfigs` API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsEntry {
    /// Configuration key name.
    pub name: String,
    /// Configuration value, or `None` to reset to default.
    pub value: Option<String>,
}

impl AlterConfigsEntry {
    /// Create a config entry with a value.
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    /// Create a config entry that resets to default.
    #[must_use]
    pub fn reset(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }
}

/// One resource for the legacy `AlterConfigs` API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResource {
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name, such as a topic name or broker ID.
    pub resource_name: String,
    /// Config entries to apply.
    pub configs: Vec<AlterConfigsEntry>,
}

impl AlterConfigsResource {
    /// Create a config resource with a raw Kafka resource type.
    #[must_use]
    pub fn new<I>(resource_type: i8, resource_name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsEntry>,
    {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            configs: configs.into_iter().collect(),
        }
    }

    /// Create a topic config resource.
    #[must_use]
    pub fn topic<I>(name: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsEntry>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_TOPIC, name, configs)
    }

    /// Create a broker config resource.
    #[must_use]
    pub fn broker<I>(id: impl Into<String>, configs: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsEntry>,
    {
        Self::new(CONFIG_RESOURCE_TYPE_BROKER, id, configs)
    }
}

/// Options for a legacy `AlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsOptions {
    /// Resources to update.
    pub resources: Vec<AlterConfigsResource>,
    /// Validate the request without applying it.
    pub validate_only: bool,
}

impl AlterConfigsOptions {
    /// Create options with the supplied resources.
    #[must_use]
    pub fn new<I>(resources: I) -> Self
    where
        I: IntoIterator<Item = AlterConfigsResource>,
    {
        Self {
            resources: resources.into_iter().collect(),
            validate_only: false,
        }
    }

    /// Validate the request without applying it.
    #[must_use]
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }
}

/// Per-resource result returned by the legacy `AlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResourceResult {
    /// Per-resource broker error code.
    pub error_code: i16,
    /// Optional per-resource broker error message.
    pub error_message: Option<String>,
    /// Kafka config resource type.
    pub resource_type: i8,
    /// Resource name.
    pub resource_name: String,
}

/// Parsed response from a legacy `AlterConfigs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-resource config mutation results.
    pub responses: Vec<AlterConfigsResourceResult>,
}

/// A log directory path with topic partitions to move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to move.
    pub partitions: Vec<i32>,
}

impl AlterReplicaLogDirTopic {
    /// Create a topic partition spec for log dir alteration.
    #[must_use]
    pub fn new<I>(topic: impl Into<String>, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }
}

/// A log directory with topic partitions to move there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDir {
    /// Absolute directory path.
    pub path: String,
    /// Topics with partitions to move to this directory.
    pub topics: Vec<AlterReplicaLogDirTopic>,
}

impl AlterReplicaLogDir {
    /// Create a log directory spec.
    #[must_use]
    pub fn new(path: impl Into<String>, topics: Vec<AlterReplicaLogDirTopic>) -> Self {
        Self {
            path: path.into(),
            topics,
        }
    }
}

/// Per-partition result in an `AlterReplicaLogDirs` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition broker error code.
    pub error_code: i16,
}

/// Per-topic result in an `AlterReplicaLogDirs` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition results.
    pub partitions: Vec<AlterReplicaLogDirPartitionResult>,
}

/// Parsed response from an `AlterReplicaLogDirs` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponseData {
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic results.
    pub results: Vec<AlterReplicaLogDirTopicResult>,
}
