//! Kafka administration protocol helpers.

use bytes::Bytes;
use kafka_protocol::messages::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterPartitionReassignmentsRequest,
    AlterPartitionReassignmentsResponse, ApiKey, ConsumerGroupDescribeRequest,
    ConsumerGroupDescribeResponse, CreateAclsRequest, CreateAclsResponse, CreatePartitionsRequest,
    CreatePartitionsResponse, DeleteAclsRequest, DeleteAclsResponse, DeleteGroupsRequest,
    DeleteGroupsResponse, DeleteRecordsRequest, DeleteRecordsResponse, DescribeAclsRequest,
    DescribeAclsResponse, DescribeClientQuotasRequest, DescribeClientQuotasResponse,
    DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsRequest,
    DescribeConfigsResponse, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse,
    DescribeProducersRequest, DescribeProducersResponse, DescribeQuorumRequest,
    DescribeQuorumResponse, DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, DescribeTransactionsRequest,
    DescribeTransactionsResponse, DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentialsResponse, ElectLeadersRequest, ElectLeadersResponse, GroupId,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, ListConfigResourcesRequest,
    ListConfigResourcesResponse, ListGroupsRequest, ListGroupsResponse,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, ListTransactionsRequest,
    ListTransactionsResponse, OffsetDeleteRequest, OffsetDeleteResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, RequestHeader,
    ShareGroupDescribeRequest, ShareGroupDescribeResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::{
    API_VERSION_ALTER_CLIENT_QUOTAS, API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
    API_VERSION_CONSUMER_GROUP_DESCRIBE, API_VERSION_CREATE_ACLS, API_VERSION_CREATE_PARTITIONS,
    API_VERSION_DELETE_ACLS, API_VERSION_DELETE_GROUPS, API_VERSION_DELETE_RECORDS,
    API_VERSION_DESCRIBE_ACLS, API_VERSION_DESCRIBE_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLUSTER,
    API_VERSION_DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_DELEGATION_TOKEN,
    API_VERSION_DESCRIBE_GROUPS, API_VERSION_DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_PRODUCERS,
    API_VERSION_DESCRIBE_QUORUM, API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
    API_VERSION_DESCRIBE_TOPIC_PARTITIONS, API_VERSION_DESCRIBE_TRANSACTIONS,
    API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS, API_VERSION_ELECT_LEADERS,
    API_VERSION_INCREMENTAL_ALTER_CONFIGS, API_VERSION_LIST_CONFIG_RESOURCES,
    API_VERSION_LIST_GROUPS, API_VERSION_LIST_PARTITION_REASSIGNMENTS,
    API_VERSION_LIST_TRANSACTIONS, API_VERSION_OFFSET_DELETE, API_VERSION_OFFSET_FOR_LEADER_EPOCH,
    API_VERSION_SHARE_GROUP_DESCRIBE,
};

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

/// Build a `DescribeCluster` request.
pub fn build_describe_cluster_request(
    correlation_id: i32,
    client_id: &str,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
) -> (RequestHeader, DescribeClusterRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeCluster,
        API_VERSION_DESCRIBE_CLUSTER,
    );
    let request = DescribeClusterRequest::default()
        .with_include_cluster_authorized_operations(include_authorized_operations)
        .with_endpoint_type(DESCRIBE_CLUSTER_ENDPOINT_BROKERS)
        .with_include_fenced_brokers(include_fenced_brokers);

    (header, request)
}

/// Build a `ListGroups` request.
pub fn build_list_groups_request(
    correlation_id: i32,
    client_id: &str,
    states_filter: &[&str],
    types_filter: &[&str],
) -> (RequestHeader, ListGroupsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListGroups,
        API_VERSION_LIST_GROUPS,
    );
    let request = ListGroupsRequest::default()
        .with_states_filter(str_bytes_vec(states_filter))
        .with_types_filter(str_bytes_vec(types_filter));

    (header, request)
}

/// Build a `DeleteGroups` request.
pub fn build_delete_groups_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
) -> (RequestHeader, DeleteGroupsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DeleteGroups,
        API_VERSION_DELETE_GROUPS,
    );
    let request = DeleteGroupsRequest::default()
        .with_groups_names(groups.iter().map(|group| group_id(group)).collect());

    (header, request)
}

/// Build a `DescribeGroups` request.
pub fn build_describe_groups_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
    include_authorized_operations: bool,
) -> (RequestHeader, DescribeGroupsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeGroups,
        API_VERSION_DESCRIBE_GROUPS,
    );
    let request = DescribeGroupsRequest::default()
        .with_groups(groups.iter().map(|g| group_id(g)).collect())
        .with_include_authorized_operations(include_authorized_operations);

    (header, request)
}

/// Build a `DescribeAcls` request.
pub fn build_describe_acls_request(
    correlation_id: i32,
    client_id: &str,
    filter: &DescribeAclsFilter,
) -> (RequestHeader, DescribeAclsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeAcls,
        API_VERSION_DESCRIBE_ACLS,
    );
    let request = DescribeAclsRequest::default()
        .with_resource_type_filter(filter.resource_type_filter)
        .with_resource_name_filter(
            filter
                .resource_name_filter
                .as_ref()
                .map(|name| StrBytes::from_string(name.clone())),
        )
        .with_pattern_type_filter(filter.pattern_type_filter)
        .with_principal_filter(
            filter
                .principal_filter
                .as_ref()
                .map(|principal| StrBytes::from_string(principal.clone())),
        )
        .with_host_filter(
            filter
                .host_filter
                .as_ref()
                .map(|host| StrBytes::from_string(host.clone())),
        )
        .with_operation(filter.operation)
        .with_permission_type(filter.permission_type);

    (header, request)
}

/// Build a `CreateAcls` request.
pub fn build_create_acls_request(
    correlation_id: i32,
    client_id: &str,
    bindings: &[AclBinding],
) -> (RequestHeader, CreateAclsRequest) {
    use kafka_protocol::messages::create_acls_request::AclCreation;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::CreateAcls,
        API_VERSION_CREATE_ACLS,
    );
    let creations = bindings
        .iter()
        .map(|binding| {
            AclCreation::default()
                .with_resource_type(binding.resource_type)
                .with_resource_name(StrBytes::from_string(binding.resource_name.clone()))
                .with_resource_pattern_type(binding.resource_pattern_type)
                .with_principal(StrBytes::from_string(binding.principal.clone()))
                .with_host(StrBytes::from_string(binding.host.clone()))
                .with_operation(binding.operation)
                .with_permission_type(binding.permission_type)
        })
        .collect();
    let request = CreateAclsRequest::default().with_creations(creations);

    (header, request)
}

/// Build a `DeleteAcls` request.
pub fn build_delete_acls_request(
    correlation_id: i32,
    client_id: &str,
    filters: &[DescribeAclsFilter],
) -> (RequestHeader, DeleteAclsRequest) {
    use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter as KafkaDeleteAclsFilter;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DeleteAcls,
        API_VERSION_DELETE_ACLS,
    );
    let filters = filters
        .iter()
        .map(|filter| {
            KafkaDeleteAclsFilter::default()
                .with_resource_type_filter(filter.resource_type_filter)
                .with_resource_name_filter(
                    filter
                        .resource_name_filter
                        .as_ref()
                        .map(|name| StrBytes::from_string(name.clone())),
                )
                .with_pattern_type_filter(filter.pattern_type_filter)
                .with_principal_filter(
                    filter
                        .principal_filter
                        .as_ref()
                        .map(|principal| StrBytes::from_string(principal.clone())),
                )
                .with_host_filter(
                    filter
                        .host_filter
                        .as_ref()
                        .map(|host| StrBytes::from_string(host.clone())),
                )
                .with_operation(filter.operation)
                .with_permission_type(filter.permission_type)
        })
        .collect();
    let request = DeleteAclsRequest::default().with_filters(filters);

    (header, request)
}

/// Build a `DescribeConfigs` request.
pub fn build_describe_configs_request(
    correlation_id: i32,
    client_id: &str,
    resources: &[ConfigResource],
    include_synonyms: bool,
    include_documentation: bool,
) -> (RequestHeader, DescribeConfigsRequest) {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeConfigs,
        API_VERSION_DESCRIBE_CONFIGS,
    );
    let resources = resources
        .iter()
        .map(|resource| {
            DescribeConfigsResource::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(StrBytes::from_string(resource.resource_name.clone()))
                .with_configuration_keys(resource.configuration_keys.as_ref().map(|keys| {
                    keys.iter()
                        .map(|key| StrBytes::from_string(key.clone()))
                        .collect()
                }))
        })
        .collect();
    let request = DescribeConfigsRequest::default()
        .with_resources(resources)
        .with_include_synonyms(include_synonyms)
        .with_include_documentation(include_documentation);

    (header, request)
}

/// Build an `IncrementalAlterConfigs` request.
pub fn build_incremental_alter_configs_request(
    correlation_id: i32,
    client_id: &str,
    options: &IncrementalAlterConfigsOptions,
) -> (RequestHeader, IncrementalAlterConfigsRequest) {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::IncrementalAlterConfigs,
        API_VERSION_INCREMENTAL_ALTER_CONFIGS,
    );
    let resources = options
        .resources
        .iter()
        .map(|resource| {
            AlterConfigsResource::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(StrBytes::from_string(resource.resource_name.clone()))
                .with_configs(
                    resource
                        .configs
                        .iter()
                        .map(|config| {
                            AlterableConfig::default()
                                .with_name(StrBytes::from_string(config.name.clone()))
                                .with_config_operation(config.operation)
                                .with_value(
                                    config
                                        .value
                                        .as_ref()
                                        .map(|value| StrBytes::from_string(value.clone())),
                                )
                        })
                        .collect(),
                )
        })
        .collect();
    let request = IncrementalAlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build a `DescribeDelegationToken` request.
pub fn build_describe_delegation_token_request(
    correlation_id: i32,
    client_id: &str,
    owners: Option<&[KafkaPrincipal]>,
) -> (RequestHeader, DescribeDelegationTokenRequest) {
    use kafka_protocol::messages::describe_delegation_token_request::DescribeDelegationTokenOwner;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeDelegationToken,
        API_VERSION_DESCRIBE_DELEGATION_TOKEN,
    );
    let owners = owners.and_then(|owners| {
        if owners.is_empty() {
            None
        } else {
            Some(
                owners
                    .iter()
                    .map(|owner| {
                        DescribeDelegationTokenOwner::default()
                            .with_principal_type(StrBytes::from_string(
                                owner.principal_type.clone(),
                            ))
                            .with_principal_name(StrBytes::from_string(
                                owner.principal_name.clone(),
                            ))
                    })
                    .collect(),
            )
        }
    });
    let request = DescribeDelegationTokenRequest::default().with_owners(owners);

    (header, request)
}

/// Build a `DescribeLogDirs` request.
pub fn build_describe_log_dirs_request(
    correlation_id: i32,
    client_id: &str,
    topics: Option<&[TopicPartitionFilter]>,
) -> (RequestHeader, DescribeLogDirsRequest) {
    use kafka_protocol::messages::describe_log_dirs_request::DescribableLogDirTopic;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeLogDirs,
        API_VERSION_DESCRIBE_LOG_DIRS,
    );
    let topics = topics.map(|topics| {
        topics
            .iter()
            .map(|topic| {
                DescribableLogDirTopic::default()
                    .with_topic(StrBytes::from_string(topic.topic.clone()).into())
                    .with_partitions(topic.partitions.clone())
            })
            .collect()
    });
    let request = DescribeLogDirsRequest::default().with_topics(topics);

    (header, request)
}

/// Build a `DeleteRecords` request.
pub fn build_delete_records_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[DeleteRecordsTopicSpec],
    timeout_ms: i32,
) -> (RequestHeader, DeleteRecordsRequest) {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DeleteRecords,
        API_VERSION_DELETE_RECORDS,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            DeleteRecordsTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            DeleteRecordsPartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_offset(partition.offset)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = DeleteRecordsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(timeout_ms);

    (header, request)
}

/// Build a `ListPartitionReassignments` request.
pub fn build_list_partition_reassignments_request(
    correlation_id: i32,
    client_id: &str,
    topics: Option<&[TopicPartitionFilter]>,
    timeout_ms: i32,
) -> (RequestHeader, ListPartitionReassignmentsRequest) {
    use kafka_protocol::messages::list_partition_reassignments_request::ListPartitionReassignmentsTopics;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListPartitionReassignments,
        API_VERSION_LIST_PARTITION_REASSIGNMENTS,
    );
    let topics = topics.map(|topics| {
        topics
            .iter()
            .map(|topic| {
                ListPartitionReassignmentsTopics::default()
                    .with_name(StrBytes::from_string(topic.topic.clone()).into())
                    .with_partition_indexes(topic.partitions.clone())
            })
            .collect()
    });
    let request = ListPartitionReassignmentsRequest::default()
        .with_timeout_ms(timeout_ms)
        .with_topics(topics);

    (header, request)
}

/// Build an `AlterPartitionReassignments` request.
pub fn build_alter_partition_reassignments_request(
    correlation_id: i32,
    client_id: &str,
    options: &AlterPartitionReassignmentsOptions,
) -> (RequestHeader, AlterPartitionReassignmentsRequest) {
    use kafka_protocol::messages::alter_partition_reassignments_request::{
        ReassignablePartition, ReassignableTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterPartitionReassignments,
        API_VERSION_ALTER_PARTITION_REASSIGNMENTS,
    );
    let topics = options
        .topics
        .iter()
        .map(|topic| {
            ReassignableTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            ReassignablePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_replicas(partition.replicas.as_ref().map(|replicas| {
                                    replicas.iter().copied().map(Into::into).collect()
                                }))
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterPartitionReassignmentsRequest::default()
        .with_timeout_ms(options.timeout_ms)
        .with_allow_replication_factor_change(options.allow_replication_factor_change)
        .with_topics(topics);

    (header, request)
}

/// Build a `DescribeQuorum` request.
pub fn build_describe_quorum_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicPartitionFilter],
) -> (RequestHeader, DescribeQuorumRequest) {
    use kafka_protocol::messages::describe_quorum_request::{
        PartitionData as QuorumPartitionRequest, TopicData as QuorumTopicRequest,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeQuorum,
        API_VERSION_DESCRIBE_QUORUM,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            QuorumTopicRequest::default()
                .with_topic_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .copied()
                        .map(|partition| {
                            QuorumPartitionRequest::default().with_partition_index(partition)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = DescribeQuorumRequest::default().with_topics(topics);

    (header, request)
}

/// Build an `ElectLeaders` request.
pub fn build_elect_leaders_request(
    correlation_id: i32,
    client_id: &str,
    options: &ElectLeadersOptions,
) -> (RequestHeader, ElectLeadersRequest) {
    use kafka_protocol::messages::elect_leaders_request::TopicPartitions;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ElectLeaders,
        API_VERSION_ELECT_LEADERS,
    );
    let topic_partitions = options.topic_partitions.as_ref().map(|topics| {
        topics
            .iter()
            .map(|topic| {
                TopicPartitions::default()
                    .with_topic(StrBytes::from_string(topic.topic.clone()).into())
                    .with_partitions(topic.partitions.clone())
            })
            .collect()
    });
    let request = ElectLeadersRequest::default()
        .with_election_type(options.election_type)
        .with_topic_partitions(topic_partitions)
        .with_timeout_ms(options.timeout_ms);

    (header, request)
}

/// Build a `ConsumerGroupDescribe` request.
pub fn build_consumer_group_describe_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
    include_authorized_operations: bool,
) -> (RequestHeader, ConsumerGroupDescribeRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ConsumerGroupDescribe,
        API_VERSION_CONSUMER_GROUP_DESCRIBE,
    );
    let request = ConsumerGroupDescribeRequest::default()
        .with_group_ids(groups.iter().map(|g| group_id(g)).collect())
        .with_include_authorized_operations(include_authorized_operations);

    (header, request)
}

/// Build a `ShareGroupDescribe` request.
pub fn build_share_group_describe_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[&str],
    include_authorized_operations: bool,
) -> (RequestHeader, ShareGroupDescribeRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ShareGroupDescribe,
        API_VERSION_SHARE_GROUP_DESCRIBE,
    );
    let request = ShareGroupDescribeRequest::default()
        .with_group_ids(groups.iter().map(|g| group_id(g)).collect())
        .with_include_authorized_operations(include_authorized_operations);

    (header, request)
}

/// Build a `ListConfigResources` request.
pub fn build_list_config_resources_request(
    correlation_id: i32,
    client_id: &str,
    resource_types: &[i8],
) -> (RequestHeader, ListConfigResourcesRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListConfigResources,
        API_VERSION_LIST_CONFIG_RESOURCES,
    );
    let request =
        ListConfigResourcesRequest::default().with_resource_types(resource_types.to_vec());

    (header, request)
}

/// Build a `CreatePartitions` request.
pub fn build_create_partitions_request(
    correlation_id: i32,
    client_id: &str,
    options: &CreatePartitionsOptions,
) -> (RequestHeader, CreatePartitionsRequest) {
    use kafka_protocol::messages::create_partitions_request::{
        CreatePartitionsAssignment, CreatePartitionsTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::CreatePartitions,
        API_VERSION_CREATE_PARTITIONS,
    );
    let topics = options
        .topics
        .iter()
        .map(|topic| {
            CreatePartitionsTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_count(topic.count)
                .with_assignments(topic.assignments.as_ref().map(|assignments| {
                    assignments
                        .iter()
                        .map(|assignment| {
                            CreatePartitionsAssignment::default().with_broker_ids(
                                assignment.iter().copied().map(Into::into).collect(),
                            )
                        })
                        .collect()
                }))
        })
        .collect();
    let request = CreatePartitionsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(options.timeout_ms)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build a `DescribeTopicPartitions` request.
pub fn build_describe_topic_partitions_request(
    correlation_id: i32,
    client_id: &str,
    options: &DescribeTopicPartitionsOptions,
) -> (RequestHeader, DescribeTopicPartitionsRequest) {
    use kafka_protocol::messages::describe_topic_partitions_request::{
        Cursor as KpCursor, TopicRequest,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeTopicPartitions,
        API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
    );
    let topics = options
        .topics
        .iter()
        .map(|topic| TopicRequest::default().with_name(StrBytes::from_string(topic.clone()).into()))
        .collect();
    let cursor = options.cursor.as_ref().map(|cursor| {
        KpCursor::default()
            .with_topic_name(StrBytes::from_string(cursor.topic_name.clone()).into())
            .with_partition_index(cursor.partition_index)
    });
    let request = DescribeTopicPartitionsRequest::default()
        .with_topics(topics)
        .with_response_partition_limit(options.response_partition_limit)
        .with_cursor(cursor);

    (header, request)
}

/// Build a `DescribeShareGroupOffsets` request.
pub fn build_describe_share_group_offsets_request(
    correlation_id: i32,
    client_id: &str,
    groups: &[ShareGroupOffsetRequest],
) -> (RequestHeader, DescribeShareGroupOffsetsRequest) {
    use kafka_protocol::messages::describe_share_group_offsets_request::{
        DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeShareGroupOffsets,
        API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
    );
    let groups = groups
        .iter()
        .map(|group| {
            let topics = group.topics.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| {
                        DescribeShareGroupOffsetsRequestTopic::default()
                            .with_topic_name(StrBytes::from_string(topic.topic.clone()).into())
                            .with_partitions(topic.partitions.clone())
                    })
                    .collect()
            });
            DescribeShareGroupOffsetsRequestGroup::default()
                .with_group_id(group_id(&group.group_id))
                .with_topics(topics)
        })
        .collect();
    let request = DescribeShareGroupOffsetsRequest::default().with_groups(groups);

    (header, request)
}

/// Build a `DescribeClientQuotas` request.
pub fn build_describe_client_quotas_request(
    correlation_id: i32,
    client_id: &str,
    options: &DescribeClientQuotasOptions,
) -> (RequestHeader, DescribeClientQuotasRequest) {
    use kafka_protocol::messages::describe_client_quotas_request::ComponentData;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeClientQuotas,
        API_VERSION_DESCRIBE_CLIENT_QUOTAS,
    );
    let request = DescribeClientQuotasRequest::default()
        .with_components(
            options
                .components
                .iter()
                .map(|component| {
                    ComponentData::default()
                        .with_entity_type(StrBytes::from_string(component.entity_type.clone()))
                        .with_match_type(component.match_type)
                        .with_match(
                            component
                                .match_value
                                .as_ref()
                                .map(|value| StrBytes::from_string(value.clone())),
                        )
                })
                .collect(),
        )
        .with_strict(options.strict);

    (header, request)
}

/// Build an `AlterClientQuotas` request.
pub fn build_alter_client_quotas_request(
    correlation_id: i32,
    client_id: &str,
    options: &AlterClientQuotasOptions,
) -> (RequestHeader, AlterClientQuotasRequest) {
    use kafka_protocol::messages::alter_client_quotas_request::{EntityData, EntryData, OpData};

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterClientQuotas,
        API_VERSION_ALTER_CLIENT_QUOTAS,
    );
    let entries = options
        .entries
        .iter()
        .map(|entry| {
            EntryData::default()
                .with_entity(
                    entry
                        .entity
                        .iter()
                        .map(|entity| {
                            EntityData::default()
                                .with_entity_type(StrBytes::from_string(entity.entity_type.clone()))
                                .with_entity_name(
                                    entity
                                        .entity_name
                                        .as_ref()
                                        .map(|name| StrBytes::from_string(name.clone())),
                                )
                        })
                        .collect(),
                )
                .with_ops(
                    entry
                        .ops
                        .iter()
                        .map(|op| {
                            OpData::default()
                                .with_key(StrBytes::from_string(op.key.clone()))
                                .with_value(op.value)
                                .with_remove(op.remove)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = AlterClientQuotasRequest::default()
        .with_entries(entries)
        .with_validate_only(options.validate_only);

    (header, request)
}

/// Build a `DescribeUserScramCredentials` request.
pub fn build_describe_user_scram_credentials_request(
    correlation_id: i32,
    client_id: &str,
    users: Option<&[&str]>,
) -> (RequestHeader, DescribeUserScramCredentialsRequest) {
    use kafka_protocol::messages::describe_user_scram_credentials_request::UserName;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeUserScramCredentials,
        API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
    );
    let users = users.and_then(|users| {
        if users.is_empty() {
            None
        } else {
            Some(
                users
                    .iter()
                    .map(|user| {
                        UserName::default().with_name(StrBytes::from_string((*user).to_owned()))
                    })
                    .collect(),
            )
        }
    });
    let request = DescribeUserScramCredentialsRequest::default().with_users(users);

    (header, request)
}

/// Build a `DescribeProducers` request.
pub fn build_describe_producers_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[TopicPartitionFilter],
) -> (RequestHeader, DescribeProducersRequest) {
    use kafka_protocol::messages::describe_producers_request::TopicRequest;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeProducers,
        API_VERSION_DESCRIBE_PRODUCERS,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            TopicRequest::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partition_indexes(topic.partitions.clone())
        })
        .collect();
    let request = DescribeProducersRequest::default().with_topics(topics);

    (header, request)
}

/// Build an `OffsetForLeaderEpoch` request.
pub fn build_offset_for_leader_epoch_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[LeaderEpochTopicRequest],
) -> (RequestHeader, OffsetForLeaderEpochRequest) {
    use kafka_protocol::messages::offset_for_leader_epoch_request::{
        OffsetForLeaderPartition, OffsetForLeaderTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::OffsetForLeaderEpoch,
        API_VERSION_OFFSET_FOR_LEADER_EPOCH,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            OffsetForLeaderTopic::default()
                .with_topic(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            OffsetForLeaderPartition::default()
                                .with_partition(partition.partition)
                                .with_current_leader_epoch(partition.current_leader_epoch)
                                .with_leader_epoch(partition.leader_epoch)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = OffsetForLeaderEpochRequest::default()
        .with_replica_id((-1).into())
        .with_topics(topics);

    (header, request)
}

/// Build an `OffsetDelete` request for deleting committed group offsets.
pub fn build_offset_delete_request(
    correlation_id: i32,
    client_id: &str,
    group: &str,
    topics: &[TopicPartitionFilter],
) -> (RequestHeader, OffsetDeleteRequest) {
    use kafka_protocol::messages::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::OffsetDelete,
        API_VERSION_OFFSET_DELETE,
    );
    let topics = topics
        .iter()
        .map(|topic| {
            OffsetDeleteRequestTopic::default()
                .with_name(StrBytes::from_string(topic.topic.clone()).into())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .copied()
                        .map(|partition| {
                            OffsetDeleteRequestPartition::default().with_partition_index(partition)
                        })
                        .collect(),
                )
        })
        .collect();
    let request = OffsetDeleteRequest::default()
        .with_group_id(group_id(group))
        .with_topics(topics);

    (header, request)
}

/// Build a `ListTransactions` request.
pub fn build_list_transactions_request(
    correlation_id: i32,
    client_id: &str,
    options: &ListTransactionsOptions,
) -> (RequestHeader, ListTransactionsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ListTransactions,
        API_VERSION_LIST_TRANSACTIONS,
    );
    let request = ListTransactionsRequest::default()
        .with_state_filters(
            options
                .state_filters
                .iter()
                .map(|state| StrBytes::from_string(state.clone()))
                .collect(),
        )
        .with_producer_id_filters(
            options
                .producer_id_filters
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
        )
        .with_duration_filter(options.duration_filter_ms.unwrap_or(-1))
        .with_transactional_id_pattern(
            options
                .transactional_id_pattern
                .as_ref()
                .map(|pattern| StrBytes::from_string(pattern.clone())),
        );

    (header, request)
}

/// Build a `DescribeTransactions` request.
pub fn build_describe_transactions_request(
    correlation_id: i32,
    client_id: &str,
    transactional_ids: &[&str],
) -> (RequestHeader, DescribeTransactionsRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::DescribeTransactions,
        API_VERSION_DESCRIBE_TRANSACTIONS,
    );
    let request = DescribeTransactionsRequest::default().with_transactional_ids(
        transactional_ids
            .iter()
            .map(|id| transactional_id(id))
            .collect(),
    );

    (header, request)
}

/// Convert a generated `DescribeClusterResponse` into the crate's public shape.
pub fn convert_describe_cluster_response(
    response: DescribeClusterResponse,
) -> DescribeClusterResponseData {
    DescribeClusterResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        endpoint_type: response.endpoint_type,
        cluster_id: response.cluster_id.to_string(),
        controller_id: i32::from(response.controller_id),
        brokers: response
            .brokers
            .into_iter()
            .map(|broker| ClusterBroker {
                broker_id: i32::from(broker.broker_id),
                host: broker.host.to_string(),
                port: broker.port,
                rack: broker.rack.map(|rack| rack.to_string()),
                is_fenced: broker.is_fenced,
            })
            .collect(),
        cluster_authorized_operations: response.cluster_authorized_operations,
    }
}

/// Convert a generated `ListGroupsResponse` into the crate's public shape.
pub fn convert_list_groups_response(response: ListGroupsResponse) -> ListGroupsResponseData {
    ListGroupsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        groups: response
            .groups
            .into_iter()
            .map(|group| ListedGroup {
                group_id: group.group_id.to_string(),
                protocol_type: group.protocol_type.to_string(),
                group_state: group.group_state.to_string(),
                group_type: group.group_type.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `DeleteGroupsResponse` into the crate's public shape.
pub fn convert_delete_groups_response(response: DeleteGroupsResponse) -> DeleteGroupsResponseData {
    DeleteGroupsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| DeletedGroup {
                group_id: result.group_id.to_string(),
                error_code: result.error_code,
            })
            .collect(),
    }
}

/// Convert a generated `DescribeGroupsResponse` into the crate's public shape.
pub fn convert_describe_groups_response(
    response: DescribeGroupsResponse,
) -> DescribeGroupsResponseData {
    DescribeGroupsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| DescribedGroup {
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
                group_id: group.group_id.to_string(),
                group_state: group.group_state.to_string(),
                protocol_type: group.protocol_type.to_string(),
                protocol_data: group.protocol_data.to_string(),
                members: group
                    .members
                    .into_iter()
                    .map(|member| DescribedGroupMember {
                        member_id: member.member_id.to_string(),
                        group_instance_id: member
                            .group_instance_id
                            .map(|instance_id| instance_id.to_string()),
                        client_id: member.client_id.to_string(),
                        client_host: member.client_host.to_string(),
                        member_metadata: member.member_metadata,
                        member_assignment: member.member_assignment,
                    })
                    .collect(),
                authorized_operations: group.authorized_operations,
            })
            .collect(),
    }
}

/// Convert a generated `DescribeAclsResponse` into the crate's public shape.
pub fn convert_describe_acls_response(response: DescribeAclsResponse) -> DescribeAclsResponseData {
    DescribeAclsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        resources: response
            .resources
            .into_iter()
            .map(|resource| AclResource {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.to_string(),
                pattern_type: resource.pattern_type,
                acls: resource
                    .acls
                    .into_iter()
                    .map(|acl| AclDescription {
                        principal: acl.principal.to_string(),
                        host: acl.host.to_string(),
                        operation: acl.operation,
                        permission_type: acl.permission_type,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `CreateAclsResponse` into the crate's public shape.
pub fn convert_create_acls_response(response: CreateAclsResponse) -> CreateAclsResponseData {
    CreateAclsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| CreateAclResult {
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
            })
            .collect(),
    }
}

/// Convert a generated `DeleteAclsResponse` into the crate's public shape.
pub fn convert_delete_acls_response(response: DeleteAclsResponse) -> DeleteAclsResponseData {
    DeleteAclsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        filter_results: response
            .filter_results
            .into_iter()
            .map(|filter_result| DeleteAclsFilterResult {
                error_code: filter_result.error_code,
                error_message: filter_result
                    .error_message
                    .map(|message| message.to_string()),
                matching_acls: filter_result
                    .matching_acls
                    .into_iter()
                    .map(|acl| DeletedAcl {
                        error_code: acl.error_code,
                        error_message: acl.error_message.map(|message| message.to_string()),
                        resource_type: acl.resource_type,
                        resource_name: acl.resource_name.to_string(),
                        pattern_type: acl.pattern_type,
                        principal: acl.principal.to_string(),
                        host: acl.host.to_string(),
                        operation: acl.operation,
                        permission_type: acl.permission_type,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeConfigsResponse` into the crate's public shape.
pub fn convert_describe_configs_response(
    response: DescribeConfigsResponse,
) -> DescribeConfigsResponseData {
    DescribeConfigsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| DescribeConfigsResult {
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
                resource_type: result.resource_type,
                resource_name: result.resource_name.to_string(),
                configs: result
                    .configs
                    .into_iter()
                    .map(|config| ConfigEntry {
                        name: config.name.to_string(),
                        value: config.value.map(|value| value.to_string()),
                        read_only: config.read_only,
                        config_source: config.config_source,
                        is_sensitive: config.is_sensitive,
                        synonyms: config
                            .synonyms
                            .into_iter()
                            .map(|synonym| ConfigSynonym {
                                name: synonym.name.to_string(),
                                value: synonym.value.map(|value| value.to_string()),
                                source: synonym.source,
                            })
                            .collect(),
                        config_type: config.config_type,
                        documentation: config
                            .documentation
                            .map(|documentation| documentation.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `IncrementalAlterConfigsResponse` into the crate's public shape.
pub fn convert_incremental_alter_configs_response(
    response: IncrementalAlterConfigsResponse,
) -> IncrementalAlterConfigsResponseData {
    IncrementalAlterConfigsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        responses: response
            .responses
            .into_iter()
            .map(|result| IncrementalAlterConfigsResourceResult {
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
                resource_type: result.resource_type,
                resource_name: result.resource_name.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeLogDirsResponse` into the crate's public shape.
pub fn convert_describe_log_dirs_response(
    response: DescribeLogDirsResponse,
) -> DescribeLogDirsResponseData {
    DescribeLogDirsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        results: response
            .results
            .into_iter()
            .map(|result| LogDirDescription {
                error_code: result.error_code,
                log_dir: result.log_dir.to_string(),
                topics: result
                    .topics
                    .into_iter()
                    .map(|topic| LogDirTopic {
                        name: topic.name.to_string(),
                        partitions: topic
                            .partitions
                            .into_iter()
                            .map(|partition| LogDirPartition {
                                partition_index: partition.partition_index,
                                partition_size: partition.partition_size,
                                offset_lag: partition.offset_lag,
                                is_future_key: partition.is_future_key,
                            })
                            .collect(),
                    })
                    .collect(),
                total_bytes: result.total_bytes,
                usable_bytes: result.usable_bytes,
            })
            .collect(),
    }
}

/// Convert a generated `DeleteRecordsResponse` into the crate's public shape.
pub fn convert_delete_records_response(
    response: DeleteRecordsResponse,
) -> DeleteRecordsResponseData {
    DeleteRecordsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| DeleteRecordsTopicResult {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| DeleteRecordsPartitionResult {
                        partition_index: partition.partition_index,
                        low_watermark: partition.low_watermark,
                        error_code: partition.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeDelegationTokenResponse` into the crate's public shape.
pub fn convert_describe_delegation_token_response(
    response: DescribeDelegationTokenResponse,
) -> DescribeDelegationTokenResponseData {
    DescribeDelegationTokenResponseData {
        error_code: response.error_code,
        tokens: response
            .tokens
            .into_iter()
            .map(|token| DelegationTokenDescription {
                owner: KafkaPrincipal::new(
                    token.principal_type.to_string(),
                    token.principal_name.to_string(),
                ),
                requester: if token.token_requester_principal_type.is_empty()
                    && token.token_requester_principal_name.is_empty()
                {
                    None
                } else {
                    Some(KafkaPrincipal::new(
                        token.token_requester_principal_type.to_string(),
                        token.token_requester_principal_name.to_string(),
                    ))
                },
                issue_timestamp: token.issue_timestamp,
                expiry_timestamp: token.expiry_timestamp,
                max_timestamp: token.max_timestamp,
                token_id: token.token_id.to_string(),
                hmac: token.hmac,
                renewers: token
                    .renewers
                    .into_iter()
                    .map(|renewer| {
                        KafkaPrincipal::new(
                            renewer.principal_type.to_string(),
                            renewer.principal_name.to_string(),
                        )
                    })
                    .collect(),
            })
            .collect(),
        throttle_time_ms: response.throttle_time_ms,
    }
}

/// Convert a generated `ListPartitionReassignmentsResponse` into the crate's public shape.
pub fn convert_list_partition_reassignments_response(
    response: ListPartitionReassignmentsResponse,
) -> ListPartitionReassignmentsResponseData {
    ListPartitionReassignmentsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        topics: response
            .topics
            .into_iter()
            .map(|topic| TopicReassignment {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| PartitionReassignment {
                        partition_index: partition.partition_index,
                        replicas: partition.replicas.into_iter().map(i32::from).collect(),
                        adding_replicas: partition
                            .adding_replicas
                            .into_iter()
                            .map(i32::from)
                            .collect(),
                        removing_replicas: partition
                            .removing_replicas
                            .into_iter()
                            .map(i32::from)
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `AlterPartitionReassignmentsResponse` into the crate's public shape.
pub fn convert_alter_partition_reassignments_response(
    response: AlterPartitionReassignmentsResponse,
) -> AlterPartitionReassignmentsResponseData {
    AlterPartitionReassignmentsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        allow_replication_factor_change: response.allow_replication_factor_change,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        responses: response
            .responses
            .into_iter()
            .map(|topic| AlterPartitionReassignmentsTopicResult {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| AlterPartitionReassignmentsPartitionResult {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeQuorumResponse` into the crate's public shape.
pub fn convert_describe_quorum_response(
    response: DescribeQuorumResponse,
) -> DescribeQuorumResponseData {
    DescribeQuorumResponseData {
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        topics: response
            .topics
            .into_iter()
            .map(|topic| QuorumTopic {
                name: topic.topic_name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| QuorumPartition {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                        leader_id: i32::from(partition.leader_id),
                        leader_epoch: partition.leader_epoch,
                        high_watermark: partition.high_watermark,
                        current_voters: partition
                            .current_voters
                            .iter()
                            .map(convert_quorum_replica_state)
                            .collect(),
                        observers: partition
                            .observers
                            .iter()
                            .map(convert_quorum_replica_state)
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
        nodes: response
            .nodes
            .into_iter()
            .map(|node| QuorumNode {
                node_id: i32::from(node.node_id),
                listeners: node
                    .listeners
                    .into_iter()
                    .map(|listener| QuorumListener {
                        name: listener.name.to_string(),
                        host: listener.host.to_string(),
                        port: listener.port,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn convert_quorum_replica_state(
    replica: &kafka_protocol::messages::describe_quorum_response::ReplicaState,
) -> QuorumReplicaState {
    QuorumReplicaState {
        replica_id: i32::from(replica.replica_id),
        replica_directory_id: replica.replica_directory_id.to_string(),
        log_end_offset: replica.log_end_offset,
        last_fetch_timestamp: replica.last_fetch_timestamp,
        last_caught_up_timestamp: replica.last_caught_up_timestamp,
    }
}

/// Convert a generated `ElectLeadersResponse` into the crate's public shape.
pub fn convert_elect_leaders_response(response: ElectLeadersResponse) -> ElectLeadersResponseData {
    ElectLeadersResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        replica_election_results: response
            .replica_election_results
            .into_iter()
            .map(|topic| ElectLeadersTopicResult {
                topic: topic.topic.to_string(),
                partition_results: topic
                    .partition_result
                    .into_iter()
                    .map(|partition| ElectLeadersPartitionResult {
                        partition_id: partition.partition_id,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `ConsumerGroupDescribeResponse` into the crate's public shape.
pub fn convert_consumer_group_describe_response(
    response: ConsumerGroupDescribeResponse,
) -> ConsumerGroupDescribeResponseData {
    ConsumerGroupDescribeResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| ConsumerGroupDescription {
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
                group_id: group.group_id.to_string(),
                group_state: group.group_state.to_string(),
                group_epoch: group.group_epoch,
                assignment_epoch: group.assignment_epoch,
                assignor_name: group.assignor_name.to_string(),
                members: group
                    .members
                    .into_iter()
                    .map(|member| ConsumerGroupMemberDescription {
                        member_id: member.member_id.to_string(),
                        instance_id: member
                            .instance_id
                            .map(|instance_id| instance_id.to_string()),
                        rack_id: member.rack_id.map(|rack_id| rack_id.to_string()),
                        member_epoch: member.member_epoch,
                        client_id: member.client_id.to_string(),
                        client_host: member.client_host.to_string(),
                        subscribed_topic_names: member
                            .subscribed_topic_names
                            .into_iter()
                            .map(|topic_name| topic_name.to_string())
                            .collect(),
                        subscribed_topic_regex: member
                            .subscribed_topic_regex
                            .map(|regex| regex.to_string()),
                        assignment: convert_consumer_group_assignment(member.assignment),
                        target_assignment: convert_consumer_group_assignment(
                            member.target_assignment,
                        ),
                        member_type: member.member_type,
                    })
                    .collect(),
                authorized_operations: group.authorized_operations,
            })
            .collect(),
    }
}

fn convert_consumer_group_assignment(
    assignment: kafka_protocol::messages::consumer_group_describe_response::Assignment,
) -> ConsumerGroupAssignment {
    ConsumerGroupAssignment {
        topic_partitions: assignment
            .topic_partitions
            .into_iter()
            .map(|topic| ConsumerGroupTopicPartitions {
                topic_id: topic.topic_id.to_string(),
                topic_name: topic.topic_name.to_string(),
                partitions: topic.partitions,
            })
            .collect(),
    }
}

/// Convert a generated `ShareGroupDescribeResponse` into the crate's public shape.
pub fn convert_share_group_describe_response(
    response: ShareGroupDescribeResponse,
) -> ShareGroupDescribeResponseData {
    ShareGroupDescribeResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| ShareGroupDescription {
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
                group_id: group.group_id.to_string(),
                group_state: group.group_state.to_string(),
                group_epoch: group.group_epoch,
                assignment_epoch: group.assignment_epoch,
                assignor_name: group.assignor_name.to_string(),
                members: group
                    .members
                    .into_iter()
                    .map(|member| ShareGroupMemberDescription {
                        member_id: member.member_id.to_string(),
                        rack_id: member.rack_id.map(|rack_id| rack_id.to_string()),
                        member_epoch: member.member_epoch,
                        client_id: member.client_id.to_string(),
                        client_host: member.client_host.to_string(),
                        subscribed_topic_names: member
                            .subscribed_topic_names
                            .into_iter()
                            .map(|topic_name| topic_name.to_string())
                            .collect(),
                        assignment: convert_share_group_assignment(member.assignment),
                    })
                    .collect(),
                authorized_operations: group.authorized_operations,
            })
            .collect(),
    }
}

fn convert_share_group_assignment(
    assignment: kafka_protocol::messages::share_group_describe_response::Assignment,
) -> ShareGroupAssignment {
    ShareGroupAssignment {
        topic_partitions: assignment
            .topic_partitions
            .into_iter()
            .map(|topic| ShareGroupTopicPartitions {
                topic_id: topic.topic_id.to_string(),
                topic_name: topic.topic_name.to_string(),
                partitions: topic.partitions,
            })
            .collect(),
    }
}

/// Convert a generated `ListConfigResourcesResponse` into the crate's public shape.
pub fn convert_list_config_resources_response(
    response: ListConfigResourcesResponse,
) -> ListConfigResourcesResponseData {
    ListConfigResourcesResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        resources: response
            .config_resources
            .into_iter()
            .map(|resource| ListedConfigResource {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `CreatePartitionsResponse` into the crate's public shape.
pub fn convert_create_partitions_response(
    response: CreatePartitionsResponse,
) -> CreatePartitionsResponseData {
    CreatePartitionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| CreatePartitionsTopicResult {
                name: result.name.to_string(),
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeTopicPartitionsResponse` into the crate's public shape.
pub fn convert_describe_topic_partitions_response(
    response: DescribeTopicPartitionsResponse,
) -> DescribeTopicPartitionsResponseData {
    DescribeTopicPartitionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| DescribedTopicPartitionsTopic {
                error_code: topic.error_code,
                name: topic.name.map(|name| name.to_string()),
                topic_id: topic.topic_id.to_string(),
                is_internal: topic.is_internal,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| DescribedTopicPartition {
                        error_code: partition.error_code,
                        partition_index: partition.partition_index,
                        leader_id: i32::from(partition.leader_id),
                        leader_epoch: partition.leader_epoch,
                        replica_nodes: broker_ids_to_i32s(partition.replica_nodes),
                        isr_nodes: broker_ids_to_i32s(partition.isr_nodes),
                        eligible_leader_replicas: partition
                            .eligible_leader_replicas
                            .map(broker_ids_to_i32s),
                        last_known_elr: partition.last_known_elr.map(broker_ids_to_i32s),
                        offline_replicas: broker_ids_to_i32s(partition.offline_replicas),
                    })
                    .collect(),
                topic_authorized_operations: topic.topic_authorized_operations,
            })
            .collect(),
        next_cursor: response.next_cursor.map(|cursor| TopicPartitionsCursor {
            topic_name: cursor.topic_name.to_string(),
            partition_index: cursor.partition_index,
        }),
    }
}

fn broker_ids_to_i32s(ids: Vec<kafka_protocol::messages::BrokerId>) -> Vec<i32> {
    ids.into_iter().map(i32::from).collect()
}

/// Convert a generated `DescribeShareGroupOffsetsResponse` into the crate's public shape.
pub fn convert_describe_share_group_offsets_response(
    response: DescribeShareGroupOffsetsResponse,
) -> DescribeShareGroupOffsetsResponseData {
    DescribeShareGroupOffsetsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        groups: response
            .groups
            .into_iter()
            .map(|group| ShareGroupOffsetGroup {
                group_id: group.group_id.to_string(),
                topics: group
                    .topics
                    .into_iter()
                    .map(|topic| ShareGroupOffsetTopic {
                        topic_name: topic.topic_name.to_string(),
                        topic_id: topic.topic_id.to_string(),
                        partitions: topic
                            .partitions
                            .into_iter()
                            .map(|partition| ShareGroupOffsetPartition {
                                partition_index: partition.partition_index,
                                start_offset: partition.start_offset,
                                leader_epoch: partition.leader_epoch,
                                error_code: partition.error_code,
                                error_message: partition
                                    .error_message
                                    .map(|message| message.to_string()),
                            })
                            .collect(),
                    })
                    .collect(),
                error_code: group.error_code,
                error_message: group.error_message.map(|message| message.to_string()),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeClientQuotasResponse` into the crate's public shape.
pub fn convert_describe_client_quotas_response(
    response: DescribeClientQuotasResponse,
) -> DescribeClientQuotasResponseData {
    DescribeClientQuotasResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        entries: response.entries.map(|entries| {
            entries
                .into_iter()
                .map(|entry| ClientQuotaEntry {
                    entity: entry
                        .entity
                        .into_iter()
                        .map(|entity| ClientQuotaEntity {
                            entity_type: entity.entity_type.to_string(),
                            entity_name: entity.entity_name.map(|name| name.to_string()),
                        })
                        .collect(),
                    values: entry
                        .values
                        .into_iter()
                        .map(|value| ClientQuotaValue {
                            key: value.key.to_string(),
                            value: value.value,
                        })
                        .collect(),
                })
                .collect()
        }),
    }
}

/// Convert a generated `AlterClientQuotasResponse` into the crate's public shape.
pub fn convert_alter_client_quotas_response(
    response: AlterClientQuotasResponse,
) -> AlterClientQuotasResponseData {
    AlterClientQuotasResponseData {
        throttle_time_ms: response.throttle_time_ms,
        entries: response
            .entries
            .into_iter()
            .map(|entry| AlterClientQuotaEntryResult {
                error_code: entry.error_code,
                error_message: entry.error_message.map(|message| message.to_string()),
                entity: entry
                    .entity
                    .into_iter()
                    .map(|entity| ClientQuotaEntity {
                        entity_type: entity.entity_type.to_string(),
                        entity_name: entity.entity_name.map(|name| name.to_string()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeUserScramCredentialsResponse` into the crate's public shape.
pub fn convert_describe_user_scram_credentials_response(
    response: DescribeUserScramCredentialsResponse,
) -> DescribeUserScramCredentialsResponseData {
    DescribeUserScramCredentialsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        error_message: response.error_message.map(|message| message.to_string()),
        results: response
            .results
            .into_iter()
            .map(|result| UserScramCredentialsDescription {
                user: result.user.to_string(),
                error_code: result.error_code,
                error_message: result.error_message.map(|message| message.to_string()),
                credential_infos: result
                    .credential_infos
                    .into_iter()
                    .map(|credential| ScramCredentialInfo {
                        mechanism: credential.mechanism,
                        iterations: credential.iterations,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeProducersResponse` into the crate's public shape.
pub fn convert_describe_producers_response(
    response: DescribeProducersResponse,
) -> DescribeProducersResponseData {
    DescribeProducersResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| ProducerTopic {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| ProducerPartition {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                        error_message: partition.error_message.map(|message| message.to_string()),
                        active_producers: partition
                            .active_producers
                            .into_iter()
                            .map(|producer| ActiveProducer {
                                producer_id: i64::from(producer.producer_id),
                                producer_epoch: producer.producer_epoch,
                                last_sequence: producer.last_sequence,
                                last_timestamp: producer.last_timestamp,
                                coordinator_epoch: producer.coordinator_epoch,
                                current_txn_start_offset: producer.current_txn_start_offset,
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `OffsetForLeaderEpochResponse` into the crate's public shape.
pub fn convert_offset_for_leader_epoch_response(
    response: OffsetForLeaderEpochResponse,
) -> OffsetForLeaderEpochResponseData {
    OffsetForLeaderEpochResponseData {
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| LeaderEpochTopicOffsets {
                topic: topic.topic.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| LeaderEpochPartitionOffset {
                        error_code: partition.error_code,
                        partition: partition.partition,
                        leader_epoch: partition.leader_epoch,
                        end_offset: partition.end_offset,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `OffsetDeleteResponse` into the crate's public shape.
pub fn convert_offset_delete_response(response: OffsetDeleteResponse) -> OffsetDeleteResponseData {
    OffsetDeleteResponseData {
        error_code: response.error_code,
        throttle_time_ms: response.throttle_time_ms,
        topics: response
            .topics
            .into_iter()
            .map(|topic| OffsetDeleteTopicResult {
                name: topic.name.to_string(),
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| OffsetDeletePartitionResult {
                        partition_index: partition.partition_index,
                        error_code: partition.error_code,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Convert a generated `ListTransactionsResponse` into the crate's public shape.
pub fn convert_list_transactions_response(
    response: ListTransactionsResponse,
) -> ListTransactionsResponseData {
    ListTransactionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        error_code: response.error_code,
        unknown_state_filters: response
            .unknown_state_filters
            .into_iter()
            .map(|state| state.to_string())
            .collect(),
        transaction_states: response
            .transaction_states
            .into_iter()
            .map(|transaction| ListedTransaction {
                transactional_id: transaction.transactional_id.to_string(),
                producer_id: i64::from(transaction.producer_id),
                transaction_state: transaction.transaction_state.to_string(),
            })
            .collect(),
    }
}

/// Convert a generated `DescribeTransactionsResponse` into the crate's public shape.
pub fn convert_describe_transactions_response(
    response: DescribeTransactionsResponse,
) -> DescribeTransactionsResponseData {
    DescribeTransactionsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        transaction_states: response
            .transaction_states
            .into_iter()
            .map(|transaction| DescribedTransaction {
                error_code: transaction.error_code,
                transactional_id: transaction.transactional_id.to_string(),
                transaction_state: transaction.transaction_state.to_string(),
                transaction_timeout_ms: transaction.transaction_timeout_ms,
                transaction_start_time_ms: transaction.transaction_start_time_ms,
                producer_id: i64::from(transaction.producer_id),
                producer_epoch: transaction.producer_epoch,
                topics: transaction
                    .topics
                    .into_iter()
                    .map(|topic| TransactionTopic {
                        topic: topic.topic.to_string(),
                        partitions: topic.partitions,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn request_header(
    correlation_id: i32,
    client_id: &str,
    api_key: ApiKey,
    api_version: i16,
) -> RequestHeader {
    RequestHeader::default()
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())))
        .with_request_api_key(api_key as i16)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
}

fn str_bytes_vec(values: &[&str]) -> Vec<StrBytes> {
    values
        .iter()
        .map(|value| StrBytes::from_string((*value).to_owned()))
        .collect()
}

fn group_id(value: &str) -> GroupId {
    GroupId::from(StrBytes::from_string(value.to_owned()))
}

fn transactional_id(value: &str) -> kafka_protocol::messages::TransactionalId {
    kafka_protocol::messages::TransactionalId::from(StrBytes::from_string(value.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::alter_client_quotas_response::{
        EntityData as KpAlterClientQuotaEntity, EntryData as KpAlterClientQuotaEntry,
    };
    use kafka_protocol::messages::alter_partition_reassignments_response::{
        ReassignablePartitionResponse as KpReassignablePartitionResponse,
        ReassignableTopicResponse as KpReassignableTopicResponse,
    };
    use kafka_protocol::messages::consumer_group_describe_response::{
        Assignment as KpConsumerGroupAssignment, DescribedGroup as KpConsumerGroupDescription,
        Member as KpConsumerGroupMember, TopicPartitions as KpConsumerGroupTopicPartitions,
    };
    use kafka_protocol::messages::create_acls_response::AclCreationResult as KpAclCreationResult;
    use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult as KpCreatePartitionsTopicResult;
    use kafka_protocol::messages::delete_acls_response::{
        DeleteAclsFilterResult as KpDeleteAclsFilterResult,
        DeleteAclsMatchingAcl as KpDeleteAclsMatchingAcl,
    };
    use kafka_protocol::messages::delete_groups_response::DeletableGroupResult as KpDeletableGroupResult;
    use kafka_protocol::messages::delete_records_response::{
        DeleteRecordsPartitionResult as KpDeleteRecordsPartitionResult,
        DeleteRecordsTopicResult as KpDeleteRecordsTopicResult,
    };
    use kafka_protocol::messages::describe_acls_response::{
        AclDescription as KpAclDescription, DescribeAclsResource as KpAclResource,
    };
    use kafka_protocol::messages::describe_client_quotas_response::{
        EntityData as KpClientQuotaEntity, EntryData as KpClientQuotaEntry,
        ValueData as KpClientQuotaValue,
    };
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult as KpDescribeConfigsResourceResult,
        DescribeConfigsResult as KpDescribeConfigsResult,
        DescribeConfigsSynonym as KpDescribeConfigsSynonym,
    };
    use kafka_protocol::messages::describe_delegation_token_response::{
        DescribedDelegationToken as KpDelegationToken,
        DescribedDelegationTokenRenewer as KpDelegationTokenRenewer,
    };
    use kafka_protocol::messages::describe_groups_response::{
        DescribedGroup as KpDescribedGroup, DescribedGroupMember as KpDescribedGroupMember,
    };
    use kafka_protocol::messages::describe_log_dirs_response::{
        DescribeLogDirsPartition as KpDescribeLogDirsPartition,
        DescribeLogDirsResult as KpDescribeLogDirsResult,
        DescribeLogDirsTopic as KpDescribeLogDirsTopic,
    };
    use kafka_protocol::messages::describe_producers_response::{
        PartitionResponse as KpProducerPartition, ProducerState as KpProducerState,
        TopicResponse as KpProducerTopic,
    };
    use kafka_protocol::messages::describe_quorum_response::{
        Listener as KpQuorumListener, Node as KpQuorumNode, PartitionData as KpQuorumPartition,
        ReplicaState as KpQuorumReplica, TopicData as KpQuorumTopic,
    };
    use kafka_protocol::messages::describe_share_group_offsets_response::{
        DescribeShareGroupOffsetsResponseGroup as KpShareGroupOffsetGroup,
        DescribeShareGroupOffsetsResponsePartition as KpShareGroupOffsetPartition,
        DescribeShareGroupOffsetsResponseTopic as KpShareGroupOffsetTopic,
    };
    use kafka_protocol::messages::describe_topic_partitions_response::{
        Cursor as KpTopicPartitionsResponseCursor,
        DescribeTopicPartitionsResponsePartition as KpDescribedTopicPartition,
        DescribeTopicPartitionsResponseTopic as KpDescribedTopicPartitionsTopic,
    };
    use kafka_protocol::messages::describe_transactions_response::{
        TopicData as KpDescribeTransactionTopic, TransactionState as KpDescribedTransactionState,
    };
    use kafka_protocol::messages::describe_user_scram_credentials_response::{
        CredentialInfo as KpScramCredentialInfo,
        DescribeUserScramCredentialsResult as KpScramCredentialsResult,
    };
    use kafka_protocol::messages::elect_leaders_response::{
        PartitionResult as KpElectionPartitionResult,
        ReplicaElectionResult as KpReplicaElectionResult,
    };
    use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse as KpIncrementalAlterConfigsResourceResponse;
    use kafka_protocol::messages::list_config_resources_response::ConfigResource as KpListedConfigResource;
    use kafka_protocol::messages::list_groups_response::ListedGroup as KpListedGroup;
    use kafka_protocol::messages::list_partition_reassignments_response::{
        OngoingPartitionReassignment as KpOngoingPartitionReassignment,
        OngoingTopicReassignment as KpOngoingTopicReassignment,
    };
    use kafka_protocol::messages::list_transactions_response::TransactionState as KpListedTransactionState;
    use kafka_protocol::messages::offset_delete_response::{
        OffsetDeleteResponsePartition as KpOffsetDeletePartition,
        OffsetDeleteResponseTopic as KpOffsetDeleteTopic,
    };
    use kafka_protocol::messages::offset_for_leader_epoch_response::{
        EpochEndOffset as KpEpochEndOffset,
        OffsetForLeaderTopicResult as KpOffsetForLeaderTopicResult,
    };
    use kafka_protocol::messages::share_group_describe_response::{
        Assignment as KpShareGroupAssignment, DescribedGroup as KpShareGroupDescription,
        Member as KpShareGroupMember, TopicPartitions as KpShareGroupTopicPartitions,
    };
    use kafka_protocol::messages::{BrokerId, ProducerId};

    #[test]
    fn describe_cluster_request_uses_latest_supported_protocol_fields() {
        let (header, request) = build_describe_cluster_request(42, "client-a", true, true);

        assert_eq!(header.request_api_key, ApiKey::DescribeCluster as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_CLUSTER);
        assert_eq!(header.correlation_id, 42);
        assert_eq!(
            header.client_id.as_ref().map(ToString::to_string),
            Some("client-a".to_owned())
        );
        assert!(request.include_cluster_authorized_operations);
        assert_eq!(request.endpoint_type, DESCRIBE_CLUSTER_ENDPOINT_BROKERS);
        assert!(request.include_fenced_brokers);
    }

    #[test]
    fn list_groups_request_includes_state_and_type_filters() {
        let (header, request) =
            build_list_groups_request(7, "client-b", &["Stable"], &["consumer"]);

        assert_eq!(header.request_api_key, ApiKey::ListGroups as i16);
        assert_eq!(header.request_api_version, API_VERSION_LIST_GROUPS);
        assert_eq!(
            request.states_filter,
            vec![StrBytes::from_static_str("Stable")]
        );
        assert_eq!(
            request.types_filter,
            vec![StrBytes::from_static_str("consumer")]
        );
    }

    #[test]
    fn delete_groups_request_includes_group_ids() {
        let (header, request) = build_delete_groups_request(8, "client-c", &["group-a", "group-b"]);

        assert_eq!(header.request_api_key, ApiKey::DeleteGroups as i16);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_GROUPS);
        assert_eq!(request.groups_names[0].to_string(), "group-a");
        assert_eq!(request.groups_names[1].to_string(), "group-b");
    }

    #[test]
    fn describe_groups_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_describe_groups_request(9, "client-c", &["group-a", "group-b"], true);

        assert_eq!(header.request_api_key, ApiKey::DescribeGroups as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_GROUPS);
        assert!(request.include_authorized_operations);
        assert_eq!(request.groups[0].to_string(), "group-a");
        assert_eq!(request.groups[1].to_string(), "group-b");
    }

    #[test]
    fn describe_acls_request_accepts_resource_and_principal_filters() {
        let filter = DescribeAclsFilter::any()
            .with_resource_type(ACL_RESOURCE_TYPE_TOPIC)
            .with_resource_name("topic-a")
            .with_pattern_type(ACL_PATTERN_TYPE_LITERAL)
            .with_principal("User:alice")
            .with_host("*")
            .with_operation(ACL_OPERATION_READ)
            .with_permission_type(ACL_PERMISSION_TYPE_ALLOW);
        let (header, request) = build_describe_acls_request(9, "client-d", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeAcls as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_ACLS);
        assert_eq!(request.resource_type_filter, ACL_RESOURCE_TYPE_TOPIC);
        assert_eq!(
            request
                .resource_name_filter
                .as_ref()
                .map(ToString::to_string),
            Some("topic-a".to_owned())
        );
        assert_eq!(request.pattern_type_filter, ACL_PATTERN_TYPE_LITERAL);
        assert_eq!(
            request.principal_filter.as_ref().map(ToString::to_string),
            Some("User:alice".to_owned())
        );
        assert_eq!(
            request.host_filter.as_ref().map(ToString::to_string),
            Some("*".to_owned())
        );
        assert_eq!(request.operation, ACL_OPERATION_READ);
        assert_eq!(request.permission_type, ACL_PERMISSION_TYPE_ALLOW);
    }

    #[test]
    fn create_acls_request_preserves_binding_fields() {
        let binding = AclBinding::new(
            ACL_RESOURCE_TYPE_TOPIC,
            "topic-a",
            ACL_PATTERN_TYPE_LITERAL,
            "User:alice",
            "*",
            ACL_OPERATION_READ,
            ACL_PERMISSION_TYPE_ALLOW,
        );
        let (header, request) = build_create_acls_request(10, "client-d", &[binding]);

        assert_eq!(header.request_api_key, ApiKey::CreateAcls as i16);
        assert_eq!(header.request_api_version, API_VERSION_CREATE_ACLS);
        let creation = &request.creations[0];
        assert_eq!(creation.resource_type, ACL_RESOURCE_TYPE_TOPIC);
        assert_eq!(creation.resource_name.to_string(), "topic-a");
        assert_eq!(creation.resource_pattern_type, ACL_PATTERN_TYPE_LITERAL);
        assert_eq!(creation.principal.to_string(), "User:alice");
        assert_eq!(creation.host.to_string(), "*");
        assert_eq!(creation.operation, ACL_OPERATION_READ);
        assert_eq!(creation.permission_type, ACL_PERMISSION_TYPE_ALLOW);
    }

    #[test]
    fn delete_acls_request_uses_describe_acl_filters() {
        let filter = DescribeAclsFilter::any()
            .with_resource_type(ACL_RESOURCE_TYPE_GROUP)
            .with_resource_name("group-a")
            .with_pattern_type(ACL_PATTERN_TYPE_LITERAL)
            .with_principal("User:bob")
            .with_host("127.0.0.1")
            .with_operation(ACL_OPERATION_DESCRIBE)
            .with_permission_type(ACL_PERMISSION_TYPE_DENY);
        let (header, request) = build_delete_acls_request(11, "client-e", &[filter]);

        assert_eq!(header.request_api_key, ApiKey::DeleteAcls as i16);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_ACLS);
        let filter = &request.filters[0];
        assert_eq!(filter.resource_type_filter, ACL_RESOURCE_TYPE_GROUP);
        assert_eq!(
            filter
                .resource_name_filter
                .as_ref()
                .map(ToString::to_string),
            Some("group-a".to_owned())
        );
        assert_eq!(filter.pattern_type_filter, ACL_PATTERN_TYPE_LITERAL);
        assert_eq!(
            filter.principal_filter.as_ref().map(ToString::to_string),
            Some("User:bob".to_owned())
        );
        assert_eq!(
            filter.host_filter.as_ref().map(ToString::to_string),
            Some("127.0.0.1".to_owned())
        );
        assert_eq!(filter.operation, ACL_OPERATION_DESCRIBE);
        assert_eq!(filter.permission_type, ACL_PERMISSION_TYPE_DENY);
    }

    #[test]
    fn describe_configs_request_fetches_selected_topic_keys() {
        let resources = [ConfigResource::topic("topic-a")
            .with_configuration_keys(["retention.ms", "cleanup.policy"])];
        let (header, request) =
            build_describe_configs_request(10, "client-e", &resources, true, true);

        assert_eq!(header.request_api_key, ApiKey::DescribeConfigs as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_CONFIGS);
        assert!(request.include_synonyms);
        assert!(request.include_documentation);
        assert_eq!(
            request.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(request.resources[0].resource_name.to_string(), "topic-a");
        assert_eq!(
            request.resources[0]
                .configuration_keys
                .as_ref()
                .map(Vec::len),
            Some(2)
        );
    }

    #[test]
    fn describe_configs_request_fetches_all_broker_keys_when_keys_are_absent() {
        let resources = [ConfigResource::broker("1")];
        let (_, request) = build_describe_configs_request(11, "client-f", &resources, false, false);

        assert_eq!(
            request.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_BROKER
        );
        assert!(request.resources[0].configuration_keys.is_none());
    }

    #[test]
    fn incremental_alter_configs_request_preserves_operations() {
        let options =
            IncrementalAlterConfigsOptions::new([IncrementalAlterConfigsResource::topic(
                "topic-a",
                [
                    IncrementalAlterConfig::set("retention.ms", "60000"),
                    IncrementalAlterConfig::delete("cleanup.policy"),
                    IncrementalAlterConfig::append("leader.replication.throttled.replicas", "1:2"),
                ],
            )])
            .with_validate_only(true);
        let (header, request) = build_incremental_alter_configs_request(12, "client-f", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::IncrementalAlterConfigs as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_INCREMENTAL_ALTER_CONFIGS
        );
        assert!(request.validate_only);
        let resource = &request.resources[0];
        assert_eq!(resource.resource_type, CONFIG_RESOURCE_TYPE_TOPIC);
        assert_eq!(resource.resource_name.to_string(), "topic-a");
        assert_eq!(resource.configs[0].name.to_string(), "retention.ms");
        assert_eq!(resource.configs[0].config_operation, CONFIG_OPERATION_SET);
        assert_eq!(
            resource.configs[0].value.as_ref().map(ToString::to_string),
            Some("60000".to_owned())
        );
        assert_eq!(resource.configs[1].name.to_string(), "cleanup.policy");
        assert_eq!(
            resource.configs[1].config_operation,
            CONFIG_OPERATION_DELETE
        );
        assert!(resource.configs[1].value.is_none());
        assert_eq!(
            resource.configs[2].config_operation,
            CONFIG_OPERATION_APPEND
        );
    }

    #[test]
    fn describe_delegation_token_request_distinguishes_all_and_selected_owners() {
        let (all_header, all_request) =
            build_describe_delegation_token_request(12, "client-g", None);
        let owner = KafkaPrincipal::user("alice");
        let (selected_header, selected_request) =
            build_describe_delegation_token_request(13, "client-h", Some(&[owner]));

        assert_eq!(
            all_header.request_api_key,
            ApiKey::DescribeDelegationToken as i16
        );
        assert_eq!(
            all_header.request_api_version,
            API_VERSION_DESCRIBE_DELEGATION_TOKEN
        );
        assert!(all_request.owners.is_none());
        assert_eq!(selected_header.correlation_id, 13);
        let owners = selected_request.owners.unwrap();
        assert_eq!(owners[0].principal_type.to_string(), "User");
        assert_eq!(owners[0].principal_name.to_string(), "alice");
    }

    #[test]
    fn describe_log_dirs_request_fetches_all_topics_when_filter_is_absent() {
        let (header, request) = build_describe_log_dirs_request(14, "client-i", None);

        assert_eq!(header.request_api_key, ApiKey::DescribeLogDirs as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_LOG_DIRS);
        assert!(request.topics.is_none());
    }

    #[test]
    fn describe_log_dirs_request_fetches_selected_partitions() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 2])];
        let (_, request) = build_describe_log_dirs_request(15, "client-j", Some(&filter));

        let topic = &request.topics.as_ref().unwrap()[0];
        assert_eq!(topic.topic.to_string(), "topic-a");
        assert_eq!(topic.partitions, vec![0, 2]);
    }

    #[test]
    fn list_partition_reassignments_request_accepts_timeout_and_filter() {
        let filter = [TopicPartitionFilter::new("topic-a", [1])];
        let (header, request) =
            build_list_partition_reassignments_request(16, "client-k", Some(&filter), 5000);

        assert_eq!(
            header.request_api_key,
            ApiKey::ListPartitionReassignments as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_LIST_PARTITION_REASSIGNMENTS
        );
        assert_eq!(request.timeout_ms, 5000);
        let topic = &request.topics.as_ref().unwrap()[0];
        assert_eq!(topic.name.to_string(), "topic-a");
        assert_eq!(topic.partition_indexes, vec![1]);
    }

    #[test]
    fn delete_records_request_preserves_partition_offsets() {
        let topics = [DeleteRecordsTopicSpec::new(
            "topic-a",
            [
                DeleteRecordsPartitionSpec::new(0, 42),
                DeleteRecordsPartitionSpec::new(2, 99),
            ],
        )];
        let (header, request) = build_delete_records_request(17, "client-k", &topics, 5_000);

        assert_eq!(header.request_api_key, ApiKey::DeleteRecords as i16);
        assert_eq!(header.request_api_version, API_VERSION_DELETE_RECORDS);
        assert_eq!(request.timeout_ms, 5_000);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].offset, 42);
        assert_eq!(request.topics[0].partitions[1].partition_index, 2);
        assert_eq!(request.topics[0].partitions[1].offset, 99);
    }

    #[test]
    fn alter_partition_reassignments_request_preserves_replicas_and_cancellations() {
        let options =
            AlterPartitionReassignmentsOptions::new([PartitionReassignmentTopicSpec::new(
                "topic-a",
                [
                    PartitionReassignmentSpec::new(0, [1, 2, 3]),
                    PartitionReassignmentSpec::cancel(1),
                ],
            )])
            .with_timeout_ms(6_000)
            .with_allow_replication_factor_change(false);
        let (header, request) =
            build_alter_partition_reassignments_request(18, "client-l", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::AlterPartitionReassignments as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_ALTER_PARTITION_REASSIGNMENTS
        );
        assert_eq!(request.timeout_ms, 6_000);
        assert!(!request.allow_replication_factor_change);
        let topic = &request.topics[0];
        assert_eq!(topic.name.to_string(), "topic-a");
        assert_eq!(topic.partitions[0].partition_index, 0);
        assert_eq!(
            topic.partitions[0]
                .replicas
                .as_ref()
                .unwrap()
                .iter()
                .copied()
                .map(i32::from)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(topic.partitions[1].partition_index, 1);
        assert!(topic.partitions[1].replicas.is_none());
    }

    #[test]
    fn describe_quorum_request_uses_topic_partition_filters() {
        let filter = [TopicPartitionFilter::new("cluster-metadata", [0])];
        let (header, request) = build_describe_quorum_request(19, "client-l", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeQuorum as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_QUORUM);
        assert_eq!(request.topics[0].topic_name.to_string(), "cluster-metadata");
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    }

    #[test]
    fn elect_leaders_request_preserves_type_timeout_and_optional_scope() {
        let scoped = ElectLeadersOptions::new(
            ELECTION_TYPE_UNCLEAN,
            [TopicPartitionFilter::new("topic-a", [0, 2])],
        )
        .with_timeout_ms(7_000);
        let (header, request) = build_elect_leaders_request(20, "client-m", &scoped);

        assert_eq!(header.request_api_key, ApiKey::ElectLeaders as i16);
        assert_eq!(header.request_api_version, API_VERSION_ELECT_LEADERS);
        assert_eq!(request.election_type, ELECTION_TYPE_UNCLEAN);
        assert_eq!(request.timeout_ms, 7_000);
        let topics = request.topic_partitions.as_ref().unwrap();
        assert_eq!(topics[0].topic.to_string(), "topic-a");
        assert_eq!(topics[0].partitions, vec![0, 2]);

        let (_, all_request) = build_elect_leaders_request(
            21,
            "client-n",
            &ElectLeadersOptions::all_partitions(ELECTION_TYPE_PREFERRED),
        );
        assert!(all_request.topic_partitions.is_none());
        assert_eq!(all_request.election_type, ELECTION_TYPE_PREFERRED);
    }

    #[test]
    fn consumer_group_describe_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_consumer_group_describe_request(18, "client-m", &["group-a"], true);

        assert_eq!(header.request_api_key, ApiKey::ConsumerGroupDescribe as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_CONSUMER_GROUP_DESCRIBE
        );
        assert!(request.include_authorized_operations);
        assert_eq!(request.group_ids[0].to_string(), "group-a");
    }

    #[test]
    fn share_group_describe_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_share_group_describe_request(22, "client-n", &["share-a"], true);

        assert_eq!(header.request_api_key, ApiKey::ShareGroupDescribe as i16);
        assert_eq!(header.request_api_version, API_VERSION_SHARE_GROUP_DESCRIBE);
        assert!(request.include_authorized_operations);
        assert_eq!(request.group_ids[0].to_string(), "share-a");
    }

    #[test]
    fn describe_share_group_offsets_request_distinguishes_all_and_filtered_topics() {
        let groups = [
            ShareGroupOffsetRequest::all_partitions("share-a"),
            ShareGroupOffsetRequest::with_topics(
                "share-b",
                [TopicPartitionFilter::new("topic-a", [0, 2])],
            ),
        ];
        let (header, request) = build_describe_share_group_offsets_request(23, "client-o", &groups);

        assert_eq!(
            header.request_api_key,
            ApiKey::DescribeShareGroupOffsets as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS
        );
        assert_eq!(request.groups[0].group_id.to_string(), "share-a");
        assert!(request.groups[0].topics.is_none());
        assert_eq!(request.groups[1].group_id.to_string(), "share-b");
        let topics = request.groups[1].topics.as_ref().unwrap();
        assert_eq!(topics[0].topic_name.to_string(), "topic-a");
        assert_eq!(topics[0].partitions, vec![0, 2]);
    }

    #[test]
    fn create_partitions_request_preserves_options_and_assignments() {
        let options = CreatePartitionsOptions::new([
            CreatePartitionsTopicSpec::new("topic-a", 6).with_assignments([[1, 2], [2, 3]]),
            CreatePartitionsTopicSpec::new("topic-b", 3),
        ])
        .with_timeout_ms(8_000)
        .with_validate_only(true);
        let (header, request) = build_create_partitions_request(24, "client-p", &options);

        assert_eq!(header.request_api_key, ApiKey::CreatePartitions as i16);
        assert_eq!(header.request_api_version, API_VERSION_CREATE_PARTITIONS);
        assert_eq!(request.timeout_ms, 8_000);
        assert!(request.validate_only);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].count, 6);
        let assignments = request.topics[0].assignments.as_ref().unwrap();
        assert_eq!(
            assignments[0]
                .broker_ids
                .iter()
                .copied()
                .map(i32::from)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        assert!(request.topics[1].assignments.is_none());
    }

    #[test]
    fn offset_for_leader_epoch_request_preserves_epoch_fields() {
        let topics = [LeaderEpochTopicRequest::new(
            "topic-a",
            [LeaderEpochPartitionRequest::new(0, -1, 7)],
        )];
        let (header, request) = build_offset_for_leader_epoch_request(25, "client-q", &topics);

        assert_eq!(header.request_api_key, ApiKey::OffsetForLeaderEpoch as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_OFFSET_FOR_LEADER_EPOCH
        );
        assert_eq!(i32::from(request.replica_id), -1);
        assert_eq!(request.topics[0].topic.to_string(), "topic-a");
        let partition = &request.topics[0].partitions[0];
        assert_eq!(partition.partition, 0);
        assert_eq!(partition.current_leader_epoch, -1);
        assert_eq!(partition.leader_epoch, 7);
    }

    #[test]
    fn list_config_resources_request_accepts_resource_type_filters() {
        let (header, request) = build_list_config_resources_request(
            21,
            "client-p",
            &[CONFIG_RESOURCE_TYPE_TOPIC, CONFIG_RESOURCE_TYPE_BROKER],
        );

        assert_eq!(header.request_api_key, ApiKey::ListConfigResources as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_LIST_CONFIG_RESOURCES
        );
        assert_eq!(
            request.resource_types,
            vec![CONFIG_RESOURCE_TYPE_TOPIC, CONFIG_RESOURCE_TYPE_BROKER]
        );
    }

    #[test]
    fn describe_topic_partitions_request_accepts_topics_limit_and_cursor() {
        let options = DescribeTopicPartitionsOptions::new(250)
            .with_topics(["topic-a", "topic-b"])
            .with_cursor(TopicPartitionsCursor::new("topic-a", 3));
        let (header, request) = build_describe_topic_partitions_request(22, "client-q", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::DescribeTopicPartitions as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_TOPIC_PARTITIONS
        );
        assert_eq!(request.response_partition_limit, 250);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        let cursor = request.cursor.unwrap();
        assert_eq!(cursor.topic_name.to_string(), "topic-a");
        assert_eq!(cursor.partition_index, 3);
    }

    #[test]
    fn describe_client_quotas_request_accepts_entity_filters() {
        let options = DescribeClientQuotasOptions::new()
            .with_component(ClientQuotaEntityFilter::exact("user", "alice"))
            .with_component(ClientQuotaEntityFilter::default_entity("client-id"))
            .with_component(ClientQuotaEntityFilter::any_specified("ip"))
            .strict();
        let (header, request) = build_describe_client_quotas_request(23, "client-r", &options);

        assert_eq!(header.request_api_key, ApiKey::DescribeClientQuotas as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_CLIENT_QUOTAS
        );
        assert!(request.strict);
        assert_eq!(request.components.len(), 3);
        assert_eq!(request.components[0].entity_type.to_string(), "user");
        assert_eq!(request.components[0].match_type, CLIENT_QUOTA_MATCH_EXACT);
        assert_eq!(
            request.components[0]
                ._match
                .as_ref()
                .map(ToString::to_string),
            Some("alice".to_owned())
        );
        assert_eq!(request.components[1].match_type, CLIENT_QUOTA_MATCH_DEFAULT);
        assert!(request.components[1]._match.is_none());
        assert_eq!(
            request.components[2].match_type,
            CLIENT_QUOTA_MATCH_ANY_SPECIFIED
        );
        assert!(request.components[2]._match.is_none());
    }

    #[test]
    fn alter_client_quotas_request_preserves_entities_and_ops() {
        let options = AlterClientQuotasOptions::new([ClientQuotaAlteration::new(
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
        let (header, request) = build_alter_client_quotas_request(24, "client-r", &options);

        assert_eq!(header.request_api_key, ApiKey::AlterClientQuotas as i16);
        assert_eq!(header.request_api_version, API_VERSION_ALTER_CLIENT_QUOTAS);
        assert!(request.validate_only);
        let entry = &request.entries[0];
        assert_eq!(entry.entity[0].entity_type.to_string(), "user");
        assert_eq!(
            entry.entity[0]
                .entity_name
                .as_ref()
                .map(ToString::to_string),
            Some("alice".to_owned())
        );
        assert_eq!(entry.entity[1].entity_type.to_string(), "client-id");
        assert!(entry.entity[1].entity_name.is_none());
        assert_eq!(entry.ops[0].key.to_string(), "producer_byte_rate");
        assert!((entry.ops[0].value - 1024.5).abs() < f64::EPSILON);
        assert!(!entry.ops[0].remove);
        assert_eq!(entry.ops[1].key.to_string(), "consumer_byte_rate");
        assert!(entry.ops[1].remove);
    }

    #[test]
    fn describe_user_scram_credentials_request_distinguishes_all_and_selected_users() {
        let (all_header, all_request) =
            build_describe_user_scram_credentials_request(24, "client-s", None);
        let (selected_header, selected_request) =
            build_describe_user_scram_credentials_request(25, "client-t", Some(&["alice", "bob"]));

        assert_eq!(
            all_header.request_api_key,
            ApiKey::DescribeUserScramCredentials as i16
        );
        assert_eq!(
            all_header.request_api_version,
            API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS
        );
        assert!(all_request.users.is_none());
        assert_eq!(selected_header.correlation_id, 25);
        let users = selected_request.users.unwrap();
        assert_eq!(users[0].name.to_string(), "alice");
        assert_eq!(users[1].name.to_string(), "bob");
    }

    #[test]
    fn describe_producers_request_uses_topic_partition_filters() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 1])];
        let (header, request) = build_describe_producers_request(26, "client-u", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeProducers as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_PRODUCERS);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partition_indexes, vec![0, 1]);
    }

    #[test]
    fn offset_delete_request_uses_topic_partition_filters() {
        let filters = [
            TopicPartitionFilter::new("topic-a", [0, 2]),
            TopicPartitionFilter::new("topic-b", [1]),
        ];
        let (header, request) = build_offset_delete_request(27, "client-v", "group-a", &filters);

        assert_eq!(header.request_api_key, ApiKey::OffsetDelete as i16);
        assert_eq!(header.request_api_version, API_VERSION_OFFSET_DELETE);
        assert_eq!(request.group_id.to_string(), "group-a");
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[1].partition_index, 2);
        assert_eq!(request.topics[1].name.to_string(), "topic-b");
        assert_eq!(request.topics[1].partitions[0].partition_index, 1);
    }

    #[test]
    fn list_transactions_request_accepts_all_filters() {
        let options = ListTransactionsOptions::new()
            .with_state_filters(["Ongoing", "PrepareCommit"])
            .with_producer_id_filters([42, 43])
            .with_duration_filter_ms(30_000)
            .with_transactional_id_pattern("rustfs-.*");
        let (header, request) = build_list_transactions_request(28, "client-w", &options);

        assert_eq!(header.request_api_key, ApiKey::ListTransactions as i16);
        assert_eq!(header.request_api_version, API_VERSION_LIST_TRANSACTIONS);
        assert_eq!(
            request.state_filters,
            vec![
                StrBytes::from_static_str("Ongoing"),
                StrBytes::from_static_str("PrepareCommit"),
            ]
        );
        assert_eq!(
            request
                .producer_id_filters
                .into_iter()
                .map(i64::from)
                .collect::<Vec<_>>(),
            vec![42, 43]
        );
        assert_eq!(request.duration_filter, 30_000);
        assert_eq!(
            request
                .transactional_id_pattern
                .map(|value| value.to_string()),
            Some("rustfs-.*".to_owned())
        );
    }

    #[test]
    fn describe_transactions_request_includes_transactional_ids() {
        let (header, request) =
            build_describe_transactions_request(26, "client-u", &["txn-a", "txn-b"]);

        assert_eq!(header.request_api_key, ApiKey::DescribeTransactions as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_DESCRIBE_TRANSACTIONS
        );
        assert_eq!(request.transactional_ids[0].to_string(), "txn-a");
        assert_eq!(request.transactional_ids[1].to_string(), "txn-b");
    }

    #[test]
    fn convert_describe_cluster_response_preserves_new_fields() {
        let response = DescribeClusterResponse::default()
            .with_throttle_time_ms(10)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_endpoint_type(DESCRIBE_CLUSTER_ENDPOINT_BROKERS)
            .with_cluster_id(StrBytes::from_static_str("cluster-a"))
            .with_controller_id(BrokerId::from(1))
            .with_brokers(vec![
                DescribeClusterBroker::default()
                    .with_broker_id(BrokerId::from(1))
                    .with_host(StrBytes::from_static_str("broker-1"))
                    .with_port(9092)
                    .with_rack(Some(StrBytes::from_static_str("rack-a")))
                    .with_is_fenced(true),
            ])
            .with_cluster_authorized_operations(123);

        let converted = convert_describe_cluster_response(response);

        assert_eq!(converted.throttle_time_ms, 10);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.cluster_id, "cluster-a");
        assert_eq!(converted.controller_id, 1);
        assert_eq!(converted.cluster_authorized_operations, 123);
        assert_eq!(
            converted.brokers,
            vec![ClusterBroker {
                broker_id: 1,
                host: "broker-1".to_owned(),
                port: 9092,
                rack: Some("rack-a".to_owned()),
                is_fenced: true,
            }]
        );
    }

    #[test]
    fn convert_list_groups_response_preserves_state_and_type() {
        let response = ListGroupsResponse::default()
            .with_throttle_time_ms(11)
            .with_error_code(0)
            .with_groups(vec![
                KpListedGroup::default()
                    .with_group_id(group_id("group-a"))
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_group_type(StrBytes::from_static_str("classic")),
            ]);

        let converted = convert_list_groups_response(response);

        assert_eq!(
            converted,
            ListGroupsResponseData {
                throttle_time_ms: 11,
                error_code: 0,
                groups: vec![ListedGroup {
                    group_id: "group-a".to_owned(),
                    protocol_type: "consumer".to_owned(),
                    group_state: "Stable".to_owned(),
                    group_type: "classic".to_owned(),
                }],
            }
        );
    }

    #[test]
    fn convert_delete_groups_response_preserves_results() {
        let response = DeleteGroupsResponse::default()
            .with_throttle_time_ms(12)
            .with_results(vec![
                KpDeletableGroupResult::default()
                    .with_group_id(group_id("group-a"))
                    .with_error_code(0),
                KpDeletableGroupResult::default()
                    .with_group_id(group_id("group-b"))
                    .with_error_code(15),
            ]);

        let converted = convert_delete_groups_response(response);

        assert_eq!(
            converted,
            DeleteGroupsResponseData {
                throttle_time_ms: 12,
                results: vec![
                    DeletedGroup {
                        group_id: "group-a".to_owned(),
                        error_code: 0,
                    },
                    DeletedGroup {
                        group_id: "group-b".to_owned(),
                        error_code: 15,
                    },
                ],
            }
        );
    }

    #[test]
    fn convert_describe_groups_response_preserves_members_and_authorizations() {
        let response = DescribeGroupsResponse::default()
            .with_throttle_time_ms(12)
            .with_groups(vec![
                KpDescribedGroup::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_group_id(group_id("group-a"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
                    .with_protocol_data(StrBytes::from_static_str("range"))
                    .with_members(vec![
                        KpDescribedGroupMember::default()
                            .with_member_id(StrBytes::from_static_str("member-a"))
                            .with_group_instance_id(Some(StrBytes::from_static_str("instance-a")))
                            .with_client_id(StrBytes::from_static_str("client-a"))
                            .with_client_host(StrBytes::from_static_str("/127.0.0.1"))
                            .with_member_metadata(Bytes::from_static(b"metadata"))
                            .with_member_assignment(Bytes::from_static(b"assignment")),
                    ])
                    .with_authorized_operations(456),
            ]);

        let converted = convert_describe_groups_response(response);

        assert_eq!(converted.throttle_time_ms, 12);
        assert_eq!(converted.groups.len(), 1);
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.groups[0].group_id, "group-a");
        assert_eq!(converted.groups[0].authorized_operations, 456);
        assert_eq!(converted.groups[0].members[0].member_id, "member-a");
        assert_eq!(
            converted.groups[0].members[0].group_instance_id,
            Some("instance-a".to_owned())
        );
        assert_eq!(
            converted.groups[0].members[0].member_metadata,
            Bytes::from_static(b"metadata")
        );
    }

    #[test]
    fn convert_describe_acls_response_preserves_resource_grouping() {
        let response = DescribeAclsResponse::default()
            .with_throttle_time_ms(13)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_resources(vec![
                KpAclResource::default()
                    .with_resource_type(ACL_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a"))
                    .with_pattern_type(ACL_PATTERN_TYPE_LITERAL)
                    .with_acls(vec![
                        KpAclDescription::default()
                            .with_principal(StrBytes::from_static_str("User:alice"))
                            .with_host(StrBytes::from_static_str("*"))
                            .with_operation(ACL_OPERATION_READ)
                            .with_permission_type(ACL_PERMISSION_TYPE_ALLOW),
                    ]),
            ]);

        let converted = convert_describe_acls_response(response);

        assert_eq!(converted.throttle_time_ms, 13);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(
            converted.resources[0].resource_type,
            ACL_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(converted.resources[0].resource_name, "topic-a");
        assert_eq!(
            converted.resources[0].pattern_type,
            ACL_PATTERN_TYPE_LITERAL
        );
        assert_eq!(converted.resources[0].acls[0].principal, "User:alice");
        assert_eq!(converted.resources[0].acls[0].host, "*");
        assert_eq!(converted.resources[0].acls[0].operation, ACL_OPERATION_READ);
        assert_eq!(
            converted.resources[0].acls[0].permission_type,
            ACL_PERMISSION_TYPE_ALLOW
        );
    }

    #[test]
    fn convert_create_acls_response_preserves_results() {
        let response = CreateAclsResponse::default()
            .with_throttle_time_ms(14)
            .with_results(vec![
                KpAclCreationResult::default()
                    .with_error_code(0)
                    .with_error_message(None),
                KpAclCreationResult::default()
                    .with_error_code(31)
                    .with_error_message(Some(StrBytes::from_static_str("duplicate"))),
            ]);

        let converted = convert_create_acls_response(response);

        assert_eq!(converted.throttle_time_ms, 14);
        assert_eq!(
            converted.results,
            vec![
                CreateAclResult {
                    error_code: 0,
                    error_message: None,
                },
                CreateAclResult {
                    error_code: 31,
                    error_message: Some("duplicate".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn convert_delete_acls_response_preserves_matching_acls() {
        let response = DeleteAclsResponse::default()
            .with_throttle_time_ms(15)
            .with_filter_results(vec![
                KpDeleteAclsFilterResult::default()
                    .with_error_code(0)
                    .with_error_message(None)
                    .with_matching_acls(vec![
                        KpDeleteAclsMatchingAcl::default()
                            .with_error_code(0)
                            .with_error_message(None)
                            .with_resource_type(ACL_RESOURCE_TYPE_TOPIC)
                            .with_resource_name(StrBytes::from_static_str("topic-a"))
                            .with_pattern_type(ACL_PATTERN_TYPE_LITERAL)
                            .with_principal(StrBytes::from_static_str("User:alice"))
                            .with_host(StrBytes::from_static_str("*"))
                            .with_operation(ACL_OPERATION_READ)
                            .with_permission_type(ACL_PERMISSION_TYPE_ALLOW),
                    ]),
            ]);

        let converted = convert_delete_acls_response(response);

        assert_eq!(converted.throttle_time_ms, 15);
        assert_eq!(converted.filter_results[0].error_code, 0);
        let acl = &converted.filter_results[0].matching_acls[0];
        assert_eq!(acl.resource_type, ACL_RESOURCE_TYPE_TOPIC);
        assert_eq!(acl.resource_name, "topic-a");
        assert_eq!(acl.pattern_type, ACL_PATTERN_TYPE_LITERAL);
        assert_eq!(acl.principal, "User:alice");
        assert_eq!(acl.host, "*");
        assert_eq!(acl.operation, ACL_OPERATION_READ);
        assert_eq!(acl.permission_type, ACL_PERMISSION_TYPE_ALLOW);
    }

    #[test]
    fn convert_describe_configs_response_preserves_config_metadata() {
        let response = DescribeConfigsResponse::default()
            .with_throttle_time_ms(14)
            .with_results(vec![
                KpDescribeConfigsResult::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a"))
                    .with_configs(vec![
                        KpDescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str("retention.ms"))
                            .with_value(Some(StrBytes::from_static_str("86400000")))
                            .with_read_only(false)
                            .with_config_source(5)
                            .with_is_sensitive(false)
                            .with_synonyms(vec![
                                KpDescribeConfigsSynonym::default()
                                    .with_name(StrBytes::from_static_str("retention.ms"))
                                    .with_value(Some(StrBytes::from_static_str("86400000")))
                                    .with_source(5),
                            ])
                            .with_config_type(2)
                            .with_documentation(Some(StrBytes::from_static_str(
                                "retention window",
                            ))),
                    ]),
            ]);

        let converted = convert_describe_configs_response(response);

        assert_eq!(converted.throttle_time_ms, 14);
        assert_eq!(converted.results[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.results[0].resource_name, "topic-a");
        assert_eq!(converted.results[0].configs[0].name, "retention.ms");
        assert_eq!(
            converted.results[0].configs[0].value,
            Some("86400000".to_owned())
        );
        assert_eq!(converted.results[0].configs[0].synonyms[0].source, 5);
        assert_eq!(
            converted.results[0].configs[0].documentation,
            Some("retention window".to_owned())
        );
    }

    #[test]
    fn convert_incremental_alter_configs_response_preserves_resource_results() {
        let response = IncrementalAlterConfigsResponse::default()
            .with_throttle_time_ms(15)
            .with_responses(vec![
                KpIncrementalAlterConfigsResourceResponse::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a")),
            ]);

        let converted = convert_incremental_alter_configs_response(response);

        assert_eq!(converted.throttle_time_ms, 15);
        assert_eq!(converted.responses[0].error_code, 0);
        assert_eq!(converted.responses[0].error_message, Some("ok".to_owned()));
        assert_eq!(
            converted.responses[0].resource_type,
            CONFIG_RESOURCE_TYPE_TOPIC
        );
        assert_eq!(converted.responses[0].resource_name, "topic-a");
    }

    #[test]
    fn convert_describe_delegation_token_response_preserves_sensitive_token_metadata() {
        let response = DescribeDelegationTokenResponse::default()
            .with_error_code(0)
            .with_tokens(vec![
                KpDelegationToken::default()
                    .with_principal_type(StrBytes::from_static_str("User"))
                    .with_principal_name(StrBytes::from_static_str("alice"))
                    .with_token_requester_principal_type(StrBytes::from_static_str("User"))
                    .with_token_requester_principal_name(StrBytes::from_static_str("admin"))
                    .with_issue_timestamp(1_700_000)
                    .with_expiry_timestamp(1_800_000)
                    .with_max_timestamp(1_900_000)
                    .with_token_id(StrBytes::from_static_str("token-a"))
                    .with_hmac(Bytes::from_static(b"hmac"))
                    .with_renewers(vec![
                        KpDelegationTokenRenewer::default()
                            .with_principal_type(StrBytes::from_static_str("User"))
                            .with_principal_name(StrBytes::from_static_str("bob")),
                    ]),
            ])
            .with_throttle_time_ms(15);

        let converted = convert_describe_delegation_token_response(response);

        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.throttle_time_ms, 15);
        assert_eq!(converted.tokens[0].owner, KafkaPrincipal::user("alice"));
        assert_eq!(
            converted.tokens[0].requester,
            Some(KafkaPrincipal::user("admin"))
        );
        assert_eq!(converted.tokens[0].token_id, "token-a");
        assert_eq!(converted.tokens[0].hmac, Bytes::from_static(b"hmac"));
        assert_eq!(
            converted.tokens[0].renewers,
            vec![KafkaPrincipal::user("bob")]
        );
    }

    #[test]
    fn convert_describe_log_dirs_response_preserves_storage_details() {
        let response = DescribeLogDirsResponse::default()
            .with_throttle_time_ms(16)
            .with_error_code(0)
            .with_results(vec![
                KpDescribeLogDirsResult::default()
                    .with_error_code(0)
                    .with_log_dir(StrBytes::from_static_str("/kafka-logs"))
                    .with_total_bytes(1_000)
                    .with_usable_bytes(750)
                    .with_topics(vec![
                        KpDescribeLogDirsTopic::default()
                            .with_name(StrBytes::from_static_str("topic-a").into())
                            .with_partitions(vec![
                                KpDescribeLogDirsPartition::default()
                                    .with_partition_index(0)
                                    .with_partition_size(256)
                                    .with_offset_lag(3)
                                    .with_is_future_key(true),
                            ]),
                    ]),
            ]);

        let converted = convert_describe_log_dirs_response(response);

        assert_eq!(converted.throttle_time_ms, 16);
        assert_eq!(converted.results[0].log_dir, "/kafka-logs");
        assert_eq!(converted.results[0].total_bytes, 1_000);
        assert_eq!(converted.results[0].topics[0].name, "topic-a");
        assert_eq!(converted.results[0].topics[0].partitions[0].offset_lag, 3);
        assert!(converted.results[0].topics[0].partitions[0].is_future_key);
    }

    #[test]
    fn convert_list_partition_reassignments_response_preserves_replica_sets() {
        let response = ListPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(17)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_topics(vec![
                KpOngoingTopicReassignment::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpOngoingPartitionReassignment::default()
                            .with_partition_index(0)
                            .with_replicas(vec![BrokerId::from(1), BrokerId::from(2)])
                            .with_adding_replicas(vec![BrokerId::from(3)])
                            .with_removing_replicas(vec![BrokerId::from(1)]),
                    ]),
            ]);

        let converted = convert_list_partition_reassignments_response(response);

        assert_eq!(converted.throttle_time_ms, 17);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].replicas, vec![1, 2]);
        assert_eq!(converted.topics[0].partitions[0].adding_replicas, vec![3]);
        assert_eq!(converted.topics[0].partitions[0].removing_replicas, vec![1]);
    }

    #[test]
    fn convert_delete_records_response_preserves_low_watermarks() {
        let response = DeleteRecordsResponse::default()
            .with_throttle_time_ms(18)
            .with_topics(vec![
                KpDeleteRecordsTopicResult::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpDeleteRecordsPartitionResult::default()
                            .with_partition_index(0)
                            .with_low_watermark(42)
                            .with_error_code(0),
                    ]),
            ]);

        let converted = convert_delete_records_response(response);

        assert_eq!(converted.throttle_time_ms, 18);
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].partition_index, 0);
        assert_eq!(converted.topics[0].partitions[0].low_watermark, 42);
        assert_eq!(converted.topics[0].partitions[0].error_code, 0);
    }

    #[test]
    fn convert_alter_partition_reassignments_response_preserves_nested_errors() {
        let response = AlterPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(19)
            .with_allow_replication_factor_change(false)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_responses(vec![
                KpReassignableTopicResponse::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpReassignablePartitionResponse::default()
                            .with_partition_index(1)
                            .with_error_code(15)
                            .with_error_message(Some(StrBytes::from_static_str("denied"))),
                    ]),
            ]);

        let converted = convert_alter_partition_reassignments_response(response);

        assert_eq!(converted.throttle_time_ms, 19);
        assert!(!converted.allow_replication_factor_change);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.responses[0].name, "topic-a");
        assert_eq!(converted.responses[0].partitions[0].partition_index, 1);
        assert_eq!(converted.responses[0].partitions[0].error_code, 15);
        assert_eq!(
            converted.responses[0].partitions[0].error_message,
            Some("denied".to_owned())
        );
    }

    #[test]
    fn convert_describe_quorum_response_preserves_kraft_state() {
        let response = DescribeQuorumResponse::default()
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_topics(vec![
                KpQuorumTopic::default()
                    .with_topic_name(StrBytes::from_static_str("cluster-metadata").into())
                    .with_partitions(vec![
                        KpQuorumPartition::default()
                            .with_partition_index(0)
                            .with_error_code(0)
                            .with_error_message(Some(StrBytes::from_static_str("ok")))
                            .with_leader_id(BrokerId::from(1))
                            .with_leader_epoch(7)
                            .with_high_watermark(128)
                            .with_current_voters(vec![
                                KpQuorumReplica::default()
                                    .with_replica_id(BrokerId::from(1))
                                    .with_log_end_offset(128)
                                    .with_last_fetch_timestamp(-1)
                                    .with_last_caught_up_timestamp(1_700_000),
                            ])
                            .with_observers(vec![
                                KpQuorumReplica::default()
                                    .with_replica_id(BrokerId::from(2))
                                    .with_log_end_offset(120)
                                    .with_last_fetch_timestamp(1_699_900)
                                    .with_last_caught_up_timestamp(1_699_800),
                            ]),
                    ]),
            ])
            .with_nodes(vec![
                KpQuorumNode::default()
                    .with_node_id(BrokerId::from(1))
                    .with_listeners(vec![
                        KpQuorumListener::default()
                            .with_name(StrBytes::from_static_str("CONTROLLER"))
                            .with_host(StrBytes::from_static_str("broker-1"))
                            .with_port(9093),
                    ]),
            ]);

        let converted = convert_describe_quorum_response(response);

        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.topics[0].name, "cluster-metadata");
        let partition = &converted.topics[0].partitions[0];
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.leader_epoch, 7);
        assert_eq!(partition.high_watermark, 128);
        assert_eq!(partition.current_voters[0].replica_id, 1);
        assert_eq!(partition.observers[0].log_end_offset, 120);
        assert_eq!(
            partition.current_voters[0].replica_directory_id,
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(converted.nodes[0].listeners[0].host, "broker-1");
        assert_eq!(converted.nodes[0].listeners[0].port, 9093);
    }

    #[test]
    fn convert_elect_leaders_response_preserves_partition_errors() {
        let response = ElectLeadersResponse::default()
            .with_throttle_time_ms(20)
            .with_error_code(0)
            .with_replica_election_results(vec![
                KpReplicaElectionResult::default()
                    .with_topic(StrBytes::from_static_str("topic-a").into())
                    .with_partition_result(vec![
                        KpElectionPartitionResult::default()
                            .with_partition_id(0)
                            .with_error_code(0)
                            .with_error_message(Some(StrBytes::from_static_str("ok"))),
                    ]),
            ]);

        let converted = convert_elect_leaders_response(response);

        assert_eq!(converted.throttle_time_ms, 20);
        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.replica_election_results[0].topic, "topic-a");
        assert_eq!(
            converted.replica_election_results[0].partition_results[0].partition_id,
            0
        );
        assert_eq!(
            converted.replica_election_results[0].partition_results[0].error_message,
            Some("ok".to_owned())
        );
    }

    #[test]
    fn convert_consumer_group_describe_response_preserves_assignments() {
        let assignment = KpConsumerGroupAssignment::default().with_topic_partitions(vec![
            KpConsumerGroupTopicPartitions::default()
                .with_topic_name(StrBytes::from_static_str("topic-a").into())
                .with_partitions(vec![0, 2]),
        ]);
        let response = ConsumerGroupDescribeResponse::default()
            .with_throttle_time_ms(24)
            .with_groups(vec![
                KpConsumerGroupDescription::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_group_id(group_id("group-a"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_group_epoch(7)
                    .with_assignment_epoch(8)
                    .with_assignor_name(StrBytes::from_static_str("range"))
                    .with_members(vec![
                        KpConsumerGroupMember::default()
                            .with_member_id(StrBytes::from_static_str("member-a"))
                            .with_instance_id(Some(StrBytes::from_static_str("instance-a")))
                            .with_rack_id(Some(StrBytes::from_static_str("rack-a")))
                            .with_member_epoch(9)
                            .with_client_id(StrBytes::from_static_str("client-a"))
                            .with_client_host(StrBytes::from_static_str("/127.0.0.1"))
                            .with_subscribed_topic_names(vec![
                                StrBytes::from_static_str("topic-a").into(),
                            ])
                            .with_subscribed_topic_regex(Some(StrBytes::from_static_str(
                                "topic-.*",
                            )))
                            .with_assignment(assignment.clone())
                            .with_target_assignment(assignment)
                            .with_member_type(1),
                    ])
                    .with_authorized_operations(321),
            ]);

        let converted = convert_consumer_group_describe_response(response);

        assert_eq!(converted.throttle_time_ms, 24);
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.groups[0].group_id, "group-a");
        assert_eq!(converted.groups[0].group_epoch, 7);
        assert_eq!(converted.groups[0].assignment_epoch, 8);
        assert_eq!(converted.groups[0].assignor_name, "range");
        assert_eq!(converted.groups[0].authorized_operations, 321);
        let member = &converted.groups[0].members[0];
        assert_eq!(member.member_id, "member-a");
        assert_eq!(member.instance_id, Some("instance-a".to_owned()));
        assert_eq!(member.rack_id, Some("rack-a".to_owned()));
        assert_eq!(member.member_epoch, 9);
        assert_eq!(member.subscribed_topic_names, vec!["topic-a"]);
        assert_eq!(member.subscribed_topic_regex, Some("topic-.*".to_owned()));
        assert_eq!(member.member_type, 1);
        assert_eq!(member.assignment.topic_partitions[0].topic_name, "topic-a");
        assert_eq!(member.assignment.topic_partitions[0].partitions, vec![0, 2]);
        assert_eq!(
            member.assignment.topic_partitions[0].topic_id,
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn convert_share_group_describe_response_preserves_assignments() {
        let assignment = KpShareGroupAssignment::default().with_topic_partitions(vec![
            KpShareGroupTopicPartitions::default()
                .with_topic_name(StrBytes::from_static_str("topic-a").into())
                .with_partitions(vec![1, 3]),
        ]);
        let response = ShareGroupDescribeResponse::default()
            .with_throttle_time_ms(26)
            .with_groups(vec![
                KpShareGroupDescription::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_group_id(group_id("share-a"))
                    .with_group_state(StrBytes::from_static_str("Stable"))
                    .with_group_epoch(4)
                    .with_assignment_epoch(5)
                    .with_assignor_name(StrBytes::from_static_str("share"))
                    .with_members(vec![
                        KpShareGroupMember::default()
                            .with_member_id(StrBytes::from_static_str("member-a"))
                            .with_rack_id(Some(StrBytes::from_static_str("rack-a")))
                            .with_member_epoch(6)
                            .with_client_id(StrBytes::from_static_str("client-a"))
                            .with_client_host(StrBytes::from_static_str("/127.0.0.1"))
                            .with_subscribed_topic_names(vec![
                                StrBytes::from_static_str("topic-a").into(),
                            ])
                            .with_assignment(assignment),
                    ])
                    .with_authorized_operations(777),
            ]);

        let converted = convert_share_group_describe_response(response);

        assert_eq!(converted.throttle_time_ms, 26);
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.groups[0].group_id, "share-a");
        assert_eq!(converted.groups[0].group_state, "Stable");
        assert_eq!(converted.groups[0].group_epoch, 4);
        assert_eq!(converted.groups[0].assignment_epoch, 5);
        assert_eq!(converted.groups[0].assignor_name, "share");
        assert_eq!(converted.groups[0].authorized_operations, 777);
        let member = &converted.groups[0].members[0];
        assert_eq!(member.member_id, "member-a");
        assert_eq!(member.rack_id, Some("rack-a".to_owned()));
        assert_eq!(member.member_epoch, 6);
        assert_eq!(member.client_id, "client-a");
        assert_eq!(member.client_host, "/127.0.0.1");
        assert_eq!(member.subscribed_topic_names, vec!["topic-a"]);
        assert_eq!(member.assignment.topic_partitions[0].topic_name, "topic-a");
        assert_eq!(member.assignment.topic_partitions[0].partitions, vec![1, 3]);
        assert_eq!(
            member.assignment.topic_partitions[0].topic_id,
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn convert_list_config_resources_response_preserves_resource_types() {
        let response = ListConfigResourcesResponse::default()
            .with_throttle_time_ms(18)
            .with_error_code(0)
            .with_config_resources(vec![
                KpListedConfigResource::default()
                    .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str("topic-a")),
                KpListedConfigResource::default()
                    .with_resource_type(CONFIG_RESOURCE_TYPE_BROKER)
                    .with_resource_name(StrBytes::from_static_str("1")),
            ]);

        let converted = convert_list_config_resources_response(response);

        assert_eq!(converted.throttle_time_ms, 18);
        assert_eq!(converted.error_code, 0);
        assert_eq!(
            converted.resources,
            vec![
                ListedConfigResource {
                    resource_type: CONFIG_RESOURCE_TYPE_TOPIC,
                    resource_name: "topic-a".to_owned(),
                },
                ListedConfigResource {
                    resource_type: CONFIG_RESOURCE_TYPE_BROKER,
                    resource_name: "1".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn convert_create_partitions_response_preserves_topic_results() {
        let response = CreatePartitionsResponse::default()
            .with_throttle_time_ms(21)
            .with_results(vec![
                KpCreatePartitionsTopicResult::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
            ]);

        let converted = convert_create_partitions_response(response);

        assert_eq!(converted.throttle_time_ms, 21);
        assert_eq!(converted.results[0].name, "topic-a");
        assert_eq!(converted.results[0].error_code, 0);
        assert_eq!(converted.results[0].error_message, Some("ok".to_owned()));
    }

    #[test]
    fn convert_describe_topic_partitions_response_preserves_page_state() {
        let response = DescribeTopicPartitionsResponse::default()
            .with_throttle_time_ms(25)
            .with_topics(vec![
                KpDescribedTopicPartitionsTopic::default()
                    .with_error_code(0)
                    .with_name(Some(StrBytes::from_static_str("topic-a").into()))
                    .with_is_internal(false)
                    .with_partitions(vec![
                        KpDescribedTopicPartition::default()
                            .with_error_code(0)
                            .with_partition_index(1)
                            .with_leader_id(BrokerId::from(2))
                            .with_leader_epoch(7)
                            .with_replica_nodes(vec![BrokerId::from(1), BrokerId::from(2)])
                            .with_isr_nodes(vec![BrokerId::from(2)])
                            .with_eligible_leader_replicas(Some(vec![BrokerId::from(2)]))
                            .with_last_known_elr(Some(vec![BrokerId::from(1)]))
                            .with_offline_replicas(vec![BrokerId::from(3)]),
                    ])
                    .with_topic_authorized_operations(654),
            ])
            .with_next_cursor(Some(
                KpTopicPartitionsResponseCursor::default()
                    .with_topic_name(StrBytes::from_static_str("topic-a").into())
                    .with_partition_index(2),
            ));

        let converted = convert_describe_topic_partitions_response(response);

        assert_eq!(converted.throttle_time_ms, 25);
        assert_eq!(
            converted.next_cursor,
            Some(TopicPartitionsCursor::new("topic-a", 2))
        );
        let topic = &converted.topics[0];
        assert_eq!(topic.name, Some("topic-a".to_owned()));
        assert_eq!(topic.topic_id, "00000000-0000-0000-0000-000000000000");
        assert!(!topic.is_internal);
        assert_eq!(topic.topic_authorized_operations, 654);
        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_index, 1);
        assert_eq!(partition.leader_id, 2);
        assert_eq!(partition.leader_epoch, 7);
        assert_eq!(partition.replica_nodes, vec![1, 2]);
        assert_eq!(partition.isr_nodes, vec![2]);
        assert_eq!(partition.eligible_leader_replicas, Some(vec![2]));
        assert_eq!(partition.last_known_elr, Some(vec![1]));
        assert_eq!(partition.offline_replicas, vec![3]);
    }

    #[test]
    fn convert_describe_share_group_offsets_response_preserves_offsets() {
        let response = DescribeShareGroupOffsetsResponse::default()
            .with_throttle_time_ms(27)
            .with_groups(vec![
                KpShareGroupOffsetGroup::default()
                    .with_group_id(group_id("share-a"))
                    .with_topics(vec![
                        KpShareGroupOffsetTopic::default()
                            .with_topic_name(StrBytes::from_static_str("topic-a").into())
                            .with_partitions(vec![
                                KpShareGroupOffsetPartition::default()
                                    .with_partition_index(1)
                                    .with_start_offset(42)
                                    .with_leader_epoch(7)
                                    .with_error_code(0)
                                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
                            ]),
                    ])
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
            ]);

        let converted = convert_describe_share_group_offsets_response(response);

        assert_eq!(converted.throttle_time_ms, 27);
        assert_eq!(converted.groups[0].group_id, "share-a");
        assert_eq!(converted.groups[0].error_message, Some("ok".to_owned()));
        let topic = &converted.groups[0].topics[0];
        assert_eq!(topic.topic_name, "topic-a");
        assert_eq!(topic.topic_id, "00000000-0000-0000-0000-000000000000");
        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_index, 1);
        assert_eq!(partition.start_offset, 42);
        assert_eq!(partition.leader_epoch, 7);
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.error_message, Some("ok".to_owned()));
    }

    #[test]
    fn convert_offset_delete_response_preserves_partition_results() {
        let response = OffsetDeleteResponse::default()
            .with_error_code(0)
            .with_throttle_time_ms(28)
            .with_topics(vec![
                KpOffsetDeleteTopic::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpOffsetDeletePartition::default()
                            .with_partition_index(0)
                            .with_error_code(0),
                        KpOffsetDeletePartition::default()
                            .with_partition_index(2)
                            .with_error_code(15),
                    ]),
            ]);

        let converted = convert_offset_delete_response(response);

        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.throttle_time_ms, 28);
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(
            converted.topics[0].partitions,
            vec![
                OffsetDeletePartitionResult {
                    partition_index: 0,
                    error_code: 0,
                },
                OffsetDeletePartitionResult {
                    partition_index: 2,
                    error_code: 15,
                },
            ]
        );
    }

    #[test]
    fn convert_describe_client_quotas_response_preserves_entities_and_values() {
        let response = DescribeClientQuotasResponse::default()
            .with_throttle_time_ms(19)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_entries(Some(vec![
                KpClientQuotaEntry::default()
                    .with_entity(vec![
                        KpClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("user"))
                            .with_entity_name(Some(StrBytes::from_static_str("alice"))),
                        KpClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("client-id"))
                            .with_entity_name(None),
                    ])
                    .with_values(vec![
                        KpClientQuotaValue::default()
                            .with_key(StrBytes::from_static_str("producer_byte_rate"))
                            .with_value(1024.5),
                    ]),
            ]));

        let converted = convert_describe_client_quotas_response(response);

        assert_eq!(converted.throttle_time_ms, 19);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        let entry = &converted.entries.as_ref().unwrap()[0];
        assert_eq!(entry.entity[0].entity_type, "user");
        assert_eq!(entry.entity[0].entity_name, Some("alice".to_owned()));
        assert_eq!(entry.entity[1].entity_type, "client-id");
        assert!(entry.entity[1].entity_name.is_none());
        assert_eq!(entry.values[0].key, "producer_byte_rate");
        assert!((entry.values[0].value - 1024.5).abs() < f64::EPSILON);
    }

    #[test]
    fn convert_alter_client_quotas_response_preserves_entry_errors() {
        let response = AlterClientQuotasResponse::default()
            .with_throttle_time_ms(20)
            .with_entries(vec![
                KpAlterClientQuotaEntry::default()
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_entity(vec![
                        KpAlterClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("user"))
                            .with_entity_name(Some(StrBytes::from_static_str("alice"))),
                        KpAlterClientQuotaEntity::default()
                            .with_entity_type(StrBytes::from_static_str("client-id"))
                            .with_entity_name(None),
                    ]),
            ]);

        let converted = convert_alter_client_quotas_response(response);

        assert_eq!(converted.throttle_time_ms, 20);
        assert_eq!(converted.entries[0].error_code, 0);
        assert_eq!(converted.entries[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.entries[0].entity[0].entity_type, "user");
        assert_eq!(
            converted.entries[0].entity[0].entity_name,
            Some("alice".to_owned())
        );
        assert_eq!(converted.entries[0].entity[1].entity_type, "client-id");
        assert!(converted.entries[0].entity[1].entity_name.is_none());
    }

    #[test]
    fn convert_describe_user_scram_credentials_response_preserves_credentials() {
        let response = DescribeUserScramCredentialsResponse::default()
            .with_throttle_time_ms(20)
            .with_error_code(0)
            .with_error_message(Some(StrBytes::from_static_str("ok")))
            .with_results(vec![
                KpScramCredentialsResult::default()
                    .with_user(StrBytes::from_static_str("alice"))
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok")))
                    .with_credential_infos(vec![
                        KpScramCredentialInfo::default()
                            .with_mechanism(SCRAM_MECHANISM_SHA_256)
                            .with_iterations(4096),
                        KpScramCredentialInfo::default()
                            .with_mechanism(SCRAM_MECHANISM_SHA_512)
                            .with_iterations(8192),
                    ]),
            ]);

        let converted = convert_describe_user_scram_credentials_response(response);

        assert_eq!(converted.throttle_time_ms, 20);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.results[0].user, "alice");
        assert_eq!(converted.results[0].credential_infos.len(), 2);
        assert_eq!(
            converted.results[0].credential_infos[0].mechanism,
            SCRAM_MECHANISM_SHA_256
        );
        assert_eq!(converted.results[0].credential_infos[1].iterations, 8192);
    }

    #[test]
    fn convert_describe_producers_response_preserves_active_producers() {
        let response = DescribeProducersResponse::default()
            .with_throttle_time_ms(21)
            .with_topics(vec![
                KpProducerTopic::default()
                    .with_name(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpProducerPartition::default()
                            .with_partition_index(0)
                            .with_error_code(0)
                            .with_error_message(Some(StrBytes::from_static_str("ok")))
                            .with_active_producers(vec![
                                KpProducerState::default()
                                    .with_producer_id(ProducerId::from(42))
                                    .with_producer_epoch(2)
                                    .with_last_sequence(12)
                                    .with_last_timestamp(1_700_000)
                                    .with_coordinator_epoch(3)
                                    .with_current_txn_start_offset(99),
                            ]),
                    ]),
            ]);

        let converted = convert_describe_producers_response(response);

        assert_eq!(converted.throttle_time_ms, 21);
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(
            converted.topics[0].partitions[0].error_message,
            Some("ok".to_owned())
        );
        assert_eq!(
            converted.topics[0].partitions[0].active_producers[0].producer_id,
            42
        );
        assert_eq!(
            converted.topics[0].partitions[0].active_producers[0].current_txn_start_offset,
            99
        );
    }

    #[test]
    fn convert_offset_for_leader_epoch_response_preserves_epoch_offsets() {
        let response = OffsetForLeaderEpochResponse::default()
            .with_throttle_time_ms(22)
            .with_topics(vec![
                KpOffsetForLeaderTopicResult::default()
                    .with_topic(StrBytes::from_static_str("topic-a").into())
                    .with_partitions(vec![
                        KpEpochEndOffset::default()
                            .with_error_code(0)
                            .with_partition(0)
                            .with_leader_epoch(7)
                            .with_end_offset(420),
                    ]),
            ]);

        let converted = convert_offset_for_leader_epoch_response(response);

        assert_eq!(converted.throttle_time_ms, 22);
        assert_eq!(converted.topics[0].topic, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].partition, 0);
        assert_eq!(converted.topics[0].partitions[0].leader_epoch, 7);
        assert_eq!(converted.topics[0].partitions[0].end_offset, 420);
        assert_eq!(converted.topics[0].partitions[0].error_code, 0);
    }

    #[test]
    fn convert_list_transactions_response_preserves_state_filters_and_transactions() {
        let response = ListTransactionsResponse::default()
            .with_throttle_time_ms(22)
            .with_error_code(0)
            .with_unknown_state_filters(vec![StrBytes::from_static_str("UnknownState")])
            .with_transaction_states(vec![
                KpListedTransactionState::default()
                    .with_transactional_id(transactional_id("txn-a"))
                    .with_producer_id(ProducerId::from(42))
                    .with_transaction_state(StrBytes::from_static_str("Ongoing")),
            ]);

        let converted = convert_list_transactions_response(response);

        assert_eq!(converted.throttle_time_ms, 22);
        assert_eq!(converted.unknown_state_filters, vec!["UnknownState"]);
        assert_eq!(
            converted.transaction_states,
            vec![ListedTransaction {
                transactional_id: "txn-a".to_owned(),
                producer_id: 42,
                transaction_state: "Ongoing".to_owned(),
            }]
        );
    }

    #[test]
    fn convert_describe_transactions_response_preserves_transaction_details() {
        let response = DescribeTransactionsResponse::default()
            .with_throttle_time_ms(23)
            .with_transaction_states(vec![
                KpDescribedTransactionState::default()
                    .with_error_code(0)
                    .with_transactional_id(transactional_id("txn-a"))
                    .with_transaction_state(StrBytes::from_static_str("Ongoing"))
                    .with_transaction_timeout_ms(60_000)
                    .with_transaction_start_time_ms(1_700_000)
                    .with_producer_id(ProducerId::from(42))
                    .with_producer_epoch(3)
                    .with_topics(vec![
                        KpDescribeTransactionTopic::default()
                            .with_topic(StrBytes::from_static_str("topic-a").into())
                            .with_partitions(vec![0, 1]),
                    ]),
            ]);

        let converted = convert_describe_transactions_response(response);

        assert_eq!(converted.throttle_time_ms, 23);
        assert_eq!(converted.transaction_states[0].transactional_id, "txn-a");
        assert_eq!(
            converted.transaction_states[0].transaction_timeout_ms,
            60_000
        );
        assert_eq!(converted.transaction_states[0].producer_id, 42);
        assert_eq!(converted.transaction_states[0].producer_epoch, 3);
        assert_eq!(converted.transaction_states[0].topics[0].topic, "topic-a");
        assert_eq!(
            converted.transaction_states[0].topics[0].partitions,
            vec![0, 1]
        );
    }
}
