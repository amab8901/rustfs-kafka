//! Read-only Kafka administration protocol helpers.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, DescribeAclsRequest, DescribeAclsResponse, DescribeClientQuotasRequest,
    DescribeClientQuotasResponse, DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsRequest, DescribeConfigsResponse, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    DescribeLogDirsRequest, DescribeLogDirsResponse, DescribeProducersRequest,
    DescribeProducersResponse, DescribeTransactionsRequest, DescribeTransactionsResponse,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, GroupId,
    ListGroupsRequest, ListGroupsResponse, ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsResponse, ListTransactionsRequest, ListTransactionsResponse,
    RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::{
    API_VERSION_DESCRIBE_ACLS, API_VERSION_DESCRIBE_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLUSTER,
    API_VERSION_DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_DELEGATION_TOKEN,
    API_VERSION_DESCRIBE_GROUPS, API_VERSION_DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_PRODUCERS,
    API_VERSION_DESCRIBE_TRANSACTIONS, API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
    API_VERSION_LIST_GROUPS, API_VERSION_LIST_PARTITION_REASSIGNMENTS,
    API_VERSION_LIST_TRANSACTIONS,
};

/// Endpoint type for broker endpoints in `DescribeCluster`.
pub const DESCRIBE_CLUSTER_ENDPOINT_BROKERS: i8 = 1;

/// Topic config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_TOPIC: i8 = 2;
/// Broker config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER: i8 = 4;
/// Broker logger config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;

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
    use kafka_protocol::messages::describe_transactions_response::{
        TopicData as KpDescribeTransactionTopic, TransactionState as KpDescribedTransactionState,
    };
    use kafka_protocol::messages::describe_user_scram_credentials_response::{
        CredentialInfo as KpScramCredentialInfo,
        DescribeUserScramCredentialsResult as KpScramCredentialsResult,
    };
    use kafka_protocol::messages::list_groups_response::ListedGroup as KpListedGroup;
    use kafka_protocol::messages::list_partition_reassignments_response::{
        OngoingPartitionReassignment as KpOngoingPartitionReassignment,
        OngoingTopicReassignment as KpOngoingTopicReassignment,
    };
    use kafka_protocol::messages::list_transactions_response::TransactionState as KpListedTransactionState;
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
    fn describe_groups_request_includes_authorized_operations_flag() {
        let (header, request) =
            build_describe_groups_request(8, "client-c", &["group-a", "group-b"], true);

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
    fn describe_client_quotas_request_accepts_entity_filters() {
        let options = DescribeClientQuotasOptions::new()
            .with_component(ClientQuotaEntityFilter::exact("user", "alice"))
            .with_component(ClientQuotaEntityFilter::default_entity("client-id"))
            .with_component(ClientQuotaEntityFilter::any_specified("ip"))
            .strict();
        let (header, request) = build_describe_client_quotas_request(17, "client-l", &options);

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
    fn describe_user_scram_credentials_request_distinguishes_all_and_selected_users() {
        let (all_header, all_request) =
            build_describe_user_scram_credentials_request(18, "client-m", None);
        let (selected_header, selected_request) =
            build_describe_user_scram_credentials_request(19, "client-n", Some(&["alice", "bob"]));

        assert_eq!(
            all_header.request_api_key,
            ApiKey::DescribeUserScramCredentials as i16
        );
        assert_eq!(
            all_header.request_api_version,
            API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS
        );
        assert!(all_request.users.is_none());
        assert_eq!(selected_header.correlation_id, 19);
        let users = selected_request.users.unwrap();
        assert_eq!(users[0].name.to_string(), "alice");
        assert_eq!(users[1].name.to_string(), "bob");
    }

    #[test]
    fn describe_producers_request_uses_topic_partition_filters() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 1])];
        let (header, request) = build_describe_producers_request(20, "client-o", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeProducers as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_PRODUCERS);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partition_indexes, vec![0, 1]);
    }

    #[test]
    fn list_transactions_request_accepts_all_filters() {
        let options = ListTransactionsOptions::new()
            .with_state_filters(["Ongoing", "PrepareCommit"])
            .with_producer_id_filters([42, 43])
            .with_duration_filter_ms(30_000)
            .with_transactional_id_pattern("rustfs-.*");
        let (header, request) = build_list_transactions_request(21, "client-p", &options);

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
            build_describe_transactions_request(22, "client-q", &["txn-a", "txn-b"]);

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
    fn convert_describe_client_quotas_response_preserves_entities_and_values() {
        let response = DescribeClientQuotasResponse::default()
            .with_throttle_time_ms(18)
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

        assert_eq!(converted.throttle_time_ms, 18);
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
    fn convert_describe_user_scram_credentials_response_preserves_credentials() {
        let response = DescribeUserScramCredentialsResponse::default()
            .with_throttle_time_ms(19)
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

        assert_eq!(converted.throttle_time_ms, 19);
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
            .with_throttle_time_ms(20)
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

        assert_eq!(converted.throttle_time_ms, 20);
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
    fn convert_list_transactions_response_preserves_state_filters_and_transactions() {
        let response = ListTransactionsResponse::default()
            .with_throttle_time_ms(21)
            .with_error_code(0)
            .with_unknown_state_filters(vec![StrBytes::from_static_str("UnknownState")])
            .with_transaction_states(vec![
                KpListedTransactionState::default()
                    .with_transactional_id(transactional_id("txn-a"))
                    .with_producer_id(ProducerId::from(42))
                    .with_transaction_state(StrBytes::from_static_str("Ongoing")),
            ]);

        let converted = convert_list_transactions_response(response);

        assert_eq!(converted.throttle_time_ms, 21);
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
            .with_throttle_time_ms(22)
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

        assert_eq!(converted.throttle_time_ms, 22);
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
