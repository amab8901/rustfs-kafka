//! Read-only Kafka administration protocol helpers.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsRequest,
    DescribeConfigsResponse, DescribeGroupsRequest, DescribeGroupsResponse, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeProducersRequest, DescribeProducersResponse, GroupId,
    ListGroupsRequest, ListGroupsResponse, ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::{
    API_VERSION_DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_GROUPS,
    API_VERSION_DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_PRODUCERS, API_VERSION_LIST_GROUPS,
    API_VERSION_LIST_PARTITION_REASSIGNMENTS,
};

/// Endpoint type for broker endpoints in `DescribeCluster`.
pub const DESCRIBE_CLUSTER_ENDPOINT_BROKERS: i8 = 1;

/// Topic config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_TOPIC: i8 = 2;
/// Broker config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER: i8 = 4;
/// Broker logger config resource type for `DescribeConfigs`.
pub const CONFIG_RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;

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

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
    use kafka_protocol::messages::describe_configs_response::{
        DescribeConfigsResourceResult as KpDescribeConfigsResourceResult,
        DescribeConfigsResult as KpDescribeConfigsResult,
        DescribeConfigsSynonym as KpDescribeConfigsSynonym,
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
    use kafka_protocol::messages::list_groups_response::ListedGroup as KpListedGroup;
    use kafka_protocol::messages::list_partition_reassignments_response::{
        OngoingPartitionReassignment as KpOngoingPartitionReassignment,
        OngoingTopicReassignment as KpOngoingTopicReassignment,
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
    fn describe_configs_request_fetches_selected_topic_keys() {
        let resources = [ConfigResource::topic("topic-a")
            .with_configuration_keys(["retention.ms", "cleanup.policy"])];
        let (header, request) =
            build_describe_configs_request(9, "client-d", &resources, true, true);

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
        let (_, request) = build_describe_configs_request(10, "client-e", &resources, false, false);

        assert_eq!(
            request.resources[0].resource_type,
            CONFIG_RESOURCE_TYPE_BROKER
        );
        assert!(request.resources[0].configuration_keys.is_none());
    }

    #[test]
    fn describe_log_dirs_request_fetches_all_topics_when_filter_is_absent() {
        let (header, request) = build_describe_log_dirs_request(11, "client-f", None);

        assert_eq!(header.request_api_key, ApiKey::DescribeLogDirs as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_LOG_DIRS);
        assert!(request.topics.is_none());
    }

    #[test]
    fn describe_log_dirs_request_fetches_selected_partitions() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 2])];
        let (_, request) = build_describe_log_dirs_request(12, "client-g", Some(&filter));

        let topic = &request.topics.as_ref().unwrap()[0];
        assert_eq!(topic.topic.to_string(), "topic-a");
        assert_eq!(topic.partitions, vec![0, 2]);
    }

    #[test]
    fn list_partition_reassignments_request_accepts_timeout_and_filter() {
        let filter = [TopicPartitionFilter::new("topic-a", [1])];
        let (header, request) =
            build_list_partition_reassignments_request(13, "client-h", Some(&filter), 5000);

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
    fn describe_producers_request_uses_topic_partition_filters() {
        let filter = [TopicPartitionFilter::new("topic-a", [0, 1])];
        let (header, request) = build_describe_producers_request(14, "client-i", &filter);

        assert_eq!(header.request_api_key, ApiKey::DescribeProducers as i16);
        assert_eq!(header.request_api_version, API_VERSION_DESCRIBE_PRODUCERS);
        assert_eq!(request.topics[0].name.to_string(), "topic-a");
        assert_eq!(request.topics[0].partition_indexes, vec![0, 1]);
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
    fn convert_describe_configs_response_preserves_config_metadata() {
        let response = DescribeConfigsResponse::default()
            .with_throttle_time_ms(13)
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

        assert_eq!(converted.throttle_time_ms, 13);
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
    fn convert_describe_log_dirs_response_preserves_storage_details() {
        let response = DescribeLogDirsResponse::default()
            .with_throttle_time_ms(15)
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

        assert_eq!(converted.throttle_time_ms, 15);
        assert_eq!(converted.results[0].log_dir, "/kafka-logs");
        assert_eq!(converted.results[0].total_bytes, 1_000);
        assert_eq!(converted.results[0].topics[0].name, "topic-a");
        assert_eq!(converted.results[0].topics[0].partitions[0].offset_lag, 3);
        assert!(converted.results[0].topics[0].partitions[0].is_future_key);
    }

    #[test]
    fn convert_list_partition_reassignments_response_preserves_replica_sets() {
        let response = ListPartitionReassignmentsResponse::default()
            .with_throttle_time_ms(16)
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

        assert_eq!(converted.throttle_time_ms, 16);
        assert_eq!(converted.error_message, Some("ok".to_owned()));
        assert_eq!(converted.topics[0].name, "topic-a");
        assert_eq!(converted.topics[0].partitions[0].replicas, vec![1, 2]);
        assert_eq!(converted.topics[0].partitions[0].adding_replicas, vec![3]);
        assert_eq!(converted.topics[0].partitions[0].removing_replicas, vec![1]);
    }

    #[test]
    fn convert_describe_producers_response_preserves_active_producers() {
        let response = DescribeProducersResponse::default()
            .with_throttle_time_ms(17)
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

        assert_eq!(converted.throttle_time_ms, 17);
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
}
