//! Read-only Kafka administration protocol helpers.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, DescribeClusterRequest, DescribeClusterResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, GroupId, ListGroupsRequest, ListGroupsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::{API_VERSION_DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_GROUPS, API_VERSION_LIST_GROUPS};

/// Endpoint type for broker endpoints in `DescribeCluster`.
pub const DESCRIBE_CLUSTER_ENDPOINT_BROKERS: i8 = 1;

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
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
    use kafka_protocol::messages::describe_groups_response::{
        DescribedGroup as KpDescribedGroup, DescribedGroupMember as KpDescribedGroupMember,
    };
    use kafka_protocol::messages::list_groups_response::ListedGroup as KpListedGroup;

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
}
