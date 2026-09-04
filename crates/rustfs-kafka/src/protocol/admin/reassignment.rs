#![allow(clippy::wildcard_imports)]
//! Partition reassignment administration helpers.

use kafka_protocol::messages::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, ApiKey,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ALTER_PARTITION_REASSIGNMENTS, API_VERSION_LIST_PARTITION_REASSIGNMENTS,
};
use super::request_header;
use super::types::*;

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

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::BrokerId;
    use kafka_protocol::messages::alter_partition_reassignments_response::{
        ReassignablePartitionResponse as KpReassignablePartitionResponse,
        ReassignableTopicResponse as KpReassignableTopicResponse,
    };
    use kafka_protocol::messages::list_partition_reassignments_response::{
        OngoingPartitionReassignment as KpOngoingPartitionReassignment,
        OngoingTopicReassignment as KpOngoingTopicReassignment,
    };
    use kafka_protocol::protocol::StrBytes;

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
}
