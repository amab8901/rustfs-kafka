#![allow(clippy::wildcard_imports)]
//! ACL administration helpers.

use kafka_protocol::messages::{
    ApiKey, CreateAclsRequest, CreateAclsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{API_VERSION_CREATE_ACLS, API_VERSION_DELETE_ACLS, API_VERSION_DESCRIBE_ACLS};
use super::request_header;
use super::types::*;

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

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::create_acls_response::AclCreationResult as KpAclCreationResult;
    use kafka_protocol::messages::delete_acls_response::{
        DeleteAclsFilterResult as KpDeleteAclsFilterResult,
        DeleteAclsMatchingAcl as KpDeleteAclsMatchingAcl,
    };
    use kafka_protocol::messages::describe_acls_response::{
        AclDescription as KpAclDescription, DescribeAclsResource as KpAclResource,
    };
    use kafka_protocol::protocol::StrBytes;

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
}
