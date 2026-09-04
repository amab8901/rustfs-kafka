#![allow(clippy::wildcard_imports)]
//! Client quota administration helpers.

use kafka_protocol::messages::{
    AlterClientQuotasRequest, AlterClientQuotasResponse, ApiKey, DescribeClientQuotasRequest,
    DescribeClientQuotasResponse, RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{API_VERSION_ALTER_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLIENT_QUOTAS};
use super::request_header;
use super::types::*;

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

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::alter_client_quotas_response::{
        EntityData as KpAlterClientQuotaEntity, EntryData as KpAlterClientQuotaEntry,
    };
    use kafka_protocol::messages::describe_client_quotas_response::{
        EntityData as KpClientQuotaEntity, EntryData as KpClientQuotaEntry,
        ValueData as KpClientQuotaValue,
    };
    use kafka_protocol::protocol::StrBytes;

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
}
