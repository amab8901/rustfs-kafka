//! API version negotiation via Kafka's `ApiVersionsRequest` (API key 18).
//!
//! Infrastructure for negotiating API versions with Kafka brokers. Currently
//! used during metadata requests; full per-request version negotiation will
//! be wired up in a future release.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::protocol::{
    API_VERSION_CONSUMER_GROUP_DESCRIBE, API_VERSION_DESCRIBE_ACLS,
    API_VERSION_DESCRIBE_CLIENT_QUOTAS, API_VERSION_DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CONFIGS,
    API_VERSION_DESCRIBE_DELEGATION_TOKEN, API_VERSION_DESCRIBE_GROUPS,
    API_VERSION_DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_PRODUCERS, API_VERSION_DESCRIBE_QUORUM,
    API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS, API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
    API_VERSION_DESCRIBE_TRANSACTIONS, API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
    API_VERSION_FETCH, API_VERSION_FIND_COORDINATOR, API_VERSION_LIST_CONFIG_RESOURCES,
    API_VERSION_LIST_GROUPS, API_VERSION_LIST_OFFSETS, API_VERSION_LIST_PARTITION_REASSIGNMENTS,
    API_VERSION_LIST_TRANSACTIONS, API_VERSION_METADATA, API_VERSION_OFFSET_COMMIT,
    API_VERSION_OFFSET_FETCH, API_VERSION_PRODUCE, API_VERSION_SHARE_GROUP_DESCRIBE,
};
use tracing::{debug, info};

use crate::network::KafkaConnection;

/// API key numbers as defined by the Kafka protocol.
#[allow(dead_code)]
pub mod api_key {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const DESCRIBE_GROUPS: i16 = 15;
    pub const LIST_GROUPS: i16 = 16;
    pub const API_VERSIONS: i16 = 18;
    pub const DESCRIBE_ACLS: i16 = 29;
    pub const DESCRIBE_CONFIGS: i16 = 32;
    pub const DESCRIBE_LOG_DIRS: i16 = 35;
    pub const DESCRIBE_DELEGATION_TOKEN: i16 = 41;
    pub const LIST_PARTITION_REASSIGNMENTS: i16 = 46;
    pub const DESCRIBE_CLIENT_QUOTAS: i16 = 48;
    pub const DESCRIBE_USER_SCRAM_CREDENTIALS: i16 = 50;
    pub const DESCRIBE_QUORUM: i16 = 55;
    pub const DESCRIBE_CLUSTER: i16 = 60;
    pub const DESCRIBE_PRODUCERS: i16 = 61;
    pub const DESCRIBE_TRANSACTIONS: i16 = 65;
    pub const LIST_TRANSACTIONS: i16 = 66;
    pub const CONSUMER_GROUP_DESCRIBE: i16 = 69;
    pub const LIST_CONFIG_RESOURCES: i16 = 74;
    pub const DESCRIBE_TOPIC_PARTITIONS: i16 = 75;
    pub const SHARE_GROUP_DESCRIBE: i16 = 77;
    pub const DESCRIBE_SHARE_GROUP_OFFSETS: i16 = 90;
}

/// One Kafka API version range advertised by a broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerApiVersion {
    /// Kafka API key.
    pub api_key: i16,
    /// Minimum version supported by the broker.
    pub min_version: i16,
    /// Maximum version supported by the broker.
    pub max_version: i16,
}

/// Parsed response from an `ApiVersions` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponseData {
    /// Top-level broker error code.
    pub error_code: i16,
    /// Quota throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// API version ranges advertised by the broker.
    pub api_keys: Vec<BrokerApiVersion>,
}

/// The version of the `ApiVersions` request we send.
///
/// Use the most compatible non-flexible request to avoid broker/header schema
/// mismatches during bootstrap negotiation.
const API_VERSIONS_REQUEST_VERSION: i16 = 0;

/// Negotiated API version ranges for a single broker.
#[derive(Debug, Clone)]
pub struct BrokerApiVersions {
    #[allow(dead_code)]
    versions: HashMap<i16, (i16, i16)>, // api_key -> (min_version, max_version)
}

impl BrokerApiVersions {
    /// Create from the parsed `ApiVersions` response.
    fn from_response(resp: kafka_protocol::messages::ApiVersionsResponse) -> BrokerApiVersions {
        let response = convert_api_versions_response(resp);
        BrokerApiVersions::from_api_versions(&response.api_keys)
    }

    pub(crate) fn from_api_versions(api_versions: &[BrokerApiVersion]) -> BrokerApiVersions {
        let versions = api_versions
            .iter()
            .map(|api_version| {
                (
                    api_version.api_key,
                    (api_version.min_version, api_version.max_version),
                )
            })
            .collect();

        BrokerApiVersions { versions }
    }

    /// Get the best version for the given API key, clamped to the requested range.
    /// Returns `fallback` if the broker doesn't support the API.
    #[allow(dead_code)]
    pub fn negotiate(&self, api_key: i16, fallback: i16) -> i16 {
        if let Some(&(min, max)) = self.versions.get(&api_key) {
            if fallback < min {
                debug!(
                    "API key {}: our version {} below broker min {}, using min",
                    api_key, fallback, min
                );
                min
            } else if fallback > max {
                debug!(
                    "API key {}: our version {} above broker max {}, using max",
                    api_key, fallback, max
                );
                max
            } else {
                fallback
            }
        } else {
            debug!(
                "API key {}: not supported by broker, using fallback {}",
                api_key, fallback
            );
            fallback
        }
    }
}

/// Send an `ApiVersionsRequest` and parse the response.
pub fn fetch_api_versions(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<BrokerApiVersions> {
    let kp_resp = fetch_api_versions_response_raw(conn, correlation_id, client_id)?;
    let result = BrokerApiVersions::from_response(kp_resp);
    info!("Negotiated API versions: {:?}", result);
    Ok(result)
}

/// Send an `ApiVersionsRequest` and return public response data.
pub fn fetch_api_versions_data(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<ApiVersionsResponseData> {
    fetch_api_versions_response_raw(conn, correlation_id, client_id)
        .map(convert_api_versions_response)
}

/// Convert a generated `ApiVersionsResponse` into the crate's public shape.
#[must_use]
pub fn convert_api_versions_response(
    response: kafka_protocol::messages::ApiVersionsResponse,
) -> ApiVersionsResponseData {
    ApiVersionsResponseData {
        error_code: response.error_code,
        throttle_time_ms: response.throttle_time_ms,
        api_keys: response
            .api_keys
            .into_iter()
            .map(|api_version| BrokerApiVersion {
                api_key: api_version.api_key,
                min_version: api_version.min_version,
                max_version: api_version.max_version,
            })
            .collect(),
    }
}

fn fetch_api_versions_response_raw(
    conn: &mut KafkaConnection,
    correlation_id: i32,
    client_id: &str,
) -> Result<kafka_protocol::messages::ApiVersionsResponse> {
    use bytes::BytesMut;
    use kafka_protocol::messages::{
        ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};

    let request = ApiVersionsRequest::default();

    let header = RequestHeader::default()
        .with_request_api_key(api_key::API_VERSIONS)
        .with_request_api_version(API_VERSIONS_REQUEST_VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(kafka_protocol::protocol::StrBytes::from_string(
            client_id.to_owned(),
        )));
    let request_header_version = ApiVersionsRequest::header_version(API_VERSIONS_REQUEST_VERSION);
    let response_header_version = ApiVersionsResponse::header_version(API_VERSIONS_REQUEST_VERSION);

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, request_header_version)
        .map_err(|_| Error::codec())?;

    let mut body_buf = BytesMut::new();
    request
        .encode(&mut body_buf, API_VERSIONS_REQUEST_VERSION)
        .map_err(|_| Error::codec())?;

    let total_len = crate::protocol::usize_to_i32(header_buf.len() + body_buf.len())?;
    let out_len = crate::protocol::non_negative_i32_to_usize(total_len)?;
    let mut out = BytesMut::with_capacity(4 + out_len);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&header_buf);
    out.extend_from_slice(&body_buf);

    conn.send(&out)?;

    let size = {
        let mut buf = [0u8; 4];
        conn.read_exact(&mut buf)?;
        i32::from_be_bytes(buf)
    };
    let resp_bytes = conn.read_exact_alloc(crate::protocol::non_negative_i32_to_u64(size)?)?;
    let mut bytes = resp_bytes;
    let _resp_header =
        ResponseHeader::decode(&mut bytes, response_header_version).map_err(|_| Error::codec())?;

    let kp_resp = kafka_protocol::messages::ApiVersionsResponse::decode(
        &mut bytes,
        API_VERSIONS_REQUEST_VERSION,
    )
    .map_err(|_| Error::codec())?;

    Ok(kp_resp)
}

/// Stores negotiated API versions per broker.
#[derive(Debug, Default)]
pub struct ApiVersionCache {
    broker_versions: HashMap<String, BrokerApiVersions>,
}

impl ApiVersionCache {
    pub fn new() -> Self {
        ApiVersionCache {
            broker_versions: HashMap::new(),
        }
    }

    /// Check if we have negotiated versions for a broker.
    pub fn contains(&self, host: &str) -> bool {
        self.broker_versions.contains_key(host)
    }

    /// Insert negotiated versions for a broker.
    pub fn insert(&mut self, host: String, versions: BrokerApiVersions) {
        self.broker_versions.insert(host, versions);
    }

    /// Get or fetch API versions for a broker.
    #[allow(dead_code)]
    pub fn get_or_fetch(
        &mut self,
        host: &str,
        conn: &mut KafkaConnection,
        correlation_id: i32,
        client_id: &str,
    ) -> Result<&BrokerApiVersions> {
        if !self.broker_versions.contains_key(host) {
            let versions = fetch_api_versions(conn, correlation_id, client_id)?;
            self.broker_versions.insert(host.to_owned(), versions);
        }
        Ok(self.broker_versions.get(host).unwrap())
    }

    /// Invalidate cached versions for a broker (e.g., after reconnect).
    #[allow(dead_code)]
    pub fn invalidate(&mut self, host: &str) {
        self.broker_versions.remove(host);
    }

    /// Negotiate the best API version for a specific broker and API key.
    #[allow(dead_code)]
    pub fn negotiate(&self, host: &str, api_key: i16, fallback: i16) -> i16 {
        self.broker_versions
            .get(host)
            .map_or(fallback, |v| v.negotiate(api_key, fallback))
    }

    /// Returns the negotiated version for the given API key,
    /// falling back to a safe default if no version information is available.
    #[allow(dead_code)]
    pub fn get_or_fallback(&self, host: &str, api_key: i16) -> i16 {
        let fallback = Self::fallback_version(api_key);
        self.negotiate(host, api_key, fallback)
    }

    /// Returns the fallback (minimum supported) version for an API key.
    #[must_use]
    #[allow(dead_code)]
    pub fn fallback_version(api_key: i16) -> i16 {
        match api_key {
            api_key::PRODUCE => API_VERSION_PRODUCE,
            api_key::FETCH => API_VERSION_FETCH,
            api_key::METADATA => API_VERSION_METADATA,
            api_key::LIST_OFFSETS => API_VERSION_LIST_OFFSETS,
            api_key::FIND_COORDINATOR => API_VERSION_FIND_COORDINATOR,
            api_key::OFFSET_COMMIT => API_VERSION_OFFSET_COMMIT,
            api_key::OFFSET_FETCH => API_VERSION_OFFSET_FETCH,
            api_key::DESCRIBE_GROUPS => API_VERSION_DESCRIBE_GROUPS,
            api_key::LIST_GROUPS => API_VERSION_LIST_GROUPS,
            api_key::DESCRIBE_ACLS => API_VERSION_DESCRIBE_ACLS,
            api_key::DESCRIBE_CONFIGS => API_VERSION_DESCRIBE_CONFIGS,
            api_key::DESCRIBE_LOG_DIRS => API_VERSION_DESCRIBE_LOG_DIRS,
            api_key::DESCRIBE_DELEGATION_TOKEN => API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            api_key::LIST_PARTITION_REASSIGNMENTS => API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            api_key::DESCRIBE_CLIENT_QUOTAS => API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            api_key::DESCRIBE_USER_SCRAM_CREDENTIALS => API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            api_key::DESCRIBE_QUORUM => API_VERSION_DESCRIBE_QUORUM,
            api_key::DESCRIBE_CLUSTER => API_VERSION_DESCRIBE_CLUSTER,
            api_key::DESCRIBE_PRODUCERS => API_VERSION_DESCRIBE_PRODUCERS,
            api_key::DESCRIBE_TRANSACTIONS => API_VERSION_DESCRIBE_TRANSACTIONS,
            api_key::LIST_TRANSACTIONS => API_VERSION_LIST_TRANSACTIONS,
            api_key::CONSUMER_GROUP_DESCRIBE => API_VERSION_CONSUMER_GROUP_DESCRIBE,
            api_key::LIST_CONFIG_RESOURCES => API_VERSION_LIST_CONFIG_RESOURCES,
            api_key::DESCRIBE_TOPIC_PARTITIONS => API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            api_key::SHARE_GROUP_DESCRIBE => API_VERSION_SHARE_GROUP_DESCRIBE,
            api_key::DESCRIBE_SHARE_GROUP_OFFSETS => API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            _ => 0,
        }
    }

    /// Returns true if no broker versions have been cached.
    #[must_use]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.broker_versions.is_empty()
    }
}

/// Resolve the effective API version for a given API key using cached negotiations.
/// Falls back to hardcoded defaults if no negotiation has occurred.
#[allow(dead_code)]
pub fn resolve_api_version(cache: &ApiVersionCache, host: &str, api_key: i16, default: i16) -> i16 {
    cache.negotiate(host, api_key, default)
}

/// Resolve all our API versions for a given broker.
#[allow(dead_code)]
pub fn resolve_all_api_versions(cache: &ApiVersionCache, host: &str) -> ApiVersions {
    macro_rules! version {
        ($api_key:ident, $default:ident) => {
            resolve_api_version(cache, host, api_key::$api_key, $default)
        };
    }

    ApiVersions {
        produce: version!(PRODUCE, API_VERSION_PRODUCE),
        fetch: version!(FETCH, API_VERSION_FETCH),
        metadata: version!(METADATA, API_VERSION_METADATA),
        list_offsets: version!(LIST_OFFSETS, API_VERSION_LIST_OFFSETS),
        find_coordinator: version!(FIND_COORDINATOR, API_VERSION_FIND_COORDINATOR),
        offset_commit: version!(OFFSET_COMMIT, API_VERSION_OFFSET_COMMIT),
        offset_fetch: version!(OFFSET_FETCH, API_VERSION_OFFSET_FETCH),
        describe_groups: version!(DESCRIBE_GROUPS, API_VERSION_DESCRIBE_GROUPS),
        list_groups: version!(LIST_GROUPS, API_VERSION_LIST_GROUPS),
        describe_acls: version!(DESCRIBE_ACLS, API_VERSION_DESCRIBE_ACLS),
        describe_configs: version!(DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_CONFIGS),
        describe_log_dirs: version!(DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_LOG_DIRS),
        describe_delegation_token: version!(
            DESCRIBE_DELEGATION_TOKEN,
            API_VERSION_DESCRIBE_DELEGATION_TOKEN
        ),
        list_partition_reassignments: version!(
            LIST_PARTITION_REASSIGNMENTS,
            API_VERSION_LIST_PARTITION_REASSIGNMENTS
        ),
        describe_client_quotas: version!(
            DESCRIBE_CLIENT_QUOTAS,
            API_VERSION_DESCRIBE_CLIENT_QUOTAS
        ),
        describe_user_scram_credentials: version!(
            DESCRIBE_USER_SCRAM_CREDENTIALS,
            API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS
        ),
        describe_quorum: version!(DESCRIBE_QUORUM, API_VERSION_DESCRIBE_QUORUM),
        describe_cluster: version!(DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CLUSTER),
        describe_producers: version!(DESCRIBE_PRODUCERS, API_VERSION_DESCRIBE_PRODUCERS),
        describe_transactions: version!(DESCRIBE_TRANSACTIONS, API_VERSION_DESCRIBE_TRANSACTIONS),
        list_transactions: version!(LIST_TRANSACTIONS, API_VERSION_LIST_TRANSACTIONS),
        consumer_group_describe: version!(
            CONSUMER_GROUP_DESCRIBE,
            API_VERSION_CONSUMER_GROUP_DESCRIBE
        ),
        list_config_resources: version!(LIST_CONFIG_RESOURCES, API_VERSION_LIST_CONFIG_RESOURCES),
        describe_topic_partitions: version!(
            DESCRIBE_TOPIC_PARTITIONS,
            API_VERSION_DESCRIBE_TOPIC_PARTITIONS
        ),
        share_group_describe: version!(SHARE_GROUP_DESCRIBE, API_VERSION_SHARE_GROUP_DESCRIBE),
        describe_share_group_offsets: version!(
            DESCRIBE_SHARE_GROUP_OFFSETS,
            API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS
        ),
    }
}

/// Resolved API versions for all supported Kafka APIs.
#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub struct ApiVersions {
    pub produce: i16,
    pub fetch: i16,
    pub metadata: i16,
    pub list_offsets: i16,
    pub find_coordinator: i16,
    pub offset_commit: i16,
    pub offset_fetch: i16,
    pub describe_groups: i16,
    pub list_groups: i16,
    pub describe_acls: i16,
    pub describe_configs: i16,
    pub describe_log_dirs: i16,
    pub describe_delegation_token: i16,
    pub list_partition_reassignments: i16,
    pub describe_client_quotas: i16,
    pub describe_user_scram_credentials: i16,
    pub describe_quorum: i16,
    pub describe_cluster: i16,
    pub describe_producers: i16,
    pub describe_transactions: i16,
    pub list_transactions: i16,
    pub consumer_group_describe: i16,
    pub list_config_resources: i16,
    pub describe_topic_partitions: i16,
    pub share_group_describe: i16,
    pub describe_share_group_offsets: i16,
}

impl Default for ApiVersions {
    fn default() -> Self {
        ApiVersions {
            produce: API_VERSION_PRODUCE,
            fetch: API_VERSION_FETCH,
            metadata: API_VERSION_METADATA,
            list_offsets: API_VERSION_LIST_OFFSETS,
            find_coordinator: API_VERSION_FIND_COORDINATOR,
            offset_commit: API_VERSION_OFFSET_COMMIT,
            offset_fetch: API_VERSION_OFFSET_FETCH,
            describe_groups: API_VERSION_DESCRIBE_GROUPS,
            list_groups: API_VERSION_LIST_GROUPS,
            describe_acls: API_VERSION_DESCRIBE_ACLS,
            describe_configs: API_VERSION_DESCRIBE_CONFIGS,
            describe_log_dirs: API_VERSION_DESCRIBE_LOG_DIRS,
            describe_delegation_token: API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            list_partition_reassignments: API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            describe_client_quotas: API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            describe_user_scram_credentials: API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            describe_quorum: API_VERSION_DESCRIBE_QUORUM,
            describe_cluster: API_VERSION_DESCRIBE_CLUSTER,
            describe_producers: API_VERSION_DESCRIBE_PRODUCERS,
            describe_transactions: API_VERSION_DESCRIBE_TRANSACTIONS,
            list_transactions: API_VERSION_LIST_TRANSACTIONS,
            consumer_group_describe: API_VERSION_CONSUMER_GROUP_DESCRIBE,
            list_config_resources: API_VERSION_LIST_CONFIG_RESOURCES,
            describe_topic_partitions: API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            share_group_describe: API_VERSION_SHARE_GROUP_DESCRIBE,
            describe_share_group_offsets: API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_api_versions_from_response_empty() {
        // Simulate an empty ApiVersionsResponse (no api_keys).
        let resp = kafka_protocol::messages::ApiVersionsResponse::default();
        let bv = BrokerApiVersions::from_response(resp);
        // Negotiating anything on an empty set should return the fallback.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 3), 3);
        assert_eq!(bv.negotiate(api_key::FETCH, 4), 4);
    }

    #[test]
    fn broker_api_versions_negotiate_clamps_to_range() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let resp = kafka_protocol::messages::ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(api_key::PRODUCE)
                .with_min_version(3)
                .with_max_version(8),
        ]);
        let bv = BrokerApiVersions::from_response(resp);

        // Within range -> returned as-is.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 5), 5);
        // Below min -> clamped up.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 1), 3);
        // Above max -> clamped down.
        assert_eq!(bv.negotiate(api_key::PRODUCE, 12), 8);
        // Unknown key -> fallback.
        assert_eq!(bv.negotiate(99, 7), 7);
    }

    #[test]
    fn convert_api_versions_response_preserves_api_ranges() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let response = kafka_protocol::messages::ApiVersionsResponse::default()
            .with_error_code(0)
            .with_throttle_time_ms(14)
            .with_api_keys(vec![
                ApiVersion::default()
                    .with_api_key(api_key::DESCRIBE_CONFIGS)
                    .with_min_version(1)
                    .with_max_version(4),
            ]);

        let converted = convert_api_versions_response(response);

        assert_eq!(
            converted,
            ApiVersionsResponseData {
                error_code: 0,
                throttle_time_ms: 14,
                api_keys: vec![BrokerApiVersion {
                    api_key: api_key::DESCRIBE_CONFIGS,
                    min_version: 1,
                    max_version: 4,
                }],
            }
        );
    }

    #[test]
    fn api_version_cache_new_is_empty() {
        let cache = ApiVersionCache::new();
        assert!(!cache.contains("broker1:9092"));
        assert!(!cache.contains("any-host"));
    }

    #[test]
    fn api_version_cache_insert_and_contains() {
        let mut cache = ApiVersionCache::new();
        let bv = BrokerApiVersions::from_response(
            kafka_protocol::messages::ApiVersionsResponse::default(),
        );
        cache.insert("broker1:9092".to_string(), bv);
        assert!(cache.contains("broker1:9092"));
        assert!(!cache.contains("broker2:9092"));
    }

    #[test]
    fn api_version_cache_invalidate() {
        let mut cache = ApiVersionCache::new();
        let bv = BrokerApiVersions::from_response(
            kafka_protocol::messages::ApiVersionsResponse::default(),
        );
        cache.insert("broker1:9092".to_string(), bv);
        assert!(cache.contains("broker1:9092"));
        cache.invalidate("broker1:9092");
        assert!(!cache.contains("broker1:9092"));
    }

    #[test]
    fn api_version_cache_negotiate_falls_back_when_missing() {
        let cache = ApiVersionCache::new();
        // No broker in cache -> returns fallback.
        assert_eq!(cache.negotiate("unknown:9092", api_key::FETCH, 4), 4);
    }

    #[test]
    fn api_version_cache_negotiate_with_known_broker() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let mut cache = ApiVersionCache::new();
        let resp = kafka_protocol::messages::ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(api_key::METADATA)
                .with_min_version(1)
                .with_max_version(12),
        ]);
        let bv = BrokerApiVersions::from_response(resp);
        cache.insert("broker1:9092".to_string(), bv);

        // Within range.
        assert_eq!(cache.negotiate("broker1:9092", api_key::METADATA, 7), 7);
        // Above max.
        assert_eq!(cache.negotiate("broker1:9092", api_key::METADATA, 20), 12);
        // Unknown API key for this broker -> fallback.
        assert_eq!(cache.negotiate("broker1:9092", api_key::FETCH, 4), 4);
    }

    #[test]
    fn api_versions_default_has_expected_fields() {
        let v = ApiVersions::default();
        assert_eq!(v.produce, API_VERSION_PRODUCE);
        assert_eq!(v.fetch, API_VERSION_FETCH);
        assert_eq!(v.metadata, API_VERSION_METADATA);
        assert_eq!(v.list_offsets, API_VERSION_LIST_OFFSETS);
        assert_eq!(v.find_coordinator, API_VERSION_FIND_COORDINATOR);
        assert_eq!(v.offset_commit, API_VERSION_OFFSET_COMMIT);
        assert_eq!(v.offset_fetch, API_VERSION_OFFSET_FETCH);
        assert_eq!(v.describe_groups, API_VERSION_DESCRIBE_GROUPS);
        assert_eq!(v.list_groups, API_VERSION_LIST_GROUPS);
        assert_eq!(v.describe_acls, API_VERSION_DESCRIBE_ACLS);
        assert_eq!(v.describe_configs, API_VERSION_DESCRIBE_CONFIGS);
        assert_eq!(v.describe_log_dirs, API_VERSION_DESCRIBE_LOG_DIRS);
        assert_eq!(
            v.describe_delegation_token,
            API_VERSION_DESCRIBE_DELEGATION_TOKEN
        );
        assert_eq!(
            v.list_partition_reassignments,
            API_VERSION_LIST_PARTITION_REASSIGNMENTS
        );
        assert_eq!(v.describe_client_quotas, API_VERSION_DESCRIBE_CLIENT_QUOTAS);
        assert_eq!(
            v.describe_user_scram_credentials,
            API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS
        );
        assert_eq!(v.describe_quorum, API_VERSION_DESCRIBE_QUORUM);
        assert_eq!(v.describe_cluster, API_VERSION_DESCRIBE_CLUSTER);
        assert_eq!(v.describe_producers, API_VERSION_DESCRIBE_PRODUCERS);
        assert_eq!(v.describe_transactions, API_VERSION_DESCRIBE_TRANSACTIONS);
        assert_eq!(v.list_transactions, API_VERSION_LIST_TRANSACTIONS);
        assert_eq!(
            v.consumer_group_describe,
            API_VERSION_CONSUMER_GROUP_DESCRIBE
        );
        assert_eq!(v.list_config_resources, API_VERSION_LIST_CONFIG_RESOURCES);
        assert_eq!(
            v.describe_topic_partitions,
            API_VERSION_DESCRIBE_TOPIC_PARTITIONS
        );
        assert_eq!(v.share_group_describe, API_VERSION_SHARE_GROUP_DESCRIBE);
        assert_eq!(
            v.describe_share_group_offsets,
            API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS
        );
    }

    #[test]
    fn resolve_all_api_versions_uses_defaults_for_unknown_broker() {
        let cache = ApiVersionCache::new();
        let v = resolve_all_api_versions(&cache, "unknown");
        let d = ApiVersions::default();
        assert_eq!(v.produce, d.produce);
        assert_eq!(v.fetch, d.fetch);
        assert_eq!(v.metadata, d.metadata);
        assert_eq!(v.list_offsets, d.list_offsets);
        assert_eq!(v.find_coordinator, d.find_coordinator);
        assert_eq!(v.offset_commit, d.offset_commit);
        assert_eq!(v.offset_fetch, d.offset_fetch);
        assert_eq!(v.describe_groups, d.describe_groups);
        assert_eq!(v.list_groups, d.list_groups);
        assert_eq!(v.describe_acls, d.describe_acls);
        assert_eq!(v.describe_configs, d.describe_configs);
        assert_eq!(v.describe_log_dirs, d.describe_log_dirs);
        assert_eq!(v.describe_delegation_token, d.describe_delegation_token);
        assert_eq!(
            v.list_partition_reassignments,
            d.list_partition_reassignments
        );
        assert_eq!(v.describe_client_quotas, d.describe_client_quotas);
        assert_eq!(
            v.describe_user_scram_credentials,
            d.describe_user_scram_credentials
        );
        assert_eq!(v.describe_quorum, d.describe_quorum);
        assert_eq!(v.describe_cluster, d.describe_cluster);
        assert_eq!(v.describe_producers, d.describe_producers);
        assert_eq!(v.describe_transactions, d.describe_transactions);
        assert_eq!(v.list_transactions, d.list_transactions);
        assert_eq!(v.consumer_group_describe, d.consumer_group_describe);
        assert_eq!(v.list_config_resources, d.list_config_resources);
        assert_eq!(v.describe_topic_partitions, d.describe_topic_partitions);
        assert_eq!(v.share_group_describe, d.share_group_describe);
        assert_eq!(
            v.describe_share_group_offsets,
            d.describe_share_group_offsets
        );
    }

    #[test]
    fn fallback_version_known_apis() {
        for (api_key, expected_version) in [
            (api_key::PRODUCE, API_VERSION_PRODUCE),
            (api_key::FETCH, API_VERSION_FETCH),
            (api_key::METADATA, API_VERSION_METADATA),
            (api_key::LIST_OFFSETS, API_VERSION_LIST_OFFSETS),
            (api_key::FIND_COORDINATOR, API_VERSION_FIND_COORDINATOR),
            (api_key::OFFSET_COMMIT, API_VERSION_OFFSET_COMMIT),
            (api_key::OFFSET_FETCH, API_VERSION_OFFSET_FETCH),
            (api_key::DESCRIBE_GROUPS, API_VERSION_DESCRIBE_GROUPS),
            (api_key::LIST_GROUPS, API_VERSION_LIST_GROUPS),
            (api_key::DESCRIBE_ACLS, API_VERSION_DESCRIBE_ACLS),
            (api_key::DESCRIBE_CONFIGS, API_VERSION_DESCRIBE_CONFIGS),
            (api_key::DESCRIBE_LOG_DIRS, API_VERSION_DESCRIBE_LOG_DIRS),
            (
                api_key::DESCRIBE_DELEGATION_TOKEN,
                API_VERSION_DESCRIBE_DELEGATION_TOKEN,
            ),
            (
                api_key::LIST_PARTITION_REASSIGNMENTS,
                API_VERSION_LIST_PARTITION_REASSIGNMENTS,
            ),
            (
                api_key::DESCRIBE_CLIENT_QUOTAS,
                API_VERSION_DESCRIBE_CLIENT_QUOTAS,
            ),
            (
                api_key::DESCRIBE_USER_SCRAM_CREDENTIALS,
                API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
            ),
            (api_key::DESCRIBE_QUORUM, API_VERSION_DESCRIBE_QUORUM),
            (api_key::DESCRIBE_CLUSTER, API_VERSION_DESCRIBE_CLUSTER),
            (api_key::DESCRIBE_PRODUCERS, API_VERSION_DESCRIBE_PRODUCERS),
            (
                api_key::DESCRIBE_TRANSACTIONS,
                API_VERSION_DESCRIBE_TRANSACTIONS,
            ),
            (api_key::LIST_TRANSACTIONS, API_VERSION_LIST_TRANSACTIONS),
            (
                api_key::CONSUMER_GROUP_DESCRIBE,
                API_VERSION_CONSUMER_GROUP_DESCRIBE,
            ),
            (
                api_key::LIST_CONFIG_RESOURCES,
                API_VERSION_LIST_CONFIG_RESOURCES,
            ),
            (
                api_key::DESCRIBE_TOPIC_PARTITIONS,
                API_VERSION_DESCRIBE_TOPIC_PARTITIONS,
            ),
            (
                api_key::SHARE_GROUP_DESCRIBE,
                API_VERSION_SHARE_GROUP_DESCRIBE,
            ),
            (
                api_key::DESCRIBE_SHARE_GROUP_OFFSETS,
                API_VERSION_DESCRIBE_SHARE_GROUP_OFFSETS,
            ),
        ] {
            assert_eq!(ApiVersionCache::fallback_version(api_key), expected_version);
        }
    }

    #[test]
    fn fallback_version_unknown_api() {
        assert_eq!(ApiVersionCache::fallback_version(99), 0);
        assert_eq!(ApiVersionCache::fallback_version(-1), 0);
    }

    #[test]
    fn get_or_fallback_empty_cache_returns_fallback() {
        let cache = ApiVersionCache::new();
        assert_eq!(
            cache.get_or_fallback("unknown:9092", api_key::PRODUCE),
            API_VERSION_PRODUCE
        );
    }

    #[test]
    fn get_or_fallback_populated_cache_negotiates() {
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let mut cache = ApiVersionCache::new();
        let resp = kafka_protocol::messages::ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(api_key::PRODUCE)
                .with_min_version(3)
                .with_max_version(8),
        ]);
        let bv = BrokerApiVersions::from_response(resp);
        cache.insert("broker1:9092".to_string(), bv);

        // Known API keys are negotiated against broker ranges; unknown keys use fallback.
        assert_eq!(cache.get_or_fallback("broker1:9092", api_key::PRODUCE), 8);
        assert_eq!(
            cache.get_or_fallback("broker1:9092", api_key::FETCH),
            API_VERSION_FETCH
        );
    }
}
