#![allow(clippy::wildcard_imports)]
//! Delegation token and SCRAM credential administration helpers.

use bytes::Bytes;
use kafka_protocol::messages::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey,
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentialsResponse, ExpireDelegationTokenRequest,
    ExpireDelegationTokenResponse, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
    RequestHeader,
};
use kafka_protocol::protocol::StrBytes;

use super::super::{
    API_VERSION_ALTER_USER_SCRAM_CREDENTIALS, API_VERSION_CREATE_DELEGATION_TOKEN,
    API_VERSION_DESCRIBE_DELEGATION_TOKEN, API_VERSION_DESCRIBE_USER_SCRAM_CREDENTIALS,
    API_VERSION_EXPIRE_DELEGATION_TOKEN, API_VERSION_RENEW_DELEGATION_TOKEN,
};
use super::request_header;
use super::types::*;

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
pub fn build_create_delegation_token_request(
    correlation_id: i32,
    client_id: &str,
    options: &CreateDelegationTokenOptions,
) -> (RequestHeader, CreateDelegationTokenRequest) {
    use kafka_protocol::messages::create_delegation_token_request::CreatableRenewers;

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::CreateDelegationToken,
        API_VERSION_CREATE_DELEGATION_TOKEN,
    );
    let renewers: Vec<CreatableRenewers> = options
        .renewers
        .iter()
        .map(|r| {
            CreatableRenewers::default()
                .with_principal_type(StrBytes::from_string(r.principal_type.clone()))
                .with_principal_name(StrBytes::from_string(r.principal_name.clone()))
        })
        .collect();
    let request = CreateDelegationTokenRequest::default()
        .with_owner_principal_type(
            options
                .owner
                .as_ref()
                .map(|owner| StrBytes::from_string(owner.principal_type.clone())),
        )
        .with_owner_principal_name(
            options
                .owner
                .as_ref()
                .map(|owner| StrBytes::from_string(owner.principal_name.clone())),
        )
        .with_max_lifetime_ms(options.max_lifetime_ms)
        .with_renewers(renewers);

    (header, request)
}

/// Build a `RenewDelegationToken` request.
pub fn build_renew_delegation_token_request(
    correlation_id: i32,
    client_id: &str,
    hmac: Bytes,
    renew_period_ms: i64,
) -> (RequestHeader, RenewDelegationTokenRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::RenewDelegationToken,
        API_VERSION_RENEW_DELEGATION_TOKEN,
    );
    let request = RenewDelegationTokenRequest::default()
        .with_hmac(hmac)
        .with_renew_period_ms(renew_period_ms);

    (header, request)
}

/// Build an `ExpireDelegationToken` request.
pub fn build_expire_delegation_token_request(
    correlation_id: i32,
    client_id: &str,
    hmac: Bytes,
    expiry_time_period_ms: i64,
) -> (RequestHeader, ExpireDelegationTokenRequest) {
    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::ExpireDelegationToken,
        API_VERSION_EXPIRE_DELEGATION_TOKEN,
    );
    let request = ExpireDelegationTokenRequest::default()
        .with_hmac(hmac)
        .with_expiry_time_period_ms(expiry_time_period_ms);

    (header, request)
}

/// Build an `AlterUserScramCredentials` request.
pub fn build_alter_user_scram_credentials_request(
    correlation_id: i32,
    client_id: &str,
    options: &AlterUserScramCredentialsOptions,
) -> (RequestHeader, AlterUserScramCredentialsRequest) {
    use kafka_protocol::messages::alter_user_scram_credentials_request::{
        ScramCredentialDeletion as KpScramCredentialDeletion,
        ScramCredentialUpsertion as KpScramCredentialUpsertion,
    };

    let header = request_header(
        correlation_id,
        client_id,
        ApiKey::AlterUserScramCredentials,
        API_VERSION_ALTER_USER_SCRAM_CREDENTIALS,
    );
    let deletions: Vec<KpScramCredentialDeletion> = options
        .deletions
        .iter()
        .map(|deletion| {
            KpScramCredentialDeletion::default()
                .with_name(StrBytes::from_string(deletion.name.clone()))
                .with_mechanism(deletion.mechanism)
        })
        .collect();
    let upsertions: Vec<KpScramCredentialUpsertion> = options
        .upsertions
        .iter()
        .map(|upsertion| {
            KpScramCredentialUpsertion::default()
                .with_name(StrBytes::from_string(upsertion.name.clone()))
                .with_mechanism(upsertion.mechanism)
                .with_iterations(upsertion.iterations)
                .with_salt(upsertion.salt.clone())
                .with_salted_password(upsertion.salted_password.clone())
        })
        .collect();
    let request = AlterUserScramCredentialsRequest::default()
        .with_deletions(deletions)
        .with_upsertions(upsertions);

    (header, request)
}

/// Build an `UpdateFeatures` request.
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
pub fn convert_create_delegation_token_response(
    response: CreateDelegationTokenResponse,
) -> CreateDelegationTokenResponseData {
    CreateDelegationTokenResponseData {
        error_code: response.error_code,
        owner: KafkaPrincipal::new(
            response.principal_type.to_string(),
            response.principal_name.to_string(),
        ),
        requester: if response.token_requester_principal_type.is_empty()
            && response.token_requester_principal_name.is_empty()
        {
            None
        } else {
            Some(KafkaPrincipal::new(
                response.token_requester_principal_type.to_string(),
                response.token_requester_principal_name.to_string(),
            ))
        },
        issue_timestamp: response.issue_timestamp_ms,
        expiry_timestamp: response.expiry_timestamp_ms,
        max_timestamp: response.max_timestamp_ms,
        token_id: response.token_id.to_string(),
        hmac: response.hmac,
        throttle_time_ms: response.throttle_time_ms,
    }
}

/// Convert a generated `RenewDelegationTokenResponse` into the crate's public shape.
pub fn convert_renew_delegation_token_response(
    response: &RenewDelegationTokenResponse,
) -> RenewDelegationTokenResponseData {
    RenewDelegationTokenResponseData {
        error_code: response.error_code,
        expiry_timestamp_ms: response.expiry_timestamp_ms,
        throttle_time_ms: response.throttle_time_ms,
    }
}

/// Convert a generated `ExpireDelegationTokenResponse` into the crate's public shape.
pub fn convert_expire_delegation_token_response(
    response: &ExpireDelegationTokenResponse,
) -> ExpireDelegationTokenResponseData {
    ExpireDelegationTokenResponseData {
        error_code: response.error_code,
        expiry_timestamp_ms: response.expiry_timestamp_ms,
        throttle_time_ms: response.throttle_time_ms,
    }
}

/// Convert a generated `AlterUserScramCredentialsResponse` into the crate's public shape.
pub fn convert_alter_user_scram_credentials_response(
    response: AlterUserScramCredentialsResponse,
) -> AlterUserScramCredentialsResponseData {
    AlterUserScramCredentialsResponseData {
        throttle_time_ms: response.throttle_time_ms,
        results: response
            .results
            .into_iter()
            .map(|result| AlterUserScramCredentialResult {
                user: result.user.to_string(),
                error_code: result.error_code,
                error_message: result
                    .error_message
                    .map(|m| m.to_string())
                    .filter(|m| !m.is_empty()),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult as KpAlterUserScramCredentialsResult;
    use kafka_protocol::messages::describe_delegation_token_response::{
        DescribedDelegationToken as KpDelegationToken,
        DescribedDelegationTokenRenewer as KpDelegationTokenRenewer,
    };
    use kafka_protocol::messages::describe_user_scram_credentials_response::{
        CredentialInfo as KpScramCredentialInfo,
        DescribeUserScramCredentialsResult as KpScramCredentialsResult,
    };

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
    fn create_delegation_token_request_includes_renewers_and_max_lifetime() {
        let options = CreateDelegationTokenOptions::new()
            .with_owner(KafkaPrincipal::user("carol"))
            .with_renewers([
                KafkaPrincipal::new("User", "alice"),
                KafkaPrincipal::new("User", "bob"),
            ])
            .with_max_lifetime_ms(86_400_000);
        let (header, request) = build_create_delegation_token_request(42, "client-a", &options);

        assert_eq!(header.request_api_key, ApiKey::CreateDelegationToken as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_CREATE_DELEGATION_TOKEN
        );
        assert_eq!(header.correlation_id, 42);
        assert_eq!(
            request
                .owner_principal_type
                .as_ref()
                .map(ToString::to_string),
            Some("User".to_owned())
        );
        assert_eq!(
            request
                .owner_principal_name
                .as_ref()
                .map(ToString::to_string),
            Some("carol".to_owned())
        );
        assert_eq!(request.max_lifetime_ms, 86_400_000);
        assert_eq!(request.renewers.len(), 2);
        assert_eq!(
            request.renewers[0].principal_type,
            StrBytes::from_static_str("User")
        );
        assert_eq!(
            request.renewers[0].principal_name,
            StrBytes::from_static_str("alice")
        );
        assert_eq!(
            request.renewers[1].principal_name,
            StrBytes::from_static_str("bob")
        );
    }

    #[test]
    fn create_delegation_token_request_omits_owner_for_broker_default() {
        let options = CreateDelegationTokenOptions::default();
        let (_, request) = build_create_delegation_token_request(43, "client-a", &options);

        assert!(request.owner_principal_type.is_none());
        assert!(request.owner_principal_name.is_none());
        assert_eq!(request.max_lifetime_ms, -1);
        assert!(request.renewers.is_empty());
    }

    #[test]
    fn renew_delegation_token_request_includes_hmac_and_period() {
        let hmac = Bytes::from_static(b"test-hmac-data");
        let (header, request) =
            build_renew_delegation_token_request(7, "client-b", hmac.clone(), 3_600_000);

        assert_eq!(header.request_api_key, ApiKey::RenewDelegationToken as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_RENEW_DELEGATION_TOKEN
        );
        assert_eq!(request.hmac, hmac);
        assert_eq!(request.renew_period_ms, 3_600_000);
    }

    #[test]
    fn expire_delegation_token_request_includes_hmac_and_expiry() {
        let hmac = Bytes::from_static(b"expire-hmac");
        let (header, request) =
            build_expire_delegation_token_request(8, "client-c", hmac.clone(), 60_000);

        assert_eq!(header.request_api_key, ApiKey::ExpireDelegationToken as i16);
        assert_eq!(
            header.request_api_version,
            API_VERSION_EXPIRE_DELEGATION_TOKEN
        );
        assert_eq!(request.hmac, hmac);
        assert_eq!(request.expiry_time_period_ms, 60_000);
    }

    #[test]
    fn alter_user_scram_credentials_request_includes_deletions_and_upsertions() {
        let salt = Bytes::from_static(b"salt-value");
        let password = Bytes::from_static(b"salted-pw");
        let options = AlterUserScramCredentialsOptions::new()
            .with_deletion(ScramCredentialDeletion::new(
                "old-user",
                SCRAM_MECHANISM_SHA_256,
            ))
            .with_upsertion(ScramCredentialUpsertion::new(
                "new-user",
                SCRAM_MECHANISM_SHA_512,
                4096,
                salt.clone(),
                password.clone(),
            ));
        let (header, request) = build_alter_user_scram_credentials_request(9, "client-d", &options);

        assert_eq!(
            header.request_api_key,
            ApiKey::AlterUserScramCredentials as i16
        );
        assert_eq!(
            header.request_api_version,
            API_VERSION_ALTER_USER_SCRAM_CREDENTIALS
        );
        assert_eq!(request.deletions.len(), 1);
        assert_eq!(
            request.deletions[0].name,
            StrBytes::from_static_str("old-user")
        );
        assert_eq!(request.deletions[0].mechanism, SCRAM_MECHANISM_SHA_256);
        assert_eq!(request.upsertions.len(), 1);
        assert_eq!(
            request.upsertions[0].name,
            StrBytes::from_static_str("new-user")
        );
        assert_eq!(request.upsertions[0].mechanism, SCRAM_MECHANISM_SHA_512);
        assert_eq!(request.upsertions[0].iterations, 4096);
        assert_eq!(request.upsertions[0].salt, salt);
        assert_eq!(request.upsertions[0].salted_password, password);
    }

    #[test]
    fn create_delegation_token_response_maps_all_fields() {
        let response = CreateDelegationTokenResponse::default()
            .with_error_code(0)
            .with_principal_type(StrBytes::from_static_str("User"))
            .with_principal_name(StrBytes::from_static_str("alice"))
            .with_token_requester_principal_type(StrBytes::from_static_str("User"))
            .with_token_requester_principal_name(StrBytes::from_static_str("bob"))
            .with_issue_timestamp_ms(1_000)
            .with_expiry_timestamp_ms(2_000)
            .with_max_timestamp_ms(3_000)
            .with_token_id(StrBytes::from_static_str("token-1"))
            .with_hmac(Bytes::from_static(b"hmac-data"))
            .with_throttle_time_ms(5);

        let converted = convert_create_delegation_token_response(response);

        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.owner, KafkaPrincipal::user("alice"));
        assert_eq!(converted.requester, Some(KafkaPrincipal::user("bob")));
        assert_eq!(converted.issue_timestamp, 1_000);
        assert_eq!(converted.expiry_timestamp, 2_000);
        assert_eq!(converted.max_timestamp, 3_000);
        assert_eq!(converted.token_id, "token-1");
        assert_eq!(converted.hmac, Bytes::from_static(b"hmac-data"));
        assert_eq!(converted.throttle_time_ms, 5);
    }

    #[test]
    fn renew_delegation_token_response_maps_all_fields() {
        let response = RenewDelegationTokenResponse::default()
            .with_error_code(0)
            .with_expiry_timestamp_ms(9_000)
            .with_throttle_time_ms(10);

        let converted = convert_renew_delegation_token_response(&response);

        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.expiry_timestamp_ms, 9_000);
        assert_eq!(converted.throttle_time_ms, 10);
    }

    #[test]
    fn expire_delegation_token_response_maps_all_fields() {
        let response = ExpireDelegationTokenResponse::default()
            .with_error_code(0)
            .with_expiry_timestamp_ms(5_000)
            .with_throttle_time_ms(7);

        let converted = convert_expire_delegation_token_response(&response);

        assert_eq!(converted.error_code, 0);
        assert_eq!(converted.expiry_timestamp_ms, 5_000);
        assert_eq!(converted.throttle_time_ms, 7);
    }

    #[test]
    fn alter_user_scram_credentials_response_maps_all_fields() {
        let response = AlterUserScramCredentialsResponse::default()
            .with_throttle_time_ms(3)
            .with_results(vec![
                KpAlterUserScramCredentialsResult::default()
                    .with_user(StrBytes::from_static_str("alice"))
                    .with_error_code(0)
                    .with_error_message(Some(StrBytes::from_static_str("ok"))),
                KpAlterUserScramCredentialsResult::default()
                    .with_user(StrBytes::from_static_str("bob"))
                    .with_error_code(42),
            ]);

        let converted = convert_alter_user_scram_credentials_response(response);

        assert_eq!(converted.throttle_time_ms, 3);
        assert_eq!(converted.results.len(), 2);
        assert_eq!(converted.results[0].user, "alice");
        assert_eq!(converted.results[0].error_code, 0);
        assert_eq!(converted.results[0].error_message, Some("ok".to_owned()));
        assert_eq!(converted.results[1].user, "bob");
        assert_eq!(converted.results[1].error_code, 42);
        assert_eq!(converted.results[1].error_message, None);
    }
}
