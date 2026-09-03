# Kafka Protocol Coverage

This matrix tracks the `kafka-protocol` `0.18.0` request APIs visible to the crate and how far
`rustfs-kafka` exposes them. It is intentionally biased toward normal client/admin APIs; broker,
controller, and coordinator-internal APIs should not be exposed as stable public helpers unless there is
a clear user-facing workflow.

Source checked:

- `kafka-protocol-0.18.0/src/messages.rs`
- `kafka-protocol-0.18.0/src/messages/*_request.rs`
- `crates/rustfs-kafka/src/protocol/*`
- `crates/rustfs-kafka/src/client/mod.rs`

## Coverage Summary

- Total `kafka-protocol` API keys: 87.
- Public or high-level runtime coverage: 56 APIs.
- Internal runtime coverage without direct public API: 10 APIs.
- Client-facing backlog: 0 APIs.
- Advanced runtime backlog: 7 APIs (share-consumer and telemetry subsystems).
- Broker/controller/internal backlog: 14 APIs (quorum, coordinator, KRaft internals).

## API Matrix

| Key | Protocol | Current status | Next action |
| --- | --- | --- | --- |
| 0 | Produce | Public/runtime implemented | Keep current producer API; continue protocol-version upgrades as needed. |
| 1 | Fetch | Public/runtime implemented | Keep current consumer API; continue protocol-version upgrades as needed. |
| 2 | ListOffsets | Public implemented | Keep public offset query helpers. |
| 3 | Metadata | Public/runtime implemented | Keep metadata loading helpers. |
| 8 | OffsetCommit | Public/runtime implemented | Keep group offset commit helpers. |
| 9 | OffsetFetch | Public/runtime implemented | Keep group offset fetch helpers. |
| 10 | FindCoordinator | Internal/runtime implemented | Keep internal; expose only via higher-level group/transaction APIs. |
| 11 | JoinGroup | Internal consumer runtime implemented | Keep internal until consumer group protocol is modernized. |
| 12 | Heartbeat | Internal consumer runtime implemented | Keep internal. |
| 13 | LeaveGroup | Internal consumer runtime implemented | Keep internal. |
| 14 | SyncGroup | Internal consumer runtime implemented | Keep internal until group protocol modernization. |
| 15 | DescribeGroups | Public admin implemented | Done. |
| 16 | ListGroups | Public admin implemented | Done. |
| 17 | SaslHandshake | Internal auth runtime implemented | Keep internal auth flow. |
| 18 | ApiVersions | Public admin/runtime implemented | Done. |
| 19 | CreateTopics | Public admin implemented | Candidate: migrate manual codec to generated `kafka-protocol` request/response. |
| 20 | DeleteTopics | Public admin implemented | Candidate: migrate manual codec to generated `kafka-protocol` request/response. |
| 21 | DeleteRecords | Public admin implemented | Done. |
| 22 | InitProducerId | Internal transactional producer runtime implemented | Keep internal; extend only through transaction producer API. |
| 23 | OffsetForLeaderEpoch | Public diagnostic implemented | Done. |
| 24 | AddPartitionsToTxn | Internal transactional producer runtime implemented | Keep internal. |
| 25 | AddOffsetsToTxn | Public advanced transaction implemented | Done. |
| 26 | EndTxn | Internal transactional producer runtime implemented | Keep internal. |
| 27 | WriteTxnMarkers | Missing coordinator-internal API | Do not expose as normal client API. |
| 28 | TxnOffsetCommit | Public advanced transaction implemented | Done. |
| 29 | DescribeAcls | Public admin implemented | Done. |
| 30 | CreateAcls | Public admin implemented | Done. |
| 31 | DeleteAcls | Public admin implemented | Done. |
| 32 | DescribeConfigs | Public admin implemented | Done. |
| 33 | AlterConfigs | Public legacy admin implemented, deprecated | Prefer `IncrementalAlterConfigs`; kept only for compatibility. |
| 34 | AlterReplicaLogDirs | Public broker storage admin implemented | Done. |
| 35 | DescribeLogDirs | Public admin implemented | Done. |
| 36 | SaslAuthenticate | Internal auth runtime implemented | Keep internal auth flow. |
| 37 | CreatePartitions | Public admin implemented | Done. |
| 38 | CreateDelegationToken | Public security admin implemented | Done. |
| 39 | RenewDelegationToken | Public security admin implemented | Done. |
| 40 | ExpireDelegationToken | Public security admin implemented | Done. |
| 41 | DescribeDelegationToken | Public admin implemented | Done. |
| 42 | DeleteGroups | Public admin implemented | Done. |
| 43 | ElectLeaders | Public admin implemented | Done. |
| 44 | IncrementalAlterConfigs | Public admin implemented | Done. |
| 45 | AlterPartitionReassignments | Public admin implemented | Done. |
| 46 | ListPartitionReassignments | Public admin implemented | Done. |
| 47 | OffsetDelete | Public admin implemented | Done. |
| 48 | DescribeClientQuotas | Public admin implemented | Done. |
| 49 | AlterClientQuotas | Public admin implemented | Done. |
| 50 | DescribeUserScramCredentials | Public admin implemented | Done. |
| 51 | AlterUserScramCredentials | Public security admin implemented | Done; caller supplies precomputed SCRAM salt and salted password bytes. |
| 52 | Vote | Missing quorum-internal API | Do not expose as normal client API. |
| 53 | BeginQuorumEpoch | Missing quorum-internal API | Do not expose as normal client API. |
| 54 | EndQuorumEpoch | Missing quorum-internal API | Do not expose as normal client API. |
| 55 | DescribeQuorum | Public admin implemented | Done. |
| 56 | AlterPartition | Missing controller/internal API | Keep internal unless a controller client is deliberately added. |
| 57 | UpdateFeatures | Public KRaft feature admin implemented | Done; prefer `validate_only` before applying changes. |
| 58 | Envelope | Missing broker/controller forwarding API | Do not expose as normal client API. |
| 59 | FetchSnapshot | Missing raft snapshot API | Do not expose as normal client API. |
| 60 | DescribeCluster | Public admin implemented | Done. |
| 61 | DescribeProducers | Public diagnostic implemented | Done. |
| 62 | BrokerRegistration | Missing broker-internal API | Do not expose as normal client API. |
| 63 | BrokerHeartbeat | Missing broker-internal API | Do not expose as normal client API. |
| 64 | UnregisterBroker | Public KRaft broker lifecycle admin implemented | Done; destructive cluster operation. |
| 65 | DescribeTransactions | Public diagnostic implemented | Done. |
| 66 | ListTransactions | Public diagnostic implemented | Done. |
| 67 | AllocateProducerIds | Missing broker/internal producer-id API | Keep internal unless idempotent producer allocation is redesigned. |
| 68 | ConsumerGroupHeartbeat | Missing advanced consumer runtime API | Add as part of modern consumer group protocol runtime, not standalone admin. |
| 69 | ConsumerGroupDescribe | Public diagnostic implemented | Done. |
| 70 | ControllerRegistration | Missing controller-internal API | Do not expose as normal client API. |
| 71 | GetTelemetrySubscriptions | Missing client telemetry runtime API | Add only with a telemetry subsystem. |
| 72 | PushTelemetry | Missing client telemetry runtime API | Add only with a telemetry subsystem. |
| 73 | AssignReplicasToDirs | Public broker storage admin implemented | Done; use only with explicit JBOD/directory-assignment workflow. |
| 74 | ListConfigResources | Public admin implemented | Done. |
| 75 | DescribeTopicPartitions | Public diagnostic implemented | Done. |
| 76 | ShareGroupHeartbeat | Missing advanced share-consumer runtime API | Add as part of full share-consumer runtime. |
| 77 | ShareGroupDescribe | Public diagnostic implemented | Done. |
| 78 | ShareFetch | Missing advanced share-consumer runtime API | Add as part of full share-consumer runtime. |
| 79 | ShareAcknowledge | Missing advanced share-consumer runtime API | Add as part of full share-consumer runtime. |
| 80 | AddRaftVoter | Public KRaft quorum admin implemented | Done; explicit KRaft voter workflow only. |
| 81 | RemoveRaftVoter | Public KRaft quorum admin implemented | Done; explicit KRaft voter workflow only. |
| 82 | UpdateRaftVoter | Public KRaft quorum admin implemented | Done; explicit KRaft voter workflow only. |
| 83 | InitializeShareGroupState | Missing share coordinator internal API | Do not expose as normal client API. |
| 84 | ReadShareGroupState | Missing share coordinator internal API | Do not expose as normal client API. |
| 85 | WriteShareGroupState | Missing share coordinator internal API | Do not expose as normal client API. |
| 86 | DeleteShareGroupState | Missing share coordinator internal API | Do not expose as normal client API. |
| 87 | ReadShareGroupStateSummary | Missing share coordinator internal API | Do not expose as normal client API. |
| 90 | DescribeShareGroupOffsets | Public diagnostic implemented | Done. |
| 91 | AlterShareGroupOffsets | Public share-group admin implemented | Done. |
| 92 | DeleteShareGroupOffsets | Public share-group admin implemented | Done. |

## Recommended Implementation Batches

All client-facing admin APIs are now implemented. Remaining work falls into advanced
runtime subsystems or internal-only protocols:

1. Share consumer runtime: `ConsumerGroupHeartbeat`, `ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge`.
2. Telemetry runtime: `GetTelemetrySubscriptions`, `PushTelemetry`.
3. Keep broker, controller, coordinator, and raft-log internals unexposed unless a dedicated controller client is introduced.
