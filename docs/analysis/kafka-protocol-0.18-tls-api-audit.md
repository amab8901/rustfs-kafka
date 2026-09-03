# kafka-protocol 0.18 / TLS / API 审计结论

日期: 2026-09-03

## 结论

- `kafka-protocol` 已升级到 `0.18.0`，本项目唯一编译不兼容点是 `kafka_protocol::records::Record` 新增
  `delete_horizon` 字段。同步和异步 ProduceRecord 构造均已补为 `false`，保持普通生产消息行为不变。
- `rustls-native-certs` 已从 workspace、同步 crate、异步 crate 和 lockfile 中移除；`openssl-probe`、`schannel`、
  `security-framework` 等由它带来的平台相关 crate 也随之消失。
- 默认 TLS 信任根现在只来自 `webpki-roots`；私有 CA、企业 CA、自签证书继续通过 `SecurityConfig::with_ca_cert`
  显式配置。
- `rustfs-kafka-async` 新增公开 re-export: `TlsConfig`。这让 async 用户可以直接从
  `rustfs_kafka_async::{SecurityConfig, TlsConfig}` 完成 TLS 配置，不必绕回 sync crate path。
- async builder 中仅为兼容保留、运行时已被忽略的旧配置项已标记 deprecated:
  `with_channel_capacity` 与 `with_native_async`。

## kafka-protocol 0.18.0 兼容性

上游 `kafka-protocol 0.18.0` 仍提供 `Default::default()`、builder 方法、`Encodable`、`Decodable` 和
`HeaderVersion` 这些当前项目依赖的核心 API。该版本生成目标升级到 Kafka 4.1.0 schema，导出的协议类型按
上游说明具备 non-exhaustive/前向兼容设计，但直接使用结构体字面量构造 record 时仍需要适配新增字段。

当前项目的适配点:

- `crates/rustfs-kafka/src/protocol/produce.rs`: `Record { delete_horizon: false, ... }`
- `crates/rustfs-kafka-async/src/producer.rs`: `KpRecord { delete_horizon: false, ... }`

未发现 `RequestHeader`、`ResponseHeader`、Produce/Fetch/Metadata/Offset/SASL 相关 builder 和 encode/decode 调用的
进一步签名破坏。

## rustls-platform-verifier 可行性

`rustls-platform-verifier` 可以接入当前 rustls 0.23 构建链；其 `BuilderVerifierExt::with_platform_verifier()` 可替代
`with_root_certificates(root_store)`，并使用平台证书验证能力。它对桌面/移动应用是更贴近系统行为的选择，尤其适合需要
OS trust decisions、企业根证书、吊销信息和代理环境的客户端应用。

但不建议本轮直接替换为默认依赖:

- 当前目标要求项目中不出现 openssl 相关 crate；`rustls-platform-verifier` 在 Linux/BSD fallback 场景仍可能依赖
  native cert loading 体系，不能保证 lockfile 永久不出现相关平台 crate。
- 本项目是 Kafka client library，不是最终应用。强行默认接入平台 verifier 会把平台验证策略、Android 额外 setup、
  以及非纯 Rust verifier 的风险转嫁给所有下游。
- 现有 `webpki-roots + ca_cert_path` 方案更适合库默认值: 依赖图稳定、可复现、容器友好，企业 CA 需求也有显式配置入口。

建议后续如果要支持 platform verifier，将其作为单独 opt-in feature 评估，例如 `platform-verifier`，并要求它与当前
`webpki-roots` 默认路径互斥或明确优先级；同时加入 Linux、macOS、Windows 和容器镜像依赖树检查。

## 公开 API 建议

已公开:

- `rustfs_kafka_async::TlsConfig`

暂不公开:

- `kafka-protocol` 原始 message/record 类型。当前 crate 已通过 `protocol` 适配层隐藏 wire schema，直接公开会扩大
  semver 承诺，并把上游协议字段变动暴露给用户。
- `ApiVersionCache` 等内部协商结构。它们和 broker 兼容性、fallback 策略强耦合，应继续由 client 内部维护。

已标记过期:

- `AsyncProducerBuilder::with_channel_capacity`
- `AsyncProducerBuilder::with_native_async`
- `AsyncConsumerBuilder::with_channel_capacity`
- `AsyncConsumerBuilder::with_native_async`

这些 API 在 1.2.0 native async-only 行为下已经无实际效果，保留会误导用户以为仍存在 sync fallback 或内部 channel
调优入口。

## 验证

```bash
cargo check --workspace --all-targets --all-features
cargo tree --workspace --all-features -i rustls-native-certs
cargo tree --workspace --all-features --target all -i openssl-probe
```
