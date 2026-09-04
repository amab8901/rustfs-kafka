# 移除 `rustls-native-certs` 可行性分析

> 更新: 本文是早期可选化方案草案；当前实现已采用完全移除 `rustls-native-certs`，最新审计结论见
> `docs/analysis/kafka-protocol-0.18-tls-api-audit.md`。
>
> 日期: 2026-06-18
> 项目: rustfs-kafka / rustfs-kafka-async
> 版本: v1.2.0
> 分析范围: 移除 `rustls-native-certs` 依赖的可行性、影响范围及实施方案

---

## 目录

1. [概述](#1-概述)
2. [当前使用现状](#2-当前使用现状)
3. [依赖分析](#3-依赖分析)
4. [移除影响评估](#4-移除影响评估)
5. [替代方案对比](#5-替代方案对比)
6. [推荐方案](#6-推荐方案)
7. [实施步骤](#7-实施步骤)
8. [风险与注意事项](#8-风险与注意事项)
9. [结论](#9-结论)

---

## 1. 概述

### 1.1 背景

`rustls-native-certs` 是一个用于从操作系统原生证书存储加载根证书的 Rust crate。在当前项目中，它作为 TLS 连接的证书来源之一，与 `webpki-roots`（Mozilla 内置根证书）共同使用。

### 1.2 分析目的

评估从项目中完全移除 `rustls-native-certs` 的可行性，分析其对功能、安全性、兼容性和维护性的影响，并提出推荐方案。

---

## 2. 当前使用现状

### 2.1 依赖声明

| 文件 | 声明方式 | 可选性 |
|------|----------|--------|
| `Cargo.toml`（workspace root） | `rustls-native-certs = "0.8.3"` | workspace 级声明 |
| `crates/rustfs-kafka/Cargo.toml` | `{ workspace = true, optional = true }` | 可选，由 `security` / `security-ring` feature 激活 |
| `crates/rustfs-kafka-async/Cargo.toml` | `rustls-native-certs.workspace = true` | **必选**（非 optional） |

### 2.2 代码调用点

项目中仅有 **2 处** 直接调用 `rustls_native_certs::load_native_certs()`：

#### 同步实现（rustfs-kafka）

**文件**: `crates/rustfs-kafka/src/tls/rustls_connector.rs:219`

```rust
fn load_root_store(tls_config: &TlsConfig) -> io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_cert_path) = &tls_config.ca_cert_path {
        // 加载自定义 CA 证书（不涉及 native-certs）
        // ...
    } else {
        // 默认路径：webpki-roots + native-certs
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());  // L216
        let native_certs = rustls_native_certs::load_native_certs();         // L219
        for cert in native_certs.certs {                                      // L220
            let _ = root_store.add(cert);                                     // L221
        }
        if let Some(e) = native_certs.errors.first() {                       // L223
            debug!("Failed to load some native certs (using webpki-roots as fallback): {}", e);
        }
    }
    Ok(root_store)
}
```

#### 异步实现（rustfs-kafka-async）

**文件**: `crates/rustfs-kafka-async/src/connection.rs:688`

```rust
async fn load_root_store(tls_config: &TlsConfig) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_cert_path) = &tls_config.ca_cert_path {
        // 加载自定义 CA 证书（不涉及 native-certs）
        // ...
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());  // L687
        let native_certs = rustls_native_certs::load_native_certs();         // L688
        for cert in native_certs.certs {                                      // L689
            let _ = root_store.add(cert);                                     // L690
        }
        if let Some(e) = native_certs.errors.first() {                       // L692
            debug!("Failed to load some native certs (using webpki-roots as fallback): {}", e);
        }
    }
    Ok(root_store)
}
```

### 2.3 使用模式总结

两处实现遵循 **完全相同的模式**：

1. **当用户指定了 `ca_cert_path`**：仅加载用户提供的 CA 证书，**不使用** `rustls-native-certs`
2. **当未指定 `ca_cert_path`（默认情况）**：
   - 先加载 `webpki_roots::TLS_SERVER_ROOTS`（Mozilla 根证书）
   - 再加载 `rustls_native_certs::load_native_certs()`（系统根证书）
   - 两者合并到同一个 `RootCertStore`
   - native-certs 加载失败仅记录 debug 日志，**不中断连接**

---

## 3. 依赖分析

### 3.1 `rustls-native-certs` 的依赖树

```
rustls-native-certs v0.8.3
├── rustls-pki-types v1.14.1
│   └── zeroize v1.8.2
└── security-framework v3.7.0          # macOS 特有
    ├── bitflags v2.11.1
    ├── core-foundation v0.10.1
    │   ├── core-foundation-sys v0.8.7
    │   └── libc v0.2.186
    ├── core-foundation-sys v0.8.7
    ├── libc v0.2.186
    └── security-framework-sys v2.17.0
        ├── core-foundation-sys v0.8.7
        └── libc v0.2.186
```

### 3.2 `webpki-roots` 的依赖树

```
webpki-roots v1.0.7
└── rustls-pki-types v1.14.1
    └── zeroize v1.8.2
```

### 3.3 依赖对比

| 维度 | `rustls-native-certs` | `webpki-roots` |
|------|----------------------|----------------|
| 直接依赖数量 | 2 个（rustls-pki-types + 平台 crate） | 1 个（rustls-pki-types） |
| 平台特定依赖 | 是（security-framework / schannel / openssl） | 否 |
| 传递依赖数量 | ~8 个（macOS）| ~2 个 |
| 二进制大小影响 | 增加平台库链接 | 证书嵌入约 ~200KB |
| 编译时间影响 | 较大（需编译平台绑定） | 极小 |

---

## 4. 移除影响评估

### 4.1 功能影响

#### ✅ 无影响的功能

| 功能 | 原因 |
|------|------|
| 自定义 CA 证书（`ca_cert_path`） | 不使用 native-certs |
| 客户端证书认证（mTLS） | 不使用 native-certs |
| 主机名验证 | 与证书来源无关 |
| TLS 1.2 / TLS 1.3 | 与证书来源无关 |
| insecure 模式 | 跳过所有证书验证 |

#### ⚠️ 受影响的功能

| 功能 | 影响程度 | 说明 |
|------|----------|------|
| 默认 TLS 连接 | **中等** | 移除后仅使用 `webpki-roots` 的 Mozilla 根证书，丢失系统自定义根证书 |
| 企业内部 CA 支持 | **高** | 使用企业自签 CA 的用户将无法通过默认配置连接 |
| 容器/嵌入式环境 | **低** | 这些环境通常无系统证书存储，native-certs 已无效 |

### 4.2 兼容性影响

| 平台 | 影响 |
|------|------|
| Linux | 使用系统证书存储（`/etc/ssl/certs`）的企业用户受影响 |
| macOS | 使用 Keychain 中自定义根证书的用户受影响 |
| Windows | 使用 Windows 证书存储中自定义根证书的用户受影响 |
| 容器（Docker/K8s） | 通常无系统证书，**基本无影响** |

### 4.3 安全性影响

| 维度 | 移除前 | 移除后 |
|------|--------|--------|
| 证书来源 | webpki-roots + 系统证书 | 仅 webpki-roots |
| 证书更新 | webpki 随 crate 更新；系统证书随 OS 更新 | 仅随 crate 更新 |
| 企业 PKI | 自动信任企业根 CA | 需手动配置 `ca_cert_path` |
| 吊销的根 CA | 系统更新可吊销；webpki 需发版 | 仅依赖 webpki 发版 |
| 供应链安全 | 多一个依赖入口 | 依赖更精简 |

### 4.4 运维影响

- **开发环境**：开发者通常使用公共 CA 签发的证书，影响极小
- **生产环境**：使用企业内部 CA 的 Kafka 集群需要额外配置 `ca_cert_path`
- **CI/CD**：测试环境通常使用公共证书或自签证书（通过 `ca_cert_path` 配置），影响可控

---

## 5. 替代方案对比

### 方案 A：完全移除 `rustls-native-certs`

**做法**：删除依赖，仅保留 `webpki-roots` 作为默认证书来源。

| 优势 | 劣势 |
|------|------|
| 减少依赖和编译时间 | 丢失系统证书支持 |
| 消除平台特定代码 | 企业用户需手动配置 CA |
| 简化 TLS 初始化逻辑 | 行为变更，可能影响现有用户 |
| 减少二进制大小（移除平台绑定） | — |

### 方案 B：将 `rustls-native-certs` 改为可选 feature

**做法**：新增 `native-certs` feature flag，默认不启用。用户按需启用。

| 优势 | 劣势 |
|------|------|
| 默认行为更可控 | 需维护额外 feature flag |
| 企业用户可通过 feature 启用 | 异步 crate 需重构为 optional 依赖 |
| 向后兼容（feature 启用时行为不变） | 增加配置复杂度 |

### 方案 C：保持现状

**做法**：不做变更。

| 优势 | 劣势 |
|------|------|
| 零风险 | 维持平台特定依赖开销 |
| 最大兼容性 | 异步 crate 永久携带非必要依赖 |
| — | 容器环境中 native-certs 本身无实际作用 |

### 方案 D：替换为 `rustls-platform-verifier`

**做法**：使用 `rustls-platform-verifier` 替代 native-certs，它同时处理证书加载和验证。

| 优势 | 劣势 |
|------|------|
| 更现代的平台集成 | 引入新的依赖 |
| 自动处理平台差异 | 与当前架构差异较大，重构成本高 |
| Google 的维护支持 | 可能与 webpki-roots 功能重叠 |

---

## 6. 推荐方案

### 🏆 推荐：方案 B — 将 `rustls-native-certs` 改为可选 feature

**理由**：

1. **风险最低**：保留功能完整性，不破坏现有用户
2. **收益明显**：默认不编译平台依赖，减少编译时间和二进制大小
3. **灵活可控**：企业用户可通过 feature flag 启用系统证书支持
4. **渐进式**：可在后续版本中评估是否进一步移除

### 具体设计

```toml
# crates/rustfs-kafka/Cargo.toml
[features]
security = [
    "dep:rustls",
    "dep:webpki-roots",
    # "dep:rustls-native-certs",  # 从默认 feature 中移除
    "dep:base64",
    "dep:hmac",
    "dep:sha2",
    "dep:pbkdf2",
    "dep:rand",
    "rustls/aws-lc-rs",
]

# 新增 feature
native-certs = ["security", "dep:rustls-native-certs"]
```

```toml
# crates/rustfs-kafka-async/Cargo.toml
[dependencies]
rustls-native-certs = { workspace = true, optional = true }  # 改为 optional

[features]
default = ["metrics"]
native-certs = ["dep:rustls-native-certs"]
```

---

## 7. 实施步骤

### 7.1 代码修改

#### 同步 crate（rustfs-kafka）

**文件**: `crates/rustfs-kafka/src/tls/rustls_connector.rs`

```rust
fn load_root_store(tls_config: &TlsConfig) -> io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_cert_path) = &tls_config.ca_cert_path {
        // 加载自定义 CA 证书（不变）
        // ...
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // 条件编译：仅在启用 native-certs feature 时加载系统证书
        #[cfg(feature = "native-certs")]
        {
            let native_certs = rustls_native_certs::load_native_certs();
            for cert in native_certs.certs {
                let _ = root_store.add(cert);
            }
            if let Some(e) = native_certs.errors.first() {
                debug!(
                    "Failed to load some native certs (using webpki-roots as fallback): {}",
                    e
                );
            }
        }
    }
    Ok(root_store)
}
```

#### 异步 crate（rustfs-kafka-async）

**文件**: `crates/rustfs-kafka-async/src/connection.rs`

```rust
async fn load_root_store(tls_config: &TlsConfig) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_cert_path) = &tls_config.ca_cert_path {
        // 加载自定义 CA 证书（不变）
        // ...
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        // 条件编译：仅在启用 native-certs feature 时加载系统证书
        #[cfg(feature = "native-certs")]
        {
            let native_certs = rustls_native_certs::load_native_certs();
            for cert in native_certs.certs {
                let _ = root_store.add(cert);
            }
            if let Some(e) = native_certs.errors.first() {
                debug!(
                    "Failed to load some native certs (using webpki-roots as fallback): {}",
                    e
                );
            }
        }
    }

    Ok(root_store)
}
```

### 7.2 Cargo.toml 修改

详见 [第 6 节](#6-推荐方案) 中的 Cargo.toml 配置示例。

### 7.3 测试验证

1. **编译测试**：
   ```bash
   # 默认编译（不含 native-certs）
   cargo build --features security

   # 含 native-certs 编译
   cargo build --features native-certs

   # 异步 crate 默认编译
   cargo build -p rustfs-kafka-async

   # 异步 crate 含 native-certs
   cargo build -p rustfs-kafka-async --features native-certs
   ```

2. **功能测试**：
   - 使用公共 CA 签发的证书测试 TLS 连接
   - 使用自签证书 + `ca_cert_path` 测试 TLS 连接
   - 使用企业 CA（如有条件）测试 `native-certs` feature

3. **依赖检查**：
   ```bash
   # 确认默认不包含 rustls-native-certs
   cargo tree -p rustfs-kafka --features security | grep native-certs
   # 应无输出

   # 确认启用 feature 后包含
   cargo tree -p rustfs-kafka --features native-certs | grep native-certs
   # 应有输出
   ```

---

## 8. 风险与注意事项

### 8.1 风险矩阵

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 企业用户默认配置失效 | 中 | 高 | 文档说明 + 迁移指南 |
| 异步 crate 用户无感知 | 中 | 中 | 在 CHANGELOG 中明确标注 |
| `webpki-roots` 证书过期 | 低 | 中 | 依赖 Mozilla 及时更新 |
| Feature flag 组合复杂 | 低 | 低 | CI 覆盖所有 feature 组合 |

### 8.2 向后兼容性

- **破坏性变更**：默认行为变更（不再加载系统证书）属于 **行为破坏**，虽然 API 未变
- **版本策略**：建议作为 **minor 版本** 发布（如 v1.3.0），并在 CHANGELOG 中明确说明
- **迁移路径**：用户只需在 `Cargo.toml` 中添加 `features = ["native-certs"]` 即可恢复原行为

### 8.3 文档更新

需要更新以下文档：

1. **README.md**：说明 TLS 证书加载行为
2. **CHANGELOG.md**：记录此变更
3. **API 文档**：`SecurityConfig` / `TlsConfig` 的文档注释
4. **示例代码**：`examples/example-rustls.rs` 如适用

---

## 9. 结论

### 9.1 可行性判定：✅ **可行**

移除 `rustls-native-certs`（或改为可选）在技术上完全可行，原因如下：

1. **调用点极少**：仅 2 处代码调用，修改成本低
2. **已有替代**：`webpki-roots` 已作为主要证书来源，native-certs 仅作为补充
3. **错误容忍**：native-certs 加载失败不中断连接，说明系统证书非必需
4. **feature 机制成熟**：项目已有 `security` / `security-ring` 的 feature 管理模式

### 9.2 综合评估

| 评估维度 | 方案 A（完全移除） | 方案 B（可选 feature） | 方案 C（保持现状） |
|----------|-------------------|----------------------|-------------------|
| 技术可行性 | ✅ 高 | ✅ 高 | ✅ 无需改动 |
| 风险等级 | ⚠️ 中 | ✅ 低 | ✅ 无 |
| 收益程度 | ✅ 高 | ✅ 中高 | ❌ 无 |
| 维护成本 | ✅ 最低 | ✅ 低 | ⚠️ 持续 |
| 用户影响 | ⚠️ 中 | ✅ 低 | ✅ 无 |

### 9.3 最终建议

**采用方案 B（可选 feature）**，理由：

- 在不破坏现有用户的前提下，优化默认依赖配置
- 为后续可能的完全移除（方案 A）做好铺垫
- 保持项目对不同部署环境（公共云 / 企业内网 / 容器）的适应性

---

## 附录

### A. 相关 Crate 链接

- [rustls-native-certs](https://crates.io/crates/rustls-native-certs) — 系统证书加载
- [webpki-roots](https://crates.io/crates/webpki-roots) — Mozilla 根证书
- [rustls](https://crates.io/crates/rustls) — TLS 实现
- [tokio-rustls](https://crates.io/crates/tokio-rustls) — 异步 TLS 适配

### B. 受影响文件清单

| 文件 | 需要修改的内容 |
|------|---------------|
| `Cargo.toml`（workspace） | 可保留声明，不影响 |
| `crates/rustfs-kafka/Cargo.toml` | 新增 `native-certs` feature，从 `security` 中移除 |
| `crates/rustfs-kafka-async/Cargo.toml` | `rustls-native-certs` 改为 optional，新增 feature |
| `crates/rustfs-kafka/src/tls/rustls_connector.rs` | 添加 `#[cfg(feature = "native-certs")]` 条件编译 |
| `crates/rustfs-kafka-async/src/connection.rs` | 添加 `#[cfg(feature = "native-certs")]` 条件编译 |

### C. Feature 组合矩阵

| security | security-ring | native-certs | 结果 |
|----------|--------------|--------------|------|
| ✅ | ❌ | ❌ | rustls + aws-lc-rs + webpki-roots（**新默认**） |
| ✅ | ❌ | ✅ | rustls + aws-lc-rs + webpki-roots + 系统证书 |
| ❌ | ✅ | ❌ | rustls + ring + webpki-roots |
| ❌ | ✅ | ✅ | rustls + ring + webpki-roots + 系统证书 |
| ❌ | ❌ | ❌ | 无 TLS 支持 |
