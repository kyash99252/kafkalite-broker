# AGENTS.md — Repository Guide for AI Coding Agents

This file defines the architecture, invariants, rules, and verification steps for maintaining and extending this repository. Always read `docs/srs/SRS.md` and relevant files in `docs/architecture/` and `docs/adr/` before making structural changes.

---

## 1. Project Identity & Scope

- **Project:** Kafka-Inspired Durable Message Broker (single-node, Java, Docker-compatible).
- **Primary Goal:** Correctness, crash safety, clean architecture, and explainability over premature complexity or production-scale features.
- **Explicit Non-Goals (Do NOT implement unless SRS explicitly changes):**
  - Multi-broker clustering, replication, leader election, or Raft/consensus.
  - Kubernetes manifests or distributed orchestration.
  - Log compaction or automated TTL/size retention deletion.
  - Exactly-once processing semantics.
  - Enterprise auth/TLS, schema registry, or graphical dashboards.
  - Heavy application frameworks (e.g., Spring Boot) — prefer standard Java libraries (`java.nio`, `java.util.concurrent`) and minimal, well-justified dependencies.

---

## 2. Versioning & Tooling Policy

- Target the **latest stable** releases of Java, Docker, Docker Compose, and build/test dependencies.
- **Do NOT hardcode or pin explicit version numbers in documentation (`SRS.md`, `README.md`, `docs/`)** unless explicitly requested.

---

## 3. Repository Structure & Dependency Boundaries

```text
broker-protocol/   -> Wire models, binary codecs, framing, CRC helpers, error codes (NO storage/network logic)
broker-core/       -> Broker lifecycle, storage engine, topic/partition manager, group coordinator, TCP server, metrics
broker-client/     -> Producer, Consumer, and Admin Java SDKs + transport client
broker-e2e-tests/  -> Integration, concurrency, and crash-recovery acceptance tests (AC-01..AC-12)
broker-benchmarks/ -> Repeatable throughput, latency, and recovery benchmarks
docs/              -> SRS, architecture specs, ADRs, benchmark reports
config/            -> Default broker properties and logging config
scripts/           -> Startup, log-inspection, benchmark, and Docker crash-test scripts
```

### Strict Module & Package Dependency Rules

- `broker-protocol` must have zero dependencies on `broker-core` or `broker-client`.
- `broker-client` must never depend on `broker-core` (all client-broker interaction must go through `broker-protocol` and network/external interfaces).
- Inside `broker-core`:
  - `storage/` must not import `network/` or `group/`.
  - `topic/` may use `storage/`, not `network/`.
  - `network/` dispatches to `topic/`, `group/`, `offset/`, and `observability/`, without containing persistence logic itself.

---

## 4. Core Engineering Invariants (Never Violate)

### Storage & Crash Recovery (`broker-core/storage`)

- **Append-only discipline:** Never mutate committed log records in place.
- **Record integrity:** Every persisted record/batch must include framing length and a checksum. Never trust length headers blindly on recovery—guard against negative, zero, or out-of-bounds lengths caused by partial writes.
- **Tail-only truncation:** Crash recovery may validate and truncate incomplete/corrupted trailing bytes, but must never silently discard valid preceding records.
- **Durability honesty:** Respect the configured flush policy (`EVERY_WRITE`, `INTERVAL`, `OS_DEFAULT`). Never claim durability guarantees in comments or docs that skip `FileChannel.force(...)`.

### Concurrency & Ordering

- Offsets within a partition must be strictly monotonic and gap-free for acknowledged writes.
- Partition appends must be serialized per partition without blocking unrelated partitions.
- Never use raw `Thread.sleep(...)` in production coordination logic or tests where deterministic synchronization, latches, or an injected `java.time.Clock` can be used.

### Error Handling & Observability

- Return structured protocol error codes (`broker-protocol`) to clients; never expose raw stack traces or internal file paths over the wire.
- Never swallow `IOException`, checksum failures, or `InterruptedException` silently (`Thread.currentThread().interrupt()` must be preserved when catching `InterruptedException`).

### Docker & Runtime Compatibility

- All persistent paths must derive from the configurable storage directory (`DATA_DIR` / broker config), never hardcoded host paths or relative paths inside ephemeral container layers.
- All key runtime parameters (port, data dir, flush policy, session timeout) must be configurable via environment variables as well as config files.
- `SIGTERM` must trigger graceful shutdown and flushing according to the active persistence policy.

---

## 5. Documentation Synchronization Rules

- **Binary Format Changes:** Any change to `.log` / `.index` layout or TCP wire framing MUST include updates to `docs/architecture/storage-format.md` or `docs/architecture/wire-protocol.md` and corresponding round-trip/corruption tests.
- **Architectural Choices:** Major design decisions (e.g., NIO vs Netty, segment size, offset store format, flush scheduler) must be recorded as a concise ADR under `docs/adr/`.
- **Requirements Traceability:** Tests verifying acceptance criteria must be tagged or named clearly against `AC-01` through `AC-12` in `docs/srs/SRS.md`.

---

## 6. Verification Checklist Before Completing Any Task

Before considering a change complete, agents MUST:

1. Run formatting/static checks and unit tests for modified modules.
2. Run `broker-e2e-tests` whenever changes touch `broker-protocol`, `storage/`, `group/`, `network/`, or `lifecycle/`.
3. Verify Docker build and container restart recovery (`scripts/docker-crash-test.sh`) whenever changes touch lifecycle, config/env loading, storage paths, or Dockerfiles.
4. Confirm no new dependencies violate Section 1 (Non-Goals) or Section 3 (Boundaries).

*(Update exact Gradle/Maven commands below once build files are scaffolded):*

- **Build & unit test:** `<build-tool> test`
- **Full verification (incl. E2E & architecture checks):** `<build-tool> check`
- **Docker build & smoke test:** `docker compose up --build -d && ./scripts/docker-crash-test.sh`