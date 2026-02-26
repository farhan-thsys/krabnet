# Phase 16: Tech Debt Closure - Research

**Researched:** 2026-02-26
**Domain:** Rust crate internals -- LLM integration, gRPC/MCP stats, WAL persistence, public API exports
**Confidence:** HIGH

## Summary

Phase 16 is a **commit-and-verify** phase. All seven DEBT requirements (DEBT-01 through DEBT-07) have already been implemented as uncommitted working-tree changes. The git diff shows 677 lines added across 10 files: `tier3.rs` (AnthropicClient), `lib.rs` (re-export), `krabnet-server.rs` (env var detection), `grpc.rs` (CompactionStats in GetStats), `mcp.rs` (CompactionStats in krabnet_stats + WAL persistence), `krabnet-mcp.rs` (WAL replay + live persistence), `krabnet.proto` (4 new optional fields), `Cargo.toml` (ureq dependency), and `Cargo.lock`.

The implementation follows the project's established patterns exactly: synchronous `LlmClient` trait with `ureq` HTTP, optional `CompactionStats` fields in protobuf, `WalWriter`/`WalReader` integration in MCP binary matching the existing krabnet-server pattern, and `AnthropicClient` added to `lib.rs` public re-exports. No architectural changes are needed -- the code is written and needs to be compiled, tested, and committed in logical groups.

**Primary recommendation:** Verify the code compiles (`cargo build`), all existing tests pass (`cargo test`), write targeted verification tests for each DEBT item, then commit in logical groupings that align with the 7 requirements.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| DEBT-01 | AnthropicClient implements LlmClient trait using ureq HTTP for real Tier 3 LLM interpretation | Code exists in `src/tier3.rs` lines 115-195. Uses `ureq` 2.x with `serde_json` for Anthropic Messages API. Implements `LlmClient::interpret()` trait method. |
| DEBT-02 | krabnet-server auto-detects ANTHROPIC_API_KEY env var and uses AnthropicClient when available, falls back to MockLlmClient with warning | Code exists in `src/bin/krabnet-server.rs` lines 54-70. Checks `std::env::var("ANTHROPIC_API_KEY")`, supports `KRABNET_LLM_MODEL` and `KRABNET_LLM_MAX_TOKENS` env vars. |
| DEBT-03 | CompactionStats exposed via gRPC GetStats response | Code exists in `src/grpc.rs` get_stats() method and `proto/krabnet.proto` fields 10-13. Uses optional uint64 protobuf fields. |
| DEBT-04 | CompactionStats exposed via MCP krabnet_stats tool response | Code exists in `src/mcp.rs` tool_stats() method. Inserts compaction fields into JSON response when `engine.compaction_stats()` returns `Some`. |
| DEBT-05 | MCP binary supports WAL persistence with crash recovery replay on startup | Code exists in `src/bin/krabnet-mcp.rs`. Replays WAL via `WalReader::replay()` on startup, same pattern as krabnet-server. |
| DEBT-06 | MCP binary persists ingest events to WAL during live operation | Code exists in `src/mcp.rs` tool_ingest() method and `src/bin/krabnet-mcp.rs`. `McpServer::with_wal()` constructor and WAL append in ingest path. |
| DEBT-07 | AnthropicClient exported from lib.rs public API | Code exists in `src/lib.rs` line 77. Changed re-export to include `AnthropicClient`. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| ureq | 2.x | Synchronous HTTP for Anthropic API | Already added to Cargo.toml; chosen over reqwest to avoid native-tls/windows-sys conflicts on GNU toolchain |
| tonic | 0.12 | gRPC server with protobuf codegen | Existing project dependency, GetStats RPC already defined |
| serde_json | 1.x | JSON serialization for MCP responses and Anthropic API | Existing project dependency |
| prost | 0.13 | Protobuf message types (generated) | Existing project dependency via tonic-build |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| crossbeam | 0.8 | Bounded channel for Tier3Worker | Already used for Tier 3 pipeline |
| tokio | 1.38.1 | Async runtime for gRPC server binary | Already used by krabnet-server |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ureq (sync HTTP) | reqwest (async HTTP) | ureq avoids native-tls build issues on GNU toolchain; sync is fine since Tier3Worker runs in dedicated thread |

**No new installation needed** -- ureq is already in Cargo.toml as an uncommitted change.

## Architecture Patterns

### Existing Project Structure (unchanged)
```
src/
  tier3.rs           # LlmClient trait + MockLlmClient + AnthropicClient (DEBT-01)
  lib.rs             # Public re-exports including AnthropicClient (DEBT-07)
  grpc.rs            # gRPC service with CompactionStats in GetStats (DEBT-03)
  mcp.rs             # MCP JSON-RPC server with CompactionStats + WAL (DEBT-04, DEBT-06)
  bin/
    krabnet-server.rs  # ANTHROPIC_API_KEY detection (DEBT-02)
    krabnet-mcp.rs     # WAL replay + persistence (DEBT-05, DEBT-06)
  compaction.rs      # CompactionStats struct (already committed, used by DEBT-03/04)
  wal.rs             # WalWriter/WalReader (already committed, used by DEBT-05/06)
  engine.rs          # Engine with compaction_stats() method (already committed)
proto/
  krabnet.proto      # GetStatsResponse with 4 new optional compaction fields (DEBT-03)
```

### Pattern 1: Trait-Based LLM Abstraction
**What:** `LlmClient` trait with `interpret(&self, prompt: &str) -> Result<String, String>` method, implemented by both `MockLlmClient` and `AnthropicClient`.
**When to use:** Any code needing LLM access uses `Box<dyn LlmClient>` -- production gets `AnthropicClient`, tests get `MockLlmClient`.
**Example:**
```rust
// Source: src/tier3.rs (already in working tree)
pub trait LlmClient: Send + Sync {
    fn interpret(&self, prompt: &str) -> Result<String, String>;
}

// Production: AnthropicClient uses ureq for sync HTTP
// Test: MockLlmClient returns configurable responses
```

### Pattern 2: Env-Var Driven Feature Detection
**What:** Binary checks environment variable at startup, constructs appropriate implementation, logs which mode is active.
**When to use:** Optional production features that need graceful degradation.
**Example:**
```rust
// Source: src/bin/krabnet-server.rs (already in working tree)
let llm_client: Box<dyn LlmClient> =
    if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
        eprintln!("Tier 3: using AnthropicClient (model={}, max_tokens={})", model, max_tokens);
        Box::new(AnthropicClient::new(api_key, model, max_tokens))
    } else {
        eprintln!("WARNING: ANTHROPIC_API_KEY not set, using MockLlmClient");
        Box::new(MockLlmClient::new(vec![]))
    };
```

### Pattern 3: Optional Stats Extension
**What:** Protobuf uses `optional` fields for stats that may not exist (e.g., compaction disabled). Rust code checks `Option<CompactionStats>` and populates fields conditionally.
**When to use:** Stats that depend on optional engine features (compaction worker may not be configured).
**Example:**
```rust
// Source: src/grpc.rs get_stats() (already in working tree)
if let Some(cs) = compaction {
    resp.compactions_completed = Some(cs.compactions_completed);
    resp.compaction_tuples_before = Some(cs.tuples_before);
    resp.compaction_tuples_after = Some(cs.tuples_after);
    resp.total_compaction_time_us = Some(cs.total_compaction_time_us);
}
```

### Pattern 4: WAL Integration in MCP Binary
**What:** Same WAL pattern as krabnet-server: replay on startup, write on ingest, flush on exit.
**When to use:** Any binary that needs crash recovery durability.
**Example:**
```rust
// Source: src/bin/krabnet-mcp.rs (already in working tree)
// Startup: replay existing WAL
if wal_path.exists() {
    match WalReader::replay(wal_path) { /* ... */ }
}
// Live: create McpServer::with_wal() for WAL persistence on ingest
// Exit: flush WAL at end of run() loop
```

### Anti-Patterns to Avoid
- **Committing all changes in one monolithic commit:** Each DEBT requirement should be traceable. Group logically but ensure each commit message references specific DEBT-XX IDs.
- **Skipping compilation check before committing:** The ureq dependency addition and proto changes must compile cleanly together. Build before first commit.
- **Testing AnthropicClient with real API calls:** Use MockLlmClient for unit tests. AnthropicClient's correctness is verified structurally (implements trait, constructs valid HTTP request).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| HTTP client for Anthropic API | Custom TCP/TLS code | ureq 2.x | Already chosen and implemented; handles TLS, JSON, error codes |
| Protobuf optional fields | Manual presence tracking | proto3 `optional` keyword | prost generates `Option<T>` automatically |
| WAL binary format | New serialization format | Existing `WalWriter`/`WalReader` | Already battle-tested with 1000-event roundtrip test |
| Compaction stats aggregation | Manual counters | `CompactionWorker::stats()` | Already tracks all 4 metrics internally |

**Key insight:** All implementation work is done. The "don't hand-roll" guidance for this phase is: don't rewrite anything. The code follows existing patterns and uses existing infrastructure. Commit what exists.

## Common Pitfalls

### Pitfall 1: Build Order -- Proto Before Rust
**What goes wrong:** If proto changes aren't compiled before Rust changes reference new fields, the build fails.
**Why it happens:** tonic-build generates Rust types from `.proto` at build time. New fields like `compactions_completed` only exist after code generation.
**How to avoid:** Commit proto and Cargo.toml changes first (or together with the code that uses them). Always `cargo build` before committing.
**Warning signs:** "field not found" compiler errors referencing proto-generated types.

### Pitfall 2: ureq Feature Flags
**What goes wrong:** ureq with `native-tls` feature may fail on some systems. The `json` feature is needed for `.send_json()` and `.into_json()`.
**Why it happens:** TLS backend configuration varies by platform.
**How to avoid:** The Cargo.toml already specifies `features = ["json", "native-tls", "gzip"]` with `default-features = false`. Don't change this.
**Warning signs:** Link errors mentioning `openssl`, `schannel`, or `security-framework`.

### Pitfall 3: Event::clone() Required for WAL Persistence
**What goes wrong:** MCP ingest needs to both pass the event to `engine.ingest()` and `wal.append()`.
**Why it happens:** `engine.ingest()` takes ownership; WAL needs a reference.
**How to avoid:** The code already uses `event.clone()` before passing to engine, then references the clone for WAL. Verify Event derives Clone (it does -- checked in types.rs).
**Warning signs:** "value used after move" compiler errors.

### Pitfall 4: Test Isolation for WAL-Enabled MCP Tests
**What goes wrong:** Tests that create WAL files can leave artifacts that affect other tests or future runs.
**Why it happens:** WAL creates files on disk; tests need cleanup.
**How to avoid:** Use `std::env::temp_dir()` with unique paths per test, clean up in test. Follow the pattern in `wal.rs` tests.
**Warning signs:** Flaky tests that pass individually but fail in parallel.

### Pitfall 5: Forgetting to Re-Export AnthropicClient
**What goes wrong:** Downstream code can't use `krabnet::AnthropicClient` even though it exists in tier3.rs.
**Why it happens:** `lib.rs` re-exports must be explicitly updated.
**How to avoid:** Already handled -- `lib.rs` diff adds `AnthropicClient` to the `pub use tier3::` line. Verify with a simple `use krabnet::AnthropicClient;` in a test.
**Warning signs:** "not found in `krabnet`" compiler error from downstream.

## Code Examples

All code examples below are **already implemented** in the working tree. These are provided for the planner to reference when writing verification steps.

### AnthropicClient Construction and Trait Implementation
```rust
// Source: src/tier3.rs lines 133-195 (in working tree)
pub struct AnthropicClient {
    agent: ureq::Agent,
    api_key: String,
    model: String,
    max_tokens: u32,
}

impl AnthropicClient {
    pub fn new(api_key: String, model: String, max_tokens: u32) -> Self {
        Self { agent: ureq::Agent::new(), api_key, model, max_tokens }
    }
}

impl LlmClient for AnthropicClient {
    fn interpret(&self, prompt: &str) -> Result<String, String> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [{ "role": "user", "content": prompt }]
        });
        // POST to https://api.anthropic.com/v1/messages
        // Headers: x-api-key, anthropic-version, content-type
        // Parse response["content"][0]["text"]
    }
}
```

### ANTHROPIC_API_KEY Detection in Server Binary
```rust
// Source: src/bin/krabnet-server.rs lines 54-70 (in working tree)
let llm_client: Box<dyn krabnet::tier3::LlmClient> =
    if let Ok(api_key) = std::env::var("ANTHROPIC_API_KEY") {
        let model = std::env::var("KRABNET_LLM_MODEL")
            .unwrap_or_else(|_| "claude-sonnet-4-6".to_string());
        let max_tokens: u32 = std::env::var("KRABNET_LLM_MAX_TOKENS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        eprintln!("Tier 3: using AnthropicClient (model={}, max_tokens={})", model, max_tokens);
        Box::new(AnthropicClient::new(api_key, model, max_tokens))
    } else {
        eprintln!("WARNING: ANTHROPIC_API_KEY not set, using MockLlmClient");
        Box::new(MockLlmClient::new(vec![]))
    };
```

### CompactionStats in Protobuf
```protobuf
// Source: proto/krabnet.proto fields 10-13 (in working tree)
message GetStatsResponse {
  // ... existing fields 1-9 ...
  optional uint64 compactions_completed    = 10;
  optional uint64 compaction_tuples_before = 11;
  optional uint64 compaction_tuples_after  = 12;
  optional uint64 total_compaction_time_us = 13;
}
```

### MCP WAL Integration
```rust
// Source: src/bin/krabnet-mcp.rs (in working tree)
// Replay on startup:
if wal_path.exists() {
    match WalReader::replay(wal_path) {
        Ok(entries) => { for (_epoch, event) in entries { engine.ingest(event); } }
        Err(e) => { eprintln!("WAL replay error (starting fresh): {}", e); }
    }
}
// Live persistence via McpServer::with_wal():
let mut server = match WalWriter::new(wal_path, 1000) {
    Ok(wal_writer) => McpServer::with_wal(engine, wal_writer),
    Err(e) => { eprintln!("WARNING: ..."); McpServer::new(engine) }
};
```

### lib.rs Public Export
```rust
// Source: src/lib.rs line 77 (in working tree)
pub use tier3::{AnthropicClient, LlmClient, MockLlmClient, Tier3Worker};
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| MockLlmClient only | AnthropicClient + MockLlmClient | v3.0 (this phase) | Real Tier 3 LLM interpretation now possible |
| No MCP WAL | MCP WAL persistence + replay | v3.0 (this phase) | MCP binary now crash-recoverable |
| GetStats without compaction | GetStats with compaction metrics | v3.0 (this phase) | Operators can monitor compaction health |

**No deprecated items** -- all changes are additive.

## Open Questions

1. **Compilation verification on Windows GNU toolchain**
   - What we know: ureq was specifically chosen to avoid native-tls/windows-sys conflicts. The `native-tls` feature is enabled.
   - What's unclear: Whether the current environment can run `cargo build` and `cargo test` (the bash shell doesn't have cargo on PATH).
   - Recommendation: The planner should include a compilation step as the first task. If `cargo` is available via a different path or shell, it should be used to verify the build. The executor may need to use a Windows-native shell or set up PATH correctly.

2. **Test coverage for AnthropicClient**
   - What we know: AnthropicClient implements LlmClient trait. Real API calls require a valid API key.
   - What's unclear: Whether there should be a unit test that verifies AnthropicClient constructs valid request bodies without actually calling the API.
   - Recommendation: Add a test that creates an AnthropicClient, verifies it implements LlmClient (compile-time check via trait bounds), and optionally verifies the JSON body construction. Do NOT test with real API calls.

3. **MCP WAL test coverage**
   - What we know: WAL roundtrip and crash recovery are already tested in `wal.rs`. MCP ingest-and-query is tested in `mcp.rs`.
   - What's unclear: Whether there's an integration test for MCP with WAL enabled (ingest via MCP, then replay and verify).
   - Recommendation: Add a test that creates an `McpServer::with_wal()`, ingests events via `handle_request()`, drops the server, replays the WAL, and verifies events are recovered.

## Verification Strategy

Since this is a commit-and-verify phase with pre-built code, verification is the primary activity:

### Build Verification
1. `cargo build` must succeed (verifies ureq dependency, proto codegen, all Rust changes compile)
2. `cargo build --bin krabnet-server` must succeed
3. `cargo build --bin krabnet-mcp` must succeed

### Test Verification
1. All existing 180 lib tests must pass: `cargo test --lib`
2. All existing 54 doc-tests must pass: `cargo test --doc`
3. Zero clippy warnings: `cargo clippy`

### Per-Requirement Verification
| DEBT ID | Verification Method |
|---------|--------------------|
| DEBT-01 | Compile check: `AnthropicClient` implements `LlmClient` trait. Structural test: creates client, verifies `interpret` method exists and returns `Result<String, String>`. |
| DEBT-02 | Compile check: krabnet-server binary uses `std::env::var("ANTHROPIC_API_KEY")`. Code inspection of log messages for both branches. |
| DEBT-03 | Existing gRPC test (test_grpc_ingest_and_query) calls GetStats. New assertion: verify compaction fields are present when engine has compaction worker. |
| DEBT-04 | MCP test calling `tool_stats()` on an engine with compaction worker configured, asserting compaction fields in JSON response. |
| DEBT-05 | Integration test: create MCP server with WAL, ingest events, verify WAL file exists and contains entries via WalReader::replay(). |
| DEBT-06 | Same as DEBT-05 -- the WAL append happens during live ingest. |
| DEBT-07 | Compile check: `use krabnet::AnthropicClient;` compiles. Already verified by `lib.rs` re-export line. |

## Sources

### Primary (HIGH confidence)
- **Working tree diff analysis** (`git diff --stat`, `git diff` per file) -- direct inspection of all 677 added lines
- **Source code inspection** -- read all 10 modified files in full
- **Proto file analysis** -- verified 4 new optional fields in GetStatsResponse
- **Cargo.toml analysis** -- verified ureq dependency specification

### Secondary (MEDIUM confidence)
- **PROJECT.md Key Decisions table** -- documents ureq choice rationale (avoid windows-sys/dlltool issues)
- **STATE.md accumulated context** -- confirms "Phase 16 tech debt code is already built (uncommitted)"

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all dependencies already in Cargo.toml, code already written and readable
- Architecture: HIGH -- follows exact patterns established in phases 12-15, no new architectural decisions
- Pitfalls: HIGH -- identified from direct code inspection and known Rust compilation patterns

**Research date:** 2026-02-26
**Valid until:** 2026-03-28 (30 days -- stable, no external dependencies changing)
