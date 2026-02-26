---
phase: 16-tech-debt-closure
plan: 01
subsystem: engine
tags: [llm, anthropic, grpc, mcp, wal, compaction, verification-tests]

# Dependency graph
requires:
  - phase: 15-harden-mcp-binary
    provides: MCP binary with engine integration, WAL module, gRPC server
provides:
  - AnthropicClient implementing LlmClient trait with ureq HTTP
  - AnthropicClient re-exported from krabnet::AnthropicClient
  - ANTHROPIC_API_KEY env var detection in krabnet-server binary
  - CompactionStats fields in gRPC GetStats and MCP krabnet_stats
  - WAL persistence and replay in krabnet-mcp binary
  - 5 verification tests covering DEBT-01 through DEBT-07
affects: [17-incremental-path-extension, 18-edge-removal, 19-node-removal]

# Tech tracking
tech-stack:
  added: [ureq]
  patterns: [env-var-based LLM client selection, optional compaction stats in proto, WAL replay on startup]

key-files:
  created: []
  modified:
    - src/tier3.rs
    - src/lib.rs
    - src/bin/krabnet-server.rs
    - src/grpc.rs
    - src/mcp.rs
    - src/bin/krabnet-mcp.rs
    - proto/krabnet.proto
    - Cargo.toml
    - Cargo.lock

key-decisions:
  - "Used ureq for sync HTTP to avoid native-tls/windows-sys conflicts on GNU toolchains"
  - "WAL fsync_interval=1 in tests for deterministic flush behavior"
  - "Epoch assertions use relative offsets (e0+1, e0+2) since engine starts at epoch 0"

patterns-established:
  - "Env-var detection pattern: ANTHROPIC_API_KEY for AnthropicClient, fallback to MockLlmClient"
  - "Optional compaction stats: fields are None when no compaction worker, Some(0) when fresh"

requirements-completed: [DEBT-01, DEBT-02, DEBT-03, DEBT-04, DEBT-05, DEBT-06, DEBT-07]

# Metrics
duration: 5min
completed: 2026-02-26
---

# Phase 16 Plan 01: Tech Debt Closure Summary

**Verification tests for 7 DEBT requirements proving AnthropicClient LlmClient impl, gRPC/MCP CompactionStats fields, and MCP WAL persistence with replay**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-26T15:45:36Z
- **Completed:** 2026-02-26T15:50:17Z
- **Tasks:** 3
- **Files modified:** 9

## Accomplishments
- 5 new verification tests covering all 7 DEBT requirements (DEBT-01 through DEBT-07)
- Full test suite passes: 185 lib tests, 54 doc-tests, zero clippy warnings
- All tech debt code (677 lines across 10 files) committed with DEBT-XX traceability

## Task Commits

Each task was committed atomically:

1. **Task 1: Verification tests for AnthropicClient and gRPC CompactionStats** - `3807328` (test)
2. **Task 2: Verification tests for MCP CompactionStats and WAL persistence** - `58531ae` (test)
3. **Task 3: Full test suite verification and commit** - `651d8c0` + `e39f3ac` (feat)

## Files Created/Modified
- `src/tier3.rs` - Added AnthropicClient struct + 2 verification tests (DEBT-01, DEBT-02, DEBT-07)
- `src/lib.rs` - Added AnthropicClient to public re-exports (DEBT-07)
- `src/bin/krabnet-server.rs` - ANTHROPIC_API_KEY env var detection with AnthropicClient fallback (DEBT-02)
- `src/grpc.rs` - CompactionStats in GetStats response + verification test (DEBT-03)
- `src/mcp.rs` - CompactionStats in krabnet_stats + WAL persistence tests (DEBT-04, DEBT-05, DEBT-06)
- `src/bin/krabnet-mcp.rs` - WAL replay on startup + live WAL persistence (DEBT-05, DEBT-06)
- `proto/krabnet.proto` - 4 optional compaction fields in GetStatsResponse (DEBT-03)
- `Cargo.toml` - ureq dependency for sync HTTP (DEBT-01)
- `Cargo.lock` - Updated lockfile

## Decisions Made
- Used ureq for sync HTTP to avoid native-tls/windows-sys conflicts on GNU toolchains
- WAL test uses fsync_interval=1 for deterministic flush on each event append
- Epoch assertions use relative offsets since engine assigns epochs starting from 0

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed epoch assertion values in WAL test**
- **Found during:** Task 2 (MCP WAL persistence test)
- **Issue:** Plan assumed epochs start at 1, but engine assigns epochs starting at 0 (from ring buffer push)
- **Fix:** Changed assertions from absolute values (1, 2, 3) to relative offsets (e0, e0+1, e0+2)
- **Files modified:** src/mcp.rs
- **Verification:** test_mcp_wal_persistence_and_replay passes
- **Committed in:** 58531ae (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug fix)
**Impact on plan:** Necessary correction for test correctness. No scope creep.

## Issues Encountered
None beyond the epoch offset fix documented above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All DEBT requirements verified with tests, tech debt is closed
- Engine is fully tested (185 lib tests, 54 doc-tests) and ready for incremental path extension work in Phase 17
- AnthropicClient is available for production use with ANTHROPIC_API_KEY env var

## Self-Check: PASSED

All 9 modified files verified present. All 4 commits verified in git log.

---
*Phase: 16-tech-debt-closure*
*Completed: 2026-02-26*
