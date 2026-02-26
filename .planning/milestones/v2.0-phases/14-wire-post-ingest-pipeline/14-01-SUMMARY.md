---
phase: 14-wire-post-ingest-pipeline
plan: 01
subsystem: api
tags: [grpc, broadcast, tier3, llm, streaming, tonic]

# Dependency graph
requires:
  - phase: 12-production-interface
    provides: "KrabnetServer gRPC with SubscribeFrame broadcast channel and Tier3Worker+channel"
provides:
  - "FrameUpdate broadcast from ingest_event() to SubscribeFrame clients"
  - "Tier2Result dispatch from ingest_event() to Tier3Worker via try_send()"
  - "KrabnetServer with_wal_and_tier3() and with_tier3() constructors"
  - "Integration test proving end-to-end ingest -> broadcast + Tier 3 pipeline"
affects: [15-close-embryonic-gap, production-deployment]

# Tech tracking
tech-stack:
  added: []
  patterns: ["post-ingest broadcast loop over all frames", "non-blocking Tier3 dispatch via try_send"]

key-files:
  created: []
  modified: [src/grpc.rs, src/bin/krabnet-server.rs]

key-decisions:
  - "Broadcast ALL registered frames on every ingest (subscriber-side filtering handles frame_id matching)"
  - "Tier 3 results verified via shared results_handle Arc<Mutex<Vec>> instead of worker thread join (avoids hang from delayed sender drop)"
  - "Separate gRPC client connection for subscribe stream to avoid HTTP/2 stream interleaving issues in test"

patterns-established:
  - "Post-ingest broadcast: list_frames + query_frame loop with frame_tx.send and tier3_sender.try_send"
  - "Integration test pattern: separate subscribe client + tokio::time::timeout for streaming assertions"

requirements-completed: [GRPC-03, TIER3-01, TIER3-02, TIER3-03, TIER3-04]

# Metrics
duration: 16min
completed: 2026-02-26
---

# Phase 14 Plan 01: Wire Post-Ingest Pipeline Summary

**FrameUpdate broadcast and Tier3 LLM dispatch wired into gRPC ingest_event() with end-to-end integration test**

## Performance

- **Duration:** 16 min
- **Started:** 2026-02-26T12:03:48Z
- **Completed:** 2026-02-26T12:20:29Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- KrabnetServer now broadcasts FrameUpdate to all SubscribeFrame clients after every ingest_event()
- KrabnetServer now dispatches Tier2Results to Tier3Worker via non-blocking try_send() after every ingest_event()
- krabnet-server binary passes Tier3Sender into KrabnetServer (no longer discarded with underscore prefix)
- Integration test proves end-to-end: ingest -> SubscribeFrame receives FrameUpdate + Tier3Worker processes result

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire broadcast and Tier3Sender into KrabnetServer ingest path** - `f87a86c` (feat)
2. **Task 2: Integration test for end-to-end ingest broadcast and Tier 3 flow** - `12627d2` (test)

## Files Created/Modified
- `src/grpc.rs` - Added tier3_sender field, with_wal_and_tier3() and with_tier3() constructors, post-ingest broadcast+dispatch in ingest_event(), integration test
- `src/bin/krabnet-server.rs` - Wired tier3_sender into KrabnetServer (removed underscore discard, switched to with_wal_and_tier3())

## Decisions Made
- Broadcast ALL registered frames on every ingest event rather than only affected frames; the SubscribeFrame stream already filters by frame_id on the subscriber side, so this is correct and simple
- Verify Tier 3 results via shared Arc<Mutex<Vec<Tier3Interpretation>>> handle rather than joining the worker thread, because server abort does not immediately drop the Tier3Sender (tonic wraps the server in Arc)
- Use separate gRPC client connection for SubscribeFrame stream in integration test to avoid HTTP/2 stream interleaving where a single connection could serialize streaming and unary RPCs

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed test hang caused by worker thread join blocking**
- **Found during:** Task 2 (integration test)
- **Issue:** Plan's test design called for `worker_handle.join()` after `server_handle.abort()`, but aborting the tonic server task does not immediately drop the KrabnetServer (and its Tier3Sender) because tonic wraps the service in `Arc`. The worker thread blocks on `recv()` indefinitely.
- **Fix:** Changed test to verify Tier 3 results via the shared `results_handle()` directly (with a brief sleep for processing) instead of joining the worker thread. Worker handle is detached via `drop()`.
- **Files modified:** src/grpc.rs (test section)
- **Verification:** Test passes reliably in <1 second (was hanging indefinitely)
- **Committed in:** 12627d2

**2. [Rule 1 - Bug] Fixed test hang caused by HTTP/2 single-connection stream interleaving**
- **Found during:** Task 2 (integration test)
- **Issue:** Plan's test used a single gRPC client for both subscribe_frame (server-streaming) and ingest_event (unary). On a single HTTP/2 connection, the server-streaming response could block processing of subsequent unary RPCs.
- **Fix:** Used a separate `sub_client` connection for the subscribe_frame call, keeping the original `client` for unary RPCs. This ensures independent HTTP/2 connections.
- **Files modified:** src/grpc.rs (test section)
- **Verification:** Test passes reliably; ingest_event completes without blocking
- **Committed in:** 12627d2

---

**Total deviations:** 2 auto-fixed (2 bugs in test design)
**Impact on plan:** Both auto-fixes necessary for test correctness. No scope creep. Core functionality matches plan exactly.

## Issues Encountered
- cargo binary not on PATH in bash shell -- resolved by prepending `$HOME/.cargo/bin` to PATH
- Previous hanging test process locked the binary file (Permission denied on link) -- resolved by killing the process with taskkill

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- SubscribeFrame broadcast and Tier 3 dispatch are fully operational
- All 180 tests pass with zero regressions, zero clippy warnings, zero doc warnings
- Ready for Phase 15 (embryonic gap closure) or production deployment

## Self-Check: PASSED

- FOUND: src/grpc.rs (tier3_sender field, with_wal_and_tier3, with_tier3, frame_tx.send, try_send)
- FOUND: src/bin/krabnet-server.rs (tier3_sender wired into KrabnetServer)
- FOUND: .planning/phases/14-wire-post-ingest-pipeline/14-01-SUMMARY.md
- FOUND: commit f87a86c (feat: wire broadcast and Tier3Sender)
- FOUND: commit 12627d2 (test: integration test)
- FOUND: commit ae56a1f (docs: complete plan)
- All 180 tests pass, 53 doc-tests pass, 0 clippy warnings, 0 doc warnings

---
*Phase: 14-wire-post-ingest-pipeline*
*Completed: 2026-02-26*
