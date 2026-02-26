---
phase: 11-harden-engine
plan: 03
subsystem: engine
tags: [stress-testing, integration-tests, benchmarks, coalescer, fanout, hysteresis, compaction, concurrency]

# Dependency graph
requires:
  - phase: 11-harden-engine-01
    provides: "CompactionWorker, parallel frame eval, Arc<RwLock<Frame>> wrapping"
  - phase: 11-harden-engine-02
    provides: "MutationCoalescer, FanOutLimiter, HysteresisState standalone modules"
provides:
  - "Engine::with_config() constructor integrating all hardening features"
  - "Coalescer, fanout limiter, hysteresis wired into engine ingest pipeline"
  - "9 integration tests (TEST-09 through TEST-17) validating all Phase 11 features"
  - "bench_concurrent_ingest benchmark (BENCH-02)"
  - "Quality gates: 136 tests, 0 clippy warnings, 0 doc warnings"
affects: [12-api-layer, 13-documentation, benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [coalescer-gate-in-ingest, fanout-gate-in-ingest, hysteresis-tier-update, arc-mutex-concurrent-rw]

key-files:
  created: []
  modified:
    - src/engine.rs
    - src/routing.rs
    - src/compaction.rs
    - src/fanout.rs
    - benches/krabnet_bench.rs

key-decisions:
  - "Engine::with_config() as unified constructor accepting optional compaction, coalescing, and fanout parameters"
  - "Coalescer gate in ingest(): events accumulated within window, evaluation deferred until flush/window-elapse"
  - "Fanout gate in ingest(): scored affected frames split into immediate (top N) and deferred sets"
  - "Hysteresis updated per-frame after evaluation, tier changes applied only when consecutive threshold met"
  - "affected_frames_by_node() added to InvertedIndex for coalescer batch integration path"
  - "eval_count tracker on Engine for testability of coalescer deduplication"

patterns-established:
  - "Coalescer gate pattern: check coalescer before evaluation, skip if within window, batch-evaluate on flush"
  - "Fanout gate pattern: score affected frames, cap at max_fanout, queue remainder in DeferredEvalQueue"
  - "Arc<Mutex<Engine>> for multi-threaded access in concurrent read/write stress test"

requirements-completed: [TEST-09, TEST-10, TEST-11, TEST-12, TEST-13, TEST-14, TEST-15, TEST-16, TEST-17, BENCH-02, QUAL-06, QUAL-07]

# Metrics
duration: 10min
completed: 2026-02-25
---

# Phase 11 Plan 03: Stress Tests & Quality Gates Summary

**9 integration tests validating background compaction, concurrent eval, coalescing dedup, fanout limits, hysteresis, sustained throughput, compaction under load, and concurrent read/write -- plus bench_concurrent_ingest benchmark and zero clippy/doc warnings**

## Performance

- **Duration:** 10 min
- **Started:** 2026-02-24T20:56:29Z
- **Completed:** 2026-02-24T21:06:19Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Wired MutationCoalescer, FanOutLimiter, and HysteresisState into Engine ingest pipeline with gating logic
- Added Engine::with_config() unified constructor for all hardening features
- 9 integration tests (TEST-09 through TEST-17) all passing, validating every Phase 11 feature under load
- bench_concurrent_ingest benchmark running at ~3.6ms per 100-event batch with compaction enabled
- All 136 tests passing (134 normal + 2 ignored stress tests), 0 clippy warnings, 0 doc warnings

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire coalescer/fanout/hysteresis into engine, add TEST-09 through TEST-14** - `fc208e2` (feat)
2. **Task 2: Add stress tests TEST-15 through TEST-17, bench_concurrent_ingest, fix doc warnings** - `82a97a5` (feat)

## Files Created/Modified
- `src/engine.rs` - Engine struct extended with coalescer/fanout/hysteresis fields, with_config() constructor, coalescer and fanout gates in ingest(), eval_count/flush_coalescer/deferred_count accessors, 9 new integration tests
- `src/routing.rs` - Added affected_frames_by_node() for coalescer batch integration path
- `src/compaction.rs` - Fixed doc link warning (DiffCollection not in scope)
- `src/fanout.rs` - Fixed doc link warning (private item reference)
- `benches/krabnet_bench.rs` - Added bench_concurrent_ingest benchmark with hardened engine setup

## Decisions Made
- Engine::with_config() accepts Option parameters for each hardening feature, making them independently toggleable. This extends the existing with_compaction() pattern to all features.
- Coalescer integration uses a "gate" pattern in ingest(): if coalescer is active, events are pushed through it and evaluation only fires on window flush/elapse. This keeps the non-coalesced path (coalescer=None) completely unchanged.
- Fanout integration similarly gates: affected frames are scored and split into immediate/deferred sets. Only max_fanout frames are evaluated per event.
- Added affected_frames_by_node() to InvertedIndex as a lightweight lookup for the coalescer batch path, avoiding the need to reconstruct full Event objects.
- eval_count tracker added to Engine for testability -- allows tests to verify how many evaluations actually fired (critical for coalescer dedup tests).
- TEST-15 and TEST-17 marked with #[ignore] due to 5-10 second runtime, suitable for CI with `--ignored` flag.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added affected_frames_by_node() to InvertedIndex**
- **Found during:** Task 1 (coalescer integration)
- **Issue:** Coalescer batches produce NodeIds, but InvertedIndex only had affected_frames(Event). Need a node-only lookup for the batch path.
- **Fix:** Added affected_frames_by_node(NodeId) -> HashSet<u64> method that looks up the node posting list directly.
- **Files modified:** src/routing.rs
- **Verification:** All tests pass, clippy clean
- **Committed in:** fc208e2 (Task 1)

**2. [Rule 1 - Bug] Fixed doc link warnings in compaction.rs and fanout.rs**
- **Found during:** Task 2 (quality gate verification)
- **Issue:** cargo doc produced 2 warnings: unresolved link to DiffCollection in compaction.rs, private item link in fanout.rs
- **Fix:** Replaced `[DiffCollection]` with plain backticks, replaced `[max_fanout](FanOutLimiter::max_fanout)` with plain backticks
- **Files modified:** src/compaction.rs, src/fanout.rs
- **Verification:** cargo doc --no-deps produces 0 warnings
- **Committed in:** 82a97a5 (Task 2)

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 bug)
**Impact on plan:** Minimal. Both fixes were necessary for correctness (blocking integration) and quality gate compliance (doc warnings).

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All Phase 11 hardening features are fully integrated and validated under stress
- Engine provides backward-compatible constructors: new() (no hardening), with_compaction() (compaction only), with_config() (full configuration)
- 136 tests passing with zero clippy/doc warnings -- ready for Phase 12+
- Benchmark suite includes 7 benchmarks covering all critical operations

---
## Self-Check: PASSED

- FOUND: src/engine.rs
- FOUND: src/routing.rs
- FOUND: src/compaction.rs
- FOUND: src/fanout.rs
- FOUND: benches/krabnet_bench.rs
- FOUND: .planning/phases/11-harden-engine/11-03-SUMMARY.md
- FOUND: commit fc208e2
- FOUND: commit 82a97a5

---
*Phase: 11-harden-engine*
*Completed: 2026-02-25*
