---
phase: 09-engine-orchestration
plan: 01
subsystem: engine
tags: [engine, orchestrator, pipeline, ingest, frames, embryonic, differential, graph]

# Dependency graph
requires:
  - phase: 02-epoch-sequencer-and-ring-buffer
    provides: RingBuffer for event ingestion with epoch assignment
  - phase: 03-property-graph-storage
    provides: Graph for mutation application
  - phase: 04-differential-mvcc-engine
    provides: DiffCollection for frame state storage
  - phase: 05-frame-materialization
    provides: Frame for parked traverser materialization
  - phase: 06-signal-routing
    provides: InvertedIndex for O(affected) event routing
  - phase: 07-interpretation-and-adaptive-tiering
    provides: tier1_check for binary delta interpretation, TierConfig
  - phase: 08-embryonic-frame-discovery
    provides: EmbryonicDiscovery for auto-promotion of candidates
provides:
  - Engine struct wiring all components into a single ingest pipeline
  - EngineStats for aggregate component statistics
  - Full ingest-update-maintain-interpret pipeline
  - Frame registration with automatic materialization and index registration
  - Embryonic auto-promotion integration
  - Compact-all and temporal snapshot operations
affects: [10-benchmarks-and-quality]

# Tech tracking
tech-stack:
  added: []
  patterns: [engine-orchestrator, pipeline-pattern, component-wiring]

key-files:
  created: [src/engine.rs]
  modified: [src/lib.rs, src/frame.rs]

key-decisions:
  - "Frame.tuple_count() accessor added to Frame for engine stats aggregation"
  - "Index registration uses NodeIds from materialized paths only (no edge keys for simplicity)"
  - "Auto-promoted frames get materialized immediately and registered in inverted index"
  - "tier_config retained as field with allow(dead_code) for future external scoring use"

patterns-established:
  - "Engine owns all components and provides single-entry-point ingest pipeline"
  - "Frame registration extracts node IDs from materialized paths for index registration"
  - "Embryonic auto-promotion creates Frame, materializes, registers in index in one step"

requirements-completed: [ENGINE-01, ENGINE-02, ENGINE-03, ENGINE-04, ENGINE-05, TEST-07, TEST-08]

# Metrics
duration: 3min
completed: 2026-02-24
---

# Phase 9 Plan 1: Engine Orchestration Summary

**Engine struct wiring RingBuffer, Graph, InvertedIndex, Frames, EmbryonicDiscovery into a full ingest-update-maintain-interpret pipeline with 13 integration tests**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-24T19:33:29Z
- **Completed:** 2026-02-24T19:37:08Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Engine struct owns all 8 prior components with full ingest pipeline: push to ring buffer, apply to graph, query inverted index, run Tier 1 check, trigger embryonic observation, auto-promote candidates
- Frame registration materializes against current graph and registers in inverted index
- 13 integration tests covering full pipeline, retraction, shared nodes, embryonic auto-promotion, compaction correctness, temporal snapshots, stats reporting, and edge cases
- 109 unit tests + 35 doc tests all pass with zero clippy warnings

## Task Commits

Each task was committed atomically:

1. **Task 1: Create engine module wiring all components** - `f3071a7` (feat)
2. **Task 2: Add integration tests for full pipeline** - `1c597f2` (feat)

**Plan metadata:** (pending final commit)

## Files Created/Modified
- `src/engine.rs` - Engine struct with ingest pipeline, register_frame, compact_all, query_frame, snapshot_frame, stats, and 13 integration tests (879 lines)
- `src/lib.rs` - Added pub mod engine and pub use engine::Engine
- `src/frame.rs` - Added tuple_count() accessor for engine stats aggregation

## Decisions Made
- Added Frame.tuple_count() to expose DiffCollection tuple count for EngineStats aggregation (Rule 3 deviation -- blocking, needed for stats)
- Index registration uses only NodeIds from materialized paths (not edge keys) for simplicity; edge-key routing works via the existing inverted index EdgeAdded event handling
- Retained tier_config field with allow(dead_code) since it is specified in the plan and will be used by external callers for scoring
- Auto-promoted embryonic frames are immediately materialized and registered, making them instantly queryable

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added Frame.tuple_count() accessor**
- **Found during:** Task 1 (Engine stats implementation)
- **Issue:** EngineStats needs total_tuples but Frame did not expose tuple_count from its private DiffCollection
- **Fix:** Added pub fn tuple_count(&self) -> usize delegating to self.state.tuple_count()
- **Files modified:** src/frame.rs
- **Verification:** cargo build + cargo clippy pass with zero warnings
- **Committed in:** f3071a7 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Minimal -- single accessor method added to Frame for correctness. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Engine orchestration complete -- all components wired end to end
- Ready for Phase 10: Benchmarks & Quality (final phase)
- All 109 unit tests + 35 doc tests pass, zero clippy warnings

## Self-Check: PASSED

- All 3 files exist (src/engine.rs, src/lib.rs, src/frame.rs)
- Commit f3071a7 found (Task 1)
- Commit 1c597f2 found (Task 2)

---
*Phase: 09-engine-orchestration*
*Completed: 2026-02-24*
