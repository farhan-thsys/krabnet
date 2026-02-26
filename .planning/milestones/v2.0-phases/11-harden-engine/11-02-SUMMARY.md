---
phase: 11-harden-engine
plan: 02
subsystem: engine
tags: [coalescer, fanout, hysteresis, deduplication, tiering, priority-queue]

# Dependency graph
requires:
  - phase: 01-core-types
    provides: "NodeId, Epoch, Event, FrameTier newtypes"
  - phase: 07-interpretation-and-adaptive-tiering
    provides: "TierConfig, priority_score, recommend_tier, FrameTier thresholds"
provides:
  - "MutationCoalescer with configurable epoch-window same-node deduplication"
  - "CoalescedBatch with deduplicated (node_id, latest_event, epoch_range) tuples"
  - "FanOutLimiter with configurable MAX_FANOUT cap on immediate evaluations"
  - "DeferredEvalQueue with priority-sorted deferred frame evaluations"
  - "HysteresisState with consecutive threshold counters for tier thrashing prevention"
affects: [11-harden-engine, engine-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [epoch-window-coalescing, priority-based-deferral, hysteresis-counters]

key-files:
  created:
    - src/coalescer.rs
    - src/fanout.rs
  modified:
    - src/tiering.rs
    - src/lib.rs

key-decisions:
  - "MutationCoalescer uses HashMap<NodeId, CoalescedEntry> for O(1) upsert within window"
  - "DeferredEvalQueue uses sorted Vec with binary_search_by for O(log n) insertion"
  - "HysteresisState returns Warm on neutral-zone scores (0.2..=0.7), resetting both counters"
  - "event_node_id helper returns source NodeId for EdgeAdded/EdgeRemoved events"

patterns-established:
  - "Epoch-window coalescing: accumulate, auto-flush on boundary, manual flush for drain"
  - "Priority-based fan-out: sort descending, take top N immediate, queue remainder"
  - "Hysteresis counters: increment one direction, reset other; neutral resets both"

requirements-completed: [COALESCE-01, COALESCE-02, COALESCE-03, FANOUT-01, FANOUT-02, HYST-01, HYST-02, HYST-03]

# Metrics
duration: 7min
completed: 2026-02-25
---

# Phase 11 Plan 02: Coalescing, Fan-Out Limits, and Hysteresis Summary

**MutationCoalescer with epoch-window deduplication, FanOutLimiter with priority-based deferral queue, and HysteresisState with consecutive-window tier thrashing prevention**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-24T20:44:12Z
- **Completed:** 2026-02-24T20:51:57Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- MutationCoalescer deduplicates same-node mutations within configurable epoch window (default 16), preserving all different-node mutations as separate triggers
- FanOutLimiter caps immediate frame evaluations at MAX_FANOUT (default 1000), queuing excess in DeferredEvalQueue sorted by priority score descending
- HysteresisState requires N consecutive windows (default 5) above/below threshold before allowing tier change; oscillating scores keep frame in Warm

## Task Commits

Each task was committed atomically:

1. **Task 1: Create MutationCoalescer with epoch-window deduplication** - `6c8887d` (feat)
2. **Task 2: Create FanOutLimiter with DeferredEvalQueue and add HysteresisState to tiering** - `32a6a28` (feat)

## Files Created/Modified
- `src/coalescer.rs` - MutationCoalescer, CoalescedEntry, CoalescedBatch, event_node_id helper, 5 tests
- `src/fanout.rs` - FanOutLimiter, DeferredEvalQueue, DeferredEvalEntry, 4 tests
- `src/tiering.rs` - Added HysteresisState struct with consecutive threshold counters, 3 tests
- `src/lib.rs` - Registered coalescer and fanout modules, added re-exports

## Decisions Made
- MutationCoalescer uses HashMap<NodeId, CoalescedEntry> for O(1) upsert within window -- simple and efficient for the expected cardinality
- DeferredEvalQueue uses sorted Vec with binary_search_by for O(log n) insertion -- suitable since bulk insertions are infrequent (only when fan-out exceeds limit)
- HysteresisState returns Warm on neutral-zone scores (0.2..=0.7) and resets both counters -- this ensures oscillating scores default to the safe middle tier
- event_node_id returns source NodeId for edge events -- consistent with the convention that the source node is the primary affected node

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Coalescer, fan-out limiter, and hysteresis state are standalone modules ready for integration into the engine ingest pipeline
- All 127 tests passing (109 original + 12 new from this plan + 6 from concurrent environment additions)
- Zero clippy warnings

## Self-Check: PASSED

- FOUND: src/coalescer.rs
- FOUND: src/fanout.rs
- FOUND: .planning/phases/11-harden-engine/11-02-SUMMARY.md
- FOUND: 6c8887d (Task 1 commit)
- FOUND: 32a6a28 (Task 2 commit)

---
*Phase: 11-harden-engine*
*Completed: 2026-02-25*
