---
phase: 19-incremental-edge-node-removal
plan: 02
subsystem: engine
tags: [incremental, retraction, engine-integration, edge-removal, node-removal, differential, oracle-tests, coalescer]

# Dependency graph
requires:
  - phase: 19-incremental-edge-node-removal
    plan: 01
    provides: "retract_edge_removed and retract_node_removed algorithms in path_extender module"
  - phase: 18-incremental-edge-addition
    provides: "maintain_and_evaluate_frames with EdgeAdded incremental dispatch"
provides:
  - "DeletionContext struct for pre-removal context capture"
  - "EdgeRemoved incremental -1 dispatch via retract_edge_removed in maintain_and_evaluate_frames"
  - "NodeRemoved incremental -1 dispatch via retract_node_removed in maintain_and_evaluate_frames"
  - "force_rematerialize parameter on maintain_and_evaluate_frames"
  - "Coalescer sentinel fix using force_rematerialize=true instead of NodeRemoved sentinel"
  - "6 new oracle tests (12-17) covering incremental removal correctness"
affects: [20-property-changed, 21-benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [force-rematerialize-parameter, event-dispatch-with-override, deletion-context-capture]

key-files:
  created: []
  modified:
    - src/engine.rs

key-decisions:
  - "force_rematerialize parameter bypasses event dispatch entirely -- cleaner than sentinel event tricks"
  - "DeletionContext captured before graph.remove_node() for future extensibility, though current retract_node_removed uses path scanning"
  - "Coalescer sentinel changed from NodeRemoved to NodeAdded with force_rematerialize=true -- NodeRemoved would incorrectly trigger incremental retraction"
  - "Oracle test numbering continues from Phase 18 (12-17) to avoid confusion with existing test numbering"

patterns-established:
  - "Event dispatch with force_rematerialize override: maintain_and_evaluate_frames checks force_rematerialize first, then dispatches on event type"
  - "Incremental retraction via snapshot+scan: frame.snapshot(MAX) captures current paths, retract functions scan for broken paths, apply_delta(-1) retracts"
  - "DeletionContext pattern: capture pre-mutation context for removal events before graph mutation destroys state"

requirements-completed: [NDEL-01, IREM-03, NDEL-03]

# Metrics
duration: 7min
completed: 2026-02-26
---

# Phase 19 Plan 02: Engine Integration Summary

**Incremental -1 retraction dispatch for EdgeRemoved and NodeRemoved events in maintain_and_evaluate_frames with force_rematerialize coalescer fix and 6 new oracle tests**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-26T18:05:01Z
- **Completed:** 2026-02-26T18:11:47Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Wired retract_edge_removed into engine for incremental -1 deltas on EdgeRemoved events (replacing full rematerialize)
- Wired retract_node_removed into engine for incremental -1 deltas on NodeRemoved events (replacing full rematerialize)
- Added force_rematerialize parameter to maintain_and_evaluate_frames, fixing coalescer sentinel issue
- Added DeletionContext struct captured before graph.remove_node() for future extensibility
- 6 new oracle tests prove incremental retraction matches full DFS for all removal scenarios
- All 228 lib tests pass (222 existing + 6 new), zero clippy warnings, zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire incremental removal dispatch into engine** - `5090d43` (feat)
2. **Task 2: Add oracle tests for incremental removal** - `dee619d` (test)

## Files Created/Modified
- `src/engine.rs` - Added DeletionContext struct; added force_rematerialize parameter to maintain_and_evaluate_frames; wired EdgeRemoved to retract_edge_removed and NodeRemoved to retract_node_removed; fixed coalescer sentinel from NodeRemoved to NodeAdded with force_rematerialize=true; imported TypeId in module scope; added 6 oracle tests (12-17)

## Decisions Made
- Used force_rematerialize parameter instead of sentinel event for coalescer -- cleaner, avoids the NodeRemoved sentinel accidentally triggering incremental retraction code
- DeletionContext captures node_id before graph mutation; currently unused by retract_node_removed (which uses path scanning) but provides future extensibility for edge-adjacency-based retraction
- Changed coalescer sentinel from `Event::NodeRemoved { node_id: NodeId(0) }` to `Event::NodeAdded { node_id: NodeId(0), type_id: TypeId(0) }` -- the event is now irrelevant since force_rematerialize skips the match entirely
- Oracle test numbering starts at 12 (continuing from Phase 18's Oracle Test 11) to maintain sequential numbering

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated stale comment in oracle test 9**
- **Found during:** Task 1
- **Issue:** Oracle test 9 comment said "falls back to full rematerialize (non-EdgeAdded event)" which is now incorrect
- **Fix:** Updated comment to "uses incremental retraction via retract_edge_removed"
- **Files modified:** src/engine.rs
- **Committed in:** 5090d43 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug -- stale comment)
**Impact on plan:** Minimal -- comment fix for correctness. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- EdgeAdded, EdgeRemoved, and NodeRemoved all use incremental path extension/retraction
- Only PropertyChanged still uses full rematerialize -- ready for Phase 20
- 17 oracle tests provide comprehensive correctness baseline for incremental dispatch
- DeletionContext struct ready for future edge-adjacency capture if needed
- All 228 tests pass with zero warnings

## Self-Check: PASSED

- FOUND: src/engine.rs
- FOUND: 19-02-SUMMARY.md
- FOUND: commit 5090d43
- FOUND: commit dee619d

---
*Phase: 19-incremental-edge-node-removal*
*Completed: 2026-02-26*
