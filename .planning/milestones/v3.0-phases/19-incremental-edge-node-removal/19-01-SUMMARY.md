---
phase: 19-incremental-edge-node-removal
plan: 01
subsystem: engine
tags: [incremental, retraction, path-extender, edge-removal, node-removal, differential]

# Dependency graph
requires:
  - phase: 18-incremental-edge-addition
    provides: "path_extender module with extend_edge_added, EdgeAddedDeltas, backward/forward DFS helpers"
provides:
  - "retract_edge_removed() with parallel-edge survival check"
  - "retract_node_removed() with any-position node scan"
  - "EdgeRemovedDeltas and NodeRemovedDeltas structs"
  - "path_broken_by_edge_removal() direction-aware hop matcher"
affects: [19-02-engine-integration, 20-property-changed, 21-benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: [parallel-edge-survival-check, direction-aware-hop-matching, post-removal-graph-query]

key-files:
  created: []
  modified:
    - src/path_extender.rs
    - src/lib.rs

key-decisions:
  - "Parallel edge survival uses graph.neighbors() on post-removal state -- implicitly validates edge type via hop constraint"
  - "Direction::Any checks both (from==source,to==target) and (from==target,to==source) orientations"
  - "Edge removal deduplicates via HashSet; node removal skips dedup since snapshot refs are unique"
  - "retract_edge_removed takes pattern+graph for hop-aware checking; retract_node_removed needs only paths+node"

patterns-established:
  - "Parallel edge survival: query graph.neighbors(from, hop.direction, hop.edge_type) to check if any remaining edge connects the same pair"
  - "Direction-aware matching: Outgoing matches (from==source,to==target), Incoming matches (from==target,to==source), Any matches either"

requirements-completed: [IREM-01, IREM-02, NDEL-02]

# Metrics
duration: 4min
completed: 2026-02-26
---

# Phase 19 Plan 01: Retraction Algorithms Summary

**retract_edge_removed with parallel-edge survival check and retract_node_removed with any-position scan in path_extender.rs**

## Performance

- **Duration:** 4 min
- **Started:** 2026-02-26T17:57:41Z
- **Completed:** 2026-02-26T18:01:35Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Implemented retract_edge_removed() scanning materialized paths for removed edge at any hop position with parallel-edge survival check
- Implemented retract_node_removed() scanning paths for removed node at any position (anchor, intermediate, terminal)
- Added EdgeRemovedDeltas and NodeRemovedDeltas structs for -1 delta results
- 13 comprehensive unit tests covering all removal scenarios including parallel edge survival
- All 222 lib tests pass with zero regressions, zero clippy warnings

## Task Commits

Each task was committed atomically:

1. **Task 1: Add retract_edge_removed and retract_node_removed** - `c9d6d00` (feat)
2. **Task 2: Add unit tests for retraction functions** - `d1bf4f8` (test)

## Files Created/Modified
- `src/path_extender.rs` - Added EdgeRemovedDeltas, NodeRemovedDeltas structs; retract_edge_removed(), path_broken_by_edge_removal(), retract_node_removed() functions; updated module doc; 13 new unit tests
- `src/lib.rs` - Added re-exports for retract_edge_removed, retract_node_removed, EdgeRemovedDeltas, NodeRemovedDeltas

## Decisions Made
- Parallel edge survival uses graph.neighbors() on post-removal state rather than tracking edge counts -- the graph already reflects removal, so neighbors() implicitly validates edge type via hop constraint
- Direction::Any checks both orientations independently for maximum correctness
- Edge removal deduplicates via HashSet to prevent double-retraction when same path matches at multiple hops; node removal skips dedup since snapshot path refs are unique
- retract_edge_removed takes pattern+graph for hop-aware direction/type checking; retract_node_removed needs only paths+node (simpler -- just contains() check)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Retraction algorithms ready for engine integration in Plan 02
- retract_edge_removed and retract_node_removed are exported from lib.rs
- Engine can call these functions from maintain_and_evaluate_frames for EdgeRemoved/NodeRemoved events
- All 26 path_extender tests provide correctness baseline

## Self-Check: PASSED

- FOUND: src/path_extender.rs
- FOUND: src/lib.rs
- FOUND: 19-01-SUMMARY.md
- FOUND: commit c9d6d00
- FOUND: commit d1bf4f8

---
*Phase: 19-incremental-edge-node-removal*
*Completed: 2026-02-26*
