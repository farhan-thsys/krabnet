---
phase: 18-incremental-edge-addition
plan: 02
subsystem: engine
tags: [incremental, path-extension, edge-added, engine-integration, oracle-tests, inverted-index]

# Dependency graph
requires:
  - phase: 18-incremental-edge-addition
    plan: 01
    provides: "extend_edge_added() stateless function, EdgeAddedDeltas struct"
  - phase: 17-re-diff-baseline
    provides: "Frame::rematerialize, maintain_and_evaluate_frames, oracle_check baseline"
provides:
  - "Incremental EdgeAdded dispatch in maintain_and_evaluate_frames via path_extender::extend_edge_added"
  - "Event-based dispatch: EdgeAdded -> incremental, all others -> full rematerialize fallback"
  - "collect_reachable_nodes() for proactive inverted index registration of intermediate pattern nodes"
  - "5 new oracle tests validating incremental EdgeAdded correctness across multi-hop scenarios"
affects: [19-edge-node-removal, 20-property-changed, 21-benchmarks]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Event-based dispatch in maintain_and_evaluate_frames for incremental vs full rematerialize", "Proactive intermediate node registration in inverted index via collect_reachable_nodes"]

key-files:
  created: []
  modified: ["src/engine.rs"]

key-decisions:
  - "Event-based dispatch in maintain_and_evaluate_frames: EdgeAdded uses PathExtender, all others use rematerialize"
  - "flush_coalescer uses NodeRemoved sentinel to force rematerialize fallback for batched events"
  - "Proactive inverted index registration includes all reachable intermediate nodes, not just complete-path nodes"

patterns-established:
  - "Pattern: Event dispatch in frame maintenance -- match on Event variant to choose incremental vs full re-traverse strategy"
  - "Pattern: Proactive index registration -- collect_reachable_nodes does partial DFS through pattern to register intermediate nodes for future edge routing"

requirements-completed: [IADD-04, IADD-05]

# Metrics
duration: 7min
completed: 2026-02-26
---

# Phase 18 Plan 02: Engine Integration Summary

**Incremental EdgeAdded dispatch wired into engine pipeline with event-based routing and 5 oracle tests proving identical results to full DFS re-traverse**

## Performance

- **Duration:** 7 min
- **Started:** 2026-02-26T17:05:02Z
- **Completed:** 2026-02-26T17:11:57Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Wired PathExtender into maintain_and_evaluate_frames with event-based dispatch: EdgeAdded events use incremental path extension, all others use full rematerialize
- Added 5 new oracle tests covering two-hop incremental, multiple sequential adds, add-then-remove, non-matching edge, and three-hop middle edge scenarios
- Fixed inverted index registration to include intermediate reachable nodes, enabling routing for multi-hop patterns where edges arrive after frame registration
- All 209 tests pass (204 existing + 5 new), zero clippy warnings
- Phase 18 complete: all 5 IADD requirements satisfied

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire PathExtender into maintain_and_evaluate_frames with event dispatch** - `a14426b` (feat)
2. **Task 2: Extend oracle test harness with incremental EdgeAdded-specific scenarios** - `a202d1a` (test)

## Files Created/Modified
- `src/engine.rs` - Added &Event parameter to maintain_and_evaluate_frames, EdgeAdded dispatch to path_extender::extend_edge_added, Delta import, flush_coalescer sentinel, collect_reachable_nodes helper, proactive index registration, 5 new oracle tests

## Decisions Made
- Event-based dispatch in maintain_and_evaluate_frames: match on Event variant to choose between incremental PathExtender (EdgeAdded) and full rematerialize (everything else)
- flush_coalescer uses `Event::NodeRemoved { node_id: NodeId(0) }` as a sentinel to force the rematerialize fallback branch, since coalesced batches span multiple events
- Proactive inverted index registration via collect_reachable_nodes: walks the graph from the anchor through partial pattern hops to register ALL intermediate nodes, not just nodes in complete paths

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed inverted index registration for multi-hop patterns**
- **Found during:** Task 2 (oracle tests for multi-hop incremental EdgeAdded)
- **Issue:** Frames registered with 0 materialized paths had no nodes in the inverted index, so subsequent EdgeAdded events on intermediate/downstream nodes never triggered frame maintenance. Multi-hop oracle tests failed because the frame was invisible to the routing layer.
- **Fix:** Replaced extract_node_ids_from_frame (which only collects from complete paths) with collect_reachable_nodes, which does a partial DFS from the anchor through all pattern hops, collecting every reachable intermediate node. This ensures routing coverage for edges anywhere in the pattern chain.
- **Files modified:** src/engine.rs (register_frame, new collect_reachable_nodes helper)
- **Verification:** All 11 oracle tests pass including 5 new multi-hop incremental scenarios
- **Committed in:** a202d1a (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Essential correctness fix for multi-hop incremental routing. Without this, incremental EdgeAdded only worked for 1-hop patterns or edges adjacent to the anchor. No scope creep.

## Issues Encountered
None beyond the auto-fixed routing bug above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 18 complete: all IADD requirements satisfied (IADD-01 through IADD-05)
- Incremental EdgeAdded path extension is wired and oracle-verified
- Ready for Phase 19 (Edge/Node Removal) which will add incremental handling for removal events
- Non-EdgeAdded events (EdgeRemoved, NodeRemoved, PropertyChanged) still use full rematerialize as fallback

## Self-Check: PASSED

- src/engine.rs: FOUND (path_extender::extend_edge_added at line 795, frame.apply_delta(path, epoch, Delta(1)) at line 804, collect_reachable_nodes at line 863)
- 18-02-SUMMARY.md: FOUND
- Commit a14426b: FOUND (Task 1 - feat)
- Commit a202d1a: FOUND (Task 2 - test)
- 209 tests pass, 0 failures, 0 clippy warnings

---
*Phase: 18-incremental-edge-addition*
*Completed: 2026-02-26*
