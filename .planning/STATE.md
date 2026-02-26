# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-26)

**Core value:** When a signal arrives, decision-relevant context is already materialized -- zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.
**Current focus:** Phase 20 -- Incremental Property Change
**Milestone:** v3.0 -- Tech Debt Closure + Incremental Path Extension

## Current Position

Phase: 20 of 21 (Incremental Property Change) -- COMPLETE
Plan: 2 of 2 in current phase (All plans COMPLETE)
Status: Phase 20 complete -- all event types except NodeAdded handled incrementally
Last activity: 2026-02-26 -- Phase 20 Plan 02 PropertyChanged engine integration executed

Progress: [███████████████████░] 95% (20/21 phases)

## Performance Metrics

**Velocity:**
- Total plans completed: 26
- Average duration: 6 min
- Total execution time: 2.49 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1 - Core Types | 1 | 13 min | 13 min |
| 2 - Epoch Sequencer & Ring Buffer | 1 | 3 min | 3 min |
| 3 - Property Graph Storage | 1 | 3 min | 3 min |
| 4 - Differential MVCC Engine | 1 | 2 min | 2 min |
| 5 - Frame Materialization | 1 | 2 min | 2 min |
| 6 - Signal Routing | 1 | 2 min | 2 min |
| 7 - Interpretation & Adaptive Tiering | 1 | 2 min | 2 min |
| 8 - Embryonic Frame Discovery | 1 | 3 min | 3 min |
| 9 - Engine Orchestration | 1 | 3 min | 3 min |
| 10 - Benchmarks & Quality | 1 | 4 min | 4 min |
| 11 - Harden the Engine | 3 | 25 min | 8 min |
| 12 - Production Interface | 4 | 23 min | 6 min |
| 14 - Wire Post-Ingest Pipeline | 1 | 16 min | 16 min |
| 15 - Harden MCP Binary | 1 | 4 min | 4 min |
| 16 - Tech Debt Closure | 1 | 5 min | 5 min |
| 17 - Re-Diff Baseline | 1 | 9 min | 9 min |
| 18 - Incremental Edge Addition | 2 | 12 min | 6 min |
| 19 - Incremental Edge & Node Removal | 2 | 11 min | 6 min |
| 20 - Incremental Property Change | 2 | 14 min | 7 min |

**Recent Trend:**
- Last 5 plans: 7m, 4m, 7m, 8m, 6m
- Trend: stable

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- v3.0: Full re-traverse (evict+DFS) on every routed event as provably-correct baseline for incremental work
- v3.0: Oracle check uses unordered HashSet comparison of path sets for correctness verification
- v3.0: Write lock contention resolved -- each frame appears at most once in affected set per fan-out
- v3.0: Incremental path extension follows layered build: re-diff baseline -> EdgeAdded -> Edge/Node removal -> PropertyChanged -> benchmarks
- v3.0: No new Cargo dependencies needed -- purely algorithmic work using existing DiffCollection and Frame::apply_delta()
- v3.0: PathExtender is stateless module taking read-only refs to Frame, Graph, Event
- v3.0: DeletionContext captures edge info before graph mutation destroys adjacency
- v3.0: PathExtender uses explicit Outgoing/Incoming/Any direction arms with edge_matches_hop_directed helper
- v3.0: Direction::Any tries both orientations independently with separate backward prefix searches
- v3.0: Path deduplication done post-collection via HashSet rather than inline during generation
- v3.0: Event-based dispatch in maintain_and_evaluate_frames: EdgeAdded uses PathExtender, all others use rematerialize
- v3.0: flush_coalescer uses NodeRemoved sentinel to force rematerialize fallback for batched events
- v3.0: Proactive inverted index registration via collect_reachable_nodes includes all intermediate pattern nodes
- v3.0: Parallel edge survival uses graph.neighbors() on post-removal state -- implicitly validates edge type via hop constraint
- v3.0: Edge removal deduplicates via HashSet; node removal skips dedup since snapshot refs are unique
- v3.0: retract_edge_removed takes pattern+graph for hop-aware checking; retract_node_removed needs only paths+node
- v3.0: force_rematerialize parameter on maintain_and_evaluate_frames bypasses event dispatch -- cleaner than sentinel event tricks
- v3.0: DeletionContext captured before graph.remove_node() for future extensibility (current algorithm uses path scanning)
- v3.0: Coalescer uses force_rematerialize=true instead of NodeRemoved sentinel to avoid triggering incremental retraction
- v3.0: reevaluate_property_changed combines retract+assert in single function for atomicity and shared retracted-set filtering
- v3.0: Early exit for patterns with no property filters avoids unnecessary path scanning on PropertyChanged events
- v3.0: node_passes_hop checks target_type + property filter but NOT edge_type (edge verified by graph adjacency)
- v3.0: find_hop_origins reverses hop direction to find nodes that could reach changed_node via the hop's edge
- v3.0: All event types except NodeAdded dispatched incrementally -- EdgeAdded, EdgeRemoved, NodeRemoved, PropertyChanged
- v3.0: PropertyChanged dispatch uses snapshot().to_vec() for reference conversion per clippy recommendation

### Pending Todos

None yet.

### Blockers/Concerns

- Backward prefix resolution is O(B^K) per mutation; may need partial path cache for deep patterns (defer to v4 unless benchmarks demand it)
- Double-buffer compaction race with incremental writes (Pitfall 10) -- design fix during Phase 21
- Re-diff baseline replaced by incremental dispatch (Phases 18-20 complete) -- O(affected) for EdgeAdded/EdgeRemoved/NodeRemoved/PropertyChanged

## Session Continuity

Last session: 2026-02-26
Stopped at: Completed 20-02-PLAN.md -- PropertyChanged dispatch wired into engine, 4 oracle tests added, Phase 20 complete
Resume file: None
