# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-26)

**Core value:** When a signal arrives, decision-relevant context is already materialized -- zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.
**Current focus:** Phase 19 -- Incremental Edge & Node Removal
**Milestone:** v3.0 -- Tech Debt Closure + Incremental Path Extension

## Current Position

Phase: 19 of 21 (Incremental Edge & Node Removal)
Plan: 1 of 2 in current phase (Plan 01 COMPLETE)
Status: Plan 01 complete -- retract_edge_removed and retract_node_removed algorithms implemented with 13 unit tests
Last activity: 2026-02-26 -- Phase 19 Plan 01 retraction algorithms executed

Progress: [██████████████████░░] 86% (18/21 phases)

## Performance Metrics

**Velocity:**
- Total plans completed: 23
- Average duration: 6 min
- Total execution time: 2.14 hours

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
| 19 - Incremental Edge & Node Removal | 1 | 4 min | 4 min |

**Recent Trend:**
- Last 5 plans: 5m, 9m, 5m, 7m, 4m
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

### Pending Todos

None yet.

### Blockers/Concerns

- Backward prefix resolution is O(B^K) per mutation; may need partial path cache for deep patterns (defer to v4 unless benchmarks demand it)
- Double-buffer compaction race with incremental writes (Pitfall 10) -- design fix during Phase 21
- Re-diff baseline is O(full_DFS) per affected frame per event; Phases 18-20 will restore O(affected) performance

## Session Continuity

Last session: 2026-02-26
Stopped at: Completed 19-01-PLAN.md -- retract_edge_removed and retract_node_removed algorithms with 13 unit tests
Resume file: None
