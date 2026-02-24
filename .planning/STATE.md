# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-25)

**Core value:** When a signal arrives, decision-relevant context is already materialized -- zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.
**Current focus:** Phase 11: Harden the Engine
**Milestone:** v2.0 Full Build Completion (67 requirements across 3 phases)

## Current Position

Phase: 11 of 13 (Harden the Engine) -- COMPLETE
Plan: 3/3 — phase 11 complete
Status: Completed 11-03 (stress tests, quality gates, benchmark)
Last activity: 2026-02-25 — Completed plan 11-03

Progress: [███████████░░] 85% (11/13 phases)

## Performance Metrics

**Velocity:**
- Total plans completed: 12
- Average duration: 4 min
- Total execution time: 0.91 hours

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

**Recent Trend:**
- Last 5 plans: 3m, 3m, 4m, 7m, 10m
- Trend: stable

*Updated after each plan completion*
| Phase 11 P01 | 8min | 2 tasks | 4 files |
| Phase 11 P02 | 7min | 2 tasks | 4 files |
| Phase 11 P03 | 10min | 2 tasks | 5 files |
| Phase 11 P03 | 10min | 2 tasks | 5 files |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Roadmap: 10 phases following strict build-dependency DAG (types -> interner -> sequencer/ring-buffer -> graph-store -> differential -> frame -> inverted-index -> prioritizer/interpreter -> embryonic -> engine -> benchmarks -> quality)
- Roadmap: Comprehensive depth with each compilation boundary as its own phase
- Phase 1: PropertyValue::Text uses u32 interned ID (not String) for zero-allocation hot path
- Phase 1: DiffTuple<T> is generic with bounds on impl blocks, not struct definition
- Phase 1: Event does not carry Epoch -- assigned by sequencer in Phase 2
- Phase 1: Switched to stable-x86_64-pc-windows-gnu toolchain (MSVC target lacked Windows SDK)
- Phase 2: RingBuffer uses &mut self for push (single-writer) -- concurrent multi-producer deferred to v2
- Phase 2: Epoch-in-slot overwrite detection -- each slot stores (Epoch, Event), reads verify epoch match
- Phase 2: Send+Sync derived automatically, no unsafe impl needed
- Phase 3: EdgeData retains edge_id/type_id fields (allow(dead_code)) for structural completeness and future use
- Phase 3: Tests co-located with implementation in graph.rs following Rust module convention
- Phase 4: Cached aggregate net_delta maintained incrementally on assert/retract, recalculated from scratch after compaction for exactness
- Phase 4: Compaction assigns frontier epoch to collapsed tuples for consistent temporal ordering
- Phase 4: Default trait implemented via delegation to new() for ergonomic construction
- Phase 5: Tests co-located with implementation in frame.rs following Rust module convention
- Phase 5: DFS uses recursive approach with path vector accumulation for clarity
- Phase 5: Frame starts Cold on creation; tier set externally or by eviction
- Phase 6: Default trait implemented via delegation to new() for ergonomic construction
- Phase 6: Tests co-located with implementation in routing.rs following Rust module convention
- Phase 6: Helper methods collect_by_node/collect_by_edge_key for DRY set-union logic
- Phase 7: Tests co-located with implementation in interpret.rs and tiering.rs following Rust module convention
- Phase 7: tier2_analysis takes epoch parameter for temporal snapshot flexibility
- Phase 7: Scoring uses ln(1+x)/10 capped at 1.0 for log-scaled activity normalization
- Phase 8: Direction matching simplified for embryonic discovery -- full path tracking deferred to engine orchestration
- Phase 8: decompose_frame generates sub-patterns shortest-to-longest for consistent ordering
- Phase 8: Tests co-located with implementation in embryonic.rs following Rust module convention
- Phase 9: Frame.tuple_count() accessor added for engine stats aggregation
- Phase 9: Index registration uses NodeIds from materialized paths only (no edge keys for simplicity)
- Phase 9: Auto-promoted embryonic frames immediately materialized and registered in inverted index
- Phase 9: Tests co-located with implementation in engine.rs following Rust module convention
- Phase 10: Dropped html_reports from criterion (windows-sys needs dlltool missing from GNU toolchain)
- Phase 10: iter_batched with SmallInput for stateful benchmarks to isolate setup cost
- Phase 10: All 4 quality gates passing: 109 tests, 35 doc-tests, 0 clippy warnings, 0 doc warnings
- Phase 11-02: MutationCoalescer uses HashMap<NodeId, CoalescedEntry> for O(1) upsert within window
- Phase 11-02: DeferredEvalQueue uses sorted Vec with binary_search_by for O(log n) insertion
- Phase 11-02: HysteresisState returns Warm on neutral-zone scores, resetting both counters
- Phase 11-02: event_node_id returns source NodeId for edge events
- [Phase 11]: Used std::sync::{RwLock, Mutex} instead of parking_lot -- GNU toolchain dlltool incompatibility on Windows
- [Phase 11]: Scoped threads via std::thread::scope for frame evaluation -- zero overhead, automatic lifetime management
- [Phase 11]: Engine::new() backward compatible (no compaction); Engine::with_compaction() for opt-in background compaction
- [Phase 11]: Delta updates from scoped threads collected as (frame_id, delta) pairs and merged on main thread
- [Phase 11-03]: Engine::with_config() as unified constructor accepting optional compaction, coalescing, and fanout parameters
- [Phase 11-03]: Coalescer gate in ingest(): events accumulated within window, evaluation deferred until flush/window-elapse
- [Phase 11-03]: Fanout gate in ingest(): scored affected frames split into immediate (top N) and deferred sets
- [Phase 11-03]: Hysteresis updated per-frame after evaluation, tier changes applied only when consecutive threshold met
- [Phase 11-03]: affected_frames_by_node() added to InvertedIndex for coalescer batch integration path
- [Phase 11]: Engine::with_config() as unified constructor for all hardening features

### Pending Todos

None yet.

### Blockers/Concerns

None yet.

## Session Continuity

Last session: 2026-02-25
Stopped at: Completed 11-03-PLAN.md (Phase 11 complete)
Resume file: None
