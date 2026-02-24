# Roadmap: Krabnet

## Overview

Krabnet is built bottom-up following the strict compilation DAG: core types and interning form the foundation, then ingestion infrastructure (sequencer + ring buffer), then the dual storage layer (property graph + differential MVCC), then the maintenance layer (frames + signal routing), then the intelligence layer (interpretation + tiering + embryonic discovery), and finally orchestration (engine wiring, integration tests, benchmarks, and quality gates). Each phase compiles and passes tests independently before the next begins. The 10 phases reflect the natural compilation boundaries of the 13-module architecture at comprehensive depth.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Core Types and String Interning** - Shared type definitions and bidirectional string-to-u32 interner used by every module (completed 2026-02-24)
- [x] **Phase 2: Epoch Sequencer and Ring Buffer** - Lock-free ingestion pipeline with monotonic epoch assignment and pre-allocated event buffer (completed 2026-02-24)
- [x] **Phase 3: Property Graph Storage** - In-memory adjacency-on-node property graph with typed nodes, edges, and directional neighbor queries (completed 2026-02-24)
- [x] **Phase 4: Differential MVCC Engine** - Mathematically exact +1/-1 multiset collection with compaction, temporal snapshots, and net delta computation (completed 2026-02-24)
- [x] **Phase 5: Frame Materialization** - Parked traversers with multi-hop DFS materialization, delta application, eviction, and re-materialization (completed 2026-02-24)
- [x] **Phase 6: Signal Routing** - Inverted index mapping node IDs and edge keys to affected frame sets for O(affected) event routing (completed 2026-02-24)
- [x] **Phase 7: Interpretation and Adaptive Tiering** - Two-tier signal interpretation (binary + structural) and hot/warm/cold frame priority scoring (completed 2026-02-24)
- [x] **Phase 8: Embryonic Frame Discovery** - Autonomous pattern detection from mutation stream with bitvec completion tracking and auto-promotion (completed 2026-02-24)
- [x] **Phase 9: Engine Orchestration** - Top-level Engine struct wiring all components into the full ingest-update-maintain-interpret pipeline (completed 2026-02-24)
- [x] **Phase 10: Benchmarks and Quality** - Criterion benchmarks, clippy compliance, documentation coverage, and final quality gates (completed 2026-02-24)

## Phase Details

### Phase 1: Core Types and String Interning
**Goal**: Every module can import shared type definitions and convert strings to integer IDs at initialization boundaries
**Depends on**: Nothing (first phase)
**Requirements**: INFRA-01, INFRA-02
**Success Criteria** (what must be TRUE):
  1. All newtypes (NodeId, EdgeId, TypeId, Epoch, Delta) and shared enums (PropertyValue, Direction, Filter, HopSpec, Event, DiffTuple, InterpretationTier, FrameTier) compile and are importable from a single types module
  2. String interner accepts strings at initialization and returns stable u32 IDs; reverse lookup from u32 to &str works for all interned strings
  3. Interning the same string twice returns the same u32 ID
  4. No heap allocation occurs after interner initialization is complete (all strings pre-allocated in arena)
**Plans**: 1 plan

Plans:
- [ ] 01-01-PLAN.md — Create crate scaffold, core types module, and string interner with tests

### Phase 2: Epoch Sequencer and Ring Buffer
**Goal**: Events can be ingested into a lock-free ring buffer with globally unique monotonic epoch assignment
**Depends on**: Phase 1
**Requirements**: INFRA-03, INFRA-04, INFRA-05, INFRA-06, TEST-02
**Success Criteria** (what must be TRUE):
  1. Epoch sequencer produces strictly increasing u64 values with no gaps under sequential calls
  2. Ring buffer accepts events into pre-allocated slots and returns the assigned epoch for each
  3. Ring buffer correctly wraps around when capacity is exceeded, and reading an unwritten slot returns None
  4. Ring buffer type implements Send and Sync with documented safety invariants and passes cargo test
  5. Zero heap allocations occur on the push/read hot path after ring buffer initialization
**Plans**: TBD

Plans:
- [ ] 02-01: TBD
- [ ] 02-02: TBD

### Phase 3: Property Graph Storage
**Goal**: A fully functional in-memory property graph supports node/edge CRUD, directional neighbor queries, and property storage with interned keys
**Depends on**: Phase 1
**Requirements**: GRAPH-01, GRAPH-02, GRAPH-03, GRAPH-04, GRAPH-05, GRAPH-06, TEST-03
**Success Criteria** (what must be TRUE):
  1. Nodes can be added with a type, retrieved in O(1), and removed with cascading edge removal from all neighbor adjacency lists
  2. Edges can be added (updating both source outgoing and target incoming adjacency) and removed (updating both endpoints)
  3. Directional neighbor queries filter by Outgoing, Incoming, or Any direction, and optionally by edge type
  4. Properties can be upserted on nodes using interned u32 keys
  5. Node count and edge count are always consistent with the actual graph contents after any sequence of mutations
**Plans**: TBD

Plans:
- [ ] 03-01: TBD
- [ ] 03-02: TBD

### Phase 4: Differential MVCC Engine
**Goal**: The differential collection correctly implements +1/-1 multiset math with compaction and temporal snapshots
**Depends on**: Phase 1
**Requirements**: DIFF-01, DIFF-02, DIFF-03, DIFF-04, DIFF-05, DIFF-06, DIFF-07, TEST-01
**Success Criteria** (what must be TRUE):
  1. Asserting (+1) and retracting (-1) the same payload at the same epoch produces net-zero (annihilation) after compaction
  2. Net delta per payload and aggregate net delta across all tuples are mathematically exact for arbitrary assertion/retraction sequences
  3. Temporal snapshot at epoch E returns exactly the payloads with positive net delta from tuples at or before E
  4. Compaction below a frontier epoch collapses survivors, annihilates net-zero tuples, and warns on negative net deltas
  5. Double-assert of the same payload produces multiplicity 2, not 1 (true multiset semantics)
**Plans**: TBD

Plans:
- [ ] 04-01: TBD
- [ ] 04-02: TBD

### Phase 5: Frame Materialization
**Goal**: Parked traversers materialize multi-hop graph patterns from an anchor node and maintain state through delta application, eviction, and re-materialization
**Depends on**: Phase 3, Phase 4
**Requirements**: FRAME-01, FRAME-02, FRAME-03, FRAME-04, FRAME-05, FRAME-06, FRAME-07, FRAME-08, TEST-04
**Success Criteria** (what must be TRUE):
  1. A frame holding a multi-hop pattern can materialize from an anchor node via DFS, collecting all complete paths as +1 assertions in its DiffCollection
  2. Materialization correctly filters by edge type, target node type, and property filter at each hop
  3. Querying a frame returns current-state paths (positive net delta) and increments query_count; snapshot queries return historical state at a given epoch
  4. Evicting a frame clears its state and sets tier to Cold; re-materializing restores the same paths from the current graph
  5. Frame compaction delegates to the underlying DiffCollection and produces correct results
**Plans**: TBD

Plans:
- [ ] 05-01: TBD
- [ ] 05-02: TBD

### Phase 6: Signal Routing
**Goal**: Events are efficiently routed to only the frames they affect via inverted index posting lists
**Depends on**: Phase 5
**Requirements**: ROUTE-01, ROUTE-02, ROUTE-03, ROUTE-04, TEST-05
**Success Criteria** (what must be TRUE):
  1. Inverted index maps node IDs to frame ID sets and (source_node, edge_type) pairs to frame ID sets
  2. affected_frames returns a deduplicated union of all frame IDs affected by a given Event
  3. Frame registration adds to all relevant posting lists; unregistration removes from all relevant posting lists
  4. A shared node appearing in multiple frames causes all those frames to appear in the affected set
**Plans**: TBD

Plans:
- [ ] 06-01: TBD
- [ ] 06-02: TBD

### Phase 7: Interpretation and Adaptive Tiering
**Goal**: Frames are scored for priority tiering and signals are interpreted through a two-tier gate (fast binary check, then structural analysis)
**Depends on**: Phase 5
**Requirements**: INTERP-01, INTERP-02, TIER-01, TIER-02
**Success Criteria** (what must be TRUE):
  1. Tier 1 binary check detects when a frame's net delta changes from its previous value (changed = true, unchanged = false)
  2. Tier 2 structural analysis identifies completed and broken hops in frame paths at a given epoch
  3. Frame priority score combines query frequency, mutation rate, and recency decay into a normalized 0.0-1.0 score
  4. Tier recommendation classifies frames as Hot (score > 0.7), Warm, or Cold (score < 0.2) based on the normalized score
**Plans**: TBD

Plans:
- [ ] 07-01: TBD
- [ ] 07-02: TBD

### Phase 8: Embryonic Frame Discovery
**Goal**: The system autonomously discovers emerging graph patterns from the mutation stream and promotes them to full parked frames when completion thresholds are met
**Depends on**: Phase 5
**Requirements**: EMBRYO-01, EMBRYO-02, EMBRYO-03, EMBRYO-04, EMBRYO-05, EMBRYO-06, TEST-06
**Success Criteria** (what must be TRUE):
  1. Pattern templates define multi-hop patterns to watch for, and decompose_frame generates all sub-patterns of length >= 2
  2. observe_edge detects when new edges extend anchor candidates' partial paths, updating bitvec completion bits
  3. Auto-promotion triggers when a candidate's completion_ratio meets or exceeds the template threshold, producing a full frame
  4. Stale candidates are pruned when they haven't progressed within the configurable epoch window
  5. Max candidates cap per template prevents unbounded memory growth
**Plans**: TBD

Plans:
- [ ] 08-01: TBD
- [ ] 08-02: TBD

### Phase 9: Engine Orchestration
**Goal**: All components are wired into a single Engine struct that executes the full ingest-update-maintain-interpret pipeline end to end
**Depends on**: Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8
**Requirements**: ENGINE-01, ENGINE-02, ENGINE-03, ENGINE-04, ENGINE-05, TEST-07, TEST-08
**Success Criteria** (what must be TRUE):
  1. Ingest pipeline works end to end: push event to ring buffer, apply to graph, query inverted index for affected frames, maintain affected frames, run Tier 1 interpretation
  2. EdgeAdded events trigger embryonic observe_edge; candidates that meet promotion threshold are auto-created as new parked frames
  3. Frame registration materializes against the current graph and registers in the inverted index; compact_all compacts all frames below a given frontier epoch
  4. Stats reporting returns accurate node/edge/frame counts, tier distribution, tuple count, and embryonic stats
  5. Integration tests pass: full pipeline, retraction pipeline, shared-node multi-frame, embryonic auto-promotion, compaction correctness, and temporal snapshot consistency
**Plans**: TBD

Plans:
- [ ] 09-01: TBD
- [ ] 09-02: TBD

### Phase 10: Benchmarks and Quality
**Goal**: The crate passes all quality gates -- benchmarks run, clippy is clean, documentation is complete, and all tests pass
**Depends on**: Phase 9
**Requirements**: BENCH-01, QUAL-01, QUAL-02, QUAL-03, QUAL-04, QUAL-05
**Success Criteria** (what must be TRUE):
  1. Criterion benchmarks for ingest_event, frame_query, inverted_index_lookup, tier1_check, embryonic_observe, and compaction all execute and produce numbers
  2. cargo test runs with zero failures across all 8 test files
  3. cargo clippy produces zero warnings
  4. cargo doc --no-deps generates documentation without warnings
  5. Every public function has a doc comment and every module has a module-level doc comment
**Plans**: TBD

Plans:
- [ ] 10-01: TBD
- [ ] 10-02: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9 -> 10

Note: Phases 2, 3, and 4 all depend only on Phase 1. Phases 6, 7, and 8 all depend on Phase 5. The sequential order above is the recommended build order from the architecture DAG, but phases sharing the same dependency level could theoretically be parallelized.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Core Types and String Interning | 0/TBD | Complete    | 2026-02-24 |
| 2. Epoch Sequencer and Ring Buffer | 1/1 | Complete    | 2026-02-24 |
| 3. Property Graph Storage | 1/1 | Complete    | 2026-02-24 |
| 4. Differential MVCC Engine | 1/1 | Complete    | 2026-02-24 |
| 5. Frame Materialization | 1/1 | Complete    | 2026-02-24 |
| 6. Signal Routing | 1/1 | Complete    | 2026-02-24 |
| 7. Interpretation and Adaptive Tiering | 0/TBD | Complete    | 2026-02-24 |
| 8. Embryonic Frame Discovery | 0/TBD | Complete    | 2026-02-24 |
| 9. Engine Orchestration | 0/TBD | Complete    | 2026-02-24 |
| 10. Benchmarks and Quality | 1/1 | Complete    | 2026-02-24 |
