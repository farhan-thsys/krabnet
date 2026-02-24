# Requirements: Krabnet

**Defined:** 2026-02-24
**Core Value:** When a signal arrives, decision-relevant context is already materialized — zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Core Infrastructure

- [x] **INFRA-01**: System defines core types (PropertyValue, PropertySet, Direction, Filter, HopSpec, Event, DiffTuple, InterpretationTier, FrameTier) shared across all modules
- [x] **INFRA-02**: String interner maps bidirectionally between String and u32 for property keys and type names at initialization
- [x] **INFRA-03**: Global monotonic epoch sequencer produces strictly increasing u64 epochs via AtomicU64 with SeqCst ordering
- [x] **INFRA-04**: Lock-free ring buffer with pre-allocated power-of-2 slots accepts events and returns assigned epochs
- [x] **INFRA-05**: Ring buffer correctly handles slot claiming, epoch assignment, and read-after-write with correct atomic ordering
- [x] **INFRA-06**: Ring buffer implements Send and Sync with documented safety invariants

### Graph Storage

- [x] **GRAPH-01**: In-memory property graph stores nodes with adjacency lists (outgoing + incoming edges on each node)
- [x] **GRAPH-02**: Node CRUD: add node (with type), remove node (cascading edge removal from neighbors), get node O(1)
- [x] **GRAPH-03**: Edge CRUD: add edge (updates both source outgoing and target incoming), remove edge (updates both nodes)
- [x] **GRAPH-04**: Directional neighbor queries filter by Direction (Outgoing/Incoming/Any) and optional edge type
- [x] **GRAPH-05**: Property upsert on nodes with interned u32 keys
- [x] **GRAPH-06**: Node count and edge count queries

### Differential MVCC

- [x] **DIFF-01**: DiffCollection stores differential tuples (payload, epoch, delta) with +1 for assertion and -1 for retraction
- [x] **DIFF-02**: Net delta computation per payload (sum of deltas for matching payloads) is mathematically exact
- [x] **DIFF-03**: Aggregate net delta (sum of all deltas across all tuples) tracks frame-level state
- [x] **DIFF-04**: Temporal snapshot returns payloads with positive net delta at or before a given epoch
- [x] **DIFF-05**: Current state returns snapshot at u64::MAX (effectively "now")
- [x] **DIFF-06**: Compaction groups tuples by payload below frontier epoch: annihilates net-zero, collapses survivors, warns on negative net
- [x] **DIFF-07**: Tuple count and emptiness queries for memory pressure monitoring

### Frame System

- [x] **FRAME-01**: Frame holds a multi-hop pattern (Vec<HopSpec>), anchor node, and DiffCollection of traversal paths
- [x] **FRAME-02**: Materialize performs DFS from anchor node following hop pattern, collecting all complete paths as +1 assertions
- [x] **FRAME-03**: Materialization filters by edge type, target node type, and property filter at each hop
- [x] **FRAME-04**: Apply delta (+1 or -1) to frame state with cached net_delta update
- [x] **FRAME-05**: Frame query returns current state (paths with positive net delta), incrementing query_count
- [x] **FRAME-06**: Frame snapshot returns historical state at a given epoch
- [x] **FRAME-07**: Frame compaction delegates to underlying DiffCollection
- [x] **FRAME-08**: Frame eviction clears state and sets tier to Cold; re-materialization restores state

### Signal Routing

- [x] **ROUTE-01**: Inverted index maps node IDs to sets of frame IDs containing that node
- [x] **ROUTE-02**: Inverted index maps (source_node, edge_type) pairs to sets of frame IDs
- [x] **ROUTE-03**: affected_frames returns deduplicated union of all frame IDs affected by a given Event
- [x] **ROUTE-04**: Frame registration adds to all relevant posting lists; unregistration removes from all

### Interpretation

- [x] **INTERP-01**: Tier 1 binary check detects when a frame's net delta changes from previous value
- [x] **INTERP-02**: Tier 2 structural analysis identifies completed and broken hops in frame paths at a given epoch

### Adaptive Tiering

- [x] **TIER-01**: Frame priority scoring uses weighted combination of query frequency, mutation rate, and recency decay
- [x] **TIER-02**: Tier recommendation classifies frames as Hot (>0.7), Warm, or Cold (<0.2) based on normalized score

### Embryonic Frame Discovery

- [ ] **EMBRYO-01**: Pattern templates define multi-hop patterns to watch for with configurable promotion threshold
- [ ] **EMBRYO-02**: decompose_frame generates all sub-patterns of length >= 2 from a full frame pattern
- [ ] **EMBRYO-03**: observe_edge detects when new edges extend anchor candidates' partial paths, updating bitvec completion
- [ ] **EMBRYO-04**: Auto-promotion triggers when candidate completion_ratio meets or exceeds template threshold
- [ ] **EMBRYO-05**: Stale candidate pruning removes candidates that haven't progressed within configurable epoch window
- [ ] **EMBRYO-06**: Max candidates cap per template prevents unbounded memory growth

### Engine Orchestration

- [ ] **ENGINE-01**: Ingest pipeline: push to ring buffer → apply to graph → query inverted index → maintain affected frames → run Tier 1 check
- [ ] **ENGINE-02**: EdgeAdded events trigger embryonic observe_edge; promotions auto-create new parked frames
- [ ] **ENGINE-03**: Frame registration materializes against current graph and registers in inverted index
- [ ] **ENGINE-04**: Compact all frames below a given frontier epoch
- [ ] **ENGINE-05**: Stats reporting returns node/edge/frame counts, tier distribution, tuple count, embryonic stats

### Testing

- [x] **TEST-01**: Differential tests verify assert/retract math, annihilation, double-assert, snapshots, compaction, negative delta warning
- [x] **TEST-02**: Ring buffer tests verify push/read, sequential epochs, wraparound, unwritten-returns-none
- [x] **TEST-03**: Graph store tests verify node/edge CRUD, cascading removal, directional neighbors, edge type filtering
- [x] **TEST-04**: Frame tests verify 2-hop materialization, no-path case, multiple paths, delta application, evict/rematerialize
- [x] **TEST-05**: Inverted index tests verify register/lookup, affected frames, shared node fan-out, unregister
- [ ] **TEST-06**: Embryonic tests verify template registration, decomposition, candidate creation, progressive completion, auto-promotion, pruning, cap
- [ ] **TEST-07**: Integration tests verify full pipeline, retraction pipeline, shared-node multi-frame, embryonic auto-promotion, compaction correctness
- [ ] **TEST-08**: Snapshot tests verify temporal consistency across frames at specific epochs

### Benchmarks

- [ ] **BENCH-01**: Criterion benchmarks for ingest_event, frame_query, inverted_index_lookup, tier1_check, embryonic_observe, compaction

### Quality

- [ ] **QUAL-01**: cargo test — zero failures
- [ ] **QUAL-02**: cargo bench — all benchmarks execute and produce numbers
- [ ] **QUAL-03**: cargo doc --no-deps — generates documentation without warnings
- [ ] **QUAL-04**: cargo clippy — zero warnings
- [ ] **QUAL-05**: Every public function has a doc comment; every module has a module-level doc comment

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Performance

- **PERF-01**: Async background compaction (move synchronous compaction to background thread)
- **PERF-02**: Multi-threaded event processing (multi-producer ring buffer)
- **PERF-03**: Incremental path extension for frame maintenance (replace re-traverse approach)
- **PERF-04**: Count-Min Sketch for query frequency tracking (replace raw counter)

### Testing

- **TEST-09**: loom-based concurrency testing for multi-producer scenarios
- **TEST-10**: Miri validation for unsafe Send/Sync implementations
- **TEST-11**: Property-based testing (proptest) for differential math

## Out of Scope

| Feature | Reason |
|---------|--------|
| Query language (Cypher, Gremlin, SPARQL) | PoC validates runtime physics, not query UX |
| Distributed execution | Single-process PoC; distributed is a fundamentally different architecture |
| Disk persistence | In-memory only; persistence is orthogonal to the differential MVCC hypothesis |
| Graph algorithms (PageRank, shortest path) | Frame-based traversal replaces traditional graph algorithms |
| External connectors (Kafka, gRPC) | PoC is a library crate, not a service |
| Web UI or visualization | Runtime only, no presentation layer |
| Nightly Rust features | Stable toolchain constraint is non-negotiable |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| INFRA-01 | Phase 1 | Complete |
| INFRA-02 | Phase 1 | Complete |
| INFRA-03 | Phase 2 | Complete |
| INFRA-04 | Phase 2 | Complete |
| INFRA-05 | Phase 2 | Complete |
| INFRA-06 | Phase 2 | Complete |
| GRAPH-01 | Phase 3 | Complete |
| GRAPH-02 | Phase 3 | Complete |
| GRAPH-03 | Phase 3 | Complete |
| GRAPH-04 | Phase 3 | Complete |
| GRAPH-05 | Phase 3 | Complete |
| GRAPH-06 | Phase 3 | Complete |
| DIFF-01 | Phase 4 | Complete |
| DIFF-02 | Phase 4 | Complete |
| DIFF-03 | Phase 4 | Complete |
| DIFF-04 | Phase 4 | Complete |
| DIFF-05 | Phase 4 | Complete |
| DIFF-06 | Phase 4 | Complete |
| DIFF-07 | Phase 4 | Complete |
| FRAME-01 | Phase 5 | Complete |
| FRAME-02 | Phase 5 | Complete |
| FRAME-03 | Phase 5 | Complete |
| FRAME-04 | Phase 5 | Complete |
| FRAME-05 | Phase 5 | Complete |
| FRAME-06 | Phase 5 | Complete |
| FRAME-07 | Phase 5 | Complete |
| FRAME-08 | Phase 5 | Complete |
| ROUTE-01 | Phase 6 | Complete |
| ROUTE-02 | Phase 6 | Complete |
| ROUTE-03 | Phase 6 | Complete |
| ROUTE-04 | Phase 6 | Complete |
| INTERP-01 | Phase 7 | Complete |
| INTERP-02 | Phase 7 | Complete |
| TIER-01 | Phase 7 | Complete |
| TIER-02 | Phase 7 | Complete |
| EMBRYO-01 | Phase 8 | Pending |
| EMBRYO-02 | Phase 8 | Pending |
| EMBRYO-03 | Phase 8 | Pending |
| EMBRYO-04 | Phase 8 | Pending |
| EMBRYO-05 | Phase 8 | Pending |
| EMBRYO-06 | Phase 8 | Pending |
| ENGINE-01 | Phase 9 | Pending |
| ENGINE-02 | Phase 9 | Pending |
| ENGINE-03 | Phase 9 | Pending |
| ENGINE-04 | Phase 9 | Pending |
| ENGINE-05 | Phase 9 | Pending |
| TEST-01 | Phase 4 | Complete |
| TEST-02 | Phase 2 | Complete |
| TEST-03 | Phase 3 | Complete |
| TEST-04 | Phase 5 | Complete |
| TEST-05 | Phase 6 | Complete |
| TEST-06 | Phase 8 | Pending |
| TEST-07 | Phase 9 | Pending |
| TEST-08 | Phase 9 | Pending |
| BENCH-01 | Phase 10 | Pending |
| QUAL-01 | Phase 10 | Pending |
| QUAL-02 | Phase 10 | Pending |
| QUAL-03 | Phase 10 | Pending |
| QUAL-04 | Phase 10 | Pending |
| QUAL-05 | Phase 10 | Pending |

**Coverage:**
- v1 requirements: 60 total
- Mapped to phases: 60
- Unmapped: 0

---
*Requirements defined: 2026-02-24*
*Last updated: 2026-02-24 after roadmap creation*
