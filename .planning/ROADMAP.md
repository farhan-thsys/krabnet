# Roadmap: Krabnet

## Milestones

- ✅ **v1.0 MVP** — Phases 1-10 (shipped 2026-02-24)
- ✅ **v2.0 Harden + Production** — Phases 11-15 (shipped 2026-02-26)
- 🚧 **v3.0 Tech Debt Closure + Incremental Path Extension** — Phases 16-21 (in progress)

## Phases

<details>
<summary>✅ v1.0 MVP (Phases 1-10) — SHIPPED 2026-02-24</summary>

- [x] Phase 1: Core Types and String Interning (1/1 plans) — completed 2026-02-24
- [x] Phase 2: Epoch Sequencer and Ring Buffer (1/1 plans) — completed 2026-02-24
- [x] Phase 3: Property Graph Storage (1/1 plans) — completed 2026-02-24
- [x] Phase 4: Differential MVCC Engine (1/1 plans) — completed 2026-02-24
- [x] Phase 5: Frame Materialization (1/1 plans) — completed 2026-02-24
- [x] Phase 6: Signal Routing (1/1 plans) — completed 2026-02-24
- [x] Phase 7: Interpretation and Adaptive Tiering (1/1 plans) — completed 2026-02-24
- [x] Phase 8: Embryonic Frame Discovery (1/1 plans) — completed 2026-02-24
- [x] Phase 9: Engine Orchestration (1/1 plans) — completed 2026-02-24
- [x] Phase 10: Benchmarks and Quality (1/1 plans) — completed 2026-02-24

</details>

<details>
<summary>✅ v2.0 Harden + Production (Phases 11-15) — SHIPPED 2026-02-26</summary>

- [x] Phase 11: Harden the Engine (3/3 plans) — completed 2026-02-24
- [x] Phase 12: Production Interface (4/4 plans) — completed 2026-02-24
- [x] Phase 13: Scale and Optimize (3/3 plans) — completed 2026-02-24
- [x] Phase 14: Wire Post-Ingest Pipeline (1/1 plans) — completed 2026-02-26
- [x] Phase 15: Harden MCP Binary (1/1 plans) — completed 2026-02-26

</details>

### 🚧 v3.0 Tech Debt Closure + Incremental Path Extension (In Progress)

**Milestone Goal:** Close all v2.0 tech debt (AnthropicClient, CompactionStats, MCP WAL) and replace full DFS re-traverse frame maintenance with incremental path extension for O(affected) updates.

- [x] **Phase 16: Tech Debt Closure** — Commit and verify already-built AnthropicClient, CompactionStats, and MCP WAL code (completed 2026-02-26)
- [x] **Phase 17: Re-Diff Baseline** — Wire frame maintenance into the ingest pipeline with full re-traverse + diff correctness oracle (completed 2026-02-26)
- [x] **Phase 18: Incremental Edge Addition** — PathExtender module with per-hop delta derivation for EdgeAdded events (completed 2026-02-26)
- [ ] **Phase 19: Incremental Edge and Node Removal** — Targeted path retraction for EdgeRemoved and NodeRemoved events
- [ ] **Phase 20: Incremental Property Change** — Filter re-evaluation for PropertyChanged events producing path assertions and retractions
- [ ] **Phase 21: Performance and Benchmarks** — Verify O(affected) scaling, Criterion benchmarks, stress test, regression gate

## Phase Details

### Phase 16: Tech Debt Closure
**Goal**: All v2.0 tech debt items are committed, tested, and available in the public API
**Depends on**: Phase 15
**Requirements**: DEBT-01, DEBT-02, DEBT-03, DEBT-04, DEBT-05, DEBT-06, DEBT-07
**Plans:** 1/1 plans complete
Plans:
- [ ] 16-01-PLAN.md — Write verification tests for all 7 DEBT items, run full test suite, commit
**Success Criteria** (what must be TRUE):
  1. AnthropicClient implementing LlmClient trait compiles and is exported from lib.rs, callable by downstream code
  2. krabnet-server binary detects ANTHROPIC_API_KEY env var at startup and logs whether real or mock LLM is active
  3. gRPC GetStats response includes compaction metrics (compactions_completed, tuples_before, tuples_after, total_compaction_time_us)
  4. MCP krabnet_stats tool response includes the same compaction metrics
  5. MCP binary persists ingest events to WAL and replays them on crash recovery startup

### Phase 17: Re-Diff Baseline
**Goal**: Frames stay in sync with the graph as it mutates, using full re-traverse + diff as the correctness baseline for all subsequent incremental phases
**Depends on**: Phase 16
**Requirements**: RDIF-01, RDIF-02, RDIF-03
**Success Criteria** (what must be TRUE):
  1. After any graph mutation routed to a frame, the frame's differential state matches what a fresh full DFS materialization would produce
  2. A correctness oracle test harness exists that compares incremental frame state against full re-traverse after every update and asserts exact match
  3. Frame maintenance runs on every ingest event that routes to at least one frame (not just at registration time)
**Plans:** 1/1 plans complete
Plans:
- [ ] 17-01-PLAN.md — Wire frame rematerialize into ingest pipeline + correctness oracle test harness

### Phase 18: Incremental Edge Addition
**Goal**: EdgeAdded events produce path deltas via targeted per-hop extension instead of full DFS re-traverse
**Depends on**: Phase 17
**Requirements**: IADD-01, IADD-02, IADD-03, IADD-04, IADD-05
**Success Criteria** (what must be TRUE):
  1. A new PathExtender module exists that takes an EdgeAdded event and affected frame references and returns path-level +1 deltas without full DFS
  2. Backward prefix resolution finds existing partial paths from anchor to the hop before the new edge
  3. Forward path extension traverses from the new edge endpoint through remaining hops to produce complete paths
  4. Incremental EdgeAdded produces identical frame state to the Phase 17 full re-traverse baseline (oracle verified for every test case)
**Plans:** 2/2 plans complete
Plans:
- [ ] 18-01-PLAN.md — Create PathExtender module with extend_edge_added, backward prefix, forward extension
- [ ] 18-02-PLAN.md — Wire PathExtender into engine ingest pipeline, extend oracle tests

### Phase 19: Incremental Edge and Node Removal
**Goal**: EdgeRemoved and NodeRemoved events retract affected paths via targeted -1 deltas without full DFS re-traverse
**Depends on**: Phase 18
**Requirements**: IREM-01, IREM-02, IREM-03, NDEL-01, NDEL-02, NDEL-03
**Success Criteria** (what must be TRUE):
  1. EdgeRemoved events identify and retract all materialized paths traversing the removed edge as -1 deltas
  2. NodeRemoved events capture edge adjacency via DeletionContext before graph mutation, then retract all paths through the removed node
  3. No ghost paths remain after any deletion event (oracle verified against full re-traverse for diamond graphs, multi-frame deletions, and cascade scenarios)
  4. Incremental removal produces identical frame state to full re-traverse baseline (oracle verified)
**Plans:** 1/2 plans executed
Plans:
- [ ] 19-01-PLAN.md — Add retract_edge_removed and retract_node_removed to path_extender module with unit tests
- [ ] 19-02-PLAN.md — Wire incremental removal dispatch into engine, DeletionContext, coalescer fix, oracle tests

### Phase 20: Incremental Property Change
**Goal**: PropertyChanged events incrementally re-evaluate hop filters, asserting newly-valid paths and retracting newly-invalid paths
**Depends on**: Phase 19
**Requirements**: PROP-01, PROP-02, PROP-03, PROP-04
**Success Criteria** (what must be TRUE):
  1. PropertyChanged events re-evaluate hop filters for all frames containing the affected node at any hop position
  2. Paths that no longer satisfy filters after the property change are retracted as -1 deltas
  3. Paths that newly satisfy filters after the property change are asserted as +1 deltas
  4. Incremental property change handling produces identical frame state to full re-traverse baseline (oracle verified)
**Plans**: TBD

### Phase 21: Performance and Benchmarks
**Goal**: Incremental path extension is verified to be O(affected) for localized mutations, benchmarked against full re-traverse, and regression-free
**Depends on**: Phase 20
**Requirements**: PERF-01, PERF-02, PERF-03, PERF-04, PERF-05
**Success Criteria** (what must be TRUE):
  1. Incremental extension cost scales with affected paths, not total frame size, for localized mutations (demonstrated via benchmark at multiple graph scales)
  2. Criterion benchmarks show incremental EdgeAdded and EdgeRemoved latency vs full re-traverse on multi-hop frames
  3. Stress test validates incremental correctness under sustained 50K+ events/sec with concurrent compaction
  4. All 180+ lib tests and 54+ doc-tests continue to pass with zero regressions
**Plans**: TBD

## Progress

**Execution Order:** Phase 16 -> 17 -> 18 -> 19 -> 20 -> 21

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Core Types and String Interning | v1.0 | 1/1 | Complete | 2026-02-24 |
| 2. Epoch Sequencer and Ring Buffer | v1.0 | 1/1 | Complete | 2026-02-24 |
| 3. Property Graph Storage | v1.0 | 1/1 | Complete | 2026-02-24 |
| 4. Differential MVCC Engine | v1.0 | 1/1 | Complete | 2026-02-24 |
| 5. Frame Materialization | v1.0 | 1/1 | Complete | 2026-02-24 |
| 6. Signal Routing | v1.0 | 1/1 | Complete | 2026-02-24 |
| 7. Interpretation and Adaptive Tiering | v1.0 | 1/1 | Complete | 2026-02-24 |
| 8. Embryonic Frame Discovery | v1.0 | 1/1 | Complete | 2026-02-24 |
| 9. Engine Orchestration | v1.0 | 1/1 | Complete | 2026-02-24 |
| 10. Benchmarks and Quality | v1.0 | 1/1 | Complete | 2026-02-24 |
| 11. Harden the Engine | v2.0 | 3/3 | Complete | 2026-02-24 |
| 12. Production Interface | v2.0 | 4/4 | Complete | 2026-02-24 |
| 13. Scale and Optimize | v2.0 | 3/3 | Complete | 2026-02-24 |
| 14. Wire Post-Ingest Pipeline | v2.0 | 1/1 | Complete | 2026-02-26 |
| 15. Harden MCP Binary | v2.0 | 1/1 | Complete | 2026-02-26 |
| 16. Tech Debt Closure | 1/1 | Complete    | 2026-02-26 | - |
| 17. Re-Diff Baseline | 1/1 | Complete    | 2026-02-26 | - |
| 18. Incremental Edge Addition | 2/2 | Complete    | 2026-02-26 | - |
| 19. Incremental Edge and Node Removal | 1/2 | In Progress|  | - |
| 20. Incremental Property Change | v3.0 | 0/? | Not started | - |
| 21. Performance and Benchmarks | v3.0 | 0/? | Not started | - |
