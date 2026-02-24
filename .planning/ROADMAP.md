# Roadmap: Krabnet

## Overview

Krabnet v1.0 (Phases 1-10) built the core engine bottom-up: types → ingestion → storage → MVCC → frames → routing → interpretation → embryonic → orchestration → quality. All 60 requirements shipped with 144 tests and 6 benchmarks.

Milestone v2.0 (Phases 11-13) hardens the engine for concurrent load, adds production interfaces (gRPC, MCP, WAL, Tier 3 LLM), and replaces PoC data structures with enterprise-grade alternatives. Phases execute strictly in order: 11 → 12 → 13.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

### Milestone v1.0 (Complete)

- [x] **Phase 1: Core Types and String Interning** - Shared type definitions and bidirectional string-to-u32 interner (completed 2026-02-24)
- [x] **Phase 2: Epoch Sequencer and Ring Buffer** - Lock-free ingestion pipeline with monotonic epoch assignment (completed 2026-02-24)
- [x] **Phase 3: Property Graph Storage** - In-memory adjacency-on-node property graph (completed 2026-02-24)
- [x] **Phase 4: Differential MVCC Engine** - Mathematically exact +1/-1 multiset collection (completed 2026-02-24)
- [x] **Phase 5: Frame Materialization** - Parked traversers with multi-hop DFS materialization (completed 2026-02-24)
- [x] **Phase 6: Signal Routing** - Inverted index for O(affected) event routing (completed 2026-02-24)
- [x] **Phase 7: Interpretation and Adaptive Tiering** - Two-tier interpretation and priority scoring (completed 2026-02-24)
- [x] **Phase 8: Embryonic Frame Discovery** - Autonomous pattern detection with bitvec tracking (completed 2026-02-24)
- [x] **Phase 9: Engine Orchestration** - Full ingest-update-maintain-interpret pipeline (completed 2026-02-24)
- [x] **Phase 10: Benchmarks and Quality** - 6 benchmarks, 144 tests, zero clippy warnings (completed 2026-02-24)

### Milestone v2.0 (Current)

- [x] **Phase 11: Harden the Engine** - Background compaction, multi-threaded frame eval, mutation coalescing, fan-out limits, hysteresis, stress tests (completed 2026-02-24)
- [x] **Phase 12: Production Interface** - gRPC server (8 RPCs), MCP server (5 tools), Tier 3 LLM, WAL persistence, binary entry points (completed 2026-02-24)
- [x] **Phase 13: Scale and Optimize** - Set-Trie inverted index, Count-Min Sketch, trunk/leaf detection, buffer pool, learned template weighting, enterprise benchmarks (completed 2026-02-24)

## Phase Details

### Phase 11: Harden the Engine
**Goal**: Make the engine survive realistic concurrent load. Address differential memory exhaustion, super-node fan-out storms, and frame prioritizer thrashing.
**Depends on**: Phase 10 (all v1.0 phases complete)
**Requirements**: COMPACT-01, COMPACT-02, COMPACT-03, COMPACT-04, EVAL-01, EVAL-02, EVAL-03, COALESCE-01, COALESCE-02, COALESCE-03, FANOUT-01, FANOUT-02, HYST-01, HYST-02, HYST-03, TEST-09, TEST-10, TEST-11, TEST-12, TEST-13, TEST-14, TEST-15, TEST-16, TEST-17, BENCH-02, QUAL-06, QUAL-07
**Success Criteria** (what must be TRUE):
  1. Background compaction worker fires automatically when tuple_count exceeds threshold, compacts without blocking hot path, and frame queries remain correct after compaction
  2. Frame evaluation fans out to thread pool; 100 frames with 10K events produce correct state under concurrent evaluation
  3. Mutation coalescer deduplicates same-node mutations within epoch window (100 mutations → 1 trigger) while preserving different-node mutations
  4. Fan-out limit caps immediate evaluations at MAX_FANOUT; excess frames queued in DeferredEvalQueue sorted by priority
  5. Hysteresis prevents tier thrashing: oscillating scores keep frame in Warm, not oscillating Hot↔Cold
  6. Stress test sustains >50K events/sec for 10 seconds with stable memory (no monotonic increase)
  7. All Phase 1-10 tests still pass; cargo clippy zero warnings
  8. bench_concurrent_ingest benchmark runs and produces throughput numbers
**Plans**: 3 plans (2 waves)

Plans:
- [ ] 11-01-PLAN.md — Background compaction worker + multi-threaded frame evaluation (Wave 1)
- [ ] 11-02-PLAN.md — Mutation coalescing + fan-out limits + hysteresis (Wave 1)
- [ ] 11-03-PLAN.md — Stress test suite + concurrent ingest benchmark + quality gates (Wave 2)

### Phase 12: Production Interface
**Goal**: Make the engine accessible to external systems and AI agents. Add persistence for crash recovery. Integrate Tier 3 LLM interpretation.
**Depends on**: Phase 11
**Requirements**: GRPC-01, GRPC-02, GRPC-03, GRPC-04, MCP-01, MCP-02, MCP-03, TIER3-01, TIER3-02, TIER3-03, TIER3-04, WAL-01, WAL-02, WAL-03, EMBRYO-07, BIN-01, BIN-02, TEST-18, TEST-19, TEST-20, TEST-21, TEST-22, TEST-23, TEST-24, QUAL-08
**Success Criteria** (what must be TRUE):
  1. gRPC server starts and handles all 8 RPC methods: IngestEvent, RegisterFrame, QueryFrame, SubscribeFrame, ListFrames, EvictFrame, RegisterEmbryonicTemplate, GetStats
  2. MCP server responds correctly to initialize + tools/list + tools/call with 5 tools
  3. Tier 3 LLM worker processes Tier 2 results via bounded channel; mock LLM called with graph-aware prompt; engine never blocks on full channel
  4. WAL enables crash recovery: ingest events → drop engine → replay WAL → state matches
  5. Frame registration auto-decomposes patterns into embryonic templates
  6. krabnet-server binary compiles and starts (gRPC + compaction + Tier 3 + WAL)
  7. krabnet-mcp binary compiles and starts (MCP stdio server)
  8. All Phase 1-11 tests still pass; cargo clippy zero warnings
**Plans**: 4 plans (3 waves)

Plans:
- [x] 12-01-PLAN.md — Phase 12 deps + Protobuf schema + gRPC server with 8 RPCs (Wave 1)
- [ ] 12-02-PLAN.md — MCP JSON-RPC server + krabnet-mcp binary (Wave 2)
- [ ] 12-03-PLAN.md — Tier 3 LLM worker + prompt serialization + bounded channel (Wave 2)
- [ ] 12-04-PLAN.md — WAL persistence + embryonic auto-decomposition + krabnet-server binary + quality (Wave 3)

### Phase 13: Scale and Optimize
**Goal**: Replace PoC data structures with production-grade alternatives. Hit enterprise performance targets.
**Depends on**: Phase 12
**Requirements**: SETTRIE-01, SETTRIE-02, CMS-01, CMS-02, TRUNK-01, TRUNK-02, BUFPOOL-01, BUFPOOL-02, LEARN-01, LEARN-02, TEST-25, TEST-26, TEST-27, TEST-28, TEST-29, TEST-30, TEST-31, BENCH-03, BENCH-04, BENCH-05, BENCH-06, BENCH-07, QUAL-09, QUAL-10
**Success Criteria** (what must be TRUE):
  1. Set-Trie passes correctness tests and benchmark shows improvement over HashMap for inverted index lookups
  2. Count-Min Sketch accuracy within expected bounds (no underestimate, overestimate <10% for heavy hitters)
  3. Trunk detection correctly identifies shared structural spines; trunk frames pinned to Hot
  4. Buffer pool allocates, frees, and evicts in correct priority order (Cold → Warm → Hot)
  5. Learned template weighting ranks successful templates higher; low-success templates deactivated
  6. Enterprise benchmarks run at scale: 100K nodes, 1M edges, 500 frames, report throughput and latency
  7. All Phase 1-12 tests still pass; cargo clippy zero warnings
  8. README.md reflects full architecture including all v2.0 features
**Plans**: 3 plans (2 waves)

Plans:
- [ ] 13-01-PLAN.md — Set-Trie inverted index + Count-Min Sketch data structures + InvertedIndex integration + CMS prioritizer (Wave 1)
- [ ] 13-02-PLAN.md — Trunk/leaf detection with Hot pinning + custom buffer pool with graph-aware eviction (Wave 1)
- [ ] 13-03-PLAN.md — Learned template weighting + enterprise benchmarks + README + quality gates (Wave 2)

## Progress

**Execution Order:**
v1.0: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 (complete)
v2.0: 11 → 12 → 13

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Core Types and String Interning | 1/1 | Complete | 2026-02-24 |
| 2. Epoch Sequencer and Ring Buffer | 1/1 | Complete | 2026-02-24 |
| 3. Property Graph Storage | 1/1 | Complete | 2026-02-24 |
| 4. Differential MVCC Engine | 1/1 | Complete | 2026-02-24 |
| 5. Frame Materialization | 1/1 | Complete | 2026-02-24 |
| 6. Signal Routing | 1/1 | Complete | 2026-02-24 |
| 7. Interpretation and Adaptive Tiering | 1/1 | Complete | 2026-02-24 |
| 8. Embryonic Frame Discovery | 1/1 | Complete | 2026-02-24 |
| 9. Engine Orchestration | 1/1 | Complete | 2026-02-24 |
| 10. Benchmarks and Quality | 1/1 | Complete | 2026-02-24 |
| 11. Harden the Engine | 3/3 | Complete    | 2026-02-24 |
| 12. Production Interface | 4/4 | Complete    | 2026-02-24 |
| 13. Scale and Optimize | 3/3 | Complete    | 2026-02-24 |
