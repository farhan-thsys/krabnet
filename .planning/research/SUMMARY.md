# Project Research Summary

**Project:** krabnet
**Domain:** Streaming graph runtime with differential MVCC and pre-materialized traversal (Rust)
**Researched:** 2026-02-24
**Confidence:** HIGH (stack and architecture), MEDIUM (features — novel combination unvalidated at scale)

## Executive Summary

Krabnet occupies a well-defined but genuinely novel niche: applying differential dataflow (+1/-1 delta) semantics to a property graph runtime that pre-materializes traversal results rather than computing them at query time. The core insight is that AI agent context graphs are read far more than they are written, and the latency-sensitive moment is not graph mutation but signal arrival — when a signal arrives, context must already be fully assembled. This read-path optimization dictates the entire architecture: frames (parked traversers) are materialized at write time and maintained incrementally on mutations, not recomputed on demand. This is a well-understood engineering direction (differential dataflow, IVM for property graphs, Materialize-style incremental views) but no existing system combines property-graph traversal semantics with differential delta math and lock-free streaming ingestion in a single-crate Rust library.

The recommended approach is Rust stable (1.85+) with a minimal dependency set: `crossbeam` for epoch-based memory reclamation and cache-padded atomics, `bitvec` for compact completion tracking in embryonic frame discovery, and `criterion` as a dev-only benchmarking harness. The implementation follows a 13-module flat structure with a strict build-dependency order (types → interner → sequencer/ring-buffer → graph store → differential engine → frames → routing → orchestration). This layering is not arbitrary — each module can be proven correct in isolation before the next layer depends on it, which is essential for a system with safety-critical `unsafe` code throughout.

The dominant risk is silent correctness failure: differential math errors, incorrect atomic memory orderings, and compaction frontier desynchronization all produce wrong results without panicking, and they are invisible on x86 but detectable under Miri, loom, and property-based tests. The mitigation strategy is to treat Phase 1 (ring buffer), Phase 2 (property graph), and Phase 3 (MVCC engine) as correctness-first phases with mandatory verification gates (Miri, loom, property tests) before any dependent module is built. Performance optimization, embryonic frame discovery, and adaptive tiering all live on top of a correct foundation.

## Key Findings

### Recommended Stack

The entire implementation uses Rust stable only. The `SyncUnsafeCell` type is nightly-only (tracking issue #95439) — the correct stable pattern is a manual `unsafe impl Sync` wrapper around `std::cell::UnsafeCell` with explicit `// SAFETY:` documentation. Atomic memory ordering follows a single rule: use the weakest ordering provably correct — `Release` on data-publishing stores, `Acquire` on data-consuming loads, `Relaxed` only for counters with no data dependencies, `SeqCst` almost never. All ring buffer slots and adjacency structures are pre-allocated at startup; `Vec::push` is banned on the hot path after initialization.

**Core technologies:**
- Rust stable 1.85+ — zero-cost abstractions, ownership prevents data races at compile time, no GC pauses
- `std::sync::atomic` (stable) — all lock-free coordination via `AtomicU64`/`AtomicBool`/`AtomicUsize`
- `std::cell::UnsafeCell` (stable) — the only legal interior mutability primitive for shared mutable state
- `crossbeam` 0.8.x — epoch-based deferred reclamation (`crossbeam-epoch`) + `CachePadded` false-sharing prevention
- `bitvec` 1.0.x — compact bit-vector completion tracking for embryonic frame discovery; `BitVec<usize, Lsb0>` is fastest configuration
- `criterion` 0.8.x (dev only) — statistical benchmarking for ring buffer throughput, graph traversal latency, and differential compaction

### Expected Features

All research was done against what a streaming graph runtime with differential MVCC must provide. The feature set decomposes cleanly into a mandatory PoC core and a post-validation set.

**Must have (table stakes — PoC v1):**
- Ring buffer with monotonic epoch sequencing — the lock-free ingestion path; without it nothing enters the system
- In-memory property graph with topological indexing — adjacency-on-node for read locality; typed nodes/edges with interned property keys
- Differential MVCC engine (+1/-1 deltas, compaction, temporal snapshots) — the mathematical core; must be correct before anything else is built on it
- Frame materialization with multi-hop patterns — pre-materialized traversal results anchored to nodes; cold-start via DFS, then incremental
- Signal-to-frame routing via inverted index — O(affected) event routing via posting lists; O(all-frames) scanning is a correctness anti-pattern
- String interning — intern at startup, use u32 IDs on hot path; required by zero-allocation constraint
- Top-level Engine struct — wires all components into a coherent ingest-update-maintain-interpret loop

**Should have (competitive differentiators — v1.x):**
- Two-tier interpretation — binary delta-sum gate (O(1)) before expensive structural path analysis; add when benchmarks show structural analysis bottleneck
- Adaptive frame tiering (hot/warm/cold) — scoring function over query frequency, mutation rate, recency; add when frame count creates measurable memory pressure
- Embryonic Frame Discovery — autonomous pattern detection from mutation stream with bitvec completion tracking; add once delta correctness is validated

**Defer (v2+):**
- Async background compaction — synchronous compaction interface is already isolated; swap when ingestion throughput demands it
- Multi-producer ring buffer — atomics are already correct for this; enabling it is configuration, not redesign
- Snapshot export/serialization — for restart and inspection; in-memory only for PoC
- External connector adapters (Kafka, etc.) — thin wrappers outside the crate; ring buffer API is the integration point

### Architecture Approach

The architecture is a layered pipeline with strict unidirectional dependencies: Foundation → Ingestion → Storage → Maintenance → Interpretation → Orchestration. This maps to 13 modules in a flat `src/` layout. The design correctly separates the graph store (current state, optimized for adjacency traversal) from the differential engine (delta history, optimized for epoch-ordered compaction) — conflating these is the dominant architectural anti-pattern in incremental graph systems. The inverted index (entity → affected frames) is the key scalability mechanism: without it, every graph mutation requires scanning all frames.

**Major components:**
1. `types.rs` — newtypes for NodeId/EdgeId/TypeId/Epoch/Delta; zero-cost foundation for the entire crate
2. `interner.rs` + `sequencer.rs` + `ring_buffer.rs` — ingestion pipeline: interning at boundary, monotonic epoch stamping, lock-free buffering
3. `graph_store.rs` + `differential.rs` — dual-store storage layer: current state (adjacency-on-node slab) + full MVCC history (+1/-1 triples)
4. `frame.rs` + `inverted_index.rs` — maintenance layer: parked traversers + efficient signal routing to affected frames only
5. `frame_prioritizer.rs` + `interpreter.rs` + `embryonic.rs` — interpretation layer: hot/warm/cold tiering, two-tier signal interpretation, autonomous pattern discovery
6. `engine.rs` + `lib.rs` — orchestration: top-level pipeline loop and public API surface

### Critical Pitfalls

1. **Unsound `unsafe impl Send/Sync` on `UnsafeCell`-containing types** — Every `unsafe impl` must have a `// SAFETY:` comment documenting the specific synchronization mechanism. Run `cargo +nightly miri test` to detect violations. Establish this pattern in Phase 1 (ring buffer) and enforce it for all subsequent modules.

2. **Incorrect atomic memory ordering (Relaxed where Acquire/Release needed)** — x86's total store order masks these bugs; they surface on ARM or under `loom`. Rule: every store that "publishes" data uses `Release`; every load that "consumes" published data uses `Acquire`. Comment every atomic with its pair: `// Release: pairs with Acquire in consumer_read()`. Add loom tests in Phase 1.

3. **Differential math edge cases (negative multiplicity, premature annihilation, compaction frontier desync)** — Use `i64` for multiplicities, assert `>= 0` after compaction, never advance the compaction frontier past the oldest active snapshot epoch. Maintain an active snapshots registry. Test with interleaved snapshots and compaction before building frame materialization on top.

4. **Graph adjacency inconsistency on node/edge removal** — Adjacency-on-node means every edge has two representations (source outgoing, target incoming). Removal must be two-phase. Add generation counters to node/edge slots. Assert adjacency symmetry in debug builds after every mutation.

5. **Zero-allocation hot path vs. Rust ownership** — Use index-based arena pattern throughout: functions return NodeId/EdgeId/FrameId (u64 indices), never `&Node`/`&Edge`. Pre-allocate all Vecs at startup. Add a counting global allocator integration test in CI from Phase 1 onward.

## Implications for Roadmap

The architecture research provides an explicit build-dependency order. The pitfalls research identifies mandatory verification gates per phase. These two sources, combined with the feature dependency graph, produce the following strongly recommended phase structure.

### Phase 1: Foundation and Ingestion Infrastructure
**Rationale:** `types.rs`, `interner.rs`, `sequencer.rs`, and `ring_buffer.rs` have zero external module dependencies and are prerequisites for every subsequent phase. The ring buffer is where the highest-density `unsafe` code lives (atomic ordering, `UnsafeCell`, ABA protection). Getting this right first establishes safety patterns and the counting-allocator test harness used in all later phases.
**Delivers:** Lock-free SPSC ring buffer with monotonic epoch sequencing, string interning, core type definitions, and criterion benchmark scaffolding.
**Addresses:** Ring buffer ingestion (P1 feature), string interning (P1 feature), zero-allocation hot path constraint.
**Avoids:** Atomic ordering errors (loom tests), ABA problem (64-bit monotonic counters), unsound Send/Sync (Miri), zero-alloc violations (counting allocator CI).
**Verification gate:** `cargo +nightly miri test` passes; loom tests cover all producer/consumer interleavings; counting allocator reports zero hot-path allocations.

### Phase 2: Property Graph Storage
**Rationale:** The graph store is the data structure that every higher-level component (differential engine, frames, inverted index) reads and writes. Adjacency consistency must be proven correct in isolation before frame traversal depends on it. Graph correctness bugs are silent and propagate upward.
**Delivers:** In-memory property graph with slab-allocated nodes/edges, adjacency-on-node (outgoing/incoming), type-based secondary indexes, and two-phase removal with generation counters.
**Addresses:** Property graph model (P1 feature), topological indexing (P1 feature).
**Avoids:** Graph adjacency inconsistency (two-phase removal, generation counters, debug-mode symmetry assertion).
**Verification gate:** All removal test topologies pass (leaf, hub, bidirectional, cycle member); adjacency symmetry assertion never fires in debug mode.

### Phase 3: Differential MVCC Engine
**Rationale:** This is the mathematical heart of Krabnet. Frame materialization (Phase 4) is entirely dependent on correct delta math and compaction. Building frames on top of incorrect differential semantics produces wrong results that are impossible to distinguish from correct results without a proven-correct differential layer beneath. Treat this as a pure math correctness phase — no performance optimization.
**Delivers:** Differential collection of `(tuple, epoch, +1/-1)` triples, epoch-based compaction with frontier tracking, temporal snapshot reads, and active snapshot registry.
**Addresses:** Differential MVCC engine (P1 feature), compaction (table stakes), temporal snapshots (table stakes).
**Avoids:** Differential math edge cases (negative multiplicity, premature annihilation), compaction frontier desync (active snapshot registry), wrong multiplicity types (i64 not u64).
**Verification gate:** Property-based tests with random assertion/retraction sequences always produce non-negative multiplicities after compaction; snapshot-interleaved-with-compaction tests pass at all intermediate versions.

### Phase 4: Frame Materialization and Signal Routing
**Rationale:** With the correct graph store and differential engine in place, the core value proposition can be built: parked traversers with incremental maintenance. The inverted index is built alongside frames because it has no utility without frames to route signals to. The DFS traversal correctness pitfalls are phase-local and can be fully addressed here with topology test fixtures.
**Delivers:** Frame struct (anchor + hop pattern + materialized result set + delta log), cold-start DFS traversal, incremental re-traversal on mutation, inverted index with posting lists, basic signal-to-frame routing.
**Addresses:** Frame materialization (P1 feature), signal-to-frame routing via inverted index (P1 feature).
**Avoids:** DFS traversal errors (per-path vs global visited, cycle termination, depth caps), DFS tested on diamond/cycle/disconnected topologies.
**Verification gate:** Frame materialization produces identical results via cold-start DFS and via incremental update from the same starting graph state on all test topologies.

### Phase 5: Top-Level Engine and Integration
**Rationale:** The engine.rs orchestrator and lib.rs public API surface are integration work that cannot happen before all components are individually proven. Once all components exist and are correct, wiring them into the ingest-update-maintain-interpret loop is straightforward. This phase also delivers the first end-to-end benchmarks.
**Delivers:** `Engine` struct owning all subsystems, full pipeline loop (drain ring buffer → apply graph mutation → record delta → route to frames → re-traverse affected frames → record frame deltas), public API (`lib.rs`), and end-to-end criterion benchmarks.
**Addresses:** Engine struct / top-level wiring (P1 feature), zero-allocation hot path validation (end-to-end).
**Avoids:** Recomputing all frames on every mutation (inverted index in place), string-based hot path (interning in place from Phase 1).
**Verification gate:** End-to-end throughput benchmark is runnable; counting allocator asserts zero allocations through full pipeline; all Phase 1-4 test suites still green.

### Phase 6: Interpretation, Tiering, and Embryonic Discovery
**Rationale:** These three components (two-tier interpretation, adaptive tiering, embryonic discovery) are v1.x features that add value but are not required to prove the core hypothesis. They are grouped together because they all build on top of the fully functional Phase 5 engine, and embryonic discovery requires confidence in delta correctness before autonomous frame creation is safe.
**Delivers:** Frame prioritizer with hot/warm/cold scoring, two-tier interpreter (binary delta-sum gate + structural path analysis), embryonic discovery engine with bitvec completion tracking and auto-promotion.
**Addresses:** Two-tier interpretation (P2 feature), adaptive frame tiering (P2 feature), embryonic frame discovery (P2 feature).
**Avoids:** Bitvec off-by-one in embryonic discovery (HopIndex newtype, boundary tests), eager interpretation of cold frames (prioritizer gate), bitvec pool pre-allocation (counting allocator CI).
**Verification gate:** Embryonic candidates do not promote when N-1 of N hops are complete; hot frames are interpreted on every cycle; cold frames are not.

### Phase Ordering Rationale

- The build-dependency DAG from ARCHITECTURE.md (`types → interner/sequencer/ring-buffer → graph-store → differential → frame → inverted-index → frame-prioritizer/embryonic → interpreter → engine → lib`) directly maps to Phases 1-6.
- The pitfall-to-phase mapping from PITFALLS.md confirms Phase 1 is the highest-risk phase (3 of 9 critical pitfalls must be addressed here) and justifies mandatory loom + Miri verification before proceeding to Phase 2.
- The feature dependency graph from FEATURES.md confirms that embryonic discovery, adaptive tiering, and two-tier interpretation have no unresolved dependencies from Phase 5 onward, validating grouping them into a single Phase 6.
- Re-traversal for frame maintenance (Phase 4) is deliberately the simple "correctness over performance" implementation. The interface is isolated for future incremental path extension, but this is explicitly deferred.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 3 (MVCC Engine):** The compaction frontier semantics and active snapshot registry design are complex enough that a short research spike on differential-dataflow's `TraceHandle`/`TraceReader` frontier management is recommended before coding. The known Issue #242 bug in the reference implementation should be read carefully.
- **Phase 4 (Frame Materialization):** Multi-hop directed DFS with correct per-path vs reachability semantics has non-obvious edge cases. The IVM for property graphs literature (ACM 2018, MV4PG 2024) has worked examples of correct delta propagation through multi-hop patterns that are worth reviewing before coding the incremental update path.
- **Phase 6 (Embryonic Discovery):** There is no reference implementation of autonomous pattern discovery in a streaming graph context. The EPDA algorithm is the closest analog but operates on event content, not graph structure. This phase will require more implementation invention than the others.

Phases with standard patterns (skip research):
- **Phase 1 (Foundation/Ingestion):** SPSC ring buffer with Acquire/Release is textbook-documented (Mara Bos, Ferrous Systems, LMAX Disruptor). The patterns are fully specified in STACK.md with implementation-ready code.
- **Phase 2 (Property Graph):** Slab-indexed adjacency-on-node storage with generation counters is established practice in ECS frameworks (bevy, hecs) and well-documented in Rust graph-handling literature.
- **Phase 5 (Engine Integration):** This is wiring work, not research work. All components are individually researched; integration follows the dependency graph mechanically.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All core technologies are well-documented stable Rust. Every code pattern in STACK.md is drawn from authoritative sources (Mara Bos, Rustonomicon, Materialize, Ferrous Systems). No speculative choices. |
| Features | MEDIUM | Table-stakes features are well-validated against DD, Materialize, Memgraph, and IVM literature. The three differentiator features (embryonic discovery, adaptive tiering, two-tier interpretation) are domain-specific to Krabnet's AI context use case and have no direct reference implementations to validate against. |
| Architecture | HIGH | The 13-module layered architecture follows standard differential dataflow and IVM patterns. The Netflix real-time graph and Materialize architecture confirm the ingestion/storage/maintenance/serving decomposition. The novel components (embryonic, two-tier) are domain additions, not replacements for standard patterns. |
| Pitfalls | HIGH | All 9 critical pitfalls are drawn from authoritative sources (Rustonomicon, Mara Bos, crossbeam docs, differential-dataflow Issue #242, Materialize engineering blog). The recovery costs and verification strategies are specific and actionable. |

**Overall confidence:** HIGH for the build approach; MEDIUM for the novel differentiators (embryonic discovery, frame tiering) which require implementation validation.

### Gaps to Address

- **Embryonic Discovery semantics:** The exact definition of a "pattern template" and the completion threshold algorithm are not drawn from any reference system. These require explicit design decisions during Phase 6 planning. Recommendation: treat the embryonic module as a research spike with a narrow scope definition before implementation begins.
- **Frame delta propagation for multi-hop patterns:** The incremental re-traversal strategy (re-traverse full pattern from anchor on mutation) is the "correctness-first" choice. The research confirms this is O(hops * edges) per affected frame, which is acceptable for PoC but will be a bottleneck at scale. The interface isolation is in place; the upgrade path to incremental path extension needs a design document before Phase 4 is complete.
- **Adaptive tiering scoring function:** FEATURES.md describes the scoring function in terms of "query frequency, mutation rate, recency" but gives no specific formula or threshold values. This requires empirical calibration against benchmark data. Treat initial values as configurable constants rather than hardcoded heuristics.
- **Compaction frequency policy:** PITFALLS.md identifies unbounded delta history as a critical risk but the research does not specify a compaction trigger policy (every N events? every N ms? at explicit epoch boundaries?). This is an implementation decision that must be made in Phase 3 and documented as a configuration parameter.

## Sources

### Primary (HIGH confidence)
- [Mara Bos: Rust Atomics and Locks, Chapter 3](https://mara.nl/atomics/memory-ordering.html) — atomic ordering semantics, Acquire/Release protocol
- [The Rustonomicon: Send and Sync](https://doc.rust-lang.org/nomicon/send-and-sync.html) — unsafe impl correctness rules
- [Ferrous Systems: Lock-Free Ring Buffer](https://ferrous-systems.com/blog/lock-free-ring-buffer/) — SPSC ring buffer design with Rust atomics
- [Differential Dataflow (GitHub)](https://github.com/TimelyDataflow/differential-dataflow) — reference implementation, compaction semantics, arrangements
- [Materialize: Building Differential Dataflow from Scratch](https://materialize.com/blog/differential-from-scratch/) — differential collection model explained
- [Materialize: Architecture Blog](https://materialize.com/blog/architecture/) — production incremental computation system architecture
- [Netflix Real-Time Distributed Graph](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-1-ingesting-and-processing-data-80113e124acc) — streaming graph: ingestion/processing/storage/serving layers
- [differential-dataflow Issue #242](https://github.com/TimelyDataflow/differential-dataflow/issues/242) — compaction frontier desync bug in reference implementation
- [crossbeam-utils CachePadded docs](https://docs.rs/crossbeam-utils/latest/crossbeam_utils/struct.CachePadded.html) — false sharing prevention
- [bitvec crate docs](https://docs.rs/bitvec/latest/bitvec/) — BitVec type parameters and usage
- [matklad: Fast Simple Rust Interner](https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html) — string interning pattern
- [SyncUnsafeCell tracking issue #95439](https://github.com/rust-lang/rust/issues/95439) — confirms nightly-only status
- [Loom: Concurrency permutation testing](https://docs.rs/loom/latest/loom/) — atomic ordering verification

### Secondary (MEDIUM confidence)
- [MV4PG: Materialized Views for Property Graphs (arXiv 2024)](https://arxiv.org/abs/2411.18847) — graph-specific materialized views; closest academic analog to Krabnet frames; template-based maintenance rather than differential
- [IVM for Property Graph Queries (ACM 2018)](https://dl.acm.org/doi/abs/10.1145/3183713.3183724) — incremental view maintenance for property graph traversals; 28-100x speedup over recomputation
- [VeilGraph: Incremental Graph Stream Processing](https://journalofbigdata.springeropen.com/articles/10.1186/s40537-022-00565-8) — streaming graph processing patterns
- [Elasticsearch Data Tiers](https://www.elastic.co/docs/manage-data/lifecycle/data-tiers) — hot/warm/cold tiering pattern (different domain, established concept)
- [kmdreko: Simple Lock-Free Ring Buffer](https://kmdreko.github.io/posts/20191003/a-simple-lock-free-ring-buffer/) — ring buffer correctness patterns
- [The Rust Performance Book: Heap Allocations](https://nnethercote.github.io/perf-book/heap-allocations.html) — pre-allocation patterns

### Tertiary (LOW confidence)
- [EPDA: Emergent Pattern Detection Algorithm](https://hal.science/hal-02558083/document) — closest analog to embryonic discovery; operates on event content not graph structure; requires validation that concepts transfer
- [Incremental Pattern Discovery on Streams, Graphs and Tensors (CMU 2007)](http://reports-archive.adm.cs.cmu.edu/anon/2007/CMU-CS-07-149.pdf) — academic foundation for graph stream pattern discovery; foundational but dated

---
*Research completed: 2026-02-24*
*Ready for roadmap: yes*
