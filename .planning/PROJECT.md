# Krabnet

## What This Is

Krabnet is a streaming graph runtime with differential MVCC that pre-materializes graph traversal results for AI agent context systems. Instead of querying a graph when a signal arrives, the system pre-computes ("parks") graph traversal results and updates them incrementally using differential dataflow semantics (+1/-1 deltas). It also autonomously discovers emerging patterns in the mutation stream (Embryonic Frame Discovery). This is a single Rust crate — no external graph dependencies.

## Core Value

When a signal arrives, decision-relevant context is already materialized — zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Lock-free ring buffer for event ingestion with monotonic epoch sequencer
- [ ] In-memory property graph with topological indexing (adjacency on nodes)
- [ ] Differential MVCC engine with +1/-1 multiset semantics, compaction, and temporal snapshots
- [ ] Parked traverser (frame) system with multi-hop pattern materialization and incremental delta maintenance
- [ ] Signal-to-frame routing via inverted index (node and edge posting lists)
- [ ] Adaptive frame tiering (hot/warm/cold) with query frequency, mutation rate, and recency scoring
- [ ] Two-tier interpretation: Tier 1 binary delta-sum check, Tier 2 structural path analysis
- [ ] Embryonic Frame Discovery engine with pattern templates, progressive completion tracking, and auto-promotion
- [ ] Top-level Engine struct wiring all components with full ingest pipeline
- [ ] String interning for property keys and type names (integer IDs on hot path)
- [ ] Zero heap allocation on hot path after initialization
- [ ] Lock-free concurrency primitives (AtomicU64/AtomicBool with correct ordering)
- [ ] Comprehensive test suite (8 test files) and Criterion benchmarks

### Out of Scope

- Async background compaction — synchronous only for this sprint, but interface isolated for future
- Multi-threaded event processing — single-threaded for now, but atomics must be correct for future multi-producer
- External graph crates (petgraph) — all graph structures built from scratch
- Nightly Rust features — stable toolchain only
- Any dependencies beyond crossbeam, bitvec, and criterion

## Context

This is a proof-of-concept that proves the physics of differential MVCC for graph traversals. The system eliminates query-time graph traversal by pre-computing results and maintaining them incrementally. Key architectural decisions:

- **Adjacency on nodes:** Edges stored on the Node struct (outgoing + incoming). Trades write cost for read locality.
- **Differential math:** +1 assertion, -1 retraction. Multiset semantics. +1 + (-1) = 0 means annihilation. Compaction collapses surviving tuples.
- **Frame materialization:** Cold start does full DFS traversal from anchor node following hop pattern. After that, incremental: re-traverse affected frames on each event, diff against previous state.
- **Embryonic discovery:** Watches for forming patterns via bit-vector completion tracking. Auto-promotes to full parked frame when threshold met.

Project structure: 13 source modules in `src/`, 8 test files in `tests/`, 1 benchmark file in `benches/`. Exact module signatures, types, and test cases specified.

## Constraints

- **Toolchain**: Rust stable — no nightly features
- **Hot path allocation**: Zero heap allocation after initialization. All Vecs, buffers, index structures pre-allocated at startup
- **Concurrency**: Lock-free on ingestion path. AtomicU64/AtomicBool with Acquire/Release or SeqCst. No Mutex/RwLock on hot path
- **Identifiers**: All integers — u64 for node/edge IDs, u32 for type IDs and property keys. Zero String on hot path
- **Dependencies**: Only crossbeam, bitvec (runtime), criterion (dev). Nothing else
- **Build order**: Strict sequential — each module must compile and pass tests before proceeding to next

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Adjacency stored on Node struct | Read locality over write cost | — Pending |
| Single-producer ring buffer with correct multi-producer atomics | Ship PoC fast, future-proof the concurrency model | — Pending |
| Synchronous compaction with isolated interface | Avoid async complexity now, easy to move to background thread later | — Pending |
| Re-traverse for frame maintenance (not incremental path extension) | Correctness over performance for PoC | — Pending |
| bitvec for embryonic completion tracking | Efficient per-hop completion bits, minimal dependency | — Pending |

---
*Last updated: 2026-02-24 after initialization*
