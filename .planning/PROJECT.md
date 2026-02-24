# Krabnet

## What This Is

Krabnet is a streaming graph runtime with differential MVCC that pre-materializes graph traversal results for AI agent context systems. Instead of querying a graph when a signal arrives, the system pre-computes ("parks") graph traversal results and updates them incrementally using differential dataflow semantics (+1/-1 deltas). It also autonomously discovers emerging patterns in the mutation stream (Embryonic Frame Discovery). This is a single Rust crate — no external graph dependencies.

## Core Value

When a signal arrives, decision-relevant context is already materialized — zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.

## Current Milestone: v2.0 Full Build Completion

**Goal:** Harden the engine for concurrent load, add production interfaces (gRPC + MCP + WAL + Tier 3 LLM), and replace PoC data structures with enterprise-grade alternatives.

**Target features:**
- Async background compaction, multi-threaded frame evaluation, mutation coalescing
- Super-node fan-out limits and frame prioritizer hysteresis
- gRPC server (8 RPC methods) and MCP server (5 tools) for external access
- Tier 3 LLM integration with graph-aware prompt serialization
- Write-ahead log for crash recovery persistence
- Set-Trie inverted index, Count-Min Sketch, trunk/leaf detection
- Custom buffer pool manager with graph-aware eviction
- Learned template weighting for embryonic discovery
- Enterprise-scale benchmarks (100K nodes, 1M edges, 500 frames)

## Requirements

### Validated

<!-- v1.0 milestone: shipped and confirmed -->
- ✓ Lock-free ring buffer with monotonic epoch sequencer — v1.0
- ✓ In-memory property graph with adjacency-on-node storage — v1.0
- ✓ Differential MVCC engine with exact +1/-1 math — v1.0
- ✓ Parked traverser (frame) system with DFS materialization — v1.0
- ✓ Signal-to-frame routing via inverted index — v1.0
- ✓ Adaptive frame tiering (hot/warm/cold) — v1.0
- ✓ Two-tier interpretation (binary + structural) — v1.0
- ✓ Embryonic Frame Discovery with bitvec tracking — v1.0
- ✓ Engine orchestrator with full ingest pipeline — v1.0
- ✓ String interning (integer IDs on hot path) — v1.0
- ✓ 144 tests passing, 6 Criterion benchmarks — v1.0

### Active

- [ ] Async background compaction with double-buffering
- [ ] Multi-threaded frame evaluation with thread pool
- [ ] Mutation coalescing for burst deduplication
- [ ] Super-node fan-out limits with deferred eval queue
- [ ] Frame prioritizer hysteresis (anti-thrashing)
- [ ] gRPC server with 8 RPC methods including streaming subscriptions
- [ ] MCP JSON-RPC server over stdio with 5 tools
- [ ] Tier 3 LLM integration with graph-aware prompt serialization
- [ ] Write-ahead log with crash recovery replay
- [ ] Set-Trie inverted index replacing HashMap
- [ ] Count-Min Sketch for probabilistic frequency estimation
- [ ] Trunk/leaf path detection with pinned trunks
- [ ] Custom buffer pool manager with graph-aware eviction
- [ ] Learned template weighting for embryonic discovery
- [ ] krabnet-server and krabnet-mcp binaries
- [ ] Stress test suite (50K events/sec sustained)
- [ ] Enterprise-scale benchmarks

### Out of Scope

- Query language (Cypher, Gremlin, SPARQL) — runtime only, not query UX
- Distributed execution — single-process, distributed is different architecture
- Web UI or visualization — runtime only, no presentation layer
- Nightly Rust features — stable toolchain constraint non-negotiable
- External graph crates (petgraph) — all graph structures built from scratch

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
- **Concurrency**: Lock-free on ingestion path. No Mutex/RwLock on hot path. parking_lot Mutex allowed on background threads only
- **Identifiers**: All integers — u64 for node/edge IDs, u32 for type IDs and property keys. Zero String on hot path
- **Dependencies v2.0**: crossbeam 0.8, parking_lot 0.12 (background only), bitvec 1.0, tonic 0.12, prost 0.13, tokio 1 (full), serde 1 (derive), serde_json 1, criterion 0.5 (dev)
- **Build order**: Strict sequential — Phase 11 → 12 → 13, each must compile and pass ALL tests before next

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Adjacency stored on Node struct | Read locality over write cost | — Pending |
| Single-producer ring buffer with correct multi-producer atomics | Ship PoC fast, future-proof the concurrency model | — Pending |
| Synchronous compaction with isolated interface | Avoid async complexity now, easy to move to background thread later | — Pending |
| Re-traverse for frame maintenance (not incremental path extension) | Correctness over performance for PoC | — Pending |
| bitvec for embryonic completion tracking | Efficient per-hop completion bits, minimal dependency | — Pending |

---
*Last updated: 2026-02-25 after v2.0 milestone start*
