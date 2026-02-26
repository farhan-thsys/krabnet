# Krabnet

## What This Is

Krabnet is a streaming graph runtime with differential MVCC that pre-materializes graph traversal results for AI agent context systems. Instead of querying a graph when a signal arrives, the system pre-computes ("parks") graph traversal results and updates them incrementally using differential dataflow semantics (+1/-1 deltas). It also autonomously discovers emerging patterns in the mutation stream (Embryonic Frame Discovery). This is a single Rust crate — no external graph dependencies.

## Core Value

When a signal arrives, decision-relevant context is already materialized — zero query-time graph traversal. The differential math (+1/-1 deltas) must be exact and correct.

## Current State

**Shipped:** v1.0 (core engine, 10 phases) + v2.0 (hardening + production interfaces, 5 phases)
**Codebase:** ~13,400 LOC Rust across 13 source modules, 2 binary entry points
**Tests:** 180 lib tests, 53 doc-tests, 13 Criterion benchmarks, zero clippy warnings
**Binaries:** krabnet-server (gRPC + WAL + Tier 3 LLM), krabnet-mcp (MCP stdio)

## Requirements

### Validated

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
- ✓ Background compaction with double-buffering and configurable threshold — v2.0
- ✓ Multi-threaded frame evaluation via std::thread::scope — v2.0
- ✓ Mutation coalescing (16-epoch window deduplication) — v2.0
- ✓ Super-node fan-out limits with deferred eval queue — v2.0
- ✓ Frame prioritizer hysteresis (anti-thrashing, 5-window consecutive) — v2.0
- ✓ gRPC server with 8 RPCs including SubscribeFrame streaming — v2.0
- ✓ MCP JSON-RPC server over stdio with 5 tools — v2.0
- ✓ Tier 3 LLM integration with graph-aware prompt serialization — v2.0
- ✓ Write-ahead log with crash recovery replay — v2.0
- ✓ Set-Trie inverted index replacing HashMap — v2.0
- ✓ Count-Min Sketch for probabilistic frequency estimation — v2.0
- ✓ Trunk/leaf path detection with Hot-pinned trunks — v2.0
- ✓ Custom buffer pool with graph-aware eviction (Cold-first) — v2.0
- ✓ Learned template weighting with auto-deactivation — v2.0
- ✓ krabnet-server and krabnet-mcp hardened binaries — v2.0
- ✓ Stress tests (50K+ events/sec sustained) — v2.0
- ✓ Enterprise benchmarks (100K nodes, 1M edges, 500 frames) — v2.0

### Active

(None — next milestone not yet defined)

### Out of Scope

- Query language (Cypher, Gremlin, SPARQL) — runtime only, not query UX
- Distributed execution — single-process, distributed is different architecture
- Web UI or visualization — runtime only, no presentation layer
- Nightly Rust features — stable toolchain constraint non-negotiable
- External graph crates (petgraph) — all graph structures built from scratch

## Context

Shipped v2.0 with 13,400 LOC Rust. The engine is production-ready with hardened concurrent operation, two external interfaces (gRPC and MCP), WAL persistence, and enterprise-scale data structures.

**Architecture:**
- **Adjacency on nodes:** Edges stored on the Node struct (outgoing + incoming). Trades write cost for read locality.
- **Differential math:** +1 assertion, -1 retraction. Multiset semantics. Compaction collapses surviving tuples.
- **Frame materialization:** Cold start does full DFS from anchor. After that, incremental re-traverse on each event.
- **Embryonic discovery:** Bitvec completion tracking with learned template weighting and auto-deactivation.
- **Engine hardening:** Background compaction, mutation coalescing, fan-out limits, hysteresis — all configurable via Engine::with_config().
- **Production interfaces:** gRPC (8 RPCs, broadcast subscriptions, WAL), MCP (5 tools over stdio), Tier 3 LLM pipeline.
- **Enterprise data structures:** Set-Trie inverted index, Count-Min Sketch scoring, trunk detection, custom buffer pool.

**Tech debt carried forward:**
- CompactionStats not exposed via GetStats gRPC proto
- WAL not available in MCP path
- AnthropicClient not implemented (MockLlmClient placeholder)
- krabnet-server uses MockLlmClient in production binary

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
| Adjacency stored on Node struct | Read locality over write cost | ✓ Good — enables fast DFS and Set-Trie lookups |
| Single-producer ring buffer | Ship PoC fast, future-proof concurrency | ✓ Good — sufficient for v1.0/v2.0 |
| Background compaction via dedicated thread | Avoid async complexity, crossbeam channel | ✓ Good — clean separation, no hot-path blocking |
| Re-traverse for frame maintenance | Correctness over performance for PoC | ⚠ Revisit — incremental path extension deferred to v3 |
| bitvec for embryonic completion tracking | Efficient per-hop completion bits | ✓ Good — extended with learned weighting in v2.0 |
| protox+tonic-build (no protoc) | Eliminate external build dependency | ✓ Good — clean build on any system |
| MockLlmClient as production placeholder | Ship Tier 3 pipeline without real LLM | ⚠ Revisit — needs AnthropicClient for real use |
| Engine::with_config() unified constructor | Single entry point for all hardening features | ✓ Good — used by both binaries and benchmarks |

---
*Last updated: 2026-02-26 after v2.0 milestone*
