# Stack Research: Incremental Path Extension

**Domain:** Incremental path maintenance for streaming graph runtime (replacing full DFS re-traverse)
**Researched:** 2026-02-26
**Confidence:** HIGH

## Executive Summary

After thorough analysis of the Krabnet codebase, the external Rust ecosystem for incremental computation, and the algorithmic requirements for incremental path extension, **no new crate dependencies are needed**. The existing stack (crossbeam 0.8, tokio 1, std collections) is sufficient. The work is purely algorithmic: building a delta propagation pipeline that translates graph mutation events into targeted `Frame::apply_delta()` calls, replacing the current architecture where frames are materialized once at registration and never updated during ingest.

The existing `DiffCollection<Vec<NodeId>>` already implements Z-set / differential multiset semantics with +1/-1 deltas, epoch-stamped tuples, compaction, and temporal snapshots. The existing `Frame::apply_delta()` method already accepts path-level deltas. The missing piece is the **event-to-path-delta computation**: given "edge (A,B) of type T was added", determine which frames are affected and which specific paths need to be asserted (+1) or retracted (-1).

## Current Architecture Gap

The current ingest pipeline (engine.rs `ingest()`) does:
1. Push event to ring buffer
2. Apply mutation to Graph
3. Query InvertedIndex for affected frames
4. For each affected frame: read `net_delta()`, run `tier1_check()` (comparison only)

It does NOT:
- Compute which paths in a frame are newly valid or newly broken
- Call `Frame::apply_delta()` with specific paths
- Recompute partial traversals from the mutation point

Frames are only materialized via full DFS at `register_frame()` or `rematerialize()`. Between those calls, frames go stale as the graph changes. This is the core problem incremental path extension solves.

## Recommended Stack

### Core Technologies (No Changes)

| Technology | Version | Purpose | Rationale for No Change |
|------------|---------|---------|------------------------|
| Rust stable | 1.85+ | Language | Incremental path extension is pure algorithmic work; no new language features needed |
| `std::collections::HashMap` | stable | Path indexing, hop-to-frame mapping | The reverse index from (node, hop_index) to affected paths will use HashMap, already in stdlib |
| `std::collections::HashSet` | stable | Deduplication of affected paths | Already used throughout engine.rs for frame deduplication |

### Supporting Libraries (No Changes)

| Library | Version | Purpose | Relevance to Incremental |
|---------|---------|---------|--------------------------|
| `crossbeam` | 0.8 | Scoped threads for parallel frame evaluation | Parallel delta computation across affected frames uses same pattern as current tier1_check fan-out |
| `tokio` | 1.38.1 | Async runtime for gRPC/MCP servers | Not involved in delta computation (synchronous path) |
| `serde` / `serde_json` | 1 | Serialization | CompactionStats in gRPC/MCP is a tech debt item, not related to path extension |

### New Crate Dependencies: NONE

No new crates are needed. The following were evaluated and rejected.

## Alternatives Considered and Rejected

### 1. differential-dataflow (v0.18) + timely (v0.25)

| Aspect | Assessment |
|--------|-----------|
| What it does | Full distributed incremental dataflow runtime with operators (map, filter, join, iterate) |
| Why rejected | **Massive overkill.** Pulls in an entire distributed dataflow runtime (timely) with worker threads, channels, progress tracking. Krabnet needs only local, single-process delta propagation for path tuples. differential-dataflow's dependency tree adds 15+ transitive crates. |
| When to use instead | If Krabnet needed distributed incremental SQL-style queries across multiple machines |
| Confidence | HIGH -- reviewed GitHub repo and dependency chain |

### 2. DBSP / Feldera (dbsp crate)

| Aspect | Assessment |
|--------|-----------|
| What it does | Incremental view maintenance engine using Z-sets, with SQL compilation support |
| Why rejected | **Krabnet already has Z-set semantics.** `DiffCollection<T>` with +1/-1 Delta, epoch-stamped tuples, compaction, and snapshot is Krabnet's own Z-set implementation. DBSP would be a second, redundant differential engine. Also tightly coupled with Feldera's circuit model. |
| When to use instead | If building a new system from scratch that needs SQL-to-incremental compilation |
| Confidence | HIGH -- reviewed crate structure and Z-set implementation |

### 3. adapton (v0.3)

| Aspect | Assessment |
|--------|-----------|
| What it does | Demand-driven incremental computation with a Demanded Computation Graph (DCG) |
| Why rejected | Adapton's model is pull-based (recompute on demand). Krabnet's model is push-based (propagate deltas on mutation). Architectural mismatch. Also, adapton is designed for general-purpose incremental computation, not specifically graph path maintenance. |
| When to use instead | If Krabnet's frame queries were lazy/on-demand rather than eagerly maintained |
| Confidence | HIGH -- reviewed docs.rs API |

### 4. petgraph (v0.6)

| Aspect | Assessment |
|--------|-----------|
| What it does | General graph data structure with DFS/BFS/Dijkstra algorithms |
| Why rejected | Krabnet has its own Graph with adjacency-on-node storage, property support, and cascading removal. petgraph's algorithms don't support incremental path tracking. Would be a second graph representation with no incremental benefit. |
| When to use instead | If starting a new project without custom graph storage requirements |
| Confidence | HIGH -- petgraph is well-known; its limitations for incremental use are clear |

### 5. smallvec (v1.15)

| Aspect | Assessment |
|--------|-----------|
| What it does | Stack-allocated Vec for small sizes, spilling to heap when larger |
| Why considered | Paths in frames are `Vec<NodeId>` where most paths are 2-4 hops. SmallVec<[NodeId; 4]> would avoid heap allocation for common cases. |
| Why deferred (not rejected) | Optimization, not architectural requirement. The incremental path extension algorithm works identically with Vec or SmallVec. Can be adopted later as a performance optimization once the algorithm is proven correct. Adding it now would interleave an optimization concern with an algorithmic concern. |
| When to add | After incremental path extension is working and benchmarked, if path allocation shows up as a bottleneck in profiling |
| Confidence | MEDIUM -- performance benefit is plausible but unverified for this specific use case |

## What To Build Instead

The incremental path extension requires **new code within the existing crate**, not new dependencies. Here is what needs to be built using only `std` and existing dependencies:

### 1. Reverse Path Index (new internal data structure)

```rust
/// Maps (NodeId, hop_index) -> Vec<(frame_id, path_prefix)>
///
/// When a node N appears at hop position H in a frame's pattern,
/// this index allows finding all frames where N participates at that hop.
/// This is the key structure enabling targeted delta propagation.
struct ReversePathIndex {
    /// Key: (node_id, hop_index) -> Set of frame IDs that have this node at this hop
    node_hop_to_frames: HashMap<(NodeId, usize), HashSet<u64>>,
}
```

This is built from `std::collections` -- no crate needed.

### 2. Partial Re-Traverse (new algorithm in frame.rs or engine.rs)

```rust
/// Given a mutation (edge added/removed between src and tgt of type T),
/// compute which paths in each affected frame need assertion/retraction.
///
/// Algorithm:
/// 1. Find affected frames via InvertedIndex (existing)
/// 2. For each affected frame, identify which hop(s) could include this edge
/// 3. For each matching hop:
///    a. Walk BACKWARD from src through preceding hops to find valid prefixes
///    b. Walk FORWARD from tgt through succeeding hops to find valid suffixes
///    c. Cross-product of prefixes x suffixes = affected paths
/// 4. For edge additions: assert (+1) each new complete path
/// 5. For edge removals: retract (-1) each broken complete path
fn compute_path_deltas(
    graph: &Graph,
    frame: &Frame,
    event: &Event,
) -> Vec<(Vec<NodeId>, Delta)> {
    // ... pure algorithmic work using existing Graph::neighbors()
}
```

This uses only `Graph::neighbors()`, `Frame::pattern()`, `Vec`, `HashMap` -- all existing.

### 3. Integration into Engine::ingest() (modification of existing code)

Replace the current tier1-only check with actual delta application:

```rust
// Current (check only):
let _changed = tier1_check(previous, current);

// New (compute + apply deltas):
let path_deltas = compute_path_deltas(&self.graph, &frame, &event);
for (path, delta) in path_deltas {
    frame.apply_delta(path, epoch, delta);
}
```

## What NOT to Add

| Do Not Add | Why | The Existing Alternative |
|------------|-----|------------------------|
| `differential-dataflow` | 15+ transitive deps, distributed runtime, architectural mismatch | `DiffCollection<Vec<NodeId>>` already provides Z-set semantics |
| `dbsp` | Redundant Z-set implementation, tightly coupled with Feldera circuits | `DiffCollection` + `Frame::apply_delta()` |
| `adapton` | Pull-based model, Krabnet is push-based | Custom push-based delta propagation |
| `petgraph` | No incremental path support, second graph representation | `Graph` with adjacency-on-node storage |
| `smallvec` (now) | Premature optimization; interleaves concerns | `Vec<NodeId>` -- optimize later if profiling warrants |
| Any async channel crate | Delta computation is synchronous, same-thread or scoped-thread | `std::thread::scope` (already used in engine.rs) |
| `rayon` | Parallel iterator overhead not justified; scoped threads already work | `std::thread::scope` with `crossbeam` |

## Stack Patterns for Incremental Path Extension

### Pattern 1: Backward-Forward Partial Traverse

**If the mutation is an edge addition/removal:**
- The affected hop in the frame pattern is identified by matching (direction, edge_type)
- Walk backward from the edge's source through preceding hops to reconstruct valid prefixes
- Walk forward from the edge's target through succeeding hops to reconstruct valid suffixes
- Complete paths = prefix + [source, target] + suffix
- This is O(affected_paths) rather than O(all_paths) full DFS

**Use existing:** `Graph::neighbors()`, `HopSpec` matching, `Vec<NodeId>` accumulation

### Pattern 2: Property Change Filtering

**If the mutation is a property change on node N:**
- Find all frames where N appears in a materialized path
- For each frame, re-evaluate the filter at N's hop position
- If filter now fails: retract (-1) all paths containing N at that position
- If filter now passes (was failing before): assert (+1) newly valid paths
- This requires knowing N's position in each path, which the Reverse Path Index provides

**Use existing:** `Frame::query()` to find paths containing N, `Filter::PropertyEquals` / `Filter::HasProperty` evaluation

### Pattern 3: Early Exit on Unchanged

**If delta computation produces an empty set of path changes:**
- Skip `apply_delta()` entirely
- This is the common case: most mutations affect only a few frames, and many of those frames' patterns don't match the mutation's edge type
- The InvertedIndex already provides coarse filtering; the hop-level type check provides fine filtering

**Use existing:** `InvertedIndex::affected_frames()` (already in pipeline)

## Version Compatibility

No changes to Cargo.toml dependencies. The existing versions remain:

| Package | Version | Notes |
|---------|---------|-------|
| `crossbeam` | 0.8 | Scoped threads for parallel delta computation |
| `tokio` | 1.38.1 | Unchanged; async runtime not involved |
| `tonic` | 0.12 | Unchanged; gRPC layer not involved in path extension |
| `prost` | 0.13 | Unchanged |
| `serde` | 1 | Unchanged |
| `serde_json` | 1 | Unchanged |
| `bitvec` | 1.0 | Unchanged |
| `ureq` | 2 | Unchanged; Anthropic HTTP client is separate tech debt |
| `criterion` | 0.5 | Add benchmarks for incremental vs full-DFS path computation |

## Installation

```bash
# No new dependencies. Cargo.toml unchanged for incremental path extension.
# The tech debt items (AnthropicClient, CompactionStats, WAL) may add deps
# but those are separate from path extension.
cargo build
```

## Key Insight

Krabnet's `DiffCollection` already IS an incremental view maintenance engine at the tuple level. The `apply_delta()` method already works. What is missing is the **event-to-delta translation layer** -- the algorithm that converts "edge added between A and B" into "assert path [X, A, B, Y]" and "retract path [X, A, C]". This is a ~300-500 line algorithmic module, not a dependency problem.

The existing stack was designed for exactly this use case. The DiffCollection's +1/-1 semantics, the Frame's `apply_delta()` method, the InvertedIndex's affected-frame routing, and the Graph's `neighbors()` query are all the building blocks needed. The work is connecting them with the backward-forward partial traverse algorithm.

## Sources

- [differential-dataflow on crates.io](https://crates.io/crates/differential-dataflow) -- Evaluated v0.18, rejected for architectural mismatch. **HIGH confidence**
- [DBSP / Feldera on GitHub](https://github.com/feldera/feldera) -- Evaluated Z-set implementation, redundant with DiffCollection. **HIGH confidence**
- [Adapton on docs.rs](https://docs.rs/adapton) -- Evaluated demand-driven incremental model, wrong paradigm. **HIGH confidence**
- [smallvec on crates.io](https://crates.io/crates/smallvec) -- v1.15.1 evaluated, deferred as premature optimization. **MEDIUM confidence**
- [DBSP: Automatic Incremental View Maintenance](https://docs.feldera.com/vldb23.pdf) -- Z-set theory validates DiffCollection design. **HIGH confidence**
- [Incrementalizing Graph Algorithms (SIGMOD 2021)](https://dl.acm.org/doi/10.1145/3448016.3452796) -- Academic validation of delta propagation for graph views. **MEDIUM confidence** (academic, not directly Rust)
- [Krabnet source: frame.rs](src/frame.rs) -- `apply_delta()` already exists, `materialize()` shows full DFS pattern to replace. **HIGH confidence** (primary source)
- [Krabnet source: engine.rs](src/engine.rs) -- Current ingest pipeline shows tier1-only evaluation gap. **HIGH confidence** (primary source)
- [Krabnet source: diff.rs](src/diff.rs) -- `DiffCollection` already implements Z-set semantics. **HIGH confidence** (primary source)
- [Krabnet source: routing.rs](src/routing.rs) -- `InvertedIndex::affected_frames()` provides the coarse frame routing needed. **HIGH confidence** (primary source)

---
*Stack research for: Incremental path extension in Krabnet streaming graph runtime*
*Researched: 2026-02-26*
