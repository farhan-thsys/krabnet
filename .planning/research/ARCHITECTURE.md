# Architecture Research: Incremental Path Extension

**Domain:** Incremental path extension for streaming graph runtime with differential MVCC
**Researched:** 2026-02-26
**Confidence:** HIGH (existing codebase thoroughly analyzed; patterns drawn from differential dataflow literature)

## Problem Statement

Currently, `Engine::ingest()` calls `Frame::materialize()` for every affected frame on every event. Materialization performs a full DFS from the anchor node across all hops, collecting all complete paths as +1 assertions. This is O(hops * branching_factor) per affected frame per event -- correct but expensive. The goal is to replace full re-traversal with **incremental path extension**: when an edge is added or removed, compute only the delta to existing paths rather than recomputing from scratch.

## Current Architecture (What Exists)

### System Overview

```
Event arrives
    |
    v
[Sequencer] --> [Ring Buffer] --> epoch assigned
    |
    v
[Engine::ingest()]
    |
    +---> [Graph::add_edge/add_node/etc] -- mutate graph
    |
    +---> [InvertedIndex::affected_frames()] -- O(affected) routing
    |
    v
[For each affected frame:]
    |
    +---> frame.materialize(&graph, epoch)  <-- FULL DFS RE-TRAVERSE
    |         |
    |         +---> dfs_collect() from anchor through all hops
    |         +---> assert_tuple() for each complete path
    |
    +---> tier1_check(previous_net_delta, current_net_delta)
    |
    v
[Compaction, tiering, embryonic observation...]
```

### Key Existing Components

| Component | File | Role in Path Extension |
|-----------|------|----------------------|
| `Frame` | `frame.rs` | Holds `DiffCollection<Vec<NodeId>>`, owns `materialize()` and `apply_delta()` |
| `Frame::dfs_collect()` | `frame.rs` | Recursive DFS helper -- the code being replaced |
| `Frame::apply_delta()` | `frame.rs` | Already exists: asserts/retracts individual paths without re-traversal |
| `DiffCollection<T>` | `diff.rs` | Generic differential collection with assert/retract/compact/snapshot |
| `Engine::ingest()` | `engine.rs` | Orchestrator -- currently calls `materialize()` on affected frames |
| `InvertedIndex` | `routing.rs` | Maps (node_id, edge_key) to frame_ids -- routes events to frames |
| `Graph` | `graph.rs` | Adjacency-on-node storage with neighbor queries by direction/edge_type |
| `HopSpec` | `types.rs` | Defines one hop: direction, edge_type, target_type, filter |
| `CompactionWorker` | `compaction.rs` | Background compaction with double-buffering |

### Critical Observation: `apply_delta()` Already Exists

The frame already has an `apply_delta(path, epoch, delta)` method that asserts or retracts individual paths without re-traversal. The missing piece is the **logic to compute which paths to assert/retract** given a graph mutation event. Currently, the engine skips this logic and falls through to full materialization. Incremental path extension fills this gap.

## Recommended Architecture: Per-Hop Delta Propagation

### Core Insight: Decompose Multi-Hop into Per-Hop Joins

A multi-hop path query `anchor --hop0--> N1 --hop1--> N2 --hop2--> N3` is equivalent to a multi-way join:

```
paths = hop0_results JOIN hop1_results JOIN hop2_results
```

When an edge is added/removed, it affects exactly one hop. The delta propagation strategy (from differential dataflow's delta join decomposition) is:

1. Identify which hop the edge change affects
2. For an **edge addition**: find all existing partial paths that reach the edge's source, extend them through the new edge, then continue DFS for remaining hops
3. For an **edge removal**: find all existing complete paths that traverse the removed edge, retract them

This replaces O(hops * branching_factor) full re-traversal with O(affected_paths) targeted delta computation.

### New Component: `PathExtender`

A new module `path_extender.rs` that computes path deltas given a graph event and a frame's pattern.

```
┌─────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL PATH EXTENSION                    │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  PathExtender (NEW)                        │   │
│  │                                                            │   │
│  │  extend_edge_added(frame, graph, src, tgt, etype, epoch)  │   │
│  │  retract_edge_removed(frame, graph, src, tgt, epoch)      │   │
│  │  handle_node_removed(frame, graph, node_id, epoch)        │   │
│  │  handle_property_changed(frame, graph, node_id, epoch)    │   │
│  │                                                            │   │
│  │  Internal:                                                 │   │
│  │  - match_hop(hop, src, tgt, etype, graph) -> bool          │   │
│  │  - affected_hop_index(pattern, event) -> Option<usize>     │   │
│  │  - prefix_paths(frame, graph, up_to_hop) -> Vec<Vec<NId>>  │   │
│  │  - suffix_extend(graph, from_node, remaining_hops)         │   │
│  │         -> Vec<Vec<NodeId>>                                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              |                                   │
│                              v                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │           Frame::apply_delta() (EXISTING)                  │   │
│  │           DiffCollection assert/retract (EXISTING)         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow: Edge Added

Consider frame with pattern `[hop0, hop1, hop2]` anchored at node A, and an edge `(X, Y, etype)` is added:

```
1. Identify affected hop index:
   For each hop_i in pattern:
     Does hop_i.direction match? Does hop_i.edge_type match etype?
     Does target_type filter pass for Y? Does property filter pass?
     If yes -> affected_hop = i

2. Find prefix paths reaching X:
   Query frame's existing DiffCollection for paths where path[i] == X
   (For hop 0: X must equal the anchor node)
   OR: DFS from anchor through hops 0..i-1, collecting paths that end at X

3. Extend through new edge:
   For each prefix path ending at X:
     Append Y to get partial path through hop i

4. Complete suffix:
   From Y, DFS through remaining hops (i+1..n) in the pattern
   For each complete suffix: concatenate prefix + [Y] + suffix

5. Assert new complete paths:
   For each new complete path:
     frame.apply_delta(path, epoch, Delta(+1))
```

### Data Flow: Edge Removed

```
1. Identify affected hop index (same as above)

2. Find existing paths containing the removed edge:
   Query frame's DiffCollection for paths where:
     path[i] == source AND path[i+1] == target
   (The edge at hop i connects path[i] to path[i+1])

3. Retract matching paths:
   For each matching path:
     frame.apply_delta(path, epoch, Delta(-1))
```

### Data Flow: Node Removed

```
1. Find existing paths containing the removed node:
   Query frame's DiffCollection for paths where:
     any path[j] == removed_node_id

2. Retract all matching paths:
   For each matching path:
     frame.apply_delta(path, epoch, Delta(-1))
```

### Data Flow: Property Changed

```
1. Identify which hops have property filters

2. For each hop with a property filter referencing the changed node:
   a. Find existing paths where the node at that hop position is the changed node
   b. Check if the property filter NOW passes or NEWLY fails
   c. If newly passes: extend (like edge added -- find prefix, complete suffix)
   d. If newly fails: retract matching paths (like edge removed)
```

## Component Boundaries: New vs Modified

### New Components

| Component | File | Purpose |
|-----------|------|---------|
| `PathExtender` | `src/path_extender.rs` | Core incremental logic: computes path deltas given events |
| `PathIndex` (optional) | Inside `frame.rs` or `path_extender.rs` | Secondary index on frame paths for fast per-position lookups |

### Modified Components

| Component | File | Modification |
|-----------|------|-------------|
| `Engine::ingest()` | `engine.rs` | Replace `frame.materialize()` call with `PathExtender` dispatch for hot/warm frames |
| `Frame` | `frame.rs` | Add `paths_containing_node(node_id, position) -> Vec<&Vec<NodeId>>` query helper |
| `Frame` | `frame.rs` | Add `paths_through_edge(src, tgt, hop_index) -> Vec<&Vec<NodeId>>` query helper |
| `InvertedIndex` | `routing.rs` | Enrich `affected_frames()` to also return which hop is affected (optional optimization) |
| `lib.rs` | `src/lib.rs` | Re-export `PathExtender` and any new public types |

### Unchanged Components

| Component | File | Why Unchanged |
|-----------|------|--------------|
| `DiffCollection` | `diff.rs` | Already generic; assert/retract paths unchanged |
| `Graph` | `graph.rs` | Already provides all needed neighbor/property queries |
| `HopSpec`, `Event`, other types | `types.rs` | No new types needed for core extension |
| `CompactionWorker` | `compaction.rs` | Compaction is orthogonal to how deltas arrive |
| `MutationCoalescer` | `coalescer.rs` | Coalescing is upstream of frame evaluation |
| `FanOutLimiter` | `fanout.rs` | Fan-out limiting is upstream of frame evaluation |
| `TierConfig`, `FrameActivityTracker` | `tiering.rs` | Tiering is orthogonal |
| `Trunk detection` | `trunk.rs` | Trunk detection is orthogonal |

## Architectural Patterns

### Pattern 1: Delta Join Decomposition (from Differential Dataflow)

**What:** When maintaining a multi-way join (multi-hop path), decompose updates so that a change to one input relation is joined against the current state of other relations. This avoids recomputing the entire join.

**When to use:** Any time a multi-hop path query needs incremental maintenance. This is the mathematical foundation of Materialize's delta join strategy.

**Trade-offs:**
- Pro: Work proportional to output change, not total output size
- Pro: Reuses existing path state (DiffCollection) as "arrangement"
- Con: Requires ability to query existing paths by position (needs index or scan)
- Con: More complex correctness reasoning than full re-traverse

**Example (Krabnet-specific):**
```rust
/// For a 3-hop pattern [hop0, hop1, hop2], edge added at hop 1:
/// 1. Find prefix paths: existing paths truncated to [anchor, ..., node_at_hop1]
/// 2. The new edge gives us node_at_hop2
/// 3. Extend suffix: DFS from node_at_hop2 through hop2
/// 4. Assert each complete path

fn extend_at_hop(
    frame: &Frame,
    graph: &Graph,
    hop_index: usize,
    new_source: NodeId,
    new_target: NodeId,
    epoch: Epoch,
) -> Vec<(Vec<NodeId>, Delta)> {
    let pattern = frame.pattern();
    let mut deltas = Vec::new();

    // Find prefix paths ending at new_source
    let prefixes = find_prefix_paths(frame, new_source, hop_index);

    for prefix in prefixes {
        // Build partial path through the new edge
        let mut partial = prefix.clone();
        partial.push(new_target);

        // Extend through remaining hops
        let suffixes = dfs_remaining(graph, new_target, &pattern[hop_index + 1..]);

        for suffix in suffixes {
            let mut complete = partial.clone();
            complete.extend(suffix);
            deltas.push((complete, Delta(1)));
        }
    }

    deltas
}
```

### Pattern 2: Path Position Index

**What:** A secondary index on the frame's existing paths, mapping `(hop_position, node_id) -> Vec<path_index>`. Enables O(1) lookup of which existing paths pass through a given node at a given hop position.

**When to use:** When frames have many paths and scanning all paths per event is too expensive. For frames with few paths (< ~100), a linear scan is sufficient and simpler.

**Trade-offs:**
- Pro: O(1) lookup for prefix/suffix path finding
- Pro: Makes edge-removed retraction fast (no scan needed)
- Con: Memory overhead proportional to (paths * hops)
- Con: Must be maintained in sync with DiffCollection assertions/retractions
- Con: Added complexity; can be deferred to optimization phase

**Decision:** Start without the position index. Use linear scans over `DiffCollection::current_state()`. Add the position index as an optimization if benchmarks show scanning is the bottleneck. This follows the project's existing pattern of "correct first, fast later."

### Pattern 3: Fallback to Full Materialization

**What:** For complex events that the incremental path extender cannot handle efficiently (e.g., node removal cascading to many edges, or property changes affecting hop filters at multiple positions), fall back to `Frame::rematerialize()`.

**When to use:** When the incremental approach would produce more work than full re-traverse (e.g., a node involved in many paths is removed), or for correctness verification during development.

**Trade-offs:**
- Pro: Guarantees correctness as a safety net
- Pro: Simplifies initial implementation (handle easy cases incrementally, hard cases via fallback)
- Con: Defeats the purpose if triggered too often
- Con: Must detect when fallback is appropriate

**Decision:** Implement incremental extension for `EdgeAdded` and `EdgeRemoved` first (the common case and highest value). Property changes and node removals initially use fallback, then get incremental handling in later phases.

### Anti-Pattern: Stale Path Index

**What people do:** Build a secondary position index on paths but fail to update it when `apply_delta()` modifies the DiffCollection.
**Why it's wrong:** The index becomes stale, leading to missing retractions or phantom extensions.
**Do this instead:** Either rebuild the index after each batch of deltas, or couple the index updates directly to `DiffCollection::assert_tuple/retract_tuple` via a callback or wrapper.

### Anti-Pattern: Redundant Assertions

**What people do:** When extending paths, assert a path that already exists in the DiffCollection.
**Why it's wrong:** Creates incorrect multiplicities. A path that should have multiplicity 1 ends up with multiplicity 2, and subsequent retraction only brings it to 1 instead of 0.
**Do this instead:** Check DiffCollection for existing paths before asserting. Or use the hop-index to ensure the extension is only triggered by genuinely new edges (not edges that already produced the path during initial materialization).

## Integration Points

### Engine::ingest() Modification

The core change is in `Engine::ingest()` step 4 (frame evaluation). Currently:

```rust
// Current: full re-traverse for every affected frame
let frame = arc.read().expect("RwLock poisoned");
let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
let current = frame.net_delta();
let _changed = tier1_check(previous, current);
```

This only reads the frame. The actual materialization happens at registration time. But the description from the milestone context says `frame.materialize()` is called for every affected frame on every event. Looking at the actual code, I see that `Engine::ingest()` does NOT currently re-materialize frames on each event -- it only does a read-lock tier1 check. The `materialize()` call happens at `register_frame()` time.

**Revised understanding:** The engine currently does NOT incrementally update frames at all. It materializes once at registration, and the paths become stale as the graph evolves. The inverted index routes events to frames, but the current per-event handling only does a tier1 delta check -- it does not update frame state.

This means `Frame::apply_delta()` exists but is never called from the engine's ingest pipeline. The entire incremental maintenance path is missing, not just the "smart" version of it.

### What Needs to Be Built (Revised)

The integration requires two layers:

**Layer 1: Basic incremental maintenance via full re-diff**
- After a graph mutation, for each affected frame: re-traverse (DFS) and diff against current state
- This is the "naive" incremental approach: re-traverse + diff + apply deltas
- This is what the original architecture doc described as "re-traverse and diff"

**Layer 2: Smart incremental path extension (the optimization)**
- Instead of full re-traverse, compute only the changed paths
- This is the delta join decomposition approach described above

Both layers use `Frame::apply_delta()` to apply results. Layer 1 is the correctness baseline; Layer 2 is the performance optimization.

### Engine Integration for Layer 1 (Re-Diff)

```rust
// In Engine::ingest(), after graph mutation and inverted index lookup:
for (fid, frame_arc) in &affected_frames {
    let mut frame = frame_arc.write().expect("RwLock poisoned");
    // Compute what paths SHOULD exist now
    let mut expected_paths = Vec::new();
    frame.dfs_collect_standalone(&graph, &[frame.anchor()], 0, &mut expected_paths);
    // Diff against current state
    let current = frame.query_snapshot(); // don't increment query_count
    // Assert new paths, retract removed paths
    for path in &expected_paths {
        if !current.contains(path) {
            frame.apply_delta(path.clone(), epoch, Delta(1));
        }
    }
    for path in current {
        if !expected_paths.contains(path) {
            frame.apply_delta(path.clone(), epoch, Delta(-1));
        }
    }
}
```

### Engine Integration for Layer 2 (Smart Extension)

```rust
// In Engine::ingest(), replace the re-diff with PathExtender:
for (fid, frame_arc) in &affected_frames {
    let deltas = PathExtender::compute_deltas(
        &frame_arc.read().unwrap(),
        &graph,
        &event,
        epoch,
    );
    if !deltas.is_empty() {
        let mut frame = frame_arc.write().unwrap();
        for (path, delta) in deltas {
            frame.apply_delta(path, epoch, delta);
        }
    }
}
```

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Engine -> PathExtender | Function call with `(&Frame, &Graph, &Event, Epoch)` | PathExtender is stateless; takes references |
| PathExtender -> Frame | Reads `frame.pattern()` and `frame.snapshot()` | Read-only access to frame state |
| PathExtender -> Graph | Reads `graph.neighbors()`, `get_node_type()`, `get_property()` | Read-only access to graph |
| PathExtender output -> Frame::apply_delta() | Returns `Vec<(Vec<NodeId>, Delta)>` | Engine applies deltas to frame |
| Engine -> Frame (write lock) | Only held during `apply_delta()` calls | Minimal write-lock duration |

## Suggested Build Order

Build order follows the principle: correct baseline first, then optimize.

### Phase 1: Re-Diff Baseline (Layer 1)

**Goal:** Frames are actually maintained incrementally (even if via full re-traverse + diff).

**Tasks:**
1. Add `Frame::snapshot_current()` that returns current paths without incrementing query_count (or use existing `snapshot(Epoch(u64::MAX))`)
2. Add standalone DFS helper that can be called from the engine (currently `dfs_collect` is private to Frame)
3. Modify `Engine::ingest()` to re-traverse affected frames after graph mutation and diff against current state
4. Apply deltas via `Frame::apply_delta()`
5. Tests: verify that frame state stays in sync with graph after mutations

**Dependencies:** None -- uses all existing components.
**Risk:** Low. This is mechanically applying existing `materialize()` logic in a diff-and-apply pattern.

### Phase 2: PathExtender for EdgeAdded (Layer 2, Part 1)

**Goal:** When an edge is added, compute only the new paths enabled by that edge.

**Tasks:**
1. Create `src/path_extender.rs` module
2. Implement `affected_hop_index()`: given an event and a frame's pattern, determine which hop the event affects
3. Implement `find_prefix_paths()`: query frame's current state for paths where `path[hop_index] == source_node`
4. Implement `dfs_suffix()`: DFS from target node through remaining hops (reuses `dfs_collect` logic)
5. Implement `extend_edge_added()`: orchestrates prefix lookup + suffix extension + returns delta list
6. Wire into `Engine::ingest()` for `EdgeAdded` events (keep fallback to re-diff for other events)
7. Tests: verify identical results to re-diff baseline for all edge-add scenarios

**Dependencies:** Phase 1 (baseline for correctness verification).
**Risk:** Medium. The prefix-path lookup and suffix-extension logic must be correct for all hop patterns.

### Phase 3: PathExtender for EdgeRemoved (Layer 2, Part 2)

**Goal:** When an edge is removed, compute only the paths that need retraction.

**Tasks:**
1. Implement `retract_edge_removed()`: scan existing paths for the removed edge at the affected hop position
2. Wire into `Engine::ingest()` for `EdgeRemoved` events
3. Tests: verify identical results to re-diff baseline

**Dependencies:** Phase 2 (shares `affected_hop_index()` and module structure).
**Risk:** Low. Path scanning and retraction is simpler than extension.

### Phase 4: NodeRemoved and PropertyChanged

**Goal:** Handle remaining event types incrementally.

**Tasks:**
1. Implement `handle_node_removed()`: find and retract all paths containing the removed node
2. Implement `handle_property_changed()`: evaluate filter changes at affected hop positions
3. Wire into `Engine::ingest()` for remaining event types
4. Tests: verify identical results to re-diff baseline

**Dependencies:** Phase 3.
**Risk:** Medium. Property filter changes require re-evaluating hop predicates.

### Phase 5: Path Position Index (Optimization)

**Goal:** Speed up prefix/suffix path lookups for frames with many paths.

**Tasks:**
1. Add `PathPositionIndex` to Frame: maps `(hop_position, node_id) -> Vec<path_ref>`
2. Maintain index in sync with `apply_delta()`
3. Use index in PathExtender instead of linear scans
4. Benchmark: compare with and without index at various path counts

**Dependencies:** Phases 2-4 (the queries to optimize must exist first).
**Risk:** Low. Pure optimization with clear before/after benchmarking.

### Phase 6: Verification and Correctness Audit

**Goal:** Ensure incremental path extension produces bit-identical results to full materialization.

**Tasks:**
1. Property-based test: for random graph mutations, compare PathExtender output with full rematerialization
2. Stress test: high-frequency mutations with branching paths
3. Remove re-diff fallback for events that are now handled incrementally
4. Benchmark: measure improvement over full re-traverse at various scales

**Dependencies:** All previous phases.
**Risk:** Low if previous phases have good test coverage.

### Build Order Summary

```
Phase 1: Re-Diff Baseline
    |
    v
Phase 2: PathExtender for EdgeAdded
    |
    v
Phase 3: PathExtender for EdgeRemoved
    |
    v
Phase 4: NodeRemoved + PropertyChanged
    |
    v
Phase 5: Path Position Index (optimization)
    |
    v
Phase 6: Verification + Benchmarks
```

**Phase ordering rationale:**
- Phase 1 first because it establishes the correctness baseline that all subsequent phases are tested against
- EdgeAdded before EdgeRemoved because extension is more complex than retraction, and addition is the more common event in growing graphs
- NodeRemoved/PropertyChanged deferred because they are less frequent and can use re-diff fallback initially
- Position index last because it is a pure optimization with no correctness impact
- Verification throughout, but formal audit at the end

## Scaling Considerations

| Scale | Approach |
|-------|----------|
| < 100 paths per frame | Linear scan of DiffCollection is fine. No position index needed. |
| 100-10K paths per frame | Position index starts to pay off for prefix/suffix lookups. |
| 10K+ paths per frame | Position index essential. Consider batch delta application (collect all deltas, apply in one write-lock acquisition). |
| High-frequency events | Mutation coalescer (already exists) reduces evaluation frequency. PathExtender only runs on coalesced batches. |
| Super-node fan-out | FanOutLimiter (already exists) caps evaluations. Deferred frames still use re-diff until drained. |

## Sources

- [Differential Dataflow (GitHub)](https://github.com/TimelyDataflow/differential-dataflow) -- Reference implementation, delta propagation model. HIGH confidence.
- [Frank McSherry: Differential Graph Computation](http://www.frankmcsherry.org/differential/dataflow/2015/05/12/bfs.html) -- BFS via iterate+join, 130us median response for single-edge modifications. HIGH confidence.
- [Materialize: Delta Joins and Late Materialization](https://materialize.com/blog/delta-joins/) -- Delta join decomposition: changes to one input joined against current state of others. HIGH confidence.
- [MV4PG: Materialized Views for Property Graphs](https://arxiv.org/html/2411.18847v1) -- Templated maintenance for variable-length edge patterns, per-hop decomposition. MEDIUM confidence.
- [Everything About IVM](https://materializedview.io/p/everything-to-know-incremental-view-maintenance) -- IVM taxonomy and delta view trees. MEDIUM confidence.
- [Incremental Maintenance of Materialized Path Query Views (Springer)](https://link.springer.com/chapter/10.1007/978-1-4471-0895-5_7) -- Path query view maintenance with SMX index. MEDIUM confidence.
- Existing Krabnet codebase (`frame.rs`, `engine.rs`, `diff.rs`, `routing.rs`, `graph.rs`) -- Direct source code analysis. HIGH confidence.

---
*Architecture research for: Incremental path extension in Krabnet streaming graph runtime*
*Researched: 2026-02-26*
