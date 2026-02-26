# Phase 18: Incremental Edge Addition - Research

**Researched:** 2026-02-26
**Domain:** Incremental path extension for EdgeAdded events using backward prefix + forward extension
**Confidence:** HIGH

## Summary

Phase 18 replaces the full DFS re-traverse baseline (Phase 17) with an incremental `PathExtender` module for `EdgeAdded` events. When a new edge `(src, tgt, type_id)` is added, the system must identify which frames are affected, determine which hop position(s) in each frame's pattern the new edge could satisfy, find existing partial paths (backward prefixes) from the anchor to the hop before the new edge, then extend forward through remaining hops from the new edge's target to produce complete new paths. These new paths are asserted as +1 deltas via `Frame::apply_delta` -- no full DFS re-traverse.

The core algorithmic challenge is the **backward prefix resolution**: given a frame with N hops and a new edge satisfying hop K, we must reconstruct all valid partial paths from the frame's anchor through hops 0..K-1 that terminate at the edge's source node. This is equivalent to a partial DFS from the anchor but only up to hop K-1 (not the full pattern). The **forward extension** then continues from the edge's target through hops K+1..N-1.

The correctness invariant is that incremental `EdgeAdded` handling produces **identical** frame state to the Phase 17 full re-traverse. The oracle test harness from Phase 17 (6 existing tests) remains the verification backbone -- it will be used to verify every incremental result.

**Primary recommendation:** Create a new `src/path_extender.rs` module with a stateless `extend_edge_added()` function that takes `(&Frame, &Graph, &Event::EdgeAdded, Epoch)` and returns `Vec<(Vec<NodeId>, Delta)>` path deltas. Wire this into `maintain_and_evaluate_frames` as a replacement for `frame.rematerialize()` when the event is `EdgeAdded`. Keep `rematerialize()` as fallback for non-EdgeAdded events until Phases 19-20 cover those.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| IADD-01 | EdgeAdded events trigger per-hop delta derivation identifying which hop in each affected frame the new edge satisfies | The PathExtender module must iterate each hop in the frame's pattern, checking if the new edge's direction, edge_type, source node type, and target node type match the hop's constraints. A single edge can satisfy multiple hops in the same pattern (e.g., hop 0 and hop 2 in a 3-hop pattern if both have the same edge type filter). For each matching hop position K, deltas are derived independently. |
| IADD-02 | Backward prefix resolution finds existing paths from anchor to the hop before the affected edge | For each matching hop K, a partial DFS from the frame's anchor through hops 0..K-1 is performed, collecting paths that end at the new edge's source node. This reuses the same DFS logic as `Frame::dfs_collect` but stops at hop K-1 and filters the final node to match `edge.source`. For hop K=0, the backward prefix is simply `[anchor]` if `anchor == edge.source`. |
| IADD-03 | Forward path extension traverses from the new edge through remaining hops to produce complete new paths | For each backward prefix, the new edge's target is appended, then a partial DFS continues from the target through hops K+1..N-1. This also reuses DFS logic but starts at hop K+1 with the target node. Complete paths (length == hops + 1) are collected as +1 delta assertions. |
| IADD-04 | New paths asserted as +1 deltas via Frame::apply_delta without full DFS re-traverse | `Frame::apply_delta(path, epoch, Delta(+1))` already exists and is tested. The PathExtender returns a `Vec<(Vec<NodeId>, Delta)>` which the engine applies in sequence. No `frame.evict()` or `frame.rematerialize()` is called. |
| IADD-05 | Incremental EdgeAdded produces identical frame state to full re-traverse (oracle verified) | The Phase 17 `oracle_check()` function (engine.rs:2319) is used after every incremental EdgeAdded to verify exact match. The existing 6 oracle tests are extended/augmented to cover incremental path extension specifically. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| (no new deps) | - | All work is within existing Rust crate | Purely algorithmic; uses existing Frame, Graph, DiffCollection, HopSpec APIs |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| std::collections::HashSet | stable | Deduplication of generated paths (avoid double-asserting same path) | When a single edge satisfies multiple hop positions in the same frame |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Partial DFS for backward prefix | PathPositionIndex (O(1) hop-position lookup) | Deferred to v4 (OPT-01) -- partial DFS is sufficient for current scale |
| Stateless PathExtender function | Stateful RETE-style partial path cache | Deferred to v4 (OPT-02) -- adds complexity without proven need |
| Per-path apply_delta calls | Batch delta API | Deferred to v4 (OPT-03) -- current apply_delta is simple and correct |

**Installation:**
```bash
# No new dependencies -- purely algorithmic changes within existing crate
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  path_extender.rs   # NEW: Stateless incremental path extension for EdgeAdded
  engine.rs          # MODIFIED: Wire PathExtender into maintain_and_evaluate_frames
  frame.rs           # EXISTING: Used via apply_delta, query, pattern(), anchor()
  graph.rs           # EXISTING: Used via neighbors, get_node_type, get_property
  diff.rs            # EXISTING: DiffCollection unchanged
  routing.rs         # EXISTING: InvertedIndex unchanged
```

### Pattern 1: Stateless PathExtender Module
**What:** A `path_extender.rs` module containing a pure function `extend_edge_added()` that takes read-only references to the frame's metadata (anchor, pattern), the graph, and the EdgeAdded event, and returns a vector of path deltas. The function has no mutable state, no side effects, and is independently testable.
**When to use:** Every time an EdgeAdded event is routed to a frame.
**Key design decisions from STATE.md:**
- "PathExtender is stateless module taking read-only refs to Frame, Graph, Event"
- "No new Cargo dependencies needed -- purely algorithmic work using existing DiffCollection and Frame::apply_delta()"

**Example:**
```rust
// src/path_extender.rs

use crate::graph::Graph;
use crate::types::{Delta, Direction, Epoch, Filter, HopSpec, NodeId, TypeId};

/// Result of incremental edge-added extension: a list of new paths to assert.
pub struct EdgeAddedDeltas {
    /// New complete paths to assert as +1 deltas.
    pub new_paths: Vec<Vec<NodeId>>,
}

/// Computes new paths produced by adding an edge to the graph.
///
/// For each hop position in the frame's pattern that the new edge could
/// satisfy, performs backward prefix resolution (anchor -> hop K-1 ending
/// at edge source) and forward extension (edge target -> remaining hops).
///
/// # Arguments
/// * `anchor` - The frame's anchor node
/// * `pattern` - The frame's hop pattern
/// * `graph` - The current graph state (edge already added)
/// * `source` - Source node of the new edge
/// * `target` - Target node of the new edge
/// * `edge_type` - Type of the new edge
/// * `epoch` - Current epoch
pub fn extend_edge_added(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    source: NodeId,
    target: NodeId,
    edge_type: TypeId,
) -> EdgeAddedDeltas {
    let mut new_paths = Vec::new();

    for (hop_idx, hop) in pattern.iter().enumerate() {
        // Check if this edge could satisfy this hop
        if !edge_matches_hop(hop, source, target, edge_type, graph) {
            continue;
        }

        // Backward prefix: partial DFS from anchor through hops 0..hop_idx-1,
        // collecting paths that end at `source`
        let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, source);

        // Forward extension: from `target` through hops hop_idx+1..N-1
        for prefix in prefixes {
            let mut path_so_far = prefix;
            path_so_far.push(target);

            // If this was the last hop, path is complete
            if hop_idx == pattern.len() - 1 {
                new_paths.push(path_so_far);
            } else {
                // Continue DFS from target through remaining hops
                let extensions = forward_extend(
                    graph,
                    &path_so_far,
                    pattern,
                    hop_idx + 1,
                );
                new_paths.extend(extensions);
            }
        }
    }

    EdgeAddedDeltas { new_paths }
}
```

### Pattern 2: Backward Prefix Resolution via Partial DFS
**What:** A partial DFS from the frame's anchor through hops 0..K-1, filtering to paths that terminate at the new edge's source node. Reuses the same filter logic as `Frame::dfs_collect` (direction, edge_type, target_type, property filter) but stops early.
**When to use:** For each hop position K that the new edge matches.
**Critical insight:** For hop K=0, the backward prefix is simply `[anchor]` -- no DFS needed, just check that `anchor == source`.

**Example:**
```rust
/// Finds all partial paths from `anchor` through hops 0..hop_idx-1
/// that end at `required_end` (the new edge's source node).
fn backward_prefixes(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    hop_idx: usize,
    required_end: NodeId,
) -> Vec<Vec<NodeId>> {
    // Special case: hop_idx == 0 means the edge starts from anchor
    if hop_idx == 0 {
        if anchor == required_end {
            return vec![vec![anchor]];
        } else {
            return vec![];
        }
    }

    // Partial DFS from anchor through hops 0..hop_idx-1
    let mut results = Vec::new();
    let initial = vec![anchor];
    partial_dfs(graph, &initial, pattern, 0, hop_idx, required_end, &mut results);
    results
}

/// Recursive partial DFS that collects paths of exactly `target_depth` hops
/// from anchor, ending at `required_end`.
fn partial_dfs(
    graph: &Graph,
    current_path: &[NodeId],
    pattern: &[HopSpec],
    current_hop: usize,
    target_depth: usize,
    required_end: NodeId,
    results: &mut Vec<Vec<NodeId>>,
) {
    // Base case: reached target depth
    if current_hop == target_depth {
        if *current_path.last().unwrap() == required_end {
            results.push(current_path.to_vec());
        }
        return;
    }

    let hop = &pattern[current_hop];
    let current_node = *current_path.last().unwrap();
    let neighbors = graph.neighbors(current_node, hop.direction, hop.edge_type);

    for (_edge_id, neighbor_id) in neighbors {
        // Apply same filters as Frame::dfs_collect
        if let Some(target_type) = hop.target_type {
            if graph.get_node_type(neighbor_id) != Some(target_type) {
                continue;
            }
        }
        match &hop.filter {
            Filter::None => {}
            Filter::PropertyEquals { key, value } => {
                if graph.get_property(neighbor_id, *key) != Some(value) {
                    continue;
                }
            }
            Filter::HasProperty { key } => {
                if graph.get_property(neighbor_id, *key).is_none() {
                    continue;
                }
            }
        }

        let mut next_path = current_path.to_vec();
        next_path.push(neighbor_id);
        partial_dfs(graph, &next_path, pattern, current_hop + 1,
                     target_depth, required_end, results);
    }
}
```

### Pattern 3: Forward Extension via Remaining-Hop DFS
**What:** From the new edge's target node, continue DFS through hops K+1..N-1. This is identical to `Frame::dfs_collect` but starting at hop K+1 instead of hop 0.
**When to use:** After building each backward prefix + appending the target node.

**Example:**
```rust
/// Extends paths from current position through remaining hops.
fn forward_extend(
    graph: &Graph,
    current_path: &[NodeId],
    pattern: &[HopSpec],
    start_hop: usize,
) -> Vec<Vec<NodeId>> {
    let mut results = Vec::new();
    forward_dfs(graph, current_path, pattern, start_hop, &mut results);
    results
}

/// Recursive DFS from start_hop through end of pattern.
fn forward_dfs(
    graph: &Graph,
    current_path: &[NodeId],
    pattern: &[HopSpec],
    hop_index: usize,
    results: &mut Vec<Vec<NodeId>>,
) {
    if hop_index >= pattern.len() {
        results.push(current_path.to_vec());
        return;
    }

    let hop = &pattern[hop_index];
    let current_node = *current_path.last().unwrap();
    let neighbors = graph.neighbors(current_node, hop.direction, hop.edge_type);

    for (_edge_id, neighbor_id) in neighbors {
        if let Some(target_type) = hop.target_type {
            if graph.get_node_type(neighbor_id) != Some(target_type) {
                continue;
            }
        }
        match &hop.filter {
            Filter::None => {}
            Filter::PropertyEquals { key, value } => {
                if graph.get_property(neighbor_id, *key) != Some(value) {
                    continue;
                }
            }
            Filter::HasProperty { key } => {
                if graph.get_property(neighbor_id, *key).is_none() {
                    continue;
                }
            }
        }

        let mut next_path = current_path.to_vec();
        next_path.push(neighbor_id);
        forward_dfs(graph, &next_path, pattern, hop_index + 1, results);
    }
}
```

### Pattern 4: Edge-to-Hop Matching
**What:** Determines whether a new edge `(source, target, edge_type)` could satisfy a specific hop in a frame's pattern. Checks direction compatibility, edge type filter, and target node type filter.
**When to use:** For each hop in the pattern, before attempting backward prefix resolution.

**Example:**
```rust
/// Checks if a new edge could satisfy the given hop specification.
fn edge_matches_hop(
    hop: &HopSpec,
    source: NodeId,
    target: NodeId,
    edge_type: TypeId,
    graph: &Graph,
) -> bool {
    // Check edge type filter
    if let Some(required_type) = hop.edge_type {
        if edge_type != required_type {
            return false;
        }
    }

    // Check target node type filter (the node reached by this hop)
    // For Outgoing: the reached node is `target`
    // For Incoming: the reached node is `source`
    // For Any: either endpoint could be the "reached" node
    let reached_node = match hop.direction {
        Direction::Outgoing => target,
        Direction::Incoming => source,
        Direction::Any => {
            // For Any direction, the edge could be traversed either way
            // Need to check both possibilities
            // Return true if either direction works (caller handles both)
            if let Some(target_type) = hop.target_type {
                let target_matches = graph.get_node_type(target) == Some(target_type);
                let source_matches = graph.get_node_type(source) == Some(target_type);
                return target_matches || source_matches;
            }
            return true; // No type filter, any direction works
        }
    };

    // Check target node type
    if let Some(target_type) = hop.target_type {
        if graph.get_node_type(reached_node) != Some(target_type) {
            return false;
        }
    }

    // Check property filter on the reached node
    match &hop.filter {
        Filter::None => true,
        Filter::PropertyEquals { key, value } => {
            graph.get_property(reached_node, *key) == Some(value)
        }
        Filter::HasProperty { key } => {
            graph.get_property(reached_node, *key).is_some()
        }
    }
}
```

### Pattern 5: Engine Integration -- Conditional Dispatch
**What:** `maintain_and_evaluate_frames` is modified to accept the event, and for `EdgeAdded` events, calls `extend_edge_added` + `apply_delta` instead of `rematerialize`. For all other event types, falls back to `rematerialize` (until Phases 19-20).
**When to use:** In the engine's ingest pipeline Step 4.

**Example:**
```rust
// In engine.rs -- modified maintain_and_evaluate_frames
fn maintain_and_evaluate_frames(
    frames: &[(u64, Arc<RwLock<Frame>>)],
    graph: &Graph,
    epoch: Epoch,
    prev_deltas: &HashMap<u64, i64>,
    event: &Event,  // NEW: pass event for dispatch
) -> Vec<(u64, i64)> {
    std::thread::scope(|s| {
        let handles: Vec<_> = frames.iter().map(|(frame_id, frame_arc)| {
            let fid = *frame_id;
            let arc = Arc::clone(frame_arc);
            s.spawn(move || {
                let mut frame = arc.write().expect("RwLock poisoned");

                match event {
                    Event::EdgeAdded { source, target, type_id, .. } => {
                        // Incremental: compute new paths via PathExtender
                        let deltas = path_extender::extend_edge_added(
                            frame.anchor(),
                            frame.pattern(),
                            graph,
                            *source,
                            *target,
                            *type_id,
                        );
                        for path in deltas.new_paths {
                            frame.apply_delta(path, epoch, Delta(1));
                        }
                    }
                    _ => {
                        // Fallback: full re-traverse for non-EdgeAdded events
                        frame.rematerialize(graph, epoch);
                    }
                }

                let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
                let current = frame.net_delta();
                let _changed = tier1_check(previous, current);
                (fid, current)
            })
        }).collect();

        handles.into_iter()
            .map(|h| h.join().expect("Scoped thread panicked"))
            .collect()
    })
}
```

### Anti-Patterns to Avoid
- **Modifying the DFS logic in frame.rs:** The PathExtender must NOT modify Frame::dfs_collect or Frame::materialize. Those remain the correctness baseline. PathExtender replicates the filter logic (direction, edge_type, target_type, property filter) in its own functions.
- **Forgetting Direction::Any edge matching:** A hop with `Direction::Any` means the new edge `(src, tgt)` could be traversed as `src -> tgt` (outgoing) OR `tgt -> src` (incoming). Both directions must be checked, which means for `Any` hops, the "reached node" can be either endpoint. The backward prefix must search for paths ending at either `source` or `target`.
- **Double-counting paths:** If a new edge satisfies hop K at position X and also satisfies hop K at position Y in the same frame, the same complete path could be generated twice. All generated paths should be deduplicated before asserting as deltas.
- **Asserting paths that already exist:** If a new edge creates a path that was already materialized (e.g., parallel edges), the +1 delta will produce multiplicity > 1. This is correct multiset behavior for DiffCollection, matching what full rematerialize would produce (it re-asserts all paths from scratch after evict). However, since we don't evict, we must ONLY assert genuinely NEW paths -- paths that exist because of the new edge specifically. The key insight: since the edge was just added, any path traversing this specific edge is new by definition.
- **Not passing event to maintain_and_evaluate_frames:** The current function signature doesn't include the event. It must be added to enable dispatch.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hop filter evaluation | Custom filter matching | Replicate Frame::dfs_collect filter chain | Must be identical to ensure oracle match |
| Delta application | Manual DiffCollection manipulation | Frame::apply_delta(path, epoch, delta) | Already tested, handles net_delta cache update |
| Path deduplication | Custom dedup logic | HashSet<Vec<NodeId>> | Standard library, handles all edge cases |
| Event-to-frame routing | Manual frame scanning | InvertedIndex::affected_frames() | Already O(affected) via SetTrie |
| Correctness verification | New oracle | Existing oracle_check() from Phase 17 | Battle-tested with 6 scenarios |

**Key insight:** The PathExtender is algorithmically new but all its building blocks exist: DFS traversal logic is modeled on Frame::dfs_collect, delta application uses Frame::apply_delta, and correctness is verified by Phase 17's oracle_check. The innovation is the decomposition into backward prefix + forward extension, not the individual operations.

## Common Pitfalls

### Pitfall 1: Direction Symmetry for Backward Prefix
**What goes wrong:** For a hop with `Direction::Incoming`, the new edge `(src, tgt)` means the traversal goes `tgt -> src` (following incoming edge from tgt's perspective means arriving from src). The "reached node" at this hop is `source`, not `target`. The backward prefix must end at `target` (the node FROM which the incoming edge is followed), not `source`.
**Why it happens:** Confusing "edge source/target" with "traversal source/target". An incoming hop at node N follows edges where N is the target, reaching the edge's source.
**How to avoid:** For each direction, clearly map: Outgoing hop at node N traverses edges where N is source, reaching target. Incoming hop at node N traverses edges where N is target, reaching source. For backward prefix, the "end node" before the hop is: Outgoing -> edge source, Incoming -> edge target, Any -> either.
**Warning signs:** Oracle mismatch on frames with `Direction::Incoming` hops.

### Pitfall 2: Existing Paths vs New Paths
**What goes wrong:** The incremental approach asserts +1 deltas for new paths. But the frame already contains previously materialized paths (from registration or prior events). If we call `apply_delta(+1)` for a path that already exists with multiplicity 1, the frame will show multiplicity 2 -- which differs from a fresh rematerialize that would show multiplicity 1.
**Why it happens:** Rematerialize does `evict()` (clear all) then `materialize()` (re-assert from scratch). Incremental does NOT evict -- it keeps existing state and only adds new paths.
**How to avoid:** The PathExtender must ONLY return paths that traverse the specific new edge. Since the edge was just added to the graph in Step 2 of ingest (before Step 4 maintenance), any path through this edge is genuinely new -- it could not have existed before. However, we must be careful with multi-hop patterns where the same complete path could be formed via different edges. The key invariant: a path is "new due to this edge" if and only if it traverses the new edge at some hop position. Since the edge didn't exist before, no prior materialization could have included it.
**Warning signs:** Oracle mismatch showing multiplicity > 1 on paths that should be multiplicity 1.

### Pitfall 3: Graph State Timing
**What goes wrong:** The `extend_edge_added` function runs AFTER the edge has been added to the graph (Step 2 of ingest). This means `graph.neighbors()` already includes the new edge. The backward prefix DFS must be careful: when traversing hops 0..K-1, the graph already contains the new edge. If the new edge happens to satisfy a hop OTHER than K in the backward prefix path, we might find backward prefixes that didn't exist before the edge was added.
**Why it happens:** The graph mutation is applied before frame maintenance in the ingest pipeline.
**How to avoid:** This is actually correct behavior. Consider: if adding edge E enables a new backward prefix path (because E also satisfies an earlier hop), then the full path through both the earlier hop AND hop K is genuinely new. The full DFS rematerialize would also find this path. So the backward prefix correctly uses the post-mutation graph.
**Warning signs:** This is NOT a bug -- but it means backward prefixes can include paths that traverse the new edge at earlier hops. The PathExtender must ensure no double-counting if the same complete path is generated from multiple hop positions.

### Pitfall 4: Path Deduplication Across Hop Positions
**What goes wrong:** If a new edge matches at hop positions K=1 and K=2 in a 4-hop pattern, the same complete path might be generated by both iterations (backward prefix through K=1 and forward from K=1 might produce the same path as backward prefix through K=2 and forward from K=2).
**Why it happens:** The edge satisfies multiple hops, and the DFS from different starting points can converge to the same complete path.
**How to avoid:** Collect all generated paths into a `HashSet<Vec<NodeId>>` before asserting deltas. Only assert each unique path once.
**Warning signs:** Oracle mismatch showing double-counted paths (multiplicity 2 instead of 1).

### Pitfall 5: Empty Pattern Edge Case
**What goes wrong:** A frame with zero hops (empty pattern) has `query()` returning `[[anchor]]` -- just the anchor node itself. An EdgeAdded event cannot affect a zero-hop frame because there are no hops to satisfy.
**Why it happens:** The InvertedIndex might still route EdgeAdded events to zero-hop frames if the frame's anchor is the edge's source or target.
**How to avoid:** If `pattern.is_empty()`, return empty deltas immediately. No hop can be matched, so no new paths can be generated.
**Warning signs:** Unexpected paths appearing in zero-hop frames.

### Pitfall 6: Borrow Checker with Event Reference in thread::scope
**What goes wrong:** The `maintain_and_evaluate_frames` function currently takes `frames`, `graph`, `epoch`, and `prev_deltas`. Adding `event: &Event` requires the reference to be valid across all scoped threads.
**Why it happens:** `std::thread::scope` requires all captured references to outlive the scope.
**How to avoid:** Pass `event` as `&Event` alongside `graph` -- both are borrowed from the Engine's `ingest` method and outlive the scope. The Event is cloned at the top of `ingest()` (`event.clone()`), but the original is still borrowed from the function parameter. Alternatively, clone the relevant fields (source, target, type_id) before entering the scope.
**Warning signs:** Compilation error about lifetime of event reference.

### Pitfall 7: flush_coalescer Must Also Use PathExtender
**What goes wrong:** Phase 17 research identified that `flush_coalescer()` also calls `maintain_and_evaluate_frames`. If the coalescer batches multiple events including EdgeAdded events, the flushed batch must also use incremental path extension.
**Why it happens:** The coalescer accumulates events and flushes a batch. The batch contains coalesced entries (one per node), not original events. The original events are not preserved in the batch.
**How to avoid:** For Phase 18, the coalescer path can continue to use full rematerialize as fallback. The coalescer deduplicates by node_id and doesn't preserve the original event type. Since the Phase 18 optimization targets only EdgeAdded events on the main (non-coalescer) path, this is acceptable. A future optimization could add event-type awareness to the coalescer.
**Warning signs:** No issue as long as coalescer path uses rematerialize.

## Code Examples

### Complete PathExtender Module Skeleton

```rust
// src/path_extender.rs
//! Incremental path extension for EdgeAdded events.
//!
//! Given a frame's anchor and pattern, computes new complete paths
//! produced by a newly added edge without full DFS re-traverse.

use crate::graph::Graph;
use crate::types::{Direction, Filter, HopSpec, NodeId, TypeId};

/// Result of incremental edge-added path extension.
pub struct EdgeAddedDeltas {
    /// New complete paths to assert as +1 deltas.
    pub new_paths: Vec<Vec<NodeId>>,
}

/// Computes new paths for a frame produced by a newly added edge.
pub fn extend_edge_added(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    source: NodeId,
    target: NodeId,
    edge_type: TypeId,
) -> EdgeAddedDeltas {
    if pattern.is_empty() {
        return EdgeAddedDeltas { new_paths: vec![] };
    }

    let mut all_paths = Vec::new();

    for (hop_idx, hop) in pattern.iter().enumerate() {
        if !edge_matches_hop(hop, source, target, edge_type, graph) {
            continue;
        }

        // Determine "origin node" and "reached node" based on direction
        let (origin, reached) = match hop.direction {
            Direction::Outgoing => (source, target),
            Direction::Incoming => (target, source),
            Direction::Any => {
                // Try both orientations
                let mut paths_any = Vec::new();
                // Outgoing interpretation: src -> tgt
                if edge_matches_hop_directed(hop, target, edge_type, graph) {
                    let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, source);
                    for prefix in prefixes {
                        extend_forward(graph, prefix, target, pattern, hop_idx, &mut paths_any);
                    }
                }
                // Incoming interpretation: tgt -> src
                if edge_matches_hop_directed_incoming(hop, source, edge_type, graph) {
                    let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, target);
                    for prefix in prefixes {
                        extend_forward(graph, prefix, source, pattern, hop_idx, &mut paths_any);
                    }
                }
                all_paths.extend(paths_any);
                continue;
            }
        };

        let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, origin);
        for prefix in prefixes {
            extend_forward(graph, prefix, reached, pattern, hop_idx, &mut all_paths);
        }
    }

    // Deduplicate paths
    let mut seen = std::collections::HashSet::new();
    all_paths.retain(|path| seen.insert(path.clone()));

    EdgeAddedDeltas { new_paths: all_paths }
}
```

### Engine Integration Point

```rust
// In engine.rs, modify maintain_and_evaluate_frames signature and body:

fn maintain_and_evaluate_frames(
    frames: &[(u64, Arc<RwLock<Frame>>)],
    graph: &Graph,
    epoch: Epoch,
    prev_deltas: &HashMap<u64, i64>,
    event: &Event,
) -> Vec<(u64, i64)> {
    std::thread::scope(|s| {
        let handles: Vec<_> = frames.iter().map(|(frame_id, frame_arc)| {
            let fid = *frame_id;
            let arc = Arc::clone(frame_arc);
            s.spawn(move || {
                let mut frame = arc.write().expect("RwLock poisoned");

                match event {
                    Event::EdgeAdded { source, target, type_id, .. } => {
                        let deltas = crate::path_extender::extend_edge_added(
                            frame.anchor(), frame.pattern(), graph,
                            *source, *target, *type_id,
                        );
                        for path in deltas.new_paths {
                            frame.apply_delta(path, epoch, Delta(1));
                        }
                    }
                    _ => {
                        frame.rematerialize(graph, epoch);
                    }
                }

                let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
                let current = frame.net_delta();
                let _changed = tier1_check(previous, current);
                (fid, current)
            })
        }).collect();

        handles.into_iter()
            .map(|h| h.join().expect("Scoped thread panicked"))
            .collect()
    })
}
```

### Oracle-Verified Test Scenario

```rust
#[test]
fn test_incremental_edge_added_oracle() {
    let mut engine = Engine::new(64);

    // Build initial graph
    engine.ingest(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(10) });
    engine.ingest(Event::NodeAdded { node_id: NodeId(2), type_id: TypeId(20) });
    engine.ingest(Event::NodeAdded { node_id: NodeId(3), type_id: TypeId(20) });
    engine.ingest(Event::NodeAdded { node_id: NodeId(4), type_id: TypeId(30) });

    // Initial edges
    let e1 = engine.ingest(Event::EdgeAdded {
        edge_id: EdgeId(0), source: NodeId(1), target: NodeId(2), type_id: TypeId(100),
    });

    // Register 2-hop frame: anchor=1, hop1=type100->type20, hop2=type200->type30
    let pattern = vec![
        HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        },
        HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(200)),
            target_type: Some(TypeId(30)),
            filter: Filter::None,
        },
    ];
    let fid = engine.register_frame(NodeId(1), pattern, e1);

    // No 2-hop path yet (no edge from node 2 to node 4)
    oracle_check(&mut engine, fid);
    assert_eq!(engine.query_frame(fid).unwrap().len(), 0);

    // Add the second hop edge: 2->4 type 200
    // This should incrementally produce path [1, 2, 4]
    engine.ingest(Event::EdgeAdded {
        edge_id: EdgeId(1), source: NodeId(2), target: NodeId(4), type_id: TypeId(200),
    });

    // Oracle check: incremental result must match full re-traverse
    oracle_check(&mut engine, fid);
    let paths = engine.query_frame(fid).unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], vec![NodeId(1), NodeId(2), NodeId(4)]);
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No post-registration maintenance | Full re-traverse (evict+DFS) on every mutation (Phase 17) | v3.0 Phase 17 | Frames stay in sync but O(full_DFS) per event |
| **Full re-traverse for EdgeAdded** | **Incremental per-hop extension (Phase 18)** | **v3.0 Phase 18 (this phase)** | **O(backward_prefix + forward_extension) instead of O(full_DFS)** |

**Deprecated/outdated:**
- After Phase 18, `frame.rematerialize()` is still used for non-EdgeAdded events (EdgeRemoved, NodeRemoved, PropertyChanged). It becomes the fallback path, not the primary maintenance mechanism for EdgeAdded.

## Open Questions

1. **Direction::Any with bidirectional edge matching**
   - What we know: When a hop has `Direction::Any`, the new edge `(src, tgt)` can be traversed in either direction. This means two backward prefix searches (one ending at `src`, one ending at `tgt`) and two forward extensions.
   - What's unclear: Whether existing frames in the test suite use `Direction::Any` hops, and whether the oracle tests cover this case.
   - Recommendation: Add a test specifically for `Direction::Any` with edge addition. If no existing frames use `Any`, keep the implementation simple but correct.

2. **Interaction between incremental EdgeAdded and subsequent non-EdgeAdded events**
   - What we know: After Phase 18, EdgeAdded uses incremental +1 deltas, but EdgeRemoved/NodeRemoved/PropertyChanged still use full rematerialize (evict + DFS). The rematerialize will evict the incremental state and rebuild from scratch.
   - What's unclear: Whether the incremental +1 state is compatible with subsequent rematerialize. Specifically: does `rematerialize()` correctly rebuild including the incrementally-added paths?
   - Recommendation: This should work because rematerialize does a fresh DFS on the current graph, which already includes the new edge. The incremental state is evicted and rebuilt from scratch. Oracle tests covering EdgeAdded followed by EdgeRemoved/PropertyChanged will verify this.

3. **Performance of backward prefix resolution for deep patterns**
   - What we know: Backward prefix is O(B^K) where B is branching factor and K is the hop depth before the matched position. For frames with many hops and dense graphs, this could be expensive.
   - What's unclear: Whether current test graphs are deep enough to expose performance issues.
   - Recommendation: Accept the O(B^K) cost for Phase 18. STATE.md already notes "Backward prefix resolution is O(B^K) per mutation; may need partial path cache for deep patterns (defer to v4 unless benchmarks demand it)". Phase 21 benchmarks will quantify.

## Sources

### Primary (HIGH confidence)
- `src/frame.rs` lines 86-324 -- Frame struct, materialize(), dfs_collect(), apply_delta(), rematerialize(), evict(), pattern(), anchor() accessors
- `src/engine.rs` lines 256-462 -- Full ingest pipeline, Step 2 graph mutation before Step 4 maintenance
- `src/engine.rs` lines 763-791 -- maintain_and_evaluate_frames helper (Phase 17)
- `src/engine.rs` lines 2307-2354 -- oracle_check() function and 6 oracle test scenarios
- `src/graph.rs` lines 341-369 -- Graph::neighbors() API with direction and edge_type filtering
- `src/types.rs` lines 92-198 -- Direction, HopSpec, Filter, Event::EdgeAdded definitions
- `src/diff.rs` lines 63-213 -- DiffCollection::assert_tuple, current_state, aggregate_net_delta
- `src/routing.rs` lines 56-241 -- InvertedIndex::affected_frames routing for EdgeAdded events

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` -- IADD-01 through IADD-05 requirement definitions
- `.planning/STATE.md` -- Project decisions: "PathExtender is stateless module", "No new Cargo dependencies", "O(B^K) backward prefix deferred to v4"
- `.planning/ROADMAP.md` -- Phase dependency chain: 17 -> 18 -> 19 -> 20 -> 21
- `.planning/phases/17-re-diff-baseline/17-01-SUMMARY.md` -- Phase 17 deliverables: maintain_and_evaluate_frames, oracle_check

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - No new dependencies, all existing APIs verified in source code
- Architecture: HIGH - PathExtender design follows directly from the backward prefix + forward extension algorithm, which maps cleanly to Frame::dfs_collect decomposition. Engine integration point (maintain_and_evaluate_frames) is well-understood from Phase 17.
- Pitfalls: HIGH - Direction symmetry, path deduplication, graph state timing, and borrow checker issues all identified from direct code analysis of dfs_collect, neighbors(), and thread::scope patterns.
- Algorithm correctness: HIGH - The backward prefix + forward extension decomposition is provably equivalent to full DFS when the only change is a single edge addition. Oracle verification provides the safety net.

**Research date:** 2026-02-26
**Valid until:** 2026-03-26 (stable -- no external dependencies, all internal code)
