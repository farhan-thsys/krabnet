# Phase 19: Incremental Edge and Node Removal - Research

**Researched:** 2026-02-26
**Domain:** Incremental path retraction for EdgeRemoved and NodeRemoved events via targeted -1 deltas
**Confidence:** HIGH

## Summary

Phase 19 replaces the full DFS re-traverse fallback (`frame.rematerialize()`) for `EdgeRemoved` and `NodeRemoved` events with incremental retraction. When an edge is removed, the system scans the frame's currently materialized paths and retracts (via `-1` deltas) exactly those paths that traverse the removed edge. When a node is removed, the system must capture edge adjacency information *before* the graph mutation destroys it (a `DeletionContext`), then retract all paths that traverse the removed node.

The core algorithmic challenge for edge removal is **identifying affected paths from materialized state**: given a frame's current paths (from `DiffCollection::current_state()`), filter those paths to find which ones contain the removed edge's `(source, target)` as consecutive nodes at a hop position where the edge type matches the hop's constraint. The core challenge for node removal is **ordering**: the engine's `ingest()` currently applies the graph mutation (Step 2) before frame maintenance (Step 4), meaning by the time we need to retract paths for a removed node, the node and its edges are already gone from the graph. A `DeletionContext` must be captured *before* graph mutation for `NodeRemoved` events.

The correctness invariant is identical to Phase 18: incremental handling must produce **identical** frame state to the Phase 17 full re-traverse. The existing `oracle_check()` function and oracle tests (tests 2, 3, 5, 9 already cover EdgeRemoved/NodeRemoved scenarios) serve as the verification backbone. Phase 19 converts these from "tests that pass via full rematerialize fallback" to "tests that pass via incremental -1 deltas."

**Primary recommendation:** Add two new functions to `src/path_extender.rs`: `retract_edge_removed()` and `retract_node_removed()`. Wire them into `maintain_and_evaluate_frames` in the `match event` dispatch. For `NodeRemoved`, capture a `DeletionContext` struct (containing the node's connected edges with their source/target/type) in the engine's `ingest()` Step 2 *before* calling `graph.remove_node()`, then pass it through to the frame maintenance step.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| IREM-01 | EdgeRemoved events identify all materialized paths that traverse the removed edge | The `retract_edge_removed()` function receives `(source, target)` of the removed edge and scans `frame.snapshot(Epoch(u64::MAX))` (which is `current_state()` without incrementing query_count) to find paths containing the consecutive pair `(source, target)` at any hop position where the direction is compatible. Since `EdgeRemoved` does NOT carry `type_id`, the retraction must match purely on node adjacency -- any materialized path containing consecutive nodes `(source, target)` at an outgoing hop, or `(target, source)` at an incoming hop, is affected. The edge is already removed from the graph so no type verification is possible; this is correct because the path would not have been materialized unless it matched at materialization time. |
| IREM-02 | Affected paths retracted as -1 deltas via Frame::apply_delta without full DFS re-traverse | Each identified path is retracted via `frame.apply_delta(path.clone(), epoch, Delta(-1))`. No `frame.evict()` or `frame.rematerialize()` is called. The DiffCollection handles the +1/-1 math correctly: the path was previously asserted with +1, and the -1 retraction nets to zero, which `current_state()` filters out. |
| IREM-03 | Incremental EdgeRemoved produces identical frame state to full re-traverse (oracle verified) | Oracle tests 2 (EdgeRemoved), 5 (diamond EdgeRemoved), and 9 (EdgeAdded then EdgeRemoved) already exist and currently pass via full rematerialize. After wiring incremental retraction, these same tests must still pass. Additional tests for multi-hop edge removal and multiple simultaneous paths through the same edge should be added. |
| NDEL-01 | NodeRemoved events capture edge information before graph mutation destroys adjacency (DeletionContext) | A `DeletionContext` struct is captured in engine `ingest()` Step 2 *before* `graph.remove_node()` is called. It contains `Vec<(NodeId, NodeId, TypeId)>` -- the `(source, target, type_id)` for every edge connected to the removed node. This is gathered via `graph.neighbors(node_id, Direction::Any, None)` which returns all connected edges. The DeletionContext also stores the removed `node_id` itself. After capture, `graph.remove_node()` proceeds normally. |
| NDEL-02 | All paths traversing the removed node retracted as -1 deltas | The `retract_node_removed()` function scans materialized paths and retracts any path containing the removed `node_id` at any position. This is simpler than edge removal because any path mentioning the node is definitely affected -- the node is gone. Each affected path gets `apply_delta(path, epoch, Delta(-1))`. |
| NDEL-03 | Incremental NodeRemoved produces identical frame state to full re-traverse (oracle verified) | Oracle test 3 (NodeRemoved) already exists and passes via rematerialize. After wiring incremental node retraction, it must still pass. Additional tests for diamond graph node removal and multi-frame cascade should be added. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| (no new deps) | - | All work is within existing Rust crate | Purely algorithmic; uses existing Frame, Graph, DiffCollection, PathExtender APIs |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| std::collections::HashSet | stable | Deduplication of retracted paths (avoid double-retracting same path) | When a node removal affects multiple hop positions in the same path |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Scanning materialized paths | Maintaining a reverse index (edge -> paths) | Deferred to v4 (OPT-01) -- scanning current_state() is O(paths * hops_per_path) which is acceptable for current scale |
| DeletionContext pre-capture | Re-ordering ingest to do maintenance before mutation | Would break the established ingest pipeline ordering and affect EdgeAdded which needs post-mutation graph state |
| Per-path scanning for NodeRemoved | Using inverted index node routing | Inverted index already routes the event to affected frames; the scanning identifies *which paths within those frames* are affected |

**Installation:**
```bash
# No new dependencies -- purely algorithmic changes within existing crate
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  path_extender.rs   # MODIFIED: Add retract_edge_removed() and retract_node_removed()
  engine.rs          # MODIFIED: DeletionContext capture + dispatch in maintain_and_evaluate_frames
  frame.rs           # EXISTING: Used via snapshot(), apply_delta(), pattern(), anchor()
  graph.rs           # POSSIBLY MODIFIED: Add edges_for_node() helper if needed
  types.rs           # EXISTING: Event::EdgeRemoved, Event::NodeRemoved definitions
  diff.rs            # EXISTING: DiffCollection::current_state(), retract_tuple()
  routing.rs         # EXISTING: InvertedIndex routes EdgeRemoved/NodeRemoved to affected frames
```

### Pattern 1: Path Scanning for Edge Retraction
**What:** Given a frame's current materialized paths and a removed edge `(source, target)`, scan all paths to find those containing the edge's endpoints as consecutive nodes at a valid hop position. Retract each as a -1 delta.
**When to use:** Every time an `EdgeRemoved` event is routed to a frame.
**Key insight:** `EdgeRemoved` events do NOT carry `type_id` (see `types.rs` line 181-188). However, this is acceptable because a materialized path could only exist if the edge previously satisfied the hop's type constraint at materialization time. Since the edge is being removed, every materialized path traversing `(source, target)` or `(target, source)` at any hop is affected -- regardless of type.

**Critical direction logic:** For each hop `K` in the frame's pattern:
- `Direction::Outgoing` at hop K: the path traverses `path[K] -> path[K+1]` via an outgoing edge. The removed edge `(source, target)` affects this hop if `path[K] == source && path[K+1] == target`.
- `Direction::Incoming` at hop K: the path traverses `path[K] -> path[K+1]` but via an incoming edge at `path[K]`, meaning the edge direction is `path[K+1] -> path[K]`. The removed edge `(source, target)` affects this hop if `path[K] == target && path[K+1] == source`.
- `Direction::Any` at hop K: Either interpretation -- check both.

**However, a simpler approach is viable:** Since the edge has already been removed from the graph by the time `maintain_and_evaluate_frames` runs, we can simply check: does the path contain the pair `(source, target)` or `(target, source)` as consecutive nodes at ANY position? If yes, the path is affected. This works because:
1. If the path had `source` at position K and `target` at position K+1, the edge `(source, target)` was the outgoing edge at hop K. Since it's removed, this path is broken.
2. If the path had `target` at position K and `source` at position K+1, the edge `(source, target)` was the incoming edge at hop K. Since it's removed, this path is broken.
3. For `Direction::Any`, both orientations apply.

**Even simpler:** Since each materialized path was produced by a DFS that followed actual graph edges, and the only edges between two nodes are tracked in adjacency lists, a path containing consecutive `(A, B)` means there was an edge connecting A to B (in some direction) that satisfied the hop. If the removed edge was the only such edge, the path is invalid. If there were parallel edges, the path might still be valid -- but the full re-traverse would also re-assert it, and our oracle check would catch any mismatch.

**Parallel edge nuance:** If node A has two outgoing edges to node B (both satisfying the hop), and one is removed, the path `[..., A, B, ...]` should still exist. The full rematerialize would re-assert it once (from the remaining edge). The incremental approach must NOT retract this path. The solution: after identifying candidate paths to retract, verify that no remaining edge in the graph still supports each hop traversal. This requires checking `graph.neighbors()` for the consecutive pair at the relevant hop.

**Example:**
```rust
/// Result of incremental edge-removed retraction.
pub struct EdgeRemovedDeltas {
    /// Paths to retract as -1 deltas.
    pub retracted_paths: Vec<Vec<NodeId>>,
}

/// Computes paths to retract from a frame when an edge is removed.
///
/// Scans the frame's current materialized paths and identifies those
/// that traversed the removed edge. A path is affected if it contains
/// consecutive nodes (source, target) or (target, source) at a hop
/// position where no remaining edge in the graph supports the traversal.
pub fn retract_edge_removed(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    source: NodeId,
    target: NodeId,
) -> EdgeRemovedDeltas {
    let mut retracted = Vec::new();

    for path in current_paths {
        if path_uses_removed_edge(path, pattern, graph, source, target) {
            retracted.push((*path).clone());
        }
    }

    // Deduplicate
    let mut seen = HashSet::new();
    retracted.retain(|p| seen.insert(p.clone()));

    EdgeRemovedDeltas { retracted_paths: retracted }
}

/// Checks whether a materialized path is invalidated by the removal
/// of edge (source, target).
fn path_uses_removed_edge(
    path: &[NodeId],
    pattern: &[HopSpec],
    graph: &Graph,
    removed_source: NodeId,
    removed_target: NodeId,
) -> bool {
    // A path has pattern.len() hops and pattern.len() + 1 nodes.
    // Hop K connects path[K] to path[K+1].
    for (hop_idx, hop) in pattern.iter().enumerate() {
        let from = path[hop_idx];
        let to = path[hop_idx + 1];

        let edge_matches = match hop.direction {
            Direction::Outgoing => from == removed_source && to == removed_target,
            Direction::Incoming => from == removed_target && to == removed_source,
            Direction::Any => {
                (from == removed_source && to == removed_target)
                    || (from == removed_target && to == removed_source)
            }
        };

        if edge_matches {
            // Check if any remaining edge in the graph still supports this hop.
            // If yes, the path is NOT broken (parallel edge survives).
            let remaining = graph.neighbors(from, hop.direction, hop.edge_type);
            let still_connected = remaining.iter().any(|(_, n)| *n == to);
            if !still_connected {
                return true; // Path is broken at this hop
            }
        }
    }
    false
}
```

### Pattern 2: DeletionContext for Node Removal
**What:** Before `graph.remove_node()` destroys a node's adjacency information, capture a `DeletionContext` containing all edges connected to the node. This context is passed to the frame maintenance step so `retract_node_removed()` can identify affected paths.
**When to use:** In `engine.ingest()` Step 2, when the event is `NodeRemoved`.
**Key design decision from STATE.md:** "DeletionContext captures edge info before graph mutation destroys adjacency"

**Example:**
```rust
/// Context captured before a node is removed from the graph.
///
/// Contains the edge adjacency information that would be lost after
/// `graph.remove_node()` destroys the node's data. Used by the
/// incremental node-removal path retraction algorithm.
#[derive(Debug, Clone)]
pub struct DeletionContext {
    /// The node being removed.
    pub node_id: NodeId,
    /// Edges connected to the removed node: (source, target, type_id).
    /// Captured from the graph BEFORE the node is removed.
    pub edges: Vec<(NodeId, NodeId, TypeId)>,
}

// In engine.rs, ingest() Step 2:
Event::NodeRemoved { node_id } => {
    // Capture DeletionContext BEFORE graph mutation
    let deletion_ctx = Self::capture_deletion_context(&self.graph, *node_id);
    self.graph.remove_node(*node_id);
    // Store deletion_ctx for use in Step 4
}
```

### Pattern 3: Path Scanning for Node Retraction
**What:** Given a frame's current materialized paths and a removed node, retract all paths containing that node at any position. This is simpler than edge removal because a removed node definitively invalidates any path it appears in.
**When to use:** Every time a `NodeRemoved` event is routed to a frame.

**Example:**
```rust
/// Result of incremental node-removed retraction.
pub struct NodeRemovedDeltas {
    /// Paths to retract as -1 deltas.
    pub retracted_paths: Vec<Vec<NodeId>>,
}

/// Computes paths to retract from a frame when a node is removed.
///
/// Any materialized path containing the removed node is retracted.
pub fn retract_node_removed(
    current_paths: &[&Vec<NodeId>],
    removed_node: NodeId,
) -> NodeRemovedDeltas {
    let retracted: Vec<Vec<NodeId>> = current_paths
        .iter()
        .filter(|path| path.contains(&removed_node))
        .map(|path| (*path).clone())
        .collect();

    NodeRemovedDeltas { retracted_paths: retracted }
}
```

### Pattern 4: Engine Integration -- Extended Dispatch
**What:** `maintain_and_evaluate_frames` is extended with `EdgeRemoved` and `NodeRemoved` arms in the `match event` block. For `EdgeRemoved`, the handler calls `retract_edge_removed()` using the frame's current state (via `snapshot()`). For `NodeRemoved`, a `DeletionContext` must be available (captured before graph mutation).
**When to use:** In the engine's ingest pipeline Step 4.

**Critical design consideration:** The function signature of `maintain_and_evaluate_frames` currently takes `event: &Event`. For `NodeRemoved`, we also need the `DeletionContext`. Options:
1. Add `deletion_ctx: Option<&DeletionContext>` parameter -- cleanest, no wrapper needed.
2. Wrap event + context in an enum -- more complex but encapsulates all event metadata.
3. Store DeletionContext as a field on Engine -- works but pollutes Engine state.

**Recommendation:** Option 1 -- add an optional `DeletionContext` parameter.

**Example:**
```rust
fn maintain_and_evaluate_frames(
    frames: &[(u64, Arc<RwLock<Frame>>)],
    graph: &Graph,
    epoch: Epoch,
    prev_deltas: &HashMap<u64, i64>,
    event: &Event,
    deletion_ctx: Option<&DeletionContext>,  // NEW for NodeRemoved
) -> Vec<(u64, i64)> {
    std::thread::scope(|s| {
        let handles: Vec<_> = frames.iter().map(|(frame_id, frame_arc)| {
            let fid = *frame_id;
            let arc = Arc::clone(frame_arc);
            s.spawn(move || {
                let mut frame = arc.write().expect("RwLock poisoned");
                match event {
                    Event::EdgeAdded { source, target, type_id, .. } => {
                        // Existing incremental +1 path extension
                        let deltas = crate::path_extender::extend_edge_added(
                            frame.anchor(), frame.pattern(), graph,
                            *source, *target, *type_id,
                        );
                        for path in deltas.new_paths {
                            frame.apply_delta(path, epoch, Delta(1));
                        }
                    }
                    Event::EdgeRemoved { source, target, .. } => {
                        // NEW: Incremental -1 retraction
                        let current = frame.snapshot(Epoch(u64::MAX));
                        let deltas = crate::path_extender::retract_edge_removed(
                            frame.anchor(), frame.pattern(), graph,
                            &current, *source, *target,
                        );
                        for path in deltas.retracted_paths {
                            frame.apply_delta(path, epoch, Delta(-1));
                        }
                    }
                    Event::NodeRemoved { node_id } => {
                        // NEW: Incremental -1 retraction using DeletionContext
                        let current = frame.snapshot(Epoch(u64::MAX));
                        let deltas = crate::path_extender::retract_node_removed(
                            &current, *node_id,
                        );
                        for path in deltas.retracted_paths {
                            frame.apply_delta(path, epoch, Delta(-1));
                        }
                    }
                    _ => {
                        // Fallback: full re-traverse for remaining event types
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

### Pattern 5: Graph Helper for DeletionContext Capture
**What:** A method on Graph (or a free function taking `&Graph`) that collects all edges connected to a node before removal. Uses existing `neighbors()` API with `Direction::Any` and `None` edge type filter to get all connected edges, then retrieves edge metadata.
**When to use:** In engine `ingest()` before `graph.remove_node()`.

**Example:**
```rust
// In engine.rs (or graph.rs):
fn capture_deletion_context(graph: &Graph, node_id: NodeId) -> DeletionContext {
    // Get all neighbors in both directions (all edge types)
    let outgoing = graph.neighbors(node_id, Direction::Outgoing, None);
    let incoming = graph.neighbors(node_id, Direction::Incoming, None);

    let mut edges = Vec::new();

    // Outgoing edges: node_id is source
    for (_edge_id, target) in &outgoing {
        // We need edge type, but neighbors() doesn't return it.
        // We need to either:
        //   a) Add a neighbors_with_type() method to Graph, or
        //   b) Retrieve edge data from the edge map, or
        //   c) Change neighbors() to return type info.
        // HOWEVER: for node removal retraction, we don't actually
        // need edge types -- we just need to know which paths contain
        // the removed node. The DeletionContext edges are informational.
    }

    DeletionContext {
        node_id,
        edges,
    }
}
```

**Important realization:** For `retract_node_removed()`, we do NOT need edge adjacency information at all. We simply scan paths for the removed node's presence. The `DeletionContext` is more relevant if we wanted to retract paths using the same edge-matching logic as `retract_edge_removed()` -- treating node removal as N simultaneous edge removals. However, the simpler "scan for node presence" approach is sufficient and correct:
- Any path containing the removed node is definitively invalid (the node is gone).
- No parallel-edge concern exists (unlike edge removal) because the node itself is gone.

**Revised recommendation:** The `DeletionContext` may be simplified to just `{ node_id: NodeId }` for the retraction algorithm. The edge adjacency information is only needed if we want to use it for inverted index updates or other bookkeeping. For the core retraction, scanning paths for node presence is complete.

### Anti-Patterns to Avoid
- **Retracting paths without checking for parallel edges (EdgeRemoved):** If two edges connect A->B (both satisfying the hop), removing one should NOT retract the path. The path is still valid via the remaining edge. Must check `graph.neighbors()` after removal to verify the path is truly broken.
- **Trying to check edge types in EdgeRemoved when type_id is unavailable:** `Event::EdgeRemoved` does not carry `type_id`. Do not try to infer or look up the edge type -- by the time maintenance runs, the edge is already removed from the graph. Match on node adjacency instead.
- **Accessing graph adjacency for a removed node:** After `graph.remove_node()`, the node and all its edges are gone. Any `graph.neighbors(removed_node, ...)` returns empty. For NodeRemoved, all adjacency checks must happen BEFORE graph mutation or must be avoided entirely by using the simpler "scan for node presence" approach.
- **Modifying Frame::dfs_collect or Frame::materialize:** These remain the correctness baseline (full re-traverse oracle). PathExtender retraction functions must not touch them.
- **Forgetting to update the flush_coalescer path:** The coalescer already uses a NodeRemoved sentinel to trigger rematerialize. After Phase 19, the coalescer should continue using rematerialize as fallback because batched events cannot be decomposed into individual EdgeRemoved/NodeRemoved events for incremental dispatch.
- **Double-retracting the same path:** If a node appears at multiple positions in a path, or if an edge appears at multiple hops, the path should be retracted only once. Deduplicate retracted paths before applying deltas.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Reading materialized paths without side effects | Custom path accessor | `frame.snapshot(Epoch(u64::MAX))` | Same as `current_state()` but available on `&self` (no query count increment) |
| Delta application for retraction | Manual DiffCollection manipulation | `Frame::apply_delta(path, epoch, Delta(-1))` | Already tested, handles net_delta cache update |
| Path deduplication | Custom dedup logic | `HashSet<Vec<NodeId>>` | Standard library, handles all edge cases |
| Event-to-frame routing | Manual frame scanning | `InvertedIndex::affected_frames()` | Already O(affected) via SetTrie, handles EdgeRemoved/NodeRemoved |
| Correctness verification | New oracle | Existing `oracle_check()` from Phase 17 | Battle-tested with 11+ scenarios including EdgeRemoved/NodeRemoved |
| Parallel edge detection | Custom edge counting | `graph.neighbors(from, direction, edge_type)` | Already filters by direction and type, post-removal state is authoritative |

**Key insight:** Edge removal retraction is the inverse of edge addition extension. Where Phase 18's `extend_edge_added()` finds new paths and asserts +1, Phase 19's `retract_edge_removed()` finds broken paths and retracts -1. But the algorithm is fundamentally different: addition must construct paths (backward prefix + forward extension), while removal can simply scan existing materialized paths for the broken edge -- the paths already exist in the frame's DiffCollection.

## Common Pitfalls

### Pitfall 1: Parallel Edge False Retraction
**What goes wrong:** Two outgoing edges from A to B both satisfy hop K. Removing one edge causes the retraction algorithm to retract path `[..., A, B, ...]`, but the path is still valid via the surviving parallel edge. Full rematerialize would still produce this path.
**Why it happens:** Scanning paths for consecutive `(A, B)` without checking if another edge still supports the traversal.
**How to avoid:** After identifying a candidate path for retraction, verify with `graph.neighbors(from_node, hop.direction, hop.edge_type)` that no remaining edge connects the consecutive pair at that hop. Only retract if the traversal is truly broken. The graph is in post-removal state at this point, so `neighbors()` is authoritative.
**Warning signs:** Oracle mismatch showing fewer paths than expected after edge removal (over-retraction).

### Pitfall 2: Graph State Timing for NodeRemoved
**What goes wrong:** `graph.remove_node()` is called in Step 2, removing the node and all its edges. In Step 4, the frame maintenance tries to check edge adjacency for the removed node but gets empty results.
**Why it happens:** The established ingest pipeline applies graph mutation before frame maintenance.
**How to avoid:** For `NodeRemoved`, the DeletionContext (or more simply, just the node_id) must be captured before mutation. For the retraction algorithm itself, use "scan for node presence in path" rather than "check edge adjacency" -- this avoids needing any graph lookups for the removed node.
**Warning signs:** Retraction not happening at all, or `graph.neighbors()` returning empty for the removed node.

### Pitfall 3: EdgeRemoved Missing type_id
**What goes wrong:** The retraction algorithm tries to check edge type matching but `Event::EdgeRemoved` only carries `edge_id`, `source`, and `target` -- no `type_id`. Looking up the edge in the graph fails because the edge was already removed in Step 2.
**Why it happens:** The `Event::EdgeRemoved` variant was designed for graph mutation (which looks up by `edge_id`), not for frame maintenance (which needs `type_id` for hop matching).
**How to avoid:** Two options: (a) Scan materialized paths for consecutive `(source, target)` or `(target, source)` without type checking -- any path containing these consecutive nodes at a hop is a candidate (validated by parallel-edge check using `graph.neighbors()` which DOES filter by type). (b) Add `type_id` to `Event::EdgeRemoved` -- but this changes the public API and is not strictly necessary.
**Recommendation:** Use option (a). The `graph.neighbors()` check for parallel edges already filters by `hop.edge_type`, so type correctness is implicitly verified.
**Warning signs:** Compilation errors trying to access `type_id` on `EdgeRemoved`.

### Pitfall 4: Anchor Node Removal
**What goes wrong:** If the removed node is the frame's anchor, ALL paths in the frame are affected (every path starts with the anchor). Additionally, the frame itself is conceptually invalidated.
**Why it happens:** The anchor node is position 0 in every materialized path.
**How to avoid:** The `retract_node_removed()` "scan for node presence" approach handles this automatically -- every path contains the anchor, so all paths are retracted. However, the frame registration in the inverted index still references the anchor, which may cause future events to route to a permanently-empty frame. This is acceptable behavior (no ghost paths, just an empty frame that gets routed to harmlessly).
**Warning signs:** Not a correctness issue, but a potential performance concern with permanently-empty frames receiving events.

### Pitfall 5: Multi-hop Path with Repeated Nodes
**What goes wrong:** A path like `[1, 2, 1, 3]` (cycle through node 1) and node 1 is removed. The path contains node 1 at positions 0 and 2. The retraction should happen once, not twice.
**Why it happens:** The "scan for node presence" might return the same path multiple times if the loop iterates per-position.
**How to avoid:** Collect paths to retract into a `HashSet<Vec<NodeId>>` or deduplicate before applying deltas. The `retract_node_removed()` function filters unique paths, not positions.
**Warning signs:** Oracle mismatch showing negative net delta (double retraction).

### Pitfall 6: Interaction Between Incremental EdgeAdded and EdgeRemoved
**What goes wrong:** A path `[1, 2, 3]` was incrementally added via +1 delta (Phase 18). Then edge `1->2` is removed. The retraction must produce a -1 delta for `[1, 2, 3]`. If the retraction scans `snapshot()` but the +1 delta hasn't been compacted yet, the path still appears in `current_state()` -- this is correct.
**Why it happens:** DiffCollection stores all tuples; `current_state()` returns paths with positive net delta.
**How to avoid:** This works correctly by design. `snapshot(Epoch(u64::MAX))` considers all tuples and returns paths with net positive delta. The +1 from Phase 18 is visible, and the -1 from Phase 19 cancels it. No special handling needed.
**Warning signs:** None -- this case is handled automatically by the differential math.

### Pitfall 7: flush_coalescer Must NOT Use Incremental Removal
**What goes wrong:** The coalescer batches events and flushes them. If the batch contains EdgeRemoved or NodeRemoved events, the flusher must not try to use incremental retraction because individual events are not preserved in the batch.
**Why it happens:** The coalescer deduplicates by `node_id` and loses the original event type information.
**How to avoid:** The coalescer already uses a `NodeRemoved` sentinel to trigger the `_ => rematerialize` fallback branch. This sentinel is NOT an `EdgeRemoved` or `NodeRemoved` event that should use incremental dispatch -- it deliberately triggers the fallback. Verify that the `match event` dispatch for `Event::NodeRemoved` in `maintain_and_evaluate_frames` only fires for real `NodeRemoved` events from the main ingest path, NOT for the sentinel. **This is actually a problem**: the sentinel IS `Event::NodeRemoved { node_id: NodeId(0) }`. After Phase 19, this would hit the new `NodeRemoved` arm and try to retract paths containing `NodeId(0)` -- which likely doesn't exist, so it would be a no-op. But it would NOT trigger full rematerialize.
**Solution:** Change the sentinel to a different event type that still triggers the fallback branch, OR add a boolean/enum parameter to distinguish "real incremental" from "coalescer batch" calls.
**Warning signs:** Coalescer-flushed events not correctly rematerializing frames.

## Code Examples

### Complete retract_edge_removed Function

```rust
// In src/path_extender.rs

/// Result of incremental edge-removed retraction.
#[derive(Debug)]
pub struct EdgeRemovedDeltas {
    /// Paths to retract as -1 deltas.
    pub retracted_paths: Vec<Vec<NodeId>>,
}

/// Computes paths to retract from a frame when an edge is removed.
///
/// Scans the frame's currently materialized paths and identifies those
/// that traversed the removed edge at any hop position where no remaining
/// edge in the graph supports the traversal (parallel edge check).
///
/// # Arguments
///
/// * `pattern` - The frame's hop pattern.
/// * `graph` - The current graph state (edge already removed).
/// * `current_paths` - The frame's current materialized paths.
/// * `source` - Source node of the removed edge.
/// * `target` - Target node of the removed edge.
pub fn retract_edge_removed(
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    source: NodeId,
    target: NodeId,
) -> EdgeRemovedDeltas {
    if pattern.is_empty() || current_paths.is_empty() {
        return EdgeRemovedDeltas { retracted_paths: Vec::new() };
    }

    let mut retracted = Vec::new();

    for path in current_paths {
        if path.len() != pattern.len() + 1 {
            continue; // Invalid path length -- skip
        }
        if path_broken_by_edge_removal(path, pattern, graph, source, target) {
            retracted.push((*path).clone());
        }
    }

    // Deduplicate
    let mut seen = HashSet::new();
    retracted.retain(|p| seen.insert(p.clone()));

    EdgeRemovedDeltas { retracted_paths: retracted }
}

/// Checks whether a materialized path is broken by removing edge (source, target).
///
/// For each hop K, checks if the removed edge matches the consecutive
/// node pair (path[K], path[K+1]) in the appropriate direction. If a
/// match is found, verifies that no remaining graph edge still supports
/// the traversal (parallel edge check).
fn path_broken_by_edge_removal(
    path: &[NodeId],
    pattern: &[HopSpec],
    graph: &Graph,
    removed_source: NodeId,
    removed_target: NodeId,
) -> bool {
    for (hop_idx, hop) in pattern.iter().enumerate() {
        let from = path[hop_idx];
        let to = path[hop_idx + 1];

        let edge_could_match = match hop.direction {
            Direction::Outgoing => from == removed_source && to == removed_target,
            Direction::Incoming => from == removed_target && to == removed_source,
            Direction::Any => {
                (from == removed_source && to == removed_target)
                    || (from == removed_target && to == removed_source)
            }
        };

        if edge_could_match {
            // Check if any remaining edge still supports this hop traversal.
            let remaining = graph.neighbors(from, hop.direction, hop.edge_type);
            let still_connected = remaining.iter().any(|(_, n)| *n == to);
            if !still_connected {
                return true; // Path is broken at this hop
            }
        }
    }
    false
}
```

### Complete retract_node_removed Function

```rust
/// Result of incremental node-removed retraction.
#[derive(Debug)]
pub struct NodeRemovedDeltas {
    /// Paths to retract as -1 deltas.
    pub retracted_paths: Vec<Vec<NodeId>>,
}

/// Computes paths to retract from a frame when a node is removed.
///
/// Any materialized path containing the removed node at any position
/// is retracted. This is simpler than edge removal because a removed
/// node definitively invalidates every path it appears in.
pub fn retract_node_removed(
    current_paths: &[&Vec<NodeId>],
    removed_node: NodeId,
) -> NodeRemovedDeltas {
    let retracted: Vec<Vec<NodeId>> = current_paths
        .iter()
        .filter(|path| path.contains(&removed_node))
        .map(|path| (*path).clone())
        .collect();

    NodeRemovedDeltas { retracted_paths: retracted }
}
```

### DeletionContext Capture in Engine

```rust
// In engine.rs

/// Context captured before a node is removed from the graph.
#[derive(Debug, Clone)]
pub(crate) struct DeletionContext {
    /// The node being removed.
    pub node_id: NodeId,
}

// In ingest() Step 2:
let mut deletion_ctx: Option<DeletionContext> = None;

match &event {
    Event::NodeRemoved { node_id } => {
        // Capture context BEFORE graph mutation
        deletion_ctx = Some(DeletionContext { node_id: *node_id });
        self.graph.remove_node(*node_id);
    }
    Event::EdgeRemoved { edge_id, .. } => {
        self.graph.remove_edge(*edge_id);
    }
    // ... other variants unchanged
}

// In Step 4, pass deletion_ctx to maintain_and_evaluate_frames:
let delta_updates = Self::maintain_and_evaluate_frames(
    &affected_frames,
    graph_ref,
    epoch,
    prev_deltas,
    &event,
    deletion_ctx.as_ref(),
);
```

### Coalescer Sentinel Fix

```rust
// In flush_coalescer, change sentinel to avoid hitting incremental NodeRemoved:
// Option A: Use PropertyChanged sentinel (still hits _ fallback branch)
let sentinel = Event::PropertyChanged {
    node_id: NodeId(0),
    key: 0,
    value: PropertyValue::Integer(0),
};

// Option B: Add a dedicated parameter to maintain_and_evaluate_frames
// indicating "force rematerialize" mode
fn maintain_and_evaluate_frames(
    ...,
    force_rematerialize: bool,
) -> Vec<(u64, i64)> {
    // Inside:
    if force_rematerialize {
        frame.rematerialize(graph, epoch);
    } else {
        match event { ... }
    }
}
```

### Oracle Test Verification

```rust
// Existing oracle tests that verify removal correctness:
// Test 2: test_oracle_edge_removed -- 1-hop, remove one of two edges
// Test 3: test_oracle_node_removed -- 1-hop, node removal cascades
// Test 5: test_oracle_multi_hop_diamond -- 2-hop, edge removal in diamond
// Test 9: test_oracle_incremental_edge_added_then_removed -- add then remove

// NEW oracle tests to add:
// Test N: Multi-hop edge removal (3-hop, remove middle edge)
// Test N+1: Node removal in diamond graph (remove intermediate node)
// Test N+2: Parallel edges -- remove one, path survives
// Test N+3: Multi-frame edge removal (two frames affected by same removal)
// Test N+4: Cascade: NodeRemoved -> verify no ghost paths
// Test N+5: Sequential: EdgeAdded, EdgeAdded, EdgeRemoved, EdgeRemoved
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Full re-traverse for all non-EdgeAdded events | **Incremental -1 retraction for EdgeRemoved and NodeRemoved** | **v3.0 Phase 19 (this phase)** | **O(materialized_paths * hops) scan instead of O(full_DFS) for removal events** |
| No DeletionContext | **DeletionContext captures node info before graph mutation** | **v3.0 Phase 19** | **Enables incremental NodeRemoved without re-ordering ingest pipeline** |

**After Phase 19:**
- `frame.rematerialize()` is still used for `PropertyChanged` events only (Phase 20 will address this)
- `EdgeAdded` -> incremental +1 via PathExtender (Phase 18)
- `EdgeRemoved` -> incremental -1 via path scanning + parallel edge check (Phase 19)
- `NodeRemoved` -> incremental -1 via path scanning for node presence (Phase 19)
- `NodeAdded` -> no frame maintenance needed (nodes alone don't create paths)
- `PropertyChanged` -> fallback to full rematerialize (Phase 20)

## Open Questions

1. **Whether to add `type_id` to `Event::EdgeRemoved`**
   - What we know: The current `EdgeRemoved` variant lacks `type_id`. The retraction algorithm works without it by scanning materialized paths and using `graph.neighbors()` for parallel edge verification (which DOES filter by type).
   - What's unclear: Whether adding `type_id` to `EdgeRemoved` would simplify the algorithm or enable optimizations. It would require a public API change.
   - Recommendation: Do NOT change the Event enum for Phase 19. The path-scanning approach is correct and the parallel-edge check implicitly handles type filtering. If Phase 21 benchmarks show the scanning is a bottleneck, `type_id` can be added then.

2. **Coalescer sentinel conflict**
   - What we know: The coalescer uses `Event::NodeRemoved { node_id: NodeId(0) }` as a sentinel to trigger the rematerialize fallback. After Phase 19, this would match the `NodeRemoved` arm instead of the `_` fallback.
   - What's unclear: Whether the sentinel hitting the `NodeRemoved` arm (and trying to retract paths containing `NodeId(0)`) would be harmless or cause incorrect behavior.
   - Recommendation: Fix the sentinel. Either change it to `PropertyChanged` (which still hits `_` fallback after Phase 19), or add a `force_rematerialize` parameter to `maintain_and_evaluate_frames`. The latter is cleaner.

3. **Performance of path scanning for large frames**
   - What we know: `retract_edge_removed()` scans all materialized paths (O(paths * hops)). For frames with thousands of materialized paths, this could be significant.
   - What's unclear: Whether current test/production graph sizes produce frames with enough paths to matter.
   - Recommendation: Accept O(paths * hops) for Phase 19. This is still better than full DFS re-traverse O(graph_branching^hops). If benchmarks (Phase 21) show scanning is a bottleneck, add a reverse index (edge -> paths) as an optimization (v4 scope).

4. **Anchor node removal and frame lifecycle**
   - What we know: If a frame's anchor node is removed, all paths are retracted. The frame becomes permanently empty but remains registered.
   - What's unclear: Whether permanently-empty frames should be auto-evicted or left as-is.
   - Recommendation: Leave as-is for Phase 19. Frame eviction is a separate concern (already supported via `engine.evict_frame()`). Auto-eviction on anchor removal could be a v4 feature.

## Sources

### Primary (HIGH confidence)
- `src/engine.rs` lines 256-286 -- Ingest Step 2 graph mutation ordering (NodeRemoved calls graph.remove_node before maintenance)
- `src/engine.rs` lines 660-682 -- flush_coalescer uses `Event::NodeRemoved { node_id: NodeId(0) }` sentinel
- `src/engine.rs` lines 777-825 -- maintain_and_evaluate_frames dispatch: EdgeAdded -> PathExtender, all others -> rematerialize
- `src/engine.rs` lines 2407-2442 -- oracle_check() function
- `src/engine.rs` lines 2481-2560 -- Oracle tests 2 (EdgeRemoved) and 3 (NodeRemoved)
- `src/engine.rs` lines 2630-2702 -- Oracle test 5 (diamond EdgeRemoved)
- `src/engine.rs` lines 2868-2906 -- Oracle test 9 (EdgeAdded then EdgeRemoved)
- `src/types.rs` lines 165-188 -- Event::NodeRemoved (only node_id), Event::EdgeRemoved (edge_id, source, target -- NO type_id)
- `src/graph.rs` lines 155-191 -- Graph::remove_node cascading edge removal
- `src/graph.rs` lines 289-306 -- Graph::remove_edge
- `src/graph.rs` lines 341-369 -- Graph::neighbors() with direction and edge_type filtering
- `src/frame.rs` lines 197-206 -- Frame::apply_delta (handles both +1 and -1)
- `src/frame.rs` lines 221-223 -- Frame::snapshot (no query_count increment, takes &self)
- `src/frame.rs` lines 239-252 -- Frame::evict and Frame::rematerialize
- `src/path_extender.rs` -- Existing PathExtender module (Phase 18): extend_edge_added, backward_prefixes, forward_dfs
- `src/diff.rs` lines 80-99 -- DiffCollection::assert_tuple (+1) and retract_tuple (-1)
- `src/diff.rs` lines 142-144 -- DiffCollection::current_state (snapshot at max epoch)

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` -- IREM-01 through IREM-03, NDEL-01 through NDEL-03 requirement definitions
- `.planning/STATE.md` -- "DeletionContext captures edge info before graph mutation destroys adjacency", "Event-based dispatch in maintain_and_evaluate_frames", "flush_coalescer uses NodeRemoved sentinel"
- `.planning/phases/18-incremental-edge-addition/18-RESEARCH.md` -- Phase 18 architecture patterns (backward prefix, forward extension, engine integration, direction handling)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - No new dependencies, all existing APIs verified in source code
- Architecture: HIGH - Removal is the algorithmic inverse of addition. Path scanning against materialized state is straightforward. The parallel-edge check using post-removal `graph.neighbors()` is sound. The DeletionContext pattern follows the project decision recorded in STATE.md.
- Pitfalls: HIGH - Parallel edge false retraction, graph state timing, missing type_id, coalescer sentinel conflict, and double-retraction all identified from direct code analysis. The coalescer sentinel issue is a concrete bug that must be addressed.
- Algorithm correctness: HIGH - The retraction algorithm is simpler than the addition algorithm (scan existing paths vs construct new paths). Oracle verification from Phase 17 provides the safety net. Existing oracle tests 2, 3, 5, and 9 already test removal scenarios.

**Research date:** 2026-02-26
**Valid until:** 2026-03-26 (stable -- no external dependencies, all internal code)
