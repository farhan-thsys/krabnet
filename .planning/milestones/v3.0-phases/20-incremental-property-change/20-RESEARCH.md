# Phase 20: Incremental Property Change - Research

**Researched:** 2026-02-26
**Domain:** Incremental path re-evaluation for PropertyChanged events via targeted +1/-1 deltas
**Confidence:** HIGH

## Summary

Phase 20 replaces the full DFS re-traverse fallback (`frame.rematerialize()`) for `Event::PropertyChanged` events with incremental property change handling. When a node's property changes, the system must re-evaluate hop filters for every frame containing the affected node at any hop position, retract paths that no longer satisfy filters (as -1 deltas), and assert paths that newly satisfy filters (as +1 deltas).

After Phase 19, the dispatch in `maintain_and_evaluate_frames` (engine.rs line 826) handles `EdgeAdded` (incremental +1), `EdgeRemoved` (incremental -1), and `NodeRemoved` (incremental -1). The only remaining fallback to `frame.rematerialize()` is the `_ =>` catch-all arm, which handles `PropertyChanged` and `NodeAdded`. Phase 20 converts `PropertyChanged` to incremental dispatch, leaving only `NodeAdded` as the fallback (which is correct because node additions alone do not create paths -- edges do).

The core algorithmic challenge is **bidirectional delta computation**: unlike edge addition (always +1) or removal (always -1), a property change can simultaneously invalidate existing paths (requiring -1 deltas) and validate new paths (requiring +1 deltas). For each frame containing the affected node, the algorithm must: (1) identify which hops reference the node and have property filters, (2) check whether existing paths through that node are now invalid, (3) check whether new paths through that node are now valid, and (4) emit the correct deltas. The key insight is that this decomposes into a "retract newly-invalid" step (scan existing paths) followed by an "assert newly-valid" step (forward/backward DFS for new paths), which reuses algorithms from Phases 18 and 19.

**Primary recommendation:** Add a new function `reevaluate_property_changed()` to `src/path_extender.rs` that takes the affected `node_id`, the frame's anchor, pattern, graph (post-mutation), and current materialized paths. It returns both paths to retract and paths to assert. Wire this into the `PropertyChanged` arm of `maintain_and_evaluate_frames`.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PROP-01 | PropertyChanged events re-evaluate hop filters for all frames containing the affected node at any hop position | The inverted index already routes `PropertyChanged` events to all frames containing the affected `node_id` (routing.rs line 190: `Event::PropertyChanged { node_id, .. } => self.collect_by_node(*node_id, &mut result)`). The `collect_reachable_nodes` function (engine.rs line 919) registers all nodes reachable through the pattern including intermediates, so the affected node will be found at whatever hop position it occupies. The `reevaluate_property_changed()` function iterates each hop position in the pattern and checks if the node appears at that position in any existing path or could appear at that position in a new path. For each such hop, it re-evaluates the hop's `Filter` (None, PropertyEquals, HasProperty) against the node's current properties in the post-mutation graph. |
| PROP-02 | Paths that no longer satisfy filters retracted as -1 deltas | The retraction step scans the frame's current materialized paths (via `frame.snapshot(Epoch(u64::MAX))`). For each path containing the affected node at position `K+1` (where hop `K` has a property filter), it re-checks the filter against the node's new property state. If the filter no longer passes, the path is collected for retraction as a `-1` delta via `frame.apply_delta(path, epoch, Delta(-1))`. This reuses the scan-and-retract pattern from Phase 19's `retract_edge_removed()`. |
| PROP-03 | Paths that newly satisfy filters asserted as +1 deltas | The assertion step identifies paths that are now valid but were not before. For each hop position `K` where the affected node could be the "reached node" and the hop has a property filter that now passes (but the node was not previously in any materialized path at this position, or was filtered out), the algorithm performs backward prefix resolution from the anchor through hops 0..K-1 ending at the appropriate origin node, then forward extension through hops K+1..N-1. This reuses the backward prefix + forward extension DFS from Phase 18's `extend_edge_added()`. New paths are deduplicated against existing paths (to avoid double-asserting) and emitted as `+1` deltas. |
| PROP-04 | Incremental PropertyChanged produces identical frame state to full re-traverse (oracle verified) | Oracle test 4 (`test_oracle_property_changed`, engine.rs line 2621) already exercises PropertyChanged with property filters, changing a property to a non-matching value and back. Currently passes via full rematerialize fallback. After Phase 20 wires incremental dispatch, this test must still pass via `oracle_check()`. Additional oracle tests should cover multi-hop property changes, HasProperty filter, property changes on intermediate nodes, and property changes that simultaneously cause retraction + assertion on different paths. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| (no new deps) | - | All work is within existing Rust crate | Purely algorithmic; uses existing Frame, Graph, DiffCollection, PathExtender APIs |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| std::collections::HashSet | stable | Deduplication of retracted/asserted paths | When paths appear at multiple hop positions or overlap between retraction/assertion |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Scan existing paths + DFS for new | Full rematerialize (current approach) | Rematerialize is O(full_DFS) per event; incremental is O(affected_paths + local_DFS) |
| Separate retract + assert functions | Single combined function | Combined function avoids double-scanning; retract and assert are tightly coupled for property changes |
| Tracking old property values | Comparing before vs after state | The graph has already been mutated by Step 2 of ingest when maintenance runs; instead, re-evaluate filters against current graph state and compare with materialized paths |

**Installation:**
```bash
# No new dependencies -- purely algorithmic changes within existing crate
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  path_extender.rs   # MODIFIED: Add reevaluate_property_changed() function
  engine.rs          # MODIFIED: Wire PropertyChanged into maintain_and_evaluate_frames dispatch
  frame.rs           # EXISTING: Used via snapshot(), apply_delta(), pattern(), anchor()
  graph.rs           # EXISTING: Used via get_property(), get_node_type(), neighbors()
  types.rs           # EXISTING: Event::PropertyChanged, Filter enum definitions
  diff.rs            # EXISTING: DiffCollection::current_state()
  routing.rs         # EXISTING: InvertedIndex routes PropertyChanged by node_id
```

### Pattern 1: Bidirectional Property Change Evaluation
**What:** A function that computes both retracted paths (-1) and newly asserted paths (+1) when a node's property changes. It decomposes into two sub-steps: (1) scan existing paths for newly-invalid ones, (2) find newly-valid paths via DFS.
**When to use:** Every time a `PropertyChanged` event is routed to a frame that has property filters on at least one hop.

**Critical insight -- graph timing:** By the time `maintain_and_evaluate_frames` runs, the graph has already been mutated (Step 2 of `ingest()` calls `graph.set_property(node_id, key, value)` at engine.rs line 300). This means `graph.get_property(node_id, key)` returns the NEW value. The algorithm must:
- **For retraction:** Check if existing materialized paths still satisfy filters under the NEW property values. If a path contains the affected node and the hop's filter no longer passes, retract it.
- **For assertion:** Check if new paths are now possible because the NEW property value now satisfies a filter that previously did not. Use backward prefix + forward extension DFS (same as Phase 18) to find these paths.

**When a hop has `Filter::None`:** Property changes have NO effect on that hop. The node's property is irrelevant because no filter is applied. Skip the hop entirely during property change evaluation.

**Example:**
```rust
/// Result of incremental property-change re-evaluation.
#[derive(Debug)]
pub struct PropertyChangedDeltas {
    /// Paths to retract as -1 deltas (no longer satisfy filters).
    pub retracted_paths: Vec<Vec<NodeId>>,
    /// New paths to assert as +1 deltas (newly satisfy filters).
    pub new_paths: Vec<Vec<NodeId>>,
}

/// Re-evaluates hop filters for a frame when a node's property changes.
///
/// Scans existing materialized paths for those containing the affected
/// node at any hop position with a property filter. Paths where the
/// filter no longer passes are retracted. New paths where the filter
/// now passes (but did not before) are discovered via backward prefix
/// resolution and forward extension.
///
/// # Arguments
///
/// * `anchor` - The frame's anchor node.
/// * `pattern` - The frame's hop pattern.
/// * `graph` - The current graph state (property already changed).
/// * `current_paths` - References to the frame's currently materialized paths.
/// * `changed_node` - The node whose property changed.
/// * `changed_key` - The property key that changed.
pub fn reevaluate_property_changed(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    changed_node: NodeId,
) -> PropertyChangedDeltas {
    // Step 1: Find existing paths to retract (filter no longer passes)
    let retracted = find_newly_invalid_paths(
        pattern, graph, current_paths, changed_node,
    );

    // Step 2: Find new paths to assert (filter now passes)
    let new_paths = find_newly_valid_paths(
        anchor, pattern, graph, current_paths, changed_node,
    );

    PropertyChangedDeltas { retracted_paths: retracted, new_paths }
}
```

### Pattern 2: Retraction of Newly-Invalid Paths
**What:** Scan existing materialized paths. For each path containing the affected node at position `K+1` (the "reached" node for hop `K`), re-evaluate hop K's filter. If the filter no longer passes, the path is invalid.
**When to use:** As sub-step 1 of property change evaluation.

**Critical detail -- which position is the "reached" node?**
In a path `[N0, N1, N2, ..., Nm]`:
- Hop 0 reaches `N1` (path position 1)
- Hop 1 reaches `N2` (path position 2)
- Hop K reaches `N(K+1)` (path position K+1)

So if the affected node appears at path position `P` (where P > 0), it was "reached" by hop `P-1`. We must re-evaluate hop `P-1`'s filter.

**Also check anchor position (P=0):** If the affected node IS the anchor (path position 0), it does not have a filter applied to it by any hop (the anchor is the starting point, not a "reached" node). However, the anchor node's properties could be checked by a filter on the LAST hop of another frame where this node appears at a non-anchor position. The inverted index handles this -- the node is registered at all positions, and different frames will have different hop filters.

**Example:**
```rust
fn find_newly_invalid_paths(
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    changed_node: NodeId,
) -> Vec<Vec<NodeId>> {
    let mut retracted = Vec::new();

    for path in current_paths {
        if path.len() != pattern.len() + 1 {
            continue;
        }
        if path_invalidated_by_property_change(path, pattern, graph, changed_node) {
            retracted.push(path.to_vec());
        }
    }

    // Deduplicate
    let mut seen = HashSet::new();
    retracted.retain(|p| seen.insert(p.clone()));

    retracted
}

/// Checks whether a materialized path is invalidated because the
/// changed node's property no longer satisfies a hop filter.
fn path_invalidated_by_property_change(
    path: &[NodeId],
    pattern: &[HopSpec],
    graph: &Graph,
    changed_node: NodeId,
) -> bool {
    for (hop_idx, hop) in pattern.iter().enumerate() {
        let reached_node = path[hop_idx + 1];

        // Only check hops where the changed node is the reached node
        if reached_node != changed_node {
            continue;
        }

        // Only relevant if this hop has a property filter
        match &hop.filter {
            Filter::None => continue,
            Filter::PropertyEquals { key, value } => {
                if graph.get_property(changed_node, *key) != Some(value) {
                    return true; // Filter no longer passes
                }
            }
            Filter::HasProperty { key } => {
                if graph.get_property(changed_node, *key).is_none() {
                    return true; // Property no longer exists
                }
            }
        }
    }
    false
}
```

### Pattern 3: Assertion of Newly-Valid Paths
**What:** Find paths that now satisfy all hop filters but did not before the property change. For each hop `K` in the pattern where the changed node could be the "reached" node and the hop has a property filter that NOW passes, check if the node was previously excluded from materialized paths at this position. Use backward prefix + forward extension DFS (reuse from Phase 18) to discover complete paths through this node.
**When to use:** As sub-step 2 of property change evaluation.

**Key insight -- how to know if paths are "new":** The new paths discovered by DFS include ALL currently valid paths through the changed node. Some of these paths may already be materialized (they were valid before AND after the property change). We must NOT re-assert existing paths, as that would create multiplicity > 1. The solution: deduplicate new paths against existing materialized paths. Any path found by DFS that is NOT in the current materialized set is genuinely new.

**Example:**
```rust
fn find_newly_valid_paths(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    changed_node: NodeId,
) -> Vec<Vec<NodeId>> {
    // Collect existing paths for dedup
    let existing: HashSet<&Vec<NodeId>> = current_paths.iter().copied().collect();

    let mut new_paths: Vec<Vec<NodeId>> = Vec::new();

    for (hop_idx, hop) in pattern.iter().enumerate() {
        // Only check hops with property filters
        match &hop.filter {
            Filter::None => continue,
            _ => {}
        }

        // Check if the changed node could be the "reached" node at this hop
        // AND the filter NOW passes
        if !node_satisfies_hop_filter(hop, changed_node, graph) {
            continue;
        }

        // The changed node satisfies this hop's filter. Find all complete
        // paths that pass through the changed node at position hop_idx+1.
        // Use backward prefix resolution + forward extension (Phase 18 pattern).
        //
        // For backward prefixes: find paths from anchor through hops 0..hop_idx-1
        // that reach a node FROM which the changed node is accessible via hop_idx.
        //
        // The "origin" node for this hop depends on direction:
        // We need to find nodes that have an edge to changed_node matching the hop.
        let origins = find_hop_origins(graph, hop, changed_node);

        for origin in origins {
            let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, origin);
            for prefix in prefixes {
                extend_forward(graph, prefix, changed_node, pattern, hop_idx, &mut new_paths);
            }
        }
    }

    // Deduplicate
    let mut seen = HashSet::new();
    new_paths.retain(|p| seen.insert(p.clone()));

    // Remove paths that already exist in materialized state (avoid double-assertion)
    new_paths.retain(|p| !existing.contains(p));

    new_paths
}

/// Finds all nodes that have an edge TO `reached_node` matching the
/// hop's direction and edge type. These are the "origin" nodes that
/// could reach `reached_node` via this hop.
fn find_hop_origins(
    graph: &Graph,
    hop: &HopSpec,
    reached_node: NodeId,
) -> Vec<NodeId> {
    // The "origin" is the node from which the hop traversal starts.
    // For Outgoing hop: origin has an outgoing edge to reached_node.
    //   -> Check reached_node's incoming neighbors with the hop's edge type.
    // For Incoming hop: origin has an incoming edge from reached_node.
    //   -> Check reached_node's outgoing neighbors with the hop's edge type.
    // For Any: both directions.
    let mut origins = Vec::new();

    match hop.direction {
        Direction::Outgoing => {
            // Origin->reached via outgoing edge => reached has incoming from origin
            let neighbors = graph.neighbors(reached_node, Direction::Incoming, hop.edge_type);
            for (_eid, neighbor) in neighbors {
                origins.push(neighbor);
            }
        }
        Direction::Incoming => {
            // Origin->reached via incoming edge at origin => reached is the source,
            // origin is the target => reached has outgoing edge to origin
            let neighbors = graph.neighbors(reached_node, Direction::Outgoing, hop.edge_type);
            for (_eid, neighbor) in neighbors {
                origins.push(neighbor);
            }
        }
        Direction::Any => {
            // Try both directions
            let incoming = graph.neighbors(reached_node, Direction::Incoming, hop.edge_type);
            for (_eid, neighbor) in incoming {
                origins.push(neighbor);
            }
            let outgoing = graph.neighbors(reached_node, Direction::Outgoing, hop.edge_type);
            for (_eid, neighbor) in outgoing {
                if !origins.contains(&neighbor) {
                    origins.push(neighbor);
                }
            }
        }
    }

    origins
}
```

### Pattern 4: Optimization -- Early Exit for No Property Filters
**What:** If a frame's pattern has NO hops with property filters (all hops have `Filter::None`), then a `PropertyChanged` event cannot affect any path in that frame. Return empty deltas immediately.
**When to use:** As a fast-path check at the start of `reevaluate_property_changed()`.

**Example:**
```rust
fn has_property_filters(pattern: &[HopSpec]) -> bool {
    pattern.iter().any(|hop| !matches!(hop.filter, Filter::None))
}

pub fn reevaluate_property_changed(...) -> PropertyChangedDeltas {
    if pattern.is_empty() || !has_property_filters(pattern) {
        return PropertyChangedDeltas {
            retracted_paths: Vec::new(),
            new_paths: Vec::new(),
        };
    }
    // ... full evaluation
}
```

**Performance impact:** This optimization is important because many frames likely have `Filter::None` on all hops. The inverted index routes `PropertyChanged` events based on the node_id being present in the frame's reachable set, regardless of whether any hop has a property filter. This early exit avoids unnecessary path scanning for those frames.

### Pattern 5: Engine Integration -- PropertyChanged Dispatch
**What:** Replace the `_ => frame.rematerialize()` catch-all with an explicit `Event::PropertyChanged` arm that calls `reevaluate_property_changed()` and applies the returned deltas.
**When to use:** In `maintain_and_evaluate_frames` (engine.rs line 826).

**Example:**
```rust
// In maintain_and_evaluate_frames:
match event {
    Event::EdgeAdded { source, target, type_id, .. } => {
        // Existing Phase 18 incremental +1
        let deltas = crate::path_extender::extend_edge_added(
            frame.anchor(), frame.pattern(), graph,
            *source, *target, *type_id,
        );
        for path in deltas.new_paths {
            frame.apply_delta(path, epoch, Delta(1));
        }
    }
    Event::EdgeRemoved { source, target, .. } => {
        // Existing Phase 19 incremental -1
        let current = frame.snapshot(Epoch(u64::MAX));
        let deltas = crate::path_extender::retract_edge_removed(
            frame.pattern(), graph, &current, *source, *target,
        );
        for path in deltas.retracted_paths {
            frame.apply_delta(path, epoch, Delta(-1));
        }
    }
    Event::NodeRemoved { node_id } => {
        // Existing Phase 19 incremental -1
        let current = frame.snapshot(Epoch(u64::MAX));
        let deltas = crate::path_extender::retract_node_removed(
            &current, *node_id,
        );
        for path in deltas.retracted_paths {
            frame.apply_delta(path, epoch, Delta(-1));
        }
    }
    Event::PropertyChanged { node_id, .. } => {
        // NEW Phase 20: incremental property change
        let current = frame.snapshot(Epoch(u64::MAX));
        let deltas = crate::path_extender::reevaluate_property_changed(
            frame.anchor(), frame.pattern(), graph,
            &current, *node_id,
        );
        for path in deltas.retracted_paths {
            frame.apply_delta(path, epoch, Delta(-1));
        }
        for path in deltas.new_paths {
            frame.apply_delta(path, epoch, Delta(1));
        }
    }
    _ => {
        // Fallback: only NodeAdded (which cannot create paths by itself)
        frame.rematerialize(graph, epoch);
    }
}
```

### Anti-Patterns to Avoid
- **Trying to capture the old property value before mutation:** The graph is already mutated in Step 2 of `ingest()` (engine.rs line 300). By the time `maintain_and_evaluate_frames` runs, `graph.get_property(node_id, key)` returns the new value. Do NOT try to re-order the pipeline or capture old values. Instead, compare the current materialized paths against what the new graph state produces.
- **Re-asserting existing paths:** The DFS for newly-valid paths will find ALL valid paths through the changed node, including those that were already materialized. These MUST be filtered out before asserting +1 deltas, otherwise multiplicity will be incorrect.
- **Ignoring hops with `Filter::None`:** A property change on a node at a `Filter::None` hop position has NO effect on that hop's validity. Only hops with `PropertyEquals` or `HasProperty` filters are affected by property changes.
- **Forgetting to check `target_type` during new path discovery:** The `node_satisfies_hop_filter()` check must validate the full hop constraint (edge_type, target_type, property filter), not just the property filter. A node might satisfy the property filter but fail the target_type check.
- **Modifying Frame::dfs_collect:** The DFS logic remains the correctness baseline (oracle). All incremental logic lives in `path_extender.rs`.
- **Ignoring the anchor position:** The changed node could be the frame's anchor (path position 0). Since the anchor is not "reached" by any hop, no hop filter is applied to it. Property changes on the anchor node do NOT directly affect path validity through filter evaluation. However, if the anchor node has edges to nodes at hop 0, and those edges are filtered by property of the anchor, that is NOT how the system works -- filters are applied to the REACHED node, not the traversing node.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Reading materialized paths without side effects | Custom path accessor | `frame.snapshot(Epoch(u64::MAX))` | Available on `&self`, no query_count increment |
| Delta application | Manual DiffCollection manipulation | `Frame::apply_delta(path, epoch, Delta(+/-1))` | Already tested, handles net_delta cache update |
| Backward prefix resolution | New backward prefix DFS | Existing `backward_prefixes()` from path_extender.rs | Already tested in Phase 18, replicates dfs_collect logic |
| Forward extension | New forward DFS | Existing `extend_forward()` / `forward_dfs()` from path_extender.rs | Already tested in Phase 18 |
| Path deduplication | Custom dedup logic | `HashSet<Vec<NodeId>>` | Standard library |
| Event-to-frame routing | Manual frame scanning | `InvertedIndex::affected_frames()` already routes PropertyChanged by node_id | O(affected) via SetTrie |
| Correctness verification | New oracle | Existing `oracle_check()` from Phase 17 | Battle-tested with 15+ scenarios including PropertyChanged (test 4) |
| Hop filter matching | New filter evaluator | Existing `edge_matches_hop_directed()` for the property filter portion | Already tested in Phase 18, replicates dfs_collect filter checks |

**Key insight:** Phase 20's algorithm is a composition of Phase 18 (assert newly-valid via backward prefix + forward extension) and Phase 19 (retract newly-invalid via path scanning). The novel aspect is that both must happen for the same event, and the assertion step must deduplicate against existing paths.

## Common Pitfalls

### Pitfall 1: Double-Assertion of Existing Paths
**What goes wrong:** The DFS for newly-valid paths finds ALL currently valid paths through the changed node. Some of these paths were already materialized (e.g., the path was valid before the property change, and is still valid after). Asserting +1 for an already-materialized path creates multiplicity 2, which differs from full rematerialize (which produces multiplicity 1).
**Why it happens:** The DFS has no memory of which paths existed before; it only knows the current graph state.
**How to avoid:** After computing newly-valid paths via DFS, subtract the set of currently materialized paths. Only paths NOT in `current_paths` are genuinely new and should receive +1 deltas. Use `HashSet` comparison for efficient dedup.
**Warning signs:** Oracle mismatch showing more paths than expected, or net_delta higher than expected.

### Pitfall 2: Property Change on Non-Filtered Hops
**What goes wrong:** A frame has 3 hops, but only hop 1 has a `PropertyEquals` filter. The changed node appears at position 2 (reached by hop 1, which has the filter) AND position 3 (reached by hop 2, which has `Filter::None`). The algorithm might skip the node at position 3 because the hop has no filter, but this is correct -- no action needed for `Filter::None` hops. The risk is the reverse: evaluating `Filter::None` as if it could be affected.
**Why it happens:** Confusing "node appears in path" with "node's property is relevant to path validity."
**How to avoid:** Only check hops where the filter is NOT `Filter::None`. A property change on a node at a `Filter::None` hop has zero effect on that hop's validity.
**Warning signs:** Unnecessary retractions or assertions for nodes at unfiltered hops.

### Pitfall 3: Changed Property Key vs Filter Key Mismatch
**What goes wrong:** The PropertyChanged event carries `key: u32`. A hop's filter might check a DIFFERENT property key. Changing property key 5 has no effect on a hop filter checking property key 10.
**Why it happens:** The algorithm evaluates ALL hop filters, not just the one matching the changed key.
**How to avoid:** Two approaches: (a) Only evaluate hops whose filter key matches `changed_key` (optimization). (b) Evaluate all hops and let the filter check handle it naturally (simpler, correct, potentially slower). Approach (b) is recommended for correctness because a `HasProperty` filter on key X is NOT affected by changing key Y, and the filter check will correctly return "still passes" or "still fails." However, approach (a) can be used as an optimization: if the hop filter's key does not match the event's key, skip that hop entirely.
**Recommendation:** Implement the optimization -- pass `changed_key` to the function and skip hops whose filter key does not match. This is a significant performance win because most PropertyChanged events will not match most frame filters.
**Warning signs:** Retracting/asserting paths for hops with unrelated property keys.

### Pitfall 4: HasProperty Filter Edge Cases
**What goes wrong:** A `HasProperty { key: K }` filter only checks for existence, not value. If the property already existed and its value is changed, the filter still passes -- no retraction or assertion needed. If the property is being SET for the first time (transitioning from non-existent to existent), new paths should be asserted.
**Why it happens:** `HasProperty` is about existence, not value. `PropertyChanged` always sets a value, so after the event the property exists. The question is whether it existed BEFORE.
**How to avoid:** For `HasProperty` filters: (a) the retraction step naturally handles this -- if the property still exists (which it does after PropertyChanged), existing paths are still valid, so no retraction. (b) The assertion step must check if new paths are possible -- if the property just appeared (did not exist before), new paths through the node may now be valid. Since we cannot query the old state (graph already mutated), we rely on the dedup-against-existing-paths approach: the DFS finds all valid paths, and any not in the existing materialized set are new.
**Warning signs:** Missing new paths when a property is set for the first time on a node that already has matching edges but was filtered out by `HasProperty`.

### Pitfall 5: Retract Then Assert Ordering
**What goes wrong:** If retractions are applied before assertions, and the same path appears in both the retract and assert sets (e.g., a property changes from value A to value B, and a filter checks for value A while another filter checks for value B on different hops), the retraction -1 and assertion +1 net to zero instead of the correct state.
**Why it happens:** Theoretically possible if the same path is both retracted (fails one hop filter) and asserted (passes a different hop filter). In practice, a path cannot both fail and pass the same hop's filter for the same node.
**How to avoid:** This is actually a non-issue: a path is retracted because a hop filter NOW fails, and a path is asserted because a hop filter NOW passes. A single path cannot both fail and pass the same filter evaluation on the same hop. For DIFFERENT hops on the same path with different filters, the path must satisfy ALL hops to be valid -- if any one fails, the entire path is invalid. So a retracted path cannot simultaneously be an asserted path (different hop positions are not evaluated independently for the same complete path). The dedup-against-existing-paths in the assertion step handles any remaining overlap.
**Warning signs:** None expected, but validate with oracle.

### Pitfall 6: Interaction with Coalescer
**What goes wrong:** The coalescer batches multiple events and flushes with `force_rematerialize=true`. PropertyChanged events that go through the coalescer will NOT trigger incremental dispatch -- they will be rematerialized. This is correct and expected behavior.
**Why it happens:** The coalescer already uses `force_rematerialize=true` (engine.rs line 685), which bypasses the `match event` dispatch entirely.
**How to avoid:** No action needed. The coalescer path is separate from the main ingest path and correctly uses full rematerialize.
**Warning signs:** None -- this is by design.

### Pitfall 7: Node Satisfies Target Type but Not Property Filter
**What goes wrong:** When finding newly-valid paths, the algorithm must check the FULL hop constraint, not just the property filter. A node might now satisfy the property filter but fail the `target_type` check, or vice versa.
**Why it happens:** Incomplete hop validation.
**How to avoid:** Reuse the existing `edge_matches_hop_directed()` function from path_extender.rs, which already checks edge_type, target_type, AND property filter as a unified constraint. For property change evaluation, verify the node passes the full hop constraint, not just the property portion.
**Warning signs:** Oracle mismatch where paths are asserted for nodes that pass the property filter but fail the target_type filter.

## Code Examples

### Complete reevaluate_property_changed Function

```rust
// In src/path_extender.rs

/// Result of incremental property-change re-evaluation.
#[derive(Debug)]
pub struct PropertyChangedDeltas {
    /// Paths to retract as -1 deltas (no longer satisfy filters).
    pub retracted_paths: Vec<Vec<NodeId>>,
    /// New paths to assert as +1 deltas (newly satisfy filters).
    pub new_paths: Vec<Vec<NodeId>>,
}

/// Re-evaluates hop filters for a frame when a node's property changes.
///
/// For each hop with a property filter where the changed node is the
/// "reached" node, checks whether existing paths are invalidated and
/// whether new paths are now valid.
///
/// # Arguments
///
/// * `anchor` - The frame's anchor node.
/// * `pattern` - The frame's hop pattern.
/// * `graph` - The current graph state (property already changed).
/// * `current_paths` - References to the frame's currently materialized paths.
/// * `changed_node` - The node whose property changed.
pub fn reevaluate_property_changed(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    changed_node: NodeId,
) -> PropertyChangedDeltas {
    // Early exit: no hops have property filters
    if pattern.is_empty()
        || !pattern.iter().any(|h| !matches!(h.filter, Filter::None))
    {
        return PropertyChangedDeltas {
            retracted_paths: Vec::new(),
            new_paths: Vec::new(),
        };
    }

    // Step 1: Retract existing paths where the filter NOW fails
    let mut retracted = Vec::new();
    for path in current_paths {
        if path.len() != pattern.len() + 1 {
            continue;
        }
        for (hop_idx, hop) in pattern.iter().enumerate() {
            if path[hop_idx + 1] != changed_node {
                continue;
            }
            if matches!(hop.filter, Filter::None) {
                continue;
            }
            // Re-evaluate the full hop constraint (target_type + filter)
            if !node_passes_hop(hop, changed_node, graph) {
                retracted.push(path.to_vec());
                break; // Path is invalid, no need to check more hops
            }
        }
    }
    let mut seen = HashSet::new();
    retracted.retain(|p| seen.insert(p.clone()));

    // Step 2: Find newly-valid paths via backward prefix + forward extension
    let existing: HashSet<&Vec<NodeId>> = current_paths.iter().copied().collect();
    let mut new_paths: Vec<Vec<NodeId>> = Vec::new();

    for (hop_idx, hop) in pattern.iter().enumerate() {
        if matches!(hop.filter, Filter::None) {
            continue;
        }
        // Check if changed_node satisfies this hop's full constraint
        if !node_passes_hop(hop, changed_node, graph) {
            continue;
        }

        // Find origin nodes that connect to changed_node via this hop
        let origins = find_hop_origins(graph, hop, changed_node);
        for origin in origins {
            let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, origin);
            for prefix in prefixes {
                extend_forward(graph, prefix, changed_node, pattern, hop_idx, &mut new_paths);
            }
        }
    }

    // Deduplicate new paths
    let mut seen_new = HashSet::new();
    new_paths.retain(|p| seen_new.insert(p.clone()));

    // Remove paths that already exist (avoid double-assertion)
    new_paths.retain(|p| !existing.contains(p));

    // Also remove paths that were just retracted and re-asserted
    // (this handles the case where retraction + assertion of same path nets to 0)
    // Actually, this cannot happen: if a path is retracted (filter fails)
    // it cannot also be newly valid (filter passes). But defensive code:
    let retracted_set: HashSet<Vec<NodeId>> = retracted.iter().cloned().collect();
    new_paths.retain(|p| !retracted_set.contains(p));

    PropertyChangedDeltas { retracted_paths: retracted, new_paths }
}

/// Checks if a node satisfies a hop's target_type and property filter.
/// Does NOT check edge_type (that is verified by the edge, not the node).
fn node_passes_hop(hop: &HopSpec, node_id: NodeId, graph: &Graph) -> bool {
    // Check target type
    if let Some(target_type) = hop.target_type {
        if graph.get_node_type(node_id) != Some(target_type) {
            return false;
        }
    }
    // Check property filter
    match &hop.filter {
        Filter::None => true,
        Filter::PropertyEquals { key, value } => {
            graph.get_property(node_id, *key) == Some(value)
        }
        Filter::HasProperty { key } => {
            graph.get_property(node_id, *key).is_some()
        }
    }
}

/// Finds nodes that have edges connecting TO reached_node matching
/// the hop's direction and edge type.
fn find_hop_origins(
    graph: &Graph,
    hop: &HopSpec,
    reached_node: NodeId,
) -> Vec<NodeId> {
    let mut origins = Vec::new();
    match hop.direction {
        Direction::Outgoing => {
            // Origin has outgoing edge to reached -> reached has incoming from origin
            let neighbors = graph.neighbors(reached_node, Direction::Incoming, hop.edge_type);
            for (_eid, n) in neighbors {
                origins.push(n);
            }
        }
        Direction::Incoming => {
            // Hop follows incoming edge at origin -> reached is edge source
            // reached has outgoing edge to origin
            let neighbors = graph.neighbors(reached_node, Direction::Outgoing, hop.edge_type);
            for (_eid, n) in neighbors {
                origins.push(n);
            }
        }
        Direction::Any => {
            let incoming = graph.neighbors(reached_node, Direction::Incoming, hop.edge_type);
            for (_eid, n) in incoming {
                origins.push(n);
            }
            let outgoing = graph.neighbors(reached_node, Direction::Outgoing, hop.edge_type);
            for (_eid, n) in outgoing {
                if !origins.contains(&n) {
                    origins.push(n);
                }
            }
        }
    }
    origins
}
```

### Engine Integration

```rust
// In engine.rs maintain_and_evaluate_frames, replace the _ catch-all:
Event::PropertyChanged { node_id, .. } => {
    let current = frame.snapshot(Epoch(u64::MAX));
    let deltas = crate::path_extender::reevaluate_property_changed(
        frame.anchor(),
        frame.pattern(),
        graph,
        &current,
        *node_id,
    );
    for path in deltas.retracted_paths {
        frame.apply_delta(path, epoch, Delta(-1));
    }
    for path in deltas.new_paths {
        frame.apply_delta(path, epoch, Delta(1));
    }
}
_ => {
    // Only NodeAdded remains -- nodes alone cannot create paths
    frame.rematerialize(graph, epoch);
}
```

### Oracle Test Scenarios

```rust
// Existing oracle test that validates PropertyChanged (already passes via rematerialize):
// test_oracle_property_changed (engine.rs line 2621)
//   - Sets up 1-hop frame with PropertyEquals filter
//   - Changes property to non-matching value -> path retracted
//   - Changes property back to matching value -> path restored

// NEW oracle tests to add for Phase 20:

// Test: Multi-hop property change on intermediate node
// Setup: 2-hop frame [Out/100/type20/PropEq(42,100), Out/200/type30/None]
// Node at hop 0 has matching property. Change it to non-matching.
// Expected: path retracted, then restored when changed back.

// Test: HasProperty filter
// Setup: 1-hop frame with HasProperty filter
// Node initially has no property. Set it -> new path asserted.
// Already covered implicitly since PropertyChanged always sets a value.

// Test: Property change on node at multiple hop positions in different frames
// Setup: Two frames both containing node 2, different filters
// Change node 2 property -> different effects on each frame

// Test: Property change that simultaneously retracts one path and asserts another
// Setup: 2 nodes (B, C) both reachable from anchor A. B has matching property, C does not.
//        Change property on C to matching AND B to non-matching (two events).
//        After first event: path [A,C] asserted, paths unchanged for B.
//        After second event: path [A,B] retracted.

// Test: Property change on node where no hop has property filter
// Setup: 1-hop frame with Filter::None. Property changes on reached node.
// Expected: zero retractions, zero assertions (early exit).

// Test: Property change on anchor node
// Setup: Anchor has property. Property changes.
// Expected: no effect (anchor is not "reached" by any hop).
```

### Public API Update for lib.rs

```rust
// In src/lib.rs, add to path_extender exports:
pub use path_extender::{
    extend_edge_added, retract_edge_removed, retract_node_removed,
    reevaluate_property_changed,  // NEW
    EdgeAddedDeltas, EdgeRemovedDeltas, NodeRemovedDeltas,
    PropertyChangedDeltas,  // NEW
};
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Full re-traverse for PropertyChanged | **Incremental +1/-1 via filter re-evaluation** | **v3.0 Phase 20 (this phase)** | **O(affected_paths + local_DFS) instead of O(full_DFS) for property changes** |

**After Phase 20, all event types are incremental:**
- `EdgeAdded` -> incremental +1 via PathExtender (Phase 18)
- `EdgeRemoved` -> incremental -1 via path scanning + parallel edge check (Phase 19)
- `NodeRemoved` -> incremental -1 via path scanning for node presence (Phase 19)
- `PropertyChanged` -> incremental +1/-1 via filter re-evaluation (Phase 20)
- `NodeAdded` -> fallback to rematerialize (correct: nodes alone cannot create paths)

**Deprecated/outdated:**
- After Phase 20, `frame.rematerialize()` is used only for `NodeAdded` events (which is effectively a no-op since node additions cannot create paths -- only edges do). The rematerialize fallback path becomes vestigial for correctness but is retained for safety.

## Open Questions

1. **Whether to pass `changed_key` to reevaluate_property_changed for optimization**
   - What we know: The `Event::PropertyChanged` carries `key: u32` identifying which property key changed. Hops with `PropertyEquals { key: K, .. }` or `HasProperty { key: K }` are only affected if K matches the changed key.
   - What's unclear: Whether the optimization of skipping hops with non-matching filter keys is worth the added parameter complexity.
   - Recommendation: YES, pass `changed_key` as a parameter. This is a significant performance optimization: most property changes will NOT match most filter keys, allowing immediate skip. The implementation cost is minimal (one extra `u32` parameter, one `if` check per hop).

2. **Whether NodeAdded fallback should also skip rematerialize**
   - What we know: `NodeAdded` events cannot create paths by themselves (paths require edges). The rematerialize fallback for `NodeAdded` is technically a no-op that does unnecessary work (evict + DFS produces the same state).
   - What's unclear: Whether there are edge cases where `NodeAdded` should trigger re-evaluation (e.g., if the node's type matches a filter on an existing hop).
   - Recommendation: After Phase 20, the `_ =>` fallback only handles `NodeAdded`. Since node additions cannot create paths (no edges), the rematerialize is unnecessary but harmless. Leave it as a safety net for Phase 20. Phase 21 can convert it to a no-op if desired.

3. **Performance of find_hop_origins for dense graphs**
   - What we know: `find_hop_origins()` queries the graph's neighbor lists to find all nodes connected to the changed node. In dense graphs with many edges, this could return many origins, each triggering backward prefix resolution.
   - What's unclear: Whether current graph sizes produce enough neighbors to cause performance issues.
   - Recommendation: Accept the current approach for Phase 20. Phase 21 benchmarks will quantify. If needed, a property-to-paths reverse index could be added in v4.

4. **Whether `HasProperty` is fully covered by the current algorithm**
   - What we know: `PropertyChanged` always sets a property value. After the event, the property exists. For `HasProperty` filter, this means the filter always passes after a `PropertyChanged` event on the relevant key. Retraction can only happen if the property is REMOVED (which `PropertyChanged` does not do -- there is no `PropertyRemoved` event).
   - What's unclear: Whether there is a `PropertyRemoved` event or equivalent.
   - Recommendation: The current `Event` enum has no `PropertyRemoved` variant. `HasProperty` retraction for property deletion is not possible in the current event model. For Phase 20, `HasProperty` is only relevant for the assertion path (property now exists -> new path). The retraction path for `HasProperty` is effectively unreachable via `PropertyChanged` events (changing a value does not remove existence). Document this as a known limitation.

## Sources

### Primary (HIGH confidence)
- `src/engine.rs` lines 295-301 -- PropertyChanged handling in ingest() Step 2: `graph.set_property(*node_id, *key, value.clone())`
- `src/engine.rs` lines 826-865 -- `maintain_and_evaluate_frames` dispatch: EdgeAdded/EdgeRemoved/NodeRemoved incremental, `_ =>` rematerialize fallback for PropertyChanged
- `src/engine.rs` lines 919-960 -- `collect_reachable_nodes`: BFS through pattern hops to register all reachable nodes in inverted index
- `src/engine.rs` lines 2618-2684 -- Oracle test 4: `test_oracle_property_changed` with PropertyEquals filter
- `src/routing.rs` lines 190-192 -- InvertedIndex routes PropertyChanged by `node_id` via `collect_by_node`
- `src/types.rs` lines 190-197 -- `Event::PropertyChanged { node_id, key, value }` definition
- `src/types.rs` lines 109-125 -- `Filter` enum: None, PropertyEquals { key, value }, HasProperty { key }
- `src/types.rs` lines 127-144 -- `HopSpec` with direction, edge_type, target_type, filter fields
- `src/frame.rs` lines 145-192 -- `dfs_collect`: filter logic (direction, edge_type, target_type, property filter)
- `src/frame.rs` lines 197-206 -- `Frame::apply_delta(path, epoch, delta)`
- `src/frame.rs` lines 221-223 -- `Frame::snapshot(epoch)` for read-only path access
- `src/path_extender.rs` lines 82-147 -- `extend_edge_added`: backward prefix + forward extension pattern
- `src/path_extender.rs` lines 300-334 -- `edge_matches_hop_directed`: filter evaluation for reached node
- `src/path_extender.rs` lines 344-439 -- `backward_prefixes` and `partial_dfs`: reusable for property change assertion
- `src/path_extender.rs` lines 445-514 -- `extend_forward` and `forward_dfs`: reusable for property change assertion
- `src/graph.rs` lines 389-417 -- `Graph::set_property` and `Graph::get_property`

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` -- PROP-01 through PROP-04 requirement definitions
- `.planning/STATE.md` -- "PropertyChanged -> fallback to full rematerialize (Phase 20)", "Event-based dispatch in maintain_and_evaluate_frames"
- `.planning/phases/18-incremental-edge-addition/18-RESEARCH.md` -- Phase 18 backward prefix + forward extension algorithm
- `.planning/phases/19-incremental-edge-node-removal/19-RESEARCH.md` -- Phase 19 path scanning retraction algorithm

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - No new dependencies, all existing APIs verified in source code
- Architecture: HIGH - The algorithm composes proven techniques from Phase 18 (backward prefix + forward extension for assertion) and Phase 19 (path scanning for retraction). The bidirectional nature (both +1 and -1) is the only novelty, handled by two sequential sub-steps with deduplication.
- Pitfalls: HIGH - Double-assertion, filter key mismatch, HasProperty edge cases, and graph timing all identified from direct code analysis. The dedup-against-existing-paths approach resolves the most critical pitfall. The early exit optimization for frames without property filters is important for performance.
- Algorithm correctness: HIGH - Oracle verification (test 4) already covers the core PropertyChanged scenario. The algorithm is provably correct because: (a) retraction catches all paths where a filter now fails, (b) assertion catches all new paths where a filter now passes, (c) deduplication against existing paths prevents double-assertion, (d) the oracle_check() function will verify exact match against full rematerialize.

**Research date:** 2026-02-26
**Valid until:** 2026-03-26 (stable -- no external dependencies, all internal code)
