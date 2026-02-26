# Pitfalls: Incremental Path Extension for Frame Maintenance

**Domain:** Adding incremental graph traversal maintenance to an existing full-re-traverse differential MVCC runtime
**Researched:** 2026-02-26
**Confidence:** HIGH (analysis grounded in actual codebase structures: `frame.rs`, `diff.rs`, `graph.rs`, `engine.rs`, `compaction.rs`, `routing.rs`, `trunk.rs`; cross-referenced with differential dataflow literature, incremental graph pattern matching theory, and Materialize engineering)

---

## Critical Pitfalls

These are mistakes that silently produce wrong results -- the incremental path state diverges from what full DFS re-traverse would produce, and nothing panics or errors. These are the hardest bugs to find and the most dangerous for a system whose core value proposition is "the differential math is exact."

---

### Pitfall 1: Ghost Paths After Edge Deletion (Missing Retractions)

**What goes wrong:**
An edge is deleted from the graph, but the incremental path extension logic fails to retract all paths that traversed the deleted edge. The frame's DiffCollection still contains +1 assertions for paths that no longer exist in the graph. `frame.query()` returns paths that full DFS re-traverse would not produce.

**Why it happens in Krabnet specifically:**
The current system (`frame.rs` line 128-192) does full DFS from anchor through all hops on every event affecting the frame. This is correct-by-construction: the full traversal discovers exactly the paths that exist. Incremental path extension replaces this with "only traverse the delta" -- when an edge `(A, B)` of type T is deleted, the system must identify and retract every path that used that specific edge at any hop position.

The difficulty: a single edge deletion can affect paths at multiple hop depths. Consider a 3-hop pattern `[H1, H2, H3]` anchored at node X. If edge `(B, C)` is deleted, path `[X, A, B, C, D]` must be retracted. But the incremental logic only receives the event `EdgeRemoved { source: B, target: C }`. It must:
1. Determine that node B appears at hop position 1 in some paths
2. Determine that the edge `(B, C)` satisfies hop H2's direction and type filter
3. Find all existing paths that include the sub-path `[..., B, C, ...]`
4. Retract each such path

If any of these steps is incomplete (e.g., the inverted index only tracks anchor nodes and endpoint nodes, not intermediate hop nodes), paths are silently missed.

**Consequences:**
- `frame.query()` returns paths that do not exist in the graph -- a correctness violation
- The differential math is no longer exact: the frame's DiffCollection has positive-net-delta tuples for paths that should have been annihilated
- `frame.snapshot(epoch)` diverges from what `frame.rematerialize()` would produce at the same epoch
- Every downstream consumer (Tier 1 interpretation, Tier 2 structural analysis, gRPC SubscribeFrame broadcast, MCP krabnet_query_frame) receives incorrect data

**Prevention:**
- **Path-to-edge index:** For each frame, maintain a reverse index from `(source, target, edge_type)` triples to the set of materialized paths containing that edge. When an edge is deleted, look up all paths in this index and retract each one. This index is O(total_paths * avg_path_length) memory.
- **Fallback to full re-traverse on deletion:** As a safety valve, when an edge or node is deleted, fall back to full DFS re-traverse for affected frames. Only use incremental extension for edge/node additions. This is the "conservative hybrid" approach -- incrementalism on the easy side (additions), full re-traverse on the hard side (deletions). Measure the fraction of events that are deletions; if it is low (typical for context graphs where data accumulates), this is a good tradeoff.
- **Oracle test (MANDATORY):** After every incremental update, compare the frame's `current_state()` against a shadow frame that does full `rematerialize()` on the same graph. Any difference is a bug. Run this in debug/test builds only (it is O(full_DFS) per event, defeating the purpose of incrementalism in production).

**Detection:**
- The oracle test catches this immediately
- In production: periodically run `rematerialize()` on a random sample of frames and compare against incremental state. Log divergences as critical errors.
- Warning sign in development: tests pass for edge additions but fail when edge deletions are added to the test sequence

**Phase to address:** This is the single most important pitfall. The incremental path extension design must have an explicit deletion strategy (path-to-edge index, or fallback to full re-traverse) decided and documented BEFORE any code is written.

---

### Pitfall 2: Property Filter Invalidation Without Path Awareness

**What goes wrong:**
A `PropertyChanged` event modifies a node's property, causing it to no longer satisfy a hop's `Filter::PropertyEquals` or `Filter::HasProperty` constraint. The incremental logic does not retract paths that now fail the filter, because the node is still present in the graph -- only its properties changed.

**Why it happens in Krabnet specifically:**
Looking at the current `dfs_collect` in `frame.rs` (lines 145-192), property filters are evaluated during DFS traversal:
```rust
Filter::PropertyEquals { key, value } => {
    if graph.get_property(neighbor_id, *key) != Some(value) {
        continue;
    }
}
```
Full re-traverse naturally re-evaluates these filters on every pass. But incremental path extension typically works at the edge level: "edge added -> extend paths" / "edge removed -> retract paths." A `PropertyChanged` event does not add or remove edges. The incremental extension logic may not even be triggered for property changes, leaving stale paths that no longer match the hop filter.

Conversely, a property change can also CREATE new valid paths: a node that previously failed a filter now passes it, enabling path extensions that were not possible before.

**Consequences:**
- Paths in the frame that include nodes no longer satisfying their hop's property filter
- Missing paths where a property change now enables a previously-blocked traversal
- The frame's state diverges from what full `materialize()` would produce

**Prevention:**
- **Treat PropertyChanged as a potential retraction AND assertion:** When a node's property changes, identify all paths in affected frames that pass through that node. Re-evaluate the hop filter for that node at its position in each path. If the filter now fails, retract the path. If the filter now passes (and the path would have been blocked before), assert the path.
- **Alternative: fall back to full re-traverse for PropertyChanged on filtered hops.** If the affected frame's pattern has `Filter::None` at every hop, property changes cannot affect path validity -- skip re-evaluation entirely. Only trigger re-evaluation for frames whose patterns use `PropertyEquals` or `HasProperty` at the hop position matching the changed node.
- **Inverted index must track property-sensitive frames.** The current `InvertedIndex` (`routing.rs`) routes `PropertyChanged` events to frames by node ID. This is correct for triggering re-evaluation, but the incremental logic must then determine WHICH paths through that node are affected. This requires knowing the hop position of the node within each path.

**Detection:**
- Oracle test catches this
- Test case: graph with node B having property `status=active`. Frame pattern requires `PropertyEquals { key: status_key, value: active }` at hop 1. Paths `[A, B, C]` are materialized. Change B's property to `status=inactive`. Verify incremental state matches empty (no valid paths), same as full re-traverse.

**Phase to address:** Must be designed alongside the incremental extension logic. Cannot be deferred -- PropertyChanged events are a core event type.

---

### Pitfall 3: Node Deletion Cascade Not Propagating Through All Hop Positions

**What goes wrong:**
A node is removed from the graph. The graph module (`graph.rs` lines 155-191) correctly cascades removal of all edges connected to the deleted node. But the incremental path extension logic handles the `NodeRemoved` event as a single retraction point, not realizing the node appeared at multiple hop positions across different paths and different frames.

**Why it happens in Krabnet specifically:**
Node removal in `Graph::remove_node` cascades to edge removal, generating multiple effective edge deletions. But the engine (`engine.rs` line 264-265) processes `NodeRemoved` as a single event:
```rust
Event::NodeRemoved { node_id } => {
    self.graph.remove_node(*node_id);
}
```
The inverted index routes this to affected frames, but the incremental logic receives a node removal, not the individual edge removals. If the incremental path extension only handles `EdgeAdded`/`EdgeRemoved` events (the natural delta events for path extension), it may miss the implicit edge deletions caused by node removal.

**Consequences:**
- Same as Pitfall 1 (ghost paths), but triggered by node deletion rather than edge deletion
- Potentially worse: a deleted node that appeared as an intermediate hop in many paths causes widespread ghost paths across multiple frames

**Prevention:**
- **Decompose NodeRemoved into explicit EdgeRemoved events before incremental processing.** When a `NodeRemoved` event arrives, query the graph BEFORE removing the node to enumerate all edges connected to it. Generate synthetic `EdgeRemoved` events for each. Process these through the incremental path extension logic. Then remove the node.
- **Ordering matters:** The graph mutation (`graph.remove_node`) and the frame maintenance pass must be ordered so that edge information is available when computing retractions. Currently, `engine.rs` applies the graph mutation (step 2) before frame evaluation (step 4). For incremental path extension, the logic must capture the edges-to-be-removed BEFORE the graph mutation destroys them.
- **Store edge list on deletion.** Before calling `self.graph.remove_node(node_id)`, query `self.graph.neighbors(node_id, Direction::Any, None)` to capture all connected edges. Use this list to drive path retractions.

**Detection:**
- Oracle test catches this
- Test case: 3-hop path `[A, B, C, D]`. Remove node C. Verify frame retracts `[A, B, C, D]`. Then also verify: if C had incoming edges from other nodes, all paths through C are retracted across all affected frames.

**Phase to address:** Must be resolved in the same phase as the core incremental extension logic. The event decomposition (NodeRemoved -> EdgeRemoved cascade) should be implemented in `engine.rs` ingest pipeline before the incremental maintenance pass.

---

### Pitfall 4: Compaction Destroys Path History Needed for Incremental Deltas

**What goes wrong:**
The compaction worker (`compaction.rs`) compacts a frame's DiffCollection below a frontier epoch, collapsing multiple assert/retract pairs into single entries. Later, an incremental path extension needs to determine "what paths currently exist in this frame" to compute the delta, but the pre-compaction tuple structure that would have enabled this lookup has been destroyed.

**Why it happens in Krabnet specifically:**
The current compaction logic (`diff.rs` lines 156-203) collapses all tuples at-or-before the frontier into a single tuple per payload with the summed delta. This means:
- Before compaction: tuples `[(path_A, epoch=1, +1), (path_A, epoch=5, -1), (path_A, epoch=7, +1)]`
- After compaction at frontier=6: tuples `[(path_A, epoch=6, 0)]` -- ANNIHILATED, removed entirely
- But path_A should still be present (epoch=7 assertion is after frontier)

Wait -- this specific case is actually handled correctly because tuples after the frontier are preserved. The real danger is:

- Incremental extension maintains intermediate state (e.g., "paths currently passing through node B at hop position 2") separate from the DiffCollection
- Compaction operates on the DiffCollection, which may change the set of "currently active" paths
- The incremental extension's intermediate index is now stale with respect to the compacted DiffCollection

**Consequences:**
- Incremental extension computes deltas against stale intermediate state
- Retractions target paths that no longer exist in the compacted collection (harmless but wasteful, producing -1 on zero-net tuples and triggering compaction warnings)
- Assertions miss paths that were collapsed during compaction (the intermediate index thinks the path still exists as individual tuples, but compaction merged them)

**Prevention:**
- **Rebuild incremental indexes after compaction.** When the compaction worker swaps a compacted DiffCollection into a frame (`frame.rs` `swap_diff_collection`), also trigger a rebuild of any incremental path indexes (path-to-edge index, hop-position index) from the compacted state.
- **Alternative: make incremental indexes compaction-aware.** Instead of indexing individual DiffTuples, index the derived "current state" (paths with positive net delta). This state is invariant across compaction -- compaction does not change `current_state()`, only the tuple representation. Therefore, an index over `current_state()` survives compaction without rebuilding.
- **Compaction frontier must be communicated to incremental logic.** The incremental extension must know the compaction frontier so it does not attempt to reason about tuple-level structure below the frontier.

**Detection:**
- Test case: materialize frame, apply several incremental updates, compact, then apply more incremental updates. Compare against full re-traverse. The post-compaction incremental updates must produce the same result as if no compaction had occurred.

**Phase to address:** Must be designed into the incremental extension from the start. The compaction interaction is not something that can be bolted on later -- the choice of what to index (tuple-level vs current-state-level) determines the entire incremental architecture.

---

### Pitfall 5: Incorrect Hop Position Tracking for Path Extension

**What goes wrong:**
When a new edge `(A, B)` is added, the incremental logic must determine: "At which hop position(s) in the frame's pattern could this edge participate?" If the hop position is calculated incorrectly, the path extension either:
- Extends from the wrong position, producing paths that do not match the hop pattern
- Misses a valid extension because the edge was not recognized as matching the correct hop

**Why it happens in Krabnet specifically:**
A frame's pattern is a `Vec<HopSpec>` (e.g., `[H0, H1, H2]` for a 3-hop pattern). When edge `(A, B, type=T)` is added:
- If A is the anchor node and H0 specifies `direction: Outgoing, edge_type: Some(T)`, then `[anchor, B]` is a partial path at hop 0
- If B was already at hop position 1 in existing path `[anchor, X, B]`, and H2 could be extended from B, then... wait, B is at hop 1, so extending from B uses H2 (not H1)

The hop index arithmetic is: if a node is at position `p` in the path (0 = anchor), the outgoing edge from that node corresponds to hop `p` in the pattern. Getting this off by one produces paths with the wrong number of nodes, or paths where filters from the wrong hop are applied.

**Consequences:**
- Paths with wrong length (too many or too few hops)
- Paths where an intermediate node passes the filter for the wrong hop (e.g., hop 1 filter is applied at hop 2 position)
- Paths that should be extended are missed because the hop position lookup returns "no matching hop"

**Prevention:**
- **Explicit `PathPosition` type:** Define a newtype `struct PathPosition(usize)` where 0 = anchor. The hop index for extending from position `p` is always `p` (the p-th element of `pattern`). Never use raw `usize` arithmetic on positions without the newtype.
- **Invariant assertion:** Every materialized path `[n0, n1, ..., nk]` must satisfy `k == pattern.len()` (path length = hops + 1, where n0 is the anchor). Assert this before storing any path. If a path has wrong length, it was extended from the wrong position.
- **Test with multi-hop patterns where different hops have different filters.** A 3-hop pattern `[H0: edge_type=A, H1: edge_type=B, H2: edge_type=C]` on a graph where edge types are mixed. Verify that only paths following the exact A -> B -> C edge type sequence are materialized.

**Detection:**
- Length assertion on every asserted path catches wrong-length paths immediately
- Oracle test catches all path correctness issues

**Phase to address:** This is a design-level decision. The path position tracking mechanism must be defined before coding incremental extension.

---

### Pitfall 6: Partial Path State Leaking Across Epochs

**What goes wrong:**
The incremental extension maintains intermediate state about partial paths (e.g., "from anchor, after hop 0, these nodes are reachable: {B, C}"). This state is built up across multiple epochs as edges are added. If an edge added at epoch E extends a partial path that was built at epoch E-5, the resulting full path must be recorded at epoch E (the epoch of the completing event), not at epoch E-5. If the path's epoch is misassigned, temporal snapshots return wrong results.

**Why it happens in Krabnet specifically:**
The `DiffCollection` (`diff.rs`) is epoch-stamped: `assert_tuple(data, epoch)`. The epoch determines when the path "becomes visible" in temporal snapshots. If incremental extension finds that a new edge at epoch 10 completes a path `[A, B, C]` where `A -> B` was added at epoch 3, the full path `[A, B, C]` must be asserted at epoch 10 (the epoch when it became a complete path), not epoch 3.

But what about the reverse? If at epoch 15 the edge `A -> B` is removed, the retraction of `[A, B, C]` must happen at epoch 15. The incremental logic must track which epoch caused each path to be asserted so that the retraction has the correct epoch.

**Consequences:**
- `frame.snapshot(Epoch(8))` returns paths that should not be visible until epoch 10
- Compaction merging tuples across incorrect epoch boundaries
- Time-travel queries return wrong results

**Prevention:**
- **Rule: the epoch of a path assertion/retraction is always the epoch of the event that caused it.** Not the epoch of the earliest edge in the path, and not the epoch of the latest edge. It is the epoch of the event being processed RIGHT NOW.
- **This matches the current full-re-traverse behavior.** In the current code, `frame.materialize(&g, epoch)` records all paths at the epoch of the materialize call. Incremental extension must preserve this: all path assertions/retractions from processing event at epoch E are recorded at epoch E.
- **Do NOT attempt to assign "birth epochs" to individual path segments.** This introduces version tracking complexity that is unnecessary and error-prone. A path either exists as a complete entity at epoch E or it does not.

**Detection:**
- Snapshot test: add edge A->B at epoch 3, add edge B->C at epoch 5 (completing 2-hop path). `snapshot(Epoch(4))` should NOT contain `[A, B, C]`. `snapshot(Epoch(5))` should contain `[A, B, C]`.

**Phase to address:** Design principle to be established upfront. All incremental path assertions use the current processing epoch.

---

## Moderate Pitfalls

Mistakes that cause performance problems, unnecessary complexity, or test failures but do not silently corrupt data.

---

### Pitfall 7: Inverted Index Registration Mismatch After Incremental Extension

**What goes wrong:**
When incremental path extension discovers new paths, the new paths may traverse nodes that were not registered in the `InvertedIndex` when the frame was initially materialized. Future events affecting these unregistered nodes will not route to the frame, causing missed incremental updates.

**Prevention:**
- When incremental extension discovers a new path containing new intermediate nodes, update the `InvertedIndex` to include those nodes in the frame's posting list. This means the inverted index must support dynamic expansion of a frame's registered nodes.
- Currently, `InvertedIndex::register_frame` is called once at frame creation (`engine.rs` line 499). The inverted index needs an `update_frame_nodes` method or the frame must be unregistered and re-registered with the expanded node set.
- Alternative: register ALL nodes in the graph's connected component reachable from the anchor (over-registration). This avoids the dynamic update problem at the cost of extra inverted index entries.

**Detection:**
- Test case: frame initially materializes paths through nodes {A, B}. A new edge `B -> C` is added, extending paths to include node C. Then a property change on C should trigger frame re-evaluation. If C was never registered in the inverted index, the property change is silently missed.

---

### Pitfall 8: Trunk/Leaf Classification Stale After Incremental Extension

**What goes wrong:**
The trunk detection system (`trunk.rs`) classifies frames based on shared sub-paths in their patterns. When incremental path extension changes which paths are materialized in a frame, the trunk classification is not invalidated. Frames that should be pinned to Hot (because they share structural spines) may not be, or frames may remain pinned when they no longer share trunks.

**Prevention:**
- Trunk classification operates on pattern structure (`Vec<HopSpec>`), not on materialized paths. Since incremental extension does not change the pattern (only which paths match it), trunk classification is actually invariant. This pitfall is a misconception -- but it is worth verifying that no code path inadvertently modifies the frame's pattern during incremental extension.
- The real risk: if incremental extension is used to support "adaptive patterns" (patterns that evolve based on observed data), trunk classification would break. Ensure the frame's `pattern` field remains immutable after creation.

**Detection:**
- Assert `frame.pattern()` is unchanged after every incremental update.

---

### Pitfall 9: O(all_paths) Scan Disguised as O(affected)

**What goes wrong:**
The incremental extension logic is nominally O(affected) but internally scans all existing paths in the frame to determine which ones are affected by an event. This makes the "incremental" update just as expensive as full re-traverse for frames with many paths.

**Prevention:**
- The path-to-edge index (from Pitfall 1) enables O(1) lookup of paths affected by a specific edge event, avoiding the full scan.
- For `PropertyChanged` events, an additional index from `(NodeId, hop_position)` to affected paths enables targeted re-evaluation.
- Profile: after implementing incremental extension, benchmark against full re-traverse on frames with 1000+ paths. If incremental is not measurably faster, something is wrong with the indexing.
- Measure the constant factor: even with correct asymptotic complexity, the index maintenance overhead may make incremental slower than full re-traverse for small frames (< 50 paths). Use a threshold: frames below the threshold fall back to full re-traverse.

**Detection:**
- Criterion benchmark comparing `incremental_update` vs `rematerialize` for frames with 10, 100, 1000, and 10000 paths. Incremental must be faster for >= 100 paths or the optimization is not worthwhile.

---

### Pitfall 10: Double-Buffered Compaction Race With Incremental State

**What goes wrong:**
The background compaction worker (`compaction.rs`) uses double-buffering: clone DiffCollection under read lock, compact the clone off-lock, swap back under write lock. If incremental path extension writes new assertions/retractions to the frame between the clone and the swap, those writes are lost when the compacted (stale) collection is swapped in.

**Why it happens in Krabnet specifically:**
This is already a latent issue with the current full-re-traverse approach, but incremental extension makes it worse because incremental updates are more frequent (every event, not just events that change the graph topology). The window between clone and swap is longer for large DiffCollections.

**Prevention:**
- The current double-buffer protocol already handles this: the write lock prevents concurrent writes during the swap. But incremental updates that occur AFTER the read-lock clone and BEFORE the write-lock swap are lost.
- **Solution: version stamp the DiffCollection.** Add a monotonic version counter to DiffCollection. On clone, record the version. On swap, check the version: if it has advanced since the clone, the compacted result is stale -- discard and retry, or merge the delta.
- **Alternative: epoch-aware merge.** After swapping, replay any DiffTuples with epoch > compaction frontier from the old collection into the new one. Since compaction only touches tuples <= frontier, tuples > frontier are preserved in the compacted collection anyway. The real risk is tuples added during the compaction window at epochs > frontier -- these exist in the original but not the clone.
- **Simplest fix: acquire write lock for the full duration of compaction.** This eliminates the race at the cost of blocking readers during compaction. For frames where compaction is rare (tuple count threshold is high), this may be acceptable.

**Detection:**
- Stress test: run continuous incremental updates while compaction runs. Compare frame state against oracle after every 100 events. If the race exists, the oracle will diverge after a compaction cycle.

---

## Minor Pitfalls

Mistakes that cause test failures or minor inconsistencies but are straightforward to fix.

---

### Pitfall 11: Direction Reversal Bug in Backward Path Extension

**What goes wrong:**
When extending a path backward (a new edge `(X, A)` is added where A is the anchor, enabling backward extension at hop 0 if the hop direction is `Incoming`), the direction logic is inverted. An edge `(X, A)` with A as the target means X is incoming to A -- this matches `Direction::Incoming` at hop 0. Getting the direction wrong (checking A's outgoing instead of incoming) misses valid backward extensions.

**Prevention:**
- Test every `Direction` variant explicitly: `Outgoing`, `Incoming`, `Any`. For each, verify that incremental extension matches full re-traverse.
- The existing test suite for `Graph::neighbors` covers direction filtering, but the incremental extension logic introduces a NEW place where direction must be checked. Add dedicated tests for incremental extension with each direction.

---

### Pitfall 12: Self-Loop Edge Creates Infinite Extension

**What goes wrong:**
A self-loop edge `(A, A)` is added. The incremental extension logic tries to extend paths through A, which loops back to A, which triggers another extension through A, ad infinitum.

**Prevention:**
- The current DFS (`frame.rs`) does not have explicit cycle detection but is naturally bounded by hop count (`hop_index >= self.pattern.len()`). Incremental extension must similarly cap extension depth at `pattern.len() - current_position`.
- Self-loops are a special case: they should only contribute to a path if the hop pattern explicitly allows them (i.e., the hop's target type matches the current node's type).
- Test: add self-loop on a node that appears in a 2-hop frame. Verify the frame does not produce paths with duplicate consecutive nodes (unless the pattern semantics require it).

---

### Pitfall 13: Empty Frame After All Paths Retracted Leaves Stale Index Entries

**What goes wrong:**
All paths in a frame are incrementally retracted (every path's net delta goes to zero). The frame is now empty but still registered in the inverted index. Events continue to route to this empty frame, triggering expensive but useless incremental processing.

**Prevention:**
- After incremental update, check `frame.net_delta() == 0` and `frame.query().is_empty()`. If true, consider the frame for eviction or deregistration from the inverted index.
- This is not strictly a correctness issue (the empty frame produces correct empty results) but a performance issue. The inverted index routes events to frames that have no paths to maintain.

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Designing the incremental extension API | Pitfall 5 (hop position off-by-one) | Define `PathPosition` newtype, assert path length == hops + 1 on every assertion |
| Implementing edge addition handling | Pitfall 6 (epoch misassignment) | Rule: all assertions at current processing epoch, never at edge-creation epoch |
| Implementing edge deletion handling | Pitfall 1 (ghost paths) | Path-to-edge reverse index OR fallback to full re-traverse on deletions |
| Implementing node deletion handling | Pitfall 3 (cascade not propagating) | Decompose NodeRemoved into EdgeRemoved events BEFORE graph mutation |
| Implementing property change handling | Pitfall 2 (filter invalidation) | Re-evaluate hop filter for all paths through changed node; skip if Filter::None at all hops |
| Integrating with compaction | Pitfall 4 (compacted state mismatch) | Index current_state, not individual tuples; rebuild indexes after compaction swap |
| Integrating with compaction worker | Pitfall 10 (double-buffer race) | Version-stamp DiffCollection; detect stale swaps |
| Integrating with inverted index | Pitfall 7 (unregistered nodes) | Dynamically update inverted index when new paths traverse new nodes |
| Performance validation | Pitfall 9 (O(all_paths) scan) | Benchmark incremental vs full re-traverse; ensure index-backed O(affected) lookup |
| Testing strategy | All pitfalls | Oracle test: shadow full-re-traverse after every incremental update |

---

## Required Test Cases

These tests must exist and pass before the incremental path extension feature is considered correct.

### T1: Oracle Test (Full Re-Traverse Equivalence)

**What:** After every incremental update, compare `frame.current_state()` against a shadow frame that runs `rematerialize()` on the same graph. Assert set equality.
**Why:** Catches all divergence bugs (Pitfalls 1-6). This is the single most important test.
**How:** Create a `FrameOracle` wrapper that holds two frames with identical patterns. One uses incremental extension, the other does full re-traverse. After each graph mutation, assert both produce the same `current_state()`.
**Topology:** Run on tree, diamond, cycle, star, and disconnected graphs.

### T2: Edge Deletion Retraction Completeness

**What:** Build a 3-hop frame `[A, B, C, D]`. Delete edge `B -> C`. Assert that path `[A, B, C, D]` is retracted and `frame.query()` is empty.
**Variations:** Delete first edge (A -> B), middle edge (B -> C), last edge (C -> D). Each must retract the full path.

### T3: Node Deletion Cascade

**What:** Build a 3-hop frame `[A, B, C, D]`. Remove node C. Assert all paths through C are retracted. Verify no ghost paths remain.
**Variation:** C is also part of paths in OTHER frames. All affected frames must retract.

### T4: Property Filter Invalidation

**What:** Frame with `PropertyEquals { key: k, value: v }` at hop 1. Materialized path `[A, B, C]` where B has property `k=v`. Change B's property to `k=other_value`. Assert path `[A, B, C]` is retracted.
**Variation:** Change B's property BACK to `k=v`. Assert path `[A, B, C]` is re-asserted.

### T5: Property Filter Enablement

**What:** Frame with `PropertyEquals { key: k, value: v }` at hop 1. Node B exists but does NOT have property k. Edge `A -> B -> C` exists but path is not materialized (B fails filter). Set property `k=v` on B. Assert path `[A, B, C]` is now asserted.

### T6: Multi-Frame Deletion Consistency

**What:** Three frames all contain paths through node X. Delete node X. Assert all three frames retract all paths through X. No frame retains ghost paths.

### T7: Interleaved Compaction and Incremental Updates

**What:** Materialize frame. Apply 100 incremental updates (mix of adds and deletes). Compact at epoch 50. Apply 100 more incremental updates. Compare against full re-traverse. State must match.

### T8: Diamond Graph Path Counting

**What:** Graph: `A -> B -> D` and `A -> C -> D` (diamond). 2-hop frame anchored at A. Both paths `[A, B, D]` and `[A, C, D]` are materialized. Delete edge `B -> D`. Assert only `[A, C, D]` remains. Re-add `B -> D`. Assert both paths return.

### T9: Direction Variants

**What:** Three frames with `Direction::Outgoing`, `Direction::Incoming`, and `Direction::Any` at hop 0. Same graph mutation. Each frame must produce the correct incremental result matching full re-traverse.

### T10: Epoch Correctness for Snapshots

**What:** Add edge A -> B at epoch 3. Add edge B -> C at epoch 7 (completing 2-hop path). Assert `snapshot(Epoch(5))` does NOT contain `[A, B, C]`. Assert `snapshot(Epoch(7))` DOES contain `[A, B, C]`.

### T11: Empty Extension (No Matching Neighbors)

**What:** Frame pattern requires `edge_type: Some(TypeId(100))`. Add edge with `TypeId(200)`. Assert no paths are extended (the edge does not match the hop filter). Incremental update is a no-op. State is unchanged.

### T12: Concurrent Compaction Race

**What:** Stress test. Spawn a thread doing continuous incremental updates. Background compaction worker active. Run for 10 seconds. Compare final state against full re-traverse oracle. Must match exactly.

---

## Krabnet-Specific Integration Risks

### Risk: Current `engine.rs` ingest pipeline applies graph mutation BEFORE frame maintenance

In the current pipeline (`engine.rs` lines 260-285), the graph is mutated first, then the inverted index is queried, then frames are evaluated. For incremental path extension with deletions, the frame maintenance needs to know WHAT was deleted. But after `graph.remove_node(node_id)` or `graph.remove_edge(edge_id)`, the deleted structure is gone from the graph. The incremental logic cannot query the graph to find "which edges did this node have?"

**Mitigation:** Capture deletion information BEFORE applying the graph mutation. For `NodeRemoved`, enumerate all edges. For `EdgeRemoved`, record source/target/type. Store this in a `DeletionContext` struct that is passed to the incremental maintenance logic.

### Risk: Frame evaluation currently uses read lock only (Tier 1 check)

The current frame evaluation in `engine.rs` (lines 362-382) spawns scoped threads that acquire READ locks on frames to check `net_delta()`. Incremental path extension requires WRITE locks to assert/retract paths. This changes the concurrency model: write locks block readers, potentially increasing latency for `query_frame()` and `snapshot_frame()` during the maintenance pass.

**Mitigation:** Keep the existing parallel evaluation for Tier 1 checks under read lock. Then, sequentially (or with a different locking strategy), apply incremental path updates under write lock. Alternatively, batch all path assertions/retractions and apply them in a single write-lock acquisition per frame.

### Risk: `apply_delta` signature does not fit incremental extension output

The current `Frame::apply_delta(path: Vec<NodeId>, epoch: Epoch, delta: Delta)` expects a complete path and a scalar delta. Incremental extension produces a SET of path additions and retractions. Calling `apply_delta` in a loop is correct but performs `aggregate_net_delta()` on every call (line 206 of `frame.rs`), which is O(tuples). For N incremental updates, this is O(N * tuples).

**Mitigation:** Add a `Frame::apply_deltas_batch(deltas: Vec<(Vec<NodeId>, Delta)>, epoch: Epoch)` method that applies all deltas and recomputes `net_delta` once at the end.

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Ghost paths (Pitfall 1) | LOW | Fall back to `rematerialize()` for affected frames. Add oracle test to prevent regression. |
| Property filter miss (Pitfall 2) | LOW | Fall back to `rematerialize()` for frames with property filters on affected node. |
| Node deletion cascade (Pitfall 3) | MEDIUM | Add deletion context capture in `engine.rs`. Requires pipeline restructuring. |
| Compaction state mismatch (Pitfall 4) | MEDIUM | Rebuild incremental indexes after compaction. Requires defining which indexes exist. |
| Hop position off-by-one (Pitfall 5) | LOW | Fix position arithmetic. Path length assertion catches it immediately. |
| Epoch misassignment (Pitfall 6) | LOW | Fix epoch assignment. Snapshot test catches it. |
| Inverted index mismatch (Pitfall 7) | MEDIUM | Add dynamic index update. Requires `InvertedIndex` API extension. |
| Double-buffer race (Pitfall 10) | HIGH | Requires DiffCollection versioning or protocol change. Touches compaction architecture. |

---

## Decision Framework: When to Use Incremental vs Full Re-Traverse

Not all events benefit from incremental processing. The implementation should support a hybrid approach:

| Event Type | Incremental Candidate? | Rationale |
|------------|----------------------|-----------|
| EdgeAdded | YES -- best case | Straightforward path extension: find partial paths ending at source, extend through new edge |
| EdgeRemoved | MAYBE -- use reverse index | Requires path-to-edge index for targeted retraction. Without index, full re-traverse is safer. |
| NodeAdded | NO -- usually no-op | Adding a node does not create new paths (no edges yet). Can skip frame maintenance entirely. |
| NodeRemoved | PREFER full re-traverse | Cascade complexity is high. Decomposing to edge removals helps but is still complex. |
| PropertyChanged (frame has property filter) | PREFER full re-traverse | Property changes can both create and destroy paths. Targeted re-evaluation is complex. |
| PropertyChanged (frame has no property filter) | NO-OP | Property changes cannot affect path validity. Skip frame maintenance entirely. |

**Recommendation:** Start with incremental extension for `EdgeAdded` ONLY. Use full `rematerialize()` for all other event types. This captures the dominant case (graphs grow more than they shrink) while avoiding the hardest correctness problems. Expand to edge deletion incrementalism only after the oracle test is green for all event types.

---

## Sources

- [Incremental Graph Pattern Matching (Fan et al., ACM TODS 2013)](https://dl.acm.org/doi/10.1145/2489791) -- Foundational results on bounded/unbounded incremental graph matching. Proves that incremental matching for graph simulation is bounded for deletions but UNBOUNDED for insertions with general patterns. HIGH confidence.
- [Incremental Graph Computations: Doable and Undoable (Fan et al., ACM TODS 2022)](https://dl.acm.org/doi/10.1145/3500930) -- Extends boundedness results. Shows which incremental graph computations are tractable. HIGH confidence.
- [MV4PG: Materialized Views for Property Graphs (arXiv 2024)](https://arxiv.org/html/2411.18847v1) -- Template-based view maintenance for property graphs with variable-length edges. MEDIUM confidence.
- [differential-dataflow Issue #242: Compaction and trace wrappers](https://github.com/TimelyDataflow/differential-dataflow/issues/242) -- Compaction frontier composition bug in reference implementation. HIGH confidence.
- [Building Differential Dataflow from Scratch (Materialize)](https://materialize.com/blog/differential-from-scratch/) -- Differential collection model, retraction propagation. HIGH confidence.
- [Everything About Incremental View Maintenance (materializedview.io)](https://materializedview.io/p/everything-to-know-incremental-view-maintenance) -- IVM taxonomy and correctness requirements. MEDIUM confidence.
- Krabnet source code analysis: `frame.rs`, `diff.rs`, `graph.rs`, `engine.rs`, `compaction.rs`, `routing.rs`, `trunk.rs` -- Direct code inspection. HIGH confidence.

---
*Pitfalls research for: Incremental path extension replacing full DFS re-traverse on frame maintenance*
*Researched: 2026-02-26*
*Milestone: v3.0 Tech Debt Closure + Incremental Path Extension*
