# Feature Research: Incremental Path Extension

**Domain:** Incremental frame maintenance for streaming graph runtime with differential MVCC
**Researched:** 2026-02-26
**Confidence:** MEDIUM -- techniques well-established in IVM and differential dataflow literature; applying them to Krabnet's specific frame/DiffCollection architecture requires engineering design decisions not found in any reference system

## Context: What Exists Today

The current engine (`engine.rs`) identifies affected frames via the inverted index on each mutation but does NOT update frame materialized state during `ingest`. The evaluation path reads `net_delta` via read locks and runs `tier1_check`, but the frame's `DiffCollection` of paths is never modified after initial `materialize()`. The `apply_delta` method on `Frame` exists but is only called manually in tests. In other words: **the system currently has cold-start materialization but no online frame maintenance at all.** The "full DFS re-traverse" described in PROJECT.md is the planned-but-not-yet-wired baseline, and incremental path extension is the replacement for that planned baseline.

This means the milestone must deliver both (a) wiring frame maintenance into the `ingest` pipeline and (b) making that maintenance incremental rather than full re-traverse.

## Feature Landscape

### Table Stakes (Must Have for Incremental Path Extension)

Features without which the milestone is incomplete. These are the minimum to close the gap between "frames are materialized once at registration" and "frames stay current as the graph mutates."

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Hop-localized mutation classification** | When an EdgeAdded/EdgeRemoved/PropertyChanged arrives, the system must determine WHICH hop(s) in each affected frame's pattern are touched by this specific mutation, not just that the frame is affected. Without hop localization, the only option is full re-traverse. | MEDIUM | The inverted index already identifies affected frame IDs. This feature adds a second level: for each affected frame, classify the event as touching hop 0, hop 1, ..., hop N based on where the mutated entity sits in the frame's pattern. Requires matching the event's node/edge type against each HopSpec in the pattern. |
| **Per-hop delta derivation** | Given that a mutation touches hop K of a frame's N-hop pattern, derive the +1/-1 path deltas WITHOUT re-traversing hops 0..K-1 or K+1..N. Only the paths passing through the mutated entity at hop K are affected. New paths through the mutated entity must be discovered; vanished paths must be retracted. | HIGH | This is the core algorithmic feature. For an edge addition at hop K: take the set of partial paths that reach the edge's source at hop K-1, extend them through the new edge to the target, then extend forward through hops K+1..N from the target. For edge removal: find existing complete paths that traverse the removed edge at hop K, retract them all. Property changes: re-evaluate the filter at the affected hop for all paths passing through that node. |
| **Forward extension from mutation point** | When a new edge/node qualifies at hop K, extend the partial path forward through hops K+1 to N using the existing graph. Collect all newly reachable complete paths and assert them (+1). | MEDIUM | This is a partial DFS starting from the mutation point, not the anchor. The DFS covers hops K+1..N only, with the same HopSpec filters. Bounded by branching factor to the power of (N - K), which is strictly less than the full DFS cost of branching_factor^N. |
| **Backward prefix resolution from mutation point** | When a new edge appears at hop K, identify which existing partial paths reach the edge's source node at position K. These are the prefixes that can potentially extend through the new edge. | HIGH | Two approaches: (a) walk backward from the source node through hops K-1..0 toward the anchor, collecting valid prefix paths (reverse DFS); (b) maintain a per-frame partial path index keyed by (hop_position, node_id) so prefixes can be looked up in O(1). Approach (b) has memory cost but O(1) lookup. Approach (a) has zero memory cost but O(branching_factor^K) per lookup. |
| **Delta emission into DiffCollection** | Derived path deltas (new paths = +1, removed paths = -1) must be recorded in the frame's `DiffCollection<Vec<NodeId>>` at the current epoch. This wires incremental path extension into the existing differential MVCC infrastructure. | LOW | The `Frame::apply_delta` method already exists. This feature is about calling it correctly from the incremental extension logic and ensuring the frame's `net_delta`, `mutation_count`, and `last_epoch` are updated. |
| **Pipeline integration (wiring into Engine::ingest)** | The incremental path extension must execute as part of the `ingest` pipeline, replacing the current no-op frame "evaluation" (which only reads net_delta). After inverted index lookup and fan-out limiting, each affected frame must run incremental extension and emit deltas. | MEDIUM | Currently `std::thread::scope` fans out to threads that do read-only tier1_check. This must change to a write path that acquires write locks and calls the incremental extension logic. Must respect coalescing (batch mutations before extension) and fan-out limits (cap extensions per event). |
| **Inverted index re-registration on path changes** | When incremental extension discovers new paths or retracts old ones, the inverted index must be updated to reflect the new set of entities referenced by each frame. New paths may introduce nodes not previously in the frame's posting list. | MEDIUM | Currently `register_frame` is called once at frame creation with node IDs from the initial materialization. Incremental extension may discover paths through previously unindexed nodes. The index must be updated incrementally (add new nodes, remove nodes no longer in any path) or periodically re-registered. |

### Differentiators (Competitive Advantage Beyond Baseline)

Features that make Krabnet's incremental path extension notably better than a naive implementation.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Partial path cache (prefix/suffix index)** | Maintain a per-frame index of partial paths keyed by (hop_position, node_id). Enables O(1) lookup of "which partial paths reach node X at hop K" instead of reverse-DFS. Amortizes the cost of backward prefix resolution across multiple mutations. | HIGH | Memory cost: O(total_partial_paths) per frame. For a frame with B branching factor and N hops, this is O(B^N) entries. Benefit: backward resolution drops from O(B^K) to O(matching_prefixes). This is the RETE-style "store partial matches" approach. Must be maintained incrementally as paths are added/removed. |
| **Batch delta derivation** | When the coalescer flushes a batch of same-node mutations, derive all path deltas for the batch in a single pass rather than per-event. Multiple edge additions to the same node can share prefix/suffix computation. | MEDIUM | Integrates with existing `MutationCoalescer`. Instead of running incremental extension per event, accumulate events and derive deltas for the batch. Shared prefix computation avoids redundant backward resolution. Particularly valuable for super-node mutations where many edges are added/removed in a window. |
| **Affected-hop-only cost model** | The cost of incremental extension should be O(affected_paths * remaining_hops), not O(total_paths_in_frame). When a mutation touches hop K of an N-hop pattern, only paths passing through that hop are re-evaluated. Other paths are untouched. | LOW | This is not a separate feature but a correctness property of the per-hop delta derivation. Document it as a design invariant. The cost should be O(prefixes_at_K * branching_factor^(N-K)) for additions and O(paths_through_entity) for removals. |
| **Trunk-aware extension sharing** | When multiple frames share a trunk sub-path (detected by `trunk.rs`), a mutation to the trunk portion can share the prefix/suffix computation across all frames sharing that trunk. Avoids redundant DFS for the shared portion. | HIGH | Requires a shared trunk cache that maps (trunk_pattern, hop_position, node_id) to partial path sets. When a trunk mutation occurs, compute the trunk-level delta once and distribute to all frames containing that trunk. Interacts with trunk detection and hot-pinning. |
| **Correctness oracle (debug-mode full re-traverse comparison)** | In debug builds, after incremental extension, perform a full re-traverse and compare the result set against the incrementally maintained state. Any divergence is a bug. | MEDIUM | Essential for validating correctness during development. The full re-traverse is already implemented (`Frame::materialize`). Compare the DiffCollection current_state after incremental extension against a fresh materialization. Add as a `debug_assert!` in the extension path. Can be disabled in release builds for zero cost. |

### Anti-Features (Do NOT Build)

Features that seem related to incremental path extension but would add unjustified complexity or conflict with the design.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| **Speculative forward extension** | Pre-extend paths one hop beyond the current pattern in case the pattern grows later. "Saves work if the pattern is extended." | Violates the principle that frames materialize exactly their declared pattern. Speculative extensions waste memory and computation on paths that may never be needed. Complicates the DiffCollection because speculative paths must be filtered from queries. | Extend the pattern explicitly by creating a new frame with the longer pattern. The cold-start cost is paid once; incremental maintenance handles the rest. |
| **Cross-frame delta sharing** | When two frames share intermediate nodes, propagate a delta computed for one frame to the other without re-deriving it. | Frame patterns are independently defined. Even if two frames touch the same node at the same hop, their HopSpec filters (edge_type, target_type, property filter) may differ, producing different path sets. Sharing deltas across frames with different filters produces wrong results. | Trunk-aware extension sharing (above) is the safe version: it shares computation only for frames with identical trunk sub-patterns, not merely overlapping nodes. |
| **Lazy/deferred extension** | Do not extend paths eagerly on mutation. Instead, mark frames as "dirty" and extend only when queried. | Contradicts Krabnet's core value proposition: "when a signal arrives, context is already materialized." Lazy extension means the frame may be stale at query time, requiring on-demand re-traversal -- exactly what Krabnet exists to avoid. | Eager extension on mutation. The fan-out limiter already caps the number of frames extended per event. Deferred frames in the fan-out queue still get extended, just not immediately. |
| **Cycle-aware infinite extension** | Detect cycles during extension and produce infinite-length path results. | Frame patterns have finite hop counts (Vec<HopSpec> has known length). Cycle detection is handled by the DFS naturally terminating at the hop limit. Infinite paths are meaningless in the frame model. | The existing hop limit is the cycle termination mechanism. No additional cycle handling is needed for incremental extension. |
| **Parallel per-hop extension within a single frame** | Parallelize the forward extension at each hop level within a single frame's extension computation. | For typical patterns (2-5 hops), the per-hop work is too fine-grained for thread overhead to pay off. The existing parallelism is at the frame level (multiple frames extended in parallel via `std::thread::scope`). Adding intra-frame parallelism creates contention on the frame's write lock. | Keep frame-level parallelism via `std::thread::scope`. Intra-frame work is sequential but bounded by the hop count and local branching factor. |

## Feature Dependencies

```
[Hop-Localized Mutation Classification]
    |
    v
[Per-Hop Delta Derivation]
    |
    +--requires--> [Forward Extension from Mutation Point]
    |                  |
    |                  +--uses--> Graph::neighbors() (existing)
    |                  +--uses--> HopSpec filters (existing)
    |
    +--requires--> [Backward Prefix Resolution from Mutation Point]
    |                  |
    |                  +--option-a--> Reverse DFS (zero memory, O(B^K) per lookup)
    |                  +--option-b--> Partial Path Cache (O(paths) memory, O(1) lookup)
    |
    v
[Delta Emission into DiffCollection]
    |
    +--uses--> Frame::apply_delta (existing)
    +--uses--> DiffCollection::assert_tuple / retract_tuple (existing)
    |
    v
[Pipeline Integration (Engine::ingest wiring)]
    |
    +--requires--> All of the above
    +--interacts--> MutationCoalescer (existing, batch mode)
    +--interacts--> FanOutLimiter (existing, cap per-event extensions)
    +--interacts--> std::thread::scope parallelism (existing, needs write locks)
    |
    v
[Inverted Index Re-registration]
    |
    +--requires--> Delta Emission (must know what paths changed)
    +--uses--> InvertedIndex::register_frame / unregister_frame (existing)

--- Differentiators (build after table stakes) ---

[Partial Path Cache]
    +--enhances--> Backward Prefix Resolution (O(1) vs O(B^K))
    +--requires--> Delta Emission (cache must be maintained incrementally)

[Batch Delta Derivation]
    +--enhances--> Per-Hop Delta Derivation
    +--requires--> MutationCoalescer (existing)

[Trunk-Aware Extension Sharing]
    +--requires--> Partial Path Cache
    +--requires--> trunk::detect_trunks (existing)
    +--enhances--> Per-Hop Delta Derivation (shared prefix computation)

[Correctness Oracle]
    +--requires--> Per-Hop Delta Derivation
    +--uses--> Frame::materialize (existing, for comparison)
```

### Dependency Notes

- **Hop-Localized Mutation Classification requires nothing new:** The HopSpec patterns and Event types already exist. Classification is pure pattern matching: compare the event's entity IDs and types against each HopSpec in the frame's pattern.
- **Per-Hop Delta Derivation requires both Forward Extension and Backward Prefix Resolution:** For an edge addition at hop K, you need the prefixes that reach the source (backward) and the suffixes that extend from the target (forward). Both halves are needed to produce complete paths.
- **Pipeline Integration requires all table stakes features:** It is the final wiring step. Must not be attempted until the extension logic is proven correct in isolation.
- **Partial Path Cache is the critical differentiator:** Without it, backward prefix resolution is O(B^K) per mutation. With it, backward resolution is O(matching_prefixes). The cache makes incremental extension viable for deep patterns (3+ hops) at scale. However, it adds memory cost and maintenance complexity, so it should be deferred if the initial reverse-DFS approach is sufficient for the target workloads.
- **Correctness Oracle should be implemented early:** It is a safety net for the entire extension algorithm. Build it alongside per-hop delta derivation, not after.

## MVP Definition

### Launch With (v3.0 -- Incremental Path Extension Baseline)

Minimum viable incremental path extension that replaces full DFS re-traverse.

- [ ] **Hop-localized mutation classification** -- determine which hop(s) each mutation touches per affected frame
- [ ] **Per-hop delta derivation with forward extension** -- extend new paths from mutation point forward through remaining hops
- [ ] **Backward prefix resolution via reverse DFS** -- walk backward from mutation point to anchor to find valid prefixes (zero-memory approach first)
- [ ] **Delta emission into DiffCollection** -- wire derived deltas into the existing differential infrastructure
- [ ] **Pipeline integration into Engine::ingest** -- replace the read-only tier1_check with write-path incremental extension
- [ ] **Inverted index incremental update** -- add newly discovered nodes to posting lists
- [ ] **Correctness oracle in debug builds** -- full re-traverse comparison after every incremental extension

### Add After Validation (v3.x)

Features to add once baseline incremental extension is correct and benchmarked.

- [ ] **Partial path cache** -- add when benchmarks show backward prefix resolution is the bottleneck (likely for patterns with 3+ hops and branching factor > 10)
- [ ] **Batch delta derivation** -- add when coalescer integration shows redundant prefix computation across batched events
- [ ] **Trunk-aware extension sharing** -- add when many frames share trunk sub-paths and trunk mutations dominate the workload

### Future Consideration (v4+)

- [ ] **Adaptive extension strategy** -- dynamically choose between incremental extension and full re-traverse based on the ratio of affected paths to total paths (when > 50% of paths are affected, full re-traverse may be cheaper)
- [ ] **Extension cost estimator** -- before extending, estimate the cost and compare against re-traverse cost; choose the cheaper option per frame per event

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Hop-localized mutation classification | HIGH | LOW | P1 |
| Per-hop delta derivation | HIGH | HIGH | P1 |
| Forward extension from mutation point | HIGH | MEDIUM | P1 |
| Backward prefix resolution (reverse DFS) | HIGH | MEDIUM | P1 |
| Delta emission into DiffCollection | HIGH | LOW | P1 |
| Pipeline integration (Engine::ingest) | HIGH | MEDIUM | P1 |
| Inverted index incremental update | MEDIUM | MEDIUM | P1 |
| Correctness oracle (debug mode) | HIGH | MEDIUM | P1 |
| Partial path cache | HIGH | HIGH | P2 |
| Batch delta derivation | MEDIUM | MEDIUM | P2 |
| Trunk-aware extension sharing | MEDIUM | HIGH | P3 |
| Adaptive extension strategy | LOW | MEDIUM | P3 |

**Priority key:**
- P1: Must have for v3.0 -- delivers the O(affected) update promise
- P2: Should have -- significant performance improvement for known workloads
- P3: Nice to have -- optimization for specific patterns, defer until validated

## Existing Infrastructure Dependencies

The following existing components are required by incremental path extension and must NOT be modified in ways that break their contracts:

| Component | What Incremental Extension Needs From It | Contract |
|-----------|------------------------------------------|----------|
| `Frame::apply_delta(path, epoch, delta)` | Emit +1/-1 for discovered/retracted paths | Updates mutation_count, last_epoch, net_delta, and DiffCollection |
| `Frame::pattern()` -> `&[HopSpec]` | Read the hop pattern to classify mutations and drive extension | Immutable after frame creation |
| `Frame::anchor()` -> `NodeId` | Starting point for backward prefix resolution | Immutable after frame creation |
| `DiffCollection::assert_tuple(data, epoch)` | Record new paths | Maintains exact multiset semantics |
| `DiffCollection::retract_tuple(data, epoch)` | Remove vanished paths | Maintains exact multiset semantics |
| `DiffCollection::current_state()` -> `Vec<&T>` | Query current paths for correctness oracle comparison | Returns paths with positive net delta |
| `Graph::neighbors(node, direction, edge_type)` | Forward and backward neighbor traversal during extension | Returns (EdgeId, NodeId) pairs filtered by direction and optional edge type |
| `Graph::get_node_type(node)` -> `Option<TypeId>` | Target type filtering during forward extension | Returns None for nonexistent nodes |
| `Graph::get_property(node, key)` -> `Option<&PropertyValue>` | Property filtering during forward extension | Returns None for missing properties |
| `InvertedIndex::register_frame(id, nodes, edges)` | Update posting lists when new paths are discovered | Idempotent for duplicate registrations |
| `InvertedIndex::affected_frames(event)` -> `HashSet<u64>` | Identify which frames need extension | Deduplicates across node and edge lookups |
| `MutationCoalescer` | Batch mutations before extension | Flushes CoalescedBatch with deduplicated node entries |
| `FanOutLimiter` | Cap extensions per event for super-nodes | Returns immediate + deferred frame lists |
| `std::thread::scope` | Frame-level parallelism for extension | Each thread must acquire write lock on its frame |

## Event-Type-Specific Extension Behavior

Each event type requires different incremental extension logic:

| Event Type | Extension Behavior | Affected Hops | Cost Model |
|------------|-------------------|---------------|------------|
| **EdgeAdded** | Most common and complex. Classify which hop(s) the new edge satisfies. For each hop K where (source, edge_type) matches: resolve backward prefixes to hop K, extend forward from target through hops K+1..N. Assert all newly complete paths. | Hop(s) where edge type and source node match | O(prefixes_at_K * B^(N-K)) per matching hop |
| **EdgeRemoved** | Find all existing complete paths that traverse the removed edge at any hop position. Retract all such paths (-1). | All hops where the removed edge was used | O(paths_through_edge) -- lookup from current_state |
| **NodeRemoved** | Find all existing complete paths that include the removed node at any position. Retract all such paths. Also retract paths where the removed node is a target at any hop. | All hops where the node appears | O(paths_through_node) |
| **PropertyChanged** | Re-evaluate property filters at hops where the changed node is a target. Paths that previously passed the filter but now fail: retract. Paths that previously failed but now pass: assert (requires forward extension from that hop). | Hops with property filters targeting this node | O(affected_paths * filter_cost) |
| **NodeAdded** | Typically no-op for existing frames (new isolated nodes have no edges). May affect frames with anchors at the new node ID (unlikely for incrementally maintained frames). | None for existing frames | O(1) |

## Complexity Analysis: Incremental vs Full Re-traverse

For a frame with N hops, branching factor B, and a single mutation at hop K:

| Approach | Cost | Memory | Notes |
|----------|------|--------|-------|
| Full DFS re-traverse | O(B^N) | O(B^N) paths | Current planned baseline. Always correct but expensive. |
| Incremental (reverse DFS for prefixes) | O(B^K + B^(N-K)) | O(1) extra | Backward prefix + forward extension. No extra memory. |
| Incremental (with partial path cache) | O(matching_prefixes * B^(N-K)) | O(B^N) cached partial paths | O(1) prefix lookup. Forward extension only. Best for repeated mutations. |
| Incremental (batch, M events same hop) | O(B^K + M * B^(N-K)) | O(1) or O(B^N) | Shared backward resolution across batch. M << B^K for coalesced events. |

**When incremental wins:** Mutations are localized (touch few hops), branching factor is moderate (< 100), and pattern depth is >= 2 hops. For 1-hop patterns with branching factor 1, incremental has no advantage.

**When full re-traverse wins:** When > 50% of paths are affected (e.g., anchor node removal), or when the graph is very small (re-traverse cost is negligible). The adaptive extension strategy (P3) would handle this.

## Sources

### Primary (HIGH confidence)
- [Materialize: Delta Joins and Late Materialization](https://materialize.com/blog/delta-joins/) -- delta propagation through multi-way joins without intermediate materialization
- [Frank McSherry: Differential Graph Computation](http://www.frankmcsherry.org/differential/dataflow/2015/05/12/bfs.html) -- incremental graph BFS via differential dataflow, per-operator delta propagation
- [Frank McSherry: Differential Dataflow](http://www.frankmcsherry.org/differential/dataflow/2015/04/07/differential.html) -- core mechanics: Mobius inversion, sparse differences, O(affected) updates
- [Differential Dataflow (GitHub)](https://github.com/TimelyDataflow/differential-dataflow) -- reference implementation of differential computation on graphs

### Secondary (MEDIUM confidence)
- [Localized RETE for Incremental Graph Queries (ICGT 2024)](https://arxiv.org/html/2405.01145v1) -- localized change propagation in RETE networks, marking-sensitive partial match storage
- [Incremental Graph Pattern Matching (ACM TODS 2013)](https://dl.acm.org/doi/10.1145/2489791) -- delta propagation for graph pattern matching, affected area bounded updates
- [MV4PG: Materialized Views for Property Graphs (2024)](https://arxiv.org/html/2411.18847v1) -- templated maintenance for variable-length path patterns in property graphs
- [Everything About IVM](https://materializedview.io/p/everything-to-know-incremental-view-maintenance) -- IVM taxonomy, delta rules, counting/DRed algorithms
- [RisingWave: Building Differential Dataflow](https://risingwave.com/blog/from-zero-to-hero-building-differential-dataflow/) -- practical guide to differential dataflow operator implementation

### Tertiary (LOW confidence)
- [RETE Algorithm (Wikipedia)](https://en.wikipedia.org/wiki/Rete_algorithm) -- partial match storage and incremental update propagation in rule engines
- [Incremental Graph Computations (SIGMOD 2017)](https://dl.acm.org/doi/10.1145/3035918.3035944) -- localizable vs bounded incremental graph algorithms

---
*Feature research for: Incremental path extension in streaming graph runtime*
*Researched: 2026-02-26*
