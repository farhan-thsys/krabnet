# Project Research Summary

**Project:** Krabnet v3.0 -- Incremental Path Extension
**Domain:** Incremental view maintenance for streaming graph runtime with differential MVCC
**Researched:** 2026-02-26
**Confidence:** HIGH

## Executive Summary

Krabnet is a streaming graph runtime that pre-materializes multi-hop graph traversal results ("frames") using differential dataflow semantics (+1/-1 deltas). The v3.0 milestone replaces full DFS re-traversal of frame paths on every graph mutation with incremental path extension that computes only the delta: which paths are newly valid (+1) and which are newly broken (-1). The research confirms that **no new dependencies are needed** -- the existing `DiffCollection` already implements Z-set semantics, `Frame::apply_delta()` already accepts path-level deltas, and the `InvertedIndex` already routes events to affected frames. The missing piece is purely algorithmic: an event-to-path-delta translation layer (~300-500 lines) that converts "edge added between A and B" into targeted path assertions and retractions.

The recommended approach is a **layered build strategy**: first wire basic frame maintenance into the ingest pipeline (frames are currently materialized once at registration and never updated), then replace that baseline with smart incremental extension using per-hop delta propagation derived from differential dataflow's delta join decomposition. The core algorithm identifies which hop in a frame's pattern a mutation touches, resolves backward prefixes to that hop, extends forward through remaining hops, and emits the resulting path deltas. Start with incremental handling of `EdgeAdded` events only (the dominant case in accumulating graphs), using full re-traverse as fallback for deletions and property changes, then expand incrementalism to other event types once correctness is proven via an oracle test.

The primary risks are **correctness bugs that silently corrupt frame state** -- ghost paths after edge deletion, missing retractions on property filter invalidation, and node deletion cascades that fail to propagate through all hop positions. These are mitigated by a mandatory oracle test (shadow full-re-traverse comparison after every incremental update in debug builds), a conservative hybrid approach (incremental for additions, fallback for deletions initially), and careful handling of the graph-mutation-before-frame-maintenance ordering in the ingest pipeline (deletion context must be captured before the graph mutation destroys edge information).

## Key Findings

### Recommended Stack

No changes to `Cargo.toml`. The incremental path extension is purely algorithmic work using existing Rust std library collections (`HashMap`, `HashSet`, `Vec`) and the project's existing dependencies (`crossbeam` 0.8 for scoped threads, `tokio` 1 for async runtime, `criterion` 0.5 for benchmarks). Five external crates were evaluated and rejected: `differential-dataflow` (massive overkill -- 15+ transitive deps for a distributed runtime), `DBSP/Feldera` (redundant Z-set implementation), `adapton` (pull-based model vs Krabnet's push-based), `petgraph` (no incremental path support), and `smallvec` (premature optimization, defer until profiling warrants). See [STACK.md](STACK.md) for full evaluation.

**Core technologies (unchanged):**
- **Rust stable 1.85+**: No new language features needed; pure algorithmic work
- **`std::collections`**: HashMap/HashSet for reverse path index and hop-to-frame mapping
- **`crossbeam` 0.8**: Scoped threads for parallel delta computation across affected frames
- **`DiffCollection<Vec<NodeId>>`**: Already implements Z-set semantics with +1/-1 deltas, epoch stamps, compaction, and snapshots
- **`Frame::apply_delta()`**: Already accepts individual path assertions/retractions -- the output target for incremental extension

### Expected Features

See [FEATURES.md](FEATURES.md) for full feature landscape, dependency graph, and complexity analysis.

**Must have (table stakes -- P1):**
- **Hop-localized mutation classification** -- determine which hop(s) each event touches per affected frame
- **Per-hop delta derivation** -- compute +1/-1 path deltas without re-traversing unaffected hops (HIGH complexity, core algorithm)
- **Forward extension from mutation point** -- partial DFS through remaining hops only
- **Backward prefix resolution** -- identify existing partial paths reaching the mutation point (reverse DFS first, O(B^K) per lookup)
- **Delta emission into DiffCollection** -- wire derived deltas into existing differential infrastructure via `apply_delta()`
- **Pipeline integration into Engine::ingest** -- replace current read-only tier1_check with write-path incremental extension
- **Inverted index incremental update** -- add newly discovered nodes to posting lists as new paths are found
- **Correctness oracle (debug builds)** -- full re-traverse comparison after every incremental update

**Should have (differentiators -- P2):**
- **Partial path cache** -- O(1) prefix lookup instead of O(B^K) reverse DFS; essential for 3+ hop patterns at scale
- **Batch delta derivation** -- shared prefix computation for coalesced events hitting the same node

**Defer (v4+):**
- **Trunk-aware extension sharing** -- shared trunk computation across frames with identical sub-patterns
- **Adaptive extension strategy** -- dynamically choose incremental vs full re-traverse based on affected-path ratio

### Architecture Approach

The architecture introduces a single new module `PathExtender` (in `src/path_extender.rs`) that is stateless and takes read-only references to `Frame`, `Graph`, and `Event`. It computes path deltas and returns them to the engine, which applies them under a write lock via `Frame::apply_delta()`. The design follows the delta join decomposition pattern from differential dataflow: a multi-hop path query is equivalent to a multi-way join, and a change to one hop is joined against the current state of other hops rather than recomputing the entire join. See [ARCHITECTURE.md](ARCHITECTURE.md) for component diagrams and data flow.

**Major components:**
1. **PathExtender** (NEW, `src/path_extender.rs`) -- Core incremental logic: classifies mutations by hop, resolves prefixes, extends suffixes, returns `Vec<(Vec<NodeId>, Delta)>`
2. **Engine::ingest() modification** (MODIFIED, `src/engine.rs`) -- Replaces read-only tier1 check with PathExtender dispatch + write-lock delta application
3. **Frame query helpers** (MODIFIED, `src/frame.rs`) -- Add `paths_containing_node()` and `paths_through_edge()` for targeted lookups; add `apply_deltas_batch()` for efficient bulk application
4. **DeletionContext** (NEW, in engine.rs) -- Captures edge information before graph mutation for deletion events
5. **PathPositionIndex** (DEFERRED, optimization) -- Secondary index on `(hop_position, node_id)` for O(1) path lookups

### Critical Pitfalls

See [PITFALLS.md](PITFALLS.md) for all 13 pitfalls with prevention strategies and required test cases.

1. **Ghost paths after edge deletion** -- Incremental logic fails to retract all paths traversing a deleted edge. Prevention: path-to-edge reverse index for targeted retraction, OR fallback to full re-traverse on deletions. Oracle test catches this.
2. **Property filter invalidation without path awareness** -- PropertyChanged events do not add/remove edges, so incremental edge-level logic may not trigger. Prevention: treat PropertyChanged as potential retraction AND assertion; re-evaluate hop filters for all paths through the changed node.
3. **Node deletion cascade not propagating** -- `graph.remove_node()` cascades to edge removals, but the engine processes NodeRemoved as a single event. Prevention: decompose NodeRemoved into explicit EdgeRemoved events BEFORE graph mutation; capture edge list before the node is removed.
4. **Compaction destroys incremental index state** -- Intermediate indexes built over DiffCollection tuples go stale after compaction. Prevention: index `current_state()` (invariant across compaction), not individual tuples.
5. **Hop position off-by-one** -- Wrong hop index during extension produces paths with wrong length or wrong filters. Prevention: `PathPosition` newtype, assert `path.len() == pattern.len() + 1` on every assertion.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Re-Diff Baseline (Frame Maintenance Wiring)
**Rationale:** Frames are currently materialized once at registration and never updated during ingest. Before optimizing HOW frames are updated, we must first wire frame maintenance into the pipeline at all. This also establishes the correctness baseline that all subsequent phases are tested against.
**Delivers:** Frames that stay in sync with the graph as it mutates. Full re-traverse + diff + apply_delta pattern.
**Addresses:** Pipeline integration, delta emission into DiffCollection, basic inverted index update
**Avoids:** Pitfall 6 (epoch misassignment -- establish the "all deltas at current processing epoch" rule here)
**Key tasks:** Expose standalone DFS helper, modify Engine::ingest() to re-traverse affected frames and diff against current state, apply deltas via apply_delta(), add Frame::apply_deltas_batch() for efficiency, add DeletionContext for capturing pre-mutation state

### Phase 2: PathExtender for Edge Addition (Core Incremental Algorithm)
**Rationale:** EdgeAdded is the most common and highest-value event type for incremental extension. This phase implements the core delta join decomposition algorithm. Depends on Phase 1 for correctness verification (oracle test compares incremental results against Phase 1 baseline).
**Delivers:** O(affected) path computation for edge additions instead of O(full DFS). The `PathExtender` module.
**Addresses:** Hop-localized mutation classification, per-hop delta derivation, forward extension, backward prefix resolution (reverse DFS approach)
**Avoids:** Pitfall 5 (hop position off-by-one -- define PathPosition newtype), Pitfall 6 (epoch assignment)
**Key tasks:** Create `src/path_extender.rs`, implement affected_hop_index(), find_prefix_paths(), dfs_suffix(), extend_edge_added(), wire into Engine::ingest() for EdgeAdded events, implement oracle test

### Phase 3: PathExtender for Edge and Node Removal
**Rationale:** Removal is harder than addition (Pitfall 1: ghost paths). Building it separately allows focused testing. Depends on Phase 2 for module structure and hop classification.
**Delivers:** Incremental retraction for edge and node removal events. Eliminates full re-traverse fallback for the most common event types.
**Addresses:** Edge removal retraction, node removal cascade decomposition
**Avoids:** Pitfall 1 (ghost paths -- path scanning for targeted retraction), Pitfall 3 (node deletion cascade -- decompose into EdgeRemoved events before graph mutation)
**Key tasks:** Implement retract_edge_removed(), handle_node_removed(), DeletionContext integration, exhaustive oracle testing for deletion scenarios (diamond graphs, multi-frame deletion, cascade)

### Phase 4: Property Change Handling
**Rationale:** PropertyChanged events are the most complex case (can both create and destroy paths). Deferred until edge addition/removal is proven correct. Can use full re-traverse fallback initially for frames with property filters.
**Delivers:** Incremental handling of property filter invalidation and enablement.
**Addresses:** Property filter invalidation, property filter enablement (newly passing filters create paths)
**Avoids:** Pitfall 2 (property filter invalidation without path awareness)
**Key tasks:** Implement handle_property_changed(), re-evaluate hop filters for affected paths, skip no-op for frames with Filter::None at all hops

### Phase 5: Performance Optimization (Path Position Index + Batching)
**Rationale:** Once correctness is established across all event types, optimize the hot path. The partial path cache eliminates O(B^K) backward resolution; batch delta derivation shares computation across coalesced events.
**Delivers:** O(1) prefix lookup for frames with many paths. Batch-optimized delta computation.
**Addresses:** Partial path cache (P2 differentiator), batch delta derivation (P2), inverted index dynamic update optimization
**Avoids:** Pitfall 9 (O(all_paths) scan disguised as O(affected)), Pitfall 4 (compaction state mismatch -- index current_state, not tuples)
**Key tasks:** Implement PathPositionIndex, maintain index in sync with apply_delta(), integrate with compaction (rebuild after swap), benchmark against re-diff baseline at various scales (10/100/1000/10000 paths)

### Phase 6: Verification, Benchmarking, and Hardening
**Rationale:** Final validation phase. Property-based testing, stress testing, compaction race detection, and benchmark suite to quantify improvement over full re-traverse.
**Delivers:** Confidence that incremental extension is correct and performant. Benchmark data. Removal of re-diff fallback for incrementally handled events.
**Addresses:** Correctness oracle (all event types), compaction interaction testing, concurrent stress testing
**Avoids:** Pitfall 10 (double-buffer compaction race -- version-stamp DiffCollection or detect stale swaps)
**Key tasks:** Property-based test (random mutations, oracle comparison), stress test (high-frequency mutations + concurrent compaction), Criterion benchmarks (incremental vs full re-traverse at multiple scales), threshold heuristic (small frames fall back to re-traverse)

### Phase Ordering Rationale

- **Phase 1 must come first** because frames are not currently maintained during ingest at all. Every subsequent phase depends on the pipeline wiring and the correctness baseline it establishes.
- **Phase 2 before Phase 3** because edge addition is simpler than removal (no ghost path risk) and more common in accumulating graphs. The PathExtender module structure is established here.
- **Phase 3 before Phase 4** because edge/node removal is more impactful than property changes and exercises the harder deletion path (Pitfalls 1 and 3).
- **Phase 4 after Phases 2-3** because property changes are the most complex case and can use full re-traverse fallback while earlier phases are delivered.
- **Phase 5 after correctness phases** because optimization before correctness invites Pitfall 9 (appearing fast while being wrong). The position index also interacts with compaction (Pitfall 4), which requires careful design.
- **Phase 6 at the end** because verification and benchmarking require all features to be implemented.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2:** The backward prefix resolution strategy (reverse DFS vs partial path cache) has a significant design choice. Research should validate the reverse DFS approach against expected branching factors and pattern depths in target workloads.
- **Phase 3:** The DeletionContext pattern (capturing edges before graph mutation) requires changes to the ingest pipeline ordering. Research should verify this does not break existing event processing guarantees.
- **Phase 5:** The PathPositionIndex interaction with compaction needs careful design. Research should examine the double-buffer compaction protocol and determine whether version-stamping or epoch-aware merge is the right solution (Pitfall 10).

Phases with standard patterns (skip research-phase):
- **Phase 1:** Well-documented pattern -- DFS + diff + apply is mechanically applying existing components. Low risk.
- **Phase 4:** Property filter handling follows the same pattern as edge handling but with filter re-evaluation. Standard once the PathExtender is established.
- **Phase 6:** Testing and benchmarking are standard practices with clear patterns (oracle comparison, Criterion benchmarks).

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | No new dependencies needed. All alternatives thoroughly evaluated and rejected with clear rationale. Primary sources are the codebase itself and well-known Rust crates. |
| Features | MEDIUM | Feature decomposition is sound and grounded in IVM/differential dataflow literature. The "right" approach for backward prefix resolution (reverse DFS vs partial path cache) depends on workload characteristics not yet benchmarked. |
| Architecture | HIGH | Existing codebase thoroughly analyzed. The delta join decomposition pattern is well-established in Materialize and differential-dataflow. PathExtender as a stateless module with clear boundaries is a clean design. |
| Pitfalls | HIGH | 13 pitfalls identified from codebase analysis and cross-referenced with differential dataflow literature. Critical pitfalls (ghost paths, property filter invalidation, node cascade) have concrete prevention strategies and required test cases. |

**Overall confidence:** HIGH

### Gaps to Address

- **Backward prefix resolution performance**: The reverse DFS approach is O(B^K) per mutation. For deep patterns (4+ hops) with high branching factors (50+), this may be too expensive. The partial path cache alternative is O(1) lookup but O(B^N) memory. The right choice depends on workload data not yet available. Handle during Phase 2 planning: start with reverse DFS, benchmark, add cache if needed in Phase 5.
- **Double-buffer compaction race**: Pitfall 10 identifies a latent race between the compaction worker's clone-compact-swap protocol and incremental writes. The version-stamp solution is conceptually clean but touches the compaction architecture. Handle during Phase 5/6: design the fix when optimizing, validate under stress test.
- **Write lock contention**: Incremental extension requires write locks on frames during ingest (currently only read locks for tier1 check). Impact on `query_frame()` and `snapshot_frame()` latency is unknown. Handle during Phase 1: measure lock contention, consider batched single-lock application.
- **Threshold for incremental vs full re-traverse**: For small frames (< 50 paths), the index maintenance overhead of incremental extension may exceed full re-traverse cost. The crossover point is unknown. Handle during Phase 6: benchmark to find the threshold, implement as a configurable heuristic.

## Sources

### Primary (HIGH confidence)
- Krabnet source code (`frame.rs`, `engine.rs`, `diff.rs`, `routing.rs`, `graph.rs`, `compaction.rs`, `trunk.rs`) -- direct code analysis
- [Differential Dataflow (GitHub)](https://github.com/TimelyDataflow/differential-dataflow) -- reference implementation, delta propagation model
- [Frank McSherry: Differential Dataflow](http://www.frankmcsherry.org/differential/dataflow/2015/04/07/differential.html) -- core mechanics: sparse differences, O(affected) updates
- [Materialize: Delta Joins and Late Materialization](https://materialize.com/blog/delta-joins/) -- delta join decomposition strategy
- [Building Differential Dataflow from Scratch (Materialize)](https://materialize.com/blog/differential-from-scratch/) -- differential collection model, retraction propagation
- [DBSP: Automatic Incremental View Maintenance (Feldera, VLDB 2023)](https://docs.feldera.com/vldb23.pdf) -- Z-set theory validates DiffCollection design

### Secondary (MEDIUM confidence)
- [Localized RETE for Incremental Graph Queries (ICGT 2024)](https://arxiv.org/html/2405.01145v1) -- localized change propagation, partial match storage
- [Incremental Graph Pattern Matching (ACM TODS 2013)](https://dl.acm.org/doi/10.1145/2489791) -- delta propagation for graph pattern matching, boundedness results
- [MV4PG: Materialized Views for Property Graphs (2024)](https://arxiv.org/html/2411.18847v1) -- templated maintenance for variable-length path patterns
- [Everything About IVM](https://materializedview.io/p/everything-to-know-incremental-view-maintenance) -- IVM taxonomy, delta rules, counting/DRed algorithms
- [Incrementalizing Graph Algorithms (SIGMOD 2021)](https://dl.acm.org/doi/10.1145/3448016.3452796) -- academic validation of delta propagation for graph views
- [Incremental Graph Computations (Fan et al., ACM TODS 2022)](https://dl.acm.org/doi/10.1145/3500930) -- tractability of incremental graph computations

### Tertiary (LOW confidence)
- [RETE Algorithm (Wikipedia)](https://en.wikipedia.org/wiki/Rete_algorithm) -- partial match storage and incremental update propagation in rule engines
- [Incremental Graph Computations (SIGMOD 2017)](https://dl.acm.org/doi/10.1145/3035918.3035944) -- localizable vs bounded incremental graph algorithms

---
*Research completed: 2026-02-26*
*Ready for roadmap: yes*
