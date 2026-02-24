# Architecture Research

**Domain:** Streaming graph runtime with differential MVCC and pre-materialized traversals
**Researched:** 2026-02-24
**Confidence:** HIGH

## Standard Architecture

### System Overview

Streaming graph runtimes that pre-materialize traversal results follow a layered pipeline architecture. The canonical pattern, drawn from differential dataflow (Materialize/Timely), Netflix's real-time distributed graph, and incremental view maintenance (IVM) research, decomposes into four layers: ingestion, storage, maintenance, and serving.

```
┌─────────────────────────────────────────────────────────────────┐
│                       PUBLIC API (lib.rs)                        │
├─────────────────────────────────────────────────────────────────┤
│                    ORCHESTRATION LAYER                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                   Engine (engine.rs)                       │  │
│  └──────────────────────┬────────────────────────────────────┘  │
├─────────────────────────┼───────────────────────────────────────┤
│                  INTERPRETATION LAYER                            │
│  ┌──────────────────┐  │  ┌──────────────────────────────────┐  │
│  │  Interpreter     │  │  │  Frame Prioritizer               │  │
│  │  (interpreter.rs)│  │  │  (frame_prioritizer.rs)          │  │
│  └──────────────────┘  │  └──────────────────────────────────┘  │
├─────────────────────────┼───────────────────────────────────────┤
│                  MAINTENANCE LAYER                               │
│  ┌──────────────────┐  │  ┌──────────────────┐ ┌────────────┐  │
│  │  Frame            │  │  │  Inverted Index  │ │ Embryonic  │  │
│  │  (frame.rs)       │  │  │  (inverted_      │ │ (embryonic │  │
│  │                   │  │  │   index.rs)      │ │  .rs)      │  │
│  └──────────────────┘  │  └──────────────────┘ └────────────┘  │
├─────────────────────────┼───────────────────────────────────────┤
│                  STORAGE LAYER                                   │
│  ┌──────────────────┐  │  ┌──────────────────────────────────┐  │
│  │  Graph Store      │◄─┤  │  Differential MVCC Engine        │  │
│  │  (graph_store.rs) │  │  │  (differential.rs)               │  │
│  └──────────────────┘  │  └──────────────────────────────────┘  │
├─────────────────────────┼───────────────────────────────────────┤
│                  INGESTION LAYER                                 │
│  ┌──────────────────┐  │  ┌──────────────────────────────────┐  │
│  │  Ring Buffer      │◄─┘  │  Sequencer                      │  │
│  │  (ring_buffer.rs) │     │  (sequencer.rs)                  │  │
│  └──────────────────┘     └──────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                  FOUNDATION LAYER                                │
│  ┌──────────────────┐     ┌──────────────────────────────────┐  │
│  │  Types            │     │  Interner                        │  │
│  │  (types.rs)       │     │  (interner.rs)                   │  │
│  └──────────────────┘     └──────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### How Standard Systems Compare

The dominant reference implementations in this domain are:

1. **Differential Dataflow / Timely Dataflow (Rust)** — The academic gold standard for incremental computation. Collections of `(data, time, diff)` triples flow through a dataflow graph. Arrangements (indexed traces) are shared between operators. Compaction collapses old diffs. This is the closest analog to Krabnet's differential engine.

2. **Materialize** — Production system built on differential dataflow. Three logical components: Storage (ingestion + persistence via Persist), Compute (differential dataflow operators on arrangements), and Adapter (SQL parsing, catalog, timestamp management). Data represented as streams of diffs, not full snapshots.

3. **Netflix Real-Time Distributed Graph** — Three-layer architecture (Ingestion/Processing, Storage, Serving) handling 5M+ writes/sec over 8B nodes and 150B edges. Kafka for ingestion, Flink for processing, Cassandra-backed KVDAL for storage.

4. **IVM for Property Graphs (academic)** — Research on incremental view maintenance specifically for property graph traversals with variable-length edge patterns. Pre-computes traversal results, maintains them incrementally on mutations. Speedups of 28-100x over recomputation.

**Krabnet sits at the intersection of (1) and (4):** it applies differential dataflow semantics (+1/-1 deltas) specifically to property graph traversals, pre-materializing results into "frames" rather than maintaining arbitrary SQL views. This is a narrower, more opinionated design than Materialize but shares the same mathematical foundation.

### Component Responsibilities

| Component | Responsibility | Standard Implementation |
|-----------|----------------|------------------------|
| **types.rs** | Core type definitions: NodeId, EdgeId, TypeId, PropertyKey, Event, Delta, Epoch | Newtypes wrapping u64/u32. Enums for events. Zero-cost abstractions |
| **interner.rs** | Maps string property keys and type names to integer IDs | Pre-allocated lookup table. Bidirectional: string-to-id and id-to-string. Insert-once semantics |
| **sequencer.rs** | Global monotonic epoch counter | Single AtomicU64 with Relaxed load / fetch_add. Provides total ordering for all events |
| **ring_buffer.rs** | Lock-free bounded event queue | SPSC or MPSC ring buffer. Power-of-two size. Head/tail with cache-padded atomics. Pre-allocated slots |
| **graph_store.rs** | In-memory property graph with adjacency on nodes | Vec-of-nodes with inline outgoing/incoming edge lists. HashMap for properties. Type-based secondary indexes |
| **differential.rs** | Differential MVCC engine: delta tracking, compaction, snapshots | Collection of (tuple, epoch, +1/-1) triples. Epoch-based compaction merges diffs below frontier. Multiset semantics |
| **frame.rs** | Parked traverser definitions and materialized results | Anchor node + hop pattern + materialized result set. Cold-start via DFS, then incremental maintenance |
| **inverted_index.rs** | Signal-to-frame routing | Inverted posting lists: NodeId/EdgeId -> Vec<FrameId>. On graph mutation, look up affected frames |
| **frame_prioritizer.rs** | Hot/warm/cold tiering based on access patterns | Scoring function over query frequency, mutation rate, recency. Tiering determines interpretation priority |
| **interpreter.rs** | Two-tier interpretation of frame state | Tier 1: binary delta-sum (fast, O(1)). Tier 2: structural path analysis (expensive, traversal-based) |
| **embryonic.rs** | Pattern discovery in mutation stream | Watches for forming patterns via bitvec completion tracking. Auto-promotes to full frame at threshold |
| **engine.rs** | Top-level orchestrator wiring all components | Owns all subsystem instances. Runs the ingest-update-maintain-interpret pipeline loop |
| **lib.rs** | Public API surface | Re-exports Engine, types, and builder APIs. Hides internal module structure |

## Recommended Project Structure

```
src/
├── types.rs            # NodeId, EdgeId, TypeId, PropertyKey, Event, Delta, Epoch
├── interner.rs         # StringInterner: string <-> u32 mapping
├── sequencer.rs        # EpochSequencer: AtomicU64 monotonic counter
├── ring_buffer.rs      # RingBuffer<T>: lock-free pre-allocated circular buffer
├── graph_store.rs      # GraphStore: nodes, edges, adjacency, type indexes
├── differential.rs     # DifferentialEngine: delta collection, compaction, snapshots
├── frame.rs            # Frame, HopPattern, FrameResult: parked traverser system
├── inverted_index.rs   # InvertedIndex: signal-to-frame routing tables
├── frame_prioritizer.rs # FramePrioritizer: hot/warm/cold tiering
├── interpreter.rs      # Interpreter: Tier 1 + Tier 2 frame analysis
├── embryonic.rs        # EmbryonicDiscovery: pattern detection + auto-promotion
├── engine.rs           # Engine: top-level orchestrator, pipeline loop
└── lib.rs              # Public API re-exports
```

### Structure Rationale

- **Flat module layout:** 13 files in `src/` is appropriate for a single-crate PoC. No nested module directories needed at this scale. Each file maps to one architectural concern.
- **Strict dependency ordering:** Files are listed in build-dependency order. Each module depends only on modules above it. This is a DAG, not a graph with cycles.
- **Separation of concerns:** Ingestion (ring_buffer + sequencer), Storage (graph_store + differential), Maintenance (frame + inverted_index + embryonic), Interpretation (interpreter + frame_prioritizer), and Orchestration (engine) are cleanly separated.

## Architectural Patterns

### Pattern 1: Differential Collection as `(Tuple, Epoch, Diff)`

**What:** All state changes are represented as triples of (data, timestamp, +1 or -1). An assertion is +1, a retraction is -1. Compaction collapses annihilating pairs.

**When to use:** Any system that needs incremental maintenance of derived views. This is the mathematical core of differential dataflow.

**Trade-offs:**
- Pro: Exact incremental maintenance. No approximation, no state divergence.
- Pro: Time-travel queries for free (replay diffs up to any epoch).
- Con: Memory pressure from accumulated diffs before compaction.
- Con: Compaction is a critical performance concern — must be tuned.

**Example:**
```rust
/// A single differential update
struct Delta {
    tuple: FrameTuple,  // The data being asserted/retracted
    epoch: Epoch,       // When this change happened
    diff: i64,          // +1 = assert, -1 = retract
}

/// Compaction: collapse diffs below the compaction frontier
fn compact(deltas: &mut Vec<Delta>, frontier: Epoch) {
    // Group by tuple, sum diffs for epochs <= frontier
    // Remove tuples where sum == 0 (annihilation)
    // Replace remaining groups with single delta at frontier
}
```

### Pattern 2: Pre-Materialized Frames with Incremental Maintenance

**What:** Instead of querying the graph at signal time, traversal results are pre-computed ("parked") as frames. When the graph mutates, only affected frames are updated via differential deltas, not recomputed from scratch.

**When to use:** Systems where read latency matters more than write latency. AI context systems where signal-to-decision time is critical.

**Trade-offs:**
- Pro: Zero query-time traversal. Signal arrives, context is already materialized.
- Pro: Write cost is bounded by the number of affected frames, not total frame count.
- Con: Memory cost of materializing all active frames.
- Con: Cold-start cost for new frames (full DFS traversal).
- Con: Complexity of correctly propagating graph deltas through multi-hop patterns.

**Example:**
```rust
/// A parked frame: pre-materialized traversal result
struct Frame {
    id: FrameId,
    anchor: NodeId,           // Starting node
    pattern: HopPattern,      // Multi-hop traversal pattern
    result: Vec<FrameTuple>,  // Current materialized state
    delta_log: Vec<Delta>,    // Pending differential updates
}
```

### Pattern 3: Inverted Index for Signal-to-Frame Routing

**What:** An inverted index maps graph entities (nodes, edges) to the frames that reference them. When an entity is mutated, the index immediately identifies which frames need maintenance.

**When to use:** Any system with many materialized views over shared data. Avoids scanning all frames on every mutation.

**Trade-offs:**
- Pro: O(1) lookup from mutation to affected frames (amortized).
- Pro: Enables selective maintenance — only touched frames are updated.
- Con: Must be maintained in sync with frame creation/deletion.
- Con: Memory cost proportional to total (entity, frame) pairs.

## Data Flow

### Primary Pipeline: Event Ingestion to Interpretation

```
[External Event]
    │
    ▼
[Sequencer] ─── assigns monotonic epoch ───►
    │
    ▼
[Ring Buffer] ─── lock-free enqueue ───►
    │
    ▼
[Engine.drain()] ─── dequeues batch ───►
    │
    ├──► [Graph Store] ─── applies mutation (add/remove node/edge/property) ───►
    │         │
    │         ▼
    │    [Differential Engine] ─── records (+1/-1) delta at epoch ───►
    │         │
    │         ▼
    │    [Inverted Index] ─── looks up affected FrameIds ───►
    │         │
    │         ▼
    │    [Frame Maintenance] ─── re-traverses affected frames, diffs vs previous ───►
    │         │
    │         ▼
    │    [Frame Prioritizer] ─── updates tier scores (query freq, mutation rate) ───►
    │         │
    │         ▼
    │    [Interpreter] ─── Tier 1: delta-sum check. Tier 2 if significant ───►
    │
    └──► [Embryonic Discovery] ─── watches mutation stream for forming patterns ───►
              │
              ▼
         [Auto-promote to Frame] ─── when completion threshold met
```

### Data Flow Detail

1. **Ingestion:** External events (node/edge additions, property changes) enter via the ring buffer. The sequencer stamps each with a monotonic epoch. This is the only lock-free concurrent boundary in the system.

2. **Graph Update:** The engine drains events from the ring buffer and applies them to the graph store. The graph store maintains adjacency lists on each node (both outgoing and incoming edges) for read locality. Type-based secondary indexes enable efficient pattern matching.

3. **Delta Recording:** Each graph mutation is simultaneously recorded in the differential engine as a `(tuple, epoch, +1/-1)` triple. This is the MVCC layer — the graph store has "current state" while the differential engine has "complete history."

4. **Frame Maintenance:** The inverted index maps mutated entities to affected frames. Each affected frame is re-traversed from its anchor node following its hop pattern. The new traversal result is diffed against the previous materialized state. New deltas are recorded. This is the expensive step and follows the "re-traverse and diff" strategy (correctness over performance for PoC).

5. **Interpretation:** The frame prioritizer determines which frames warrant interpretation based on their tier (hot/warm/cold). Hot frames get interpreted on every update cycle. The interpreter runs Tier 1 (binary delta-sum: is the frame's net change nonzero?) as a fast gate. If significant, Tier 2 structural path analysis examines what changed and why.

6. **Embryonic Discovery (parallel path):** Independently of the main pipeline, the embryonic discovery engine watches the mutation stream for emerging patterns. It uses bitvec completion tracking to detect when a pattern template is being "filled in" by incoming events. When completion exceeds a threshold, the embryonic frame auto-promotes to a full parked frame, and the main pipeline takes over maintenance.

7. **Compaction (periodic):** The differential engine periodically compacts old deltas below a compaction frontier. Diffs that annihilate (+1 + -1 = 0) are removed. Surviving diffs are collapsed to the frontier epoch. This bounds memory growth.

### Key Data Flows

1. **Event ingestion flow:** External -> Sequencer -> Ring Buffer -> Engine (single-threaded drain)
2. **Graph mutation flow:** Engine -> Graph Store (mutate) + Differential Engine (record delta)
3. **Frame maintenance flow:** Inverted Index (lookup) -> Frame (re-traverse + diff) -> Differential Engine (record frame deltas)
4. **Interpretation flow:** Frame Prioritizer (select) -> Interpreter (Tier 1 gate -> Tier 2 analysis)
5. **Discovery flow:** Mutation stream -> Embryonic Engine (pattern watch) -> Frame (auto-promote)
6. **Compaction flow:** Differential Engine (periodic compact below frontier)

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| PoC (current) | Single-threaded pipeline. Synchronous compaction. Re-traverse for frame maintenance. All in-memory. This is correct and sufficient for proving the differential math. |
| 10K frames | Inverted index becomes critical — without it, scanning all frames per mutation is O(n). Pre-allocated Vecs for posting lists. Frame prioritizer prevents interpreting cold frames. |
| 100K+ frames | Background compaction thread (interface already isolated). Batch multiple events before frame maintenance pass. Consider incremental path extension instead of full re-traverse. Multi-producer ring buffer for concurrent ingestion. |
| 1M+ frames | Shard frames by anchor node locality. Partition inverted index. Parallel frame maintenance across worker threads (timely dataflow model). Disk-backed cold frames. |

### Scaling Priorities

1. **First bottleneck: Frame maintenance cost.** Re-traversal on every mutation is O(hops * branching_factor) per affected frame. With many frames affected by a single mutation, this dominates. Mitigation: inverted index limits scope, frame prioritizer skips cold frames, and future incremental path extension reduces traversal cost.
2. **Second bottleneck: Differential engine memory.** Without compaction, delta history grows unboundedly. Mitigation: synchronous compaction is sufficient for PoC. Background compaction thread (with isolated interface) is the designed upgrade path.
3. **Third bottleneck: Ring buffer throughput.** Single-consumer is fine for PoC. The atomics are already correct for multi-producer (future-proofed), so scaling ingestion means adding producers without changing the buffer.

## Anti-Patterns

### Anti-Pattern 1: Recomputing All Frames on Every Mutation

**What people do:** On any graph change, iterate all frames and recompute them.
**Why it's wrong:** O(total_frames) per mutation. Doesn't scale past trivial frame counts. Defeats the purpose of incremental maintenance.
**Do this instead:** Use the inverted index to identify only affected frames. Re-traverse only those. Record differential deltas, don't recompute from scratch.

### Anti-Pattern 2: Mixing Current State and History in One Structure

**What people do:** Store both the "current graph" and "delta history" in the same data structure, toggling between modes.
**Why it's wrong:** Complicates compaction, makes snapshot queries expensive, creates coupling between read-hot (graph queries) and write-hot (delta recording) paths.
**Do this instead:** Separate the graph store (current state, optimized for adjacency traversal) from the differential engine (delta history, optimized for epoch-ordered compaction). Krabnet's architecture correctly separates these as `graph_store.rs` and `differential.rs`.

### Anti-Pattern 3: String-Based Hot Path

**What people do:** Use String keys for property lookups and type comparisons on every event.
**Why it's wrong:** Heap allocation, hash computation, and cache-unfriendly access on every operation. Kills hot-path performance.
**Do this instead:** Intern strings at ingestion time (interner.rs). Use integer IDs (u32) everywhere on the hot path. The interner is the boundary between human-readable and machine-efficient representations.

### Anti-Pattern 4: Eager Interpretation of All Frame Changes

**What people do:** Run the full interpretation pipeline on every frame that receives a delta.
**Why it's wrong:** Most deltas are noise. Interpreting cold frames wastes CPU. Tier 2 structural analysis is expensive.
**Do this instead:** Gate interpretation behind prioritization (frame_prioritizer.rs). Use Tier 1 delta-sum as a cheap filter. Only escalate to Tier 2 when the fast check indicates significance.

### Anti-Pattern 5: Unbounded Delta History

**What people do:** Accumulate all historical deltas without compaction, relying on "we'll compact later."
**Why it's wrong:** Memory grows without bound. Old deltas slow down frame maintenance (must scan through irrelevant history). Compaction cost increases the longer you wait.
**Do this instead:** Compact synchronously at well-defined points (e.g., after N events or at epoch boundaries). The compaction interface should be isolated so it can move to a background thread later, but it must run from the start.

## Integration Points

### Internal Boundaries

| Boundary | Communication | Critical Interface |
|----------|---------------|-------------------|
| Sequencer -> Ring Buffer | Epoch stamped on event before enqueue | `Event` struct must carry `Epoch` field |
| Ring Buffer -> Engine | Engine calls `drain()` or `try_pop()` | Returns `Option<Event>` or batch `&[Event]` |
| Engine -> Graph Store | Direct method calls: `add_node()`, `add_edge()`, `set_property()` | Returns mutation result with affected entity IDs |
| Engine -> Differential Engine | Records delta after each graph mutation | `record(tuple, epoch, diff)` |
| Differential Engine -> Frame Maintenance | Provides delta history for frame re-traversal diffing | `deltas_since(epoch)` or `snapshot_at(epoch)` |
| Graph Store -> Inverted Index | Inverted index is consulted after graph mutation | `lookup(entity_id) -> Vec<FrameId>` |
| Inverted Index -> Frame | Engine drives re-traversal of identified frames | Frame exposes `re_traverse(graph_store) -> Vec<Delta>` |
| Frame -> Frame Prioritizer | Updates tier score after maintenance | `update_score(frame_id, mutation_count, query_count)` |
| Frame Prioritizer -> Interpreter | Selects frames for interpretation by tier | `select_hot() -> Vec<FrameId>` |
| Mutation Stream -> Embryonic Discovery | Embryonic engine observes all mutations | Taps the same event stream as the main pipeline |
| Embryonic -> Frame | Auto-promotes completed patterns to full frames | Creates new Frame, registers in inverted index |

### Dependency Graph (Build Order)

```
types.rs ◄─── interner.rs
    ▲              ▲
    │              │
sequencer.rs  ring_buffer.rs
    ▲              ▲
    │              │
    └──────┬───────┘
           │
    graph_store.rs
           ▲
           │
    differential.rs
           ▲
           │
    frame.rs
           ▲
           │
    inverted_index.rs
           ▲
           │
    ┌──────┴───────┐
    │              │
frame_prioritizer.rs  embryonic.rs
    ▲              ▲
    │              │
    └──────┬───────┘
           │
    interpreter.rs
           ▲
           │
    engine.rs
           ▲
           │
    lib.rs
```

### Build Order (Strict Sequential)

This is the order modules should be implemented. Each module compiles and passes tests before proceeding.

| Phase | Module | Dependencies | Rationale |
|-------|--------|--------------|-----------|
| 1 | types.rs | None | Foundation types used by everything |
| 2 | interner.rs | types.rs | String interning needed before any graph operations |
| 3 | sequencer.rs | types.rs | Epoch counter needed by ring buffer and differential |
| 4 | ring_buffer.rs | types.rs | Event ingestion path, uses Event from types |
| 5 | graph_store.rs | types.rs, interner.rs | Core graph storage, uses interned type/property IDs |
| 6 | differential.rs | types.rs | Delta collection + compaction. Epoch-aware |
| 7 | frame.rs | types.rs, graph_store.rs, differential.rs | Parked traverser. Needs graph for traversal, differential for deltas |
| 8 | inverted_index.rs | types.rs, frame.rs | Maps entities to frames. Needs FrameId from frame.rs |
| 9 | frame_prioritizer.rs | types.rs, frame.rs | Tiers frames by access pattern. Needs Frame metadata |
| 10 | interpreter.rs | types.rs, frame.rs, differential.rs | Interprets frame state. Needs Frame + deltas |
| 11 | embryonic.rs | types.rs, frame.rs, graph_store.rs | Pattern discovery. Creates frames, reads graph |
| 12 | engine.rs | All above | Top-level orchestrator wiring everything |
| 13 | lib.rs | engine.rs | Public re-exports |

**Critical path:** types -> graph_store -> differential -> frame -> inverted_index -> engine. This is the longest dependency chain and determines minimum build time.

**Parallelizable:** sequencer.rs and ring_buffer.rs can be built in parallel (both depend only on types.rs). frame_prioritizer.rs and embryonic.rs can be built in parallel (different concerns at the same dependency depth).

## Krabnet vs. Standard Patterns: Assessment

| Aspect | Standard Pattern | Krabnet Design | Assessment |
|--------|-----------------|----------------|------------|
| Delta representation | (data, time, diff) triples | Same: (+1/-1) with epoch | Textbook differential dataflow. Correct. |
| Ingestion | Kafka/message queue | Lock-free ring buffer | Simpler, lower-latency for in-process use. Correct for single-crate PoC. |
| Epoch management | Logical timestamps from coordinator | AtomicU64 monotonic counter | Standard for single-node. Correct. |
| Graph storage | External graph DB or petgraph | Custom adjacency-on-node store | Correct trade-off: read locality over write cost for a traversal-heavy system. |
| View maintenance | Arbitrary SQL views / dataflow operators | Domain-specific "frames" with hop patterns | Narrower than Materialize but exactly right for graph traversal pre-materialization. |
| Routing | Dataflow graph topology | Inverted index (entity -> frames) | Standard IVM technique adapted to graph setting. Correct. |
| Compaction | Background thread with frontier tracking | Synchronous with isolated interface | Correct for PoC. Interface isolation is good forward design. |
| Interpretation | Application-specific | Two-tier (fast gate + deep analysis) | Novel. Not from standard differential dataflow. Domain-specific optimization for AI agent context. |
| Pattern discovery | Not standard in differential dataflow | Embryonic frame discovery with bitvec | Novel. Not from any reference system. Unique to Krabnet's use case. |
| Concurrency | Multi-worker (timely) | Single-threaded with correct atomics | Correct for PoC. Atomics are future-proofed for multi-producer. |

**Key insight:** Krabnet's architecture is a well-structured subset of differential dataflow, specialized for property graph traversals. The 13-module decomposition maps cleanly to standard layered architecture patterns. The two genuinely novel components (embryonic discovery and two-tier interpretation) are domain-specific additions for AI context systems, not replacements for standard patterns.

## Sources

- [Differential Dataflow (GitHub)](https://github.com/TimelyDataflow/differential-dataflow) — Reference implementation of differential dataflow in Rust. HIGH confidence.
- [Materialize Architecture Blog](https://materialize.com/blog/architecture/) — Production differential dataflow system architecture. HIGH confidence.
- [Materialize: Incremental Computation Guide](https://materialize.com/guides/incremental-computation/) — Explains traces, arrangements, compaction. HIGH confidence.
- [MV4PG: Materialized Views for Property Graphs](https://arxiv.org/html/2411.18847v1) — Academic work on pre-materialized traversals for property graphs. MEDIUM confidence.
- [Incremental View Maintenance for Property Graph Queries (ACM 2018)](https://dl.acm.org/doi/abs/10.1145/3183713.3183724) — IVM specifically for property graph traversals. MEDIUM confidence.
- [Netflix Real-Time Distributed Graph (Part 1)](https://netflixtechblog.com/how-and-why-netflix-built-a-real-time-distributed-graph-part-1-ingesting-and-processing-data-80113e124acc) — Production streaming graph: ingestion, processing, storage, serving layers. HIGH confidence.
- [Building Differential Dataflow from Scratch (Materialize)](https://materialize.com/blog/differential-from-scratch/) — Explains differential collection model. HIGH confidence.
- [Lock-Free Ring Buffer in Rust (Ferrous Systems)](https://ferrous-systems.com/blog/lock-free-ring-buffer/) — Ring buffer design patterns for Rust. HIGH confidence.
- [Everything About Incremental View Maintenance](https://materializedview.io/p/everything-to-know-incremental-view-maintenance) — IVM concepts and taxonomy. MEDIUM confidence.
- [VeilGraph: Incremental Graph Stream Processing](https://journalofbigdata.springeropen.com/articles/10.1186/s40537-022-00565-8) — Academic streaming graph processing. MEDIUM confidence.

---
*Architecture research for: Streaming graph runtime with differential MVCC*
*Researched: 2026-02-24*
