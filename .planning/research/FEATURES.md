# Feature Research

**Domain:** Streaming graph runtime with differential MVCC for pre-materialized traversal
**Researched:** 2026-02-24
**Confidence:** MEDIUM — domain is well-understood academically; Krabnet's specific combination is novel and less validated

## Feature Landscape

### Table Stakes (Users Expect These)

Features that any streaming graph runtime or differential dataflow system must have. Missing these means the system is non-functional or non-credible.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Property graph model (nodes, edges, properties) | Every graph runtime stores typed nodes and edges with key-value properties. This is the fundamental data model. Neo4j, Memgraph, TigerGraph all use it. | MEDIUM | Krabnet stores adjacency on Node struct (outgoing + incoming). Integer IDs for everything on hot path. Must support typed nodes/edges and property storage. |
| Graph mutation API (add/remove nodes, edges, properties) | Users must be able to modify the graph. All graph databases expose CRUD operations. Without this, data cannot enter the system. | LOW | Straightforward — the API surface is well-understood. The write-cost tradeoff from adjacency-on-node means mutations update two node structs per edge. |
| Event ingestion pipeline | Streaming systems must accept events. Flink, Spark Streaming, Kafka Streams, Memgraph all have ingestion paths. A streaming runtime without ingestion is a static graph. | MEDIUM | Krabnet uses a lock-free ring buffer with monotonic epoch sequencing. This is more opinionated than generic ingestion but still table stakes for a streaming system. |
| Incremental computation (avoid full recomputation) | The entire point of differential/streaming systems. Materialize, Differential Dataflow, Flink Gelly, iGraph, Ingress all do incremental updates. Full recomputation on every change is a non-starter. | HIGH | Core to Krabnet's design: +1/-1 delta semantics. Cold start does full DFS, then incremental re-traversal per event. This is the hardest table-stakes feature to get right. |
| Correct delta/change semantics | Differential dataflow's defining property: changes expressed as (data, time, diff) triples. Materialize, DD, and DBSP all require correct multiset arithmetic. +1 + (-1) = 0 (annihilation). | HIGH | Krabnet's +1/-1 multiset semantics must be mathematically exact. This is non-negotiable — incorrect delta math produces wrong results silently. |
| Temporal ordering / logical timestamps | All streaming systems need a notion of time to order events. Timely Dataflow uses partially-ordered timestamps. Flink uses watermarks. Without ordering, results are non-deterministic. | MEDIUM | Krabnet uses monotonic epoch sequencing from the ring buffer. Simpler than DD's partially-ordered timestamps but sufficient for single-node PoC. |
| Compaction / garbage collection | Differential dataflow's arrangements compact indistinguishable timestamps. Materialize compacts traces. Without compaction, memory grows unboundedly as history accumulates. | MEDIUM | Krabnet does synchronous compaction — collapses surviving tuples after annihilation. Interface isolated for future async. Essential for long-running systems. |
| Snapshot / temporal query support | Users expect to query the state at a specific point in time. DD arrangements support multi-versioned access. Materialize exposes temporal queries via SQL. | MEDIUM | Krabnet supports temporal snapshots via the MVCC engine. The compaction frontier determines how far back you can query. |
| Topological indexing | Graph queries need efficient neighbor lookup. Every graph database indexes adjacency. Without it, traversal is O(E) per hop instead of O(degree). | MEDIUM | Krabnet stores adjacency directly on Node structs and uses topological indexing. This is standard but the specific layout (outgoing + incoming on node) is an implementation choice. |

### Differentiators (Competitive Advantage)

Features that set Krabnet apart from existing systems. These are not found (or not combined) in existing graph runtimes.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Pre-materialized traversal results (parked frames) | **The core differentiator.** Existing graph DBs compute traversals at query time. Materialize pre-computes SQL views. Krabnet pre-computes *graph traversal* results and parks them. When a signal arrives, context is already materialized — zero query-time traversal. MV4PG (2024 paper) explores this for property graphs but as a database extension, not a runtime-native design. | HIGH | Frames define multi-hop patterns from anchor nodes. Cold start does full DFS, then incremental maintenance on each mutation. The frame abstraction (parked traverser) is novel — it is both the query definition and the cached result. |
| Signal-to-frame routing via inverted index | Signals (events) are routed to affected frames via posting lists, not by scanning all frames. This is how Krabnet achieves O(affected) instead of O(all-frames) per event. No comparable system combines inverted-index routing with graph traversal materialization. | MEDIUM | Node and edge posting lists map graph elements to the frames that depend on them. When a node/edge changes, only frames in its posting list are re-evaluated. Analogous to how search engines route documents to queries, but applied to graph traversal. |
| Embryonic Frame Discovery with auto-promotion | The system watches the mutation stream for *forming* patterns and auto-promotes them to full parked frames when a completion threshold is met. This is proactive — the system discovers frames nobody asked for. No existing graph runtime or differential dataflow system does autonomous pattern discovery from the mutation stream. | HIGH | Uses bit-vector completion tracking per pattern template. Progressive: partial matches tracked as "embryos," promoted when they cross threshold. This is the most speculative feature — closest analog is emergent pattern detection in stream processing (EPDA algorithm), but applied to graph structure, not event content. |
| Adaptive frame tiering (hot/warm/cold) | Frames are tiered by access frequency, mutation rate, and recency. Hot frames stay fully materialized; cold frames can be evicted or stored more compactly. This is data tiering applied to *computation results*, not raw data. Elasticsearch does hot/warm/cold for data; Krabnet does it for materialized traversals. | MEDIUM | Scoring function combines query frequency, mutation rate, and recency. Tiering decisions affect memory footprint directly. Important for bounded memory in long-running systems. Similar to ARMS (Adaptive Robust Memory Tiering) in concept but applied to computation artifacts. |
| Two-tier interpretation (binary + structural) | Tier 1: fast binary delta-sum check (did the frame's delta-sum change?). Tier 2: full structural path analysis (what specifically changed and why?). This avoids expensive structural analysis when the delta-sum says "nothing meaningful changed." | LOW | Binary check is O(1); structural analysis is O(path-length). The two-tier approach is a performance optimization that reduces unnecessary work. Novel in the graph traversal context — similar to Bloom filter pre-checks in database systems. |
| Differential MVCC as first-class graph primitive | Materialize applies differential dataflow to SQL. Krabnet applies it to *graph traversals*. The +1/-1 delta semantics are applied to graph path results, not relational tuples. This is a novel combination — DD semantics natively on a property graph with traversal-aware compaction. | HIGH | The MVCC engine must handle graph-specific concerns: path validity depends on intermediate nodes, edge deletion can invalidate paths without touching endpoints, etc. More complex than relational IVM because graph paths have structural dependencies. |
| Zero-allocation hot path | After initialization, the hot path (event ingestion through frame update) does zero heap allocation. All buffers, indexes, and structures are pre-allocated. This is a performance differentiator — most graph databases and streaming systems allocate during processing. | MEDIUM | Requires careful pre-allocation strategy: ring buffer sized at init, frame storage pre-allocated with capacity, index structures using fixed-size arrays. Constrains the design but delivers predictable latency. |

### Anti-Features (Deliberately NOT Building in PoC)

Features that seem appealing but would add complexity, dilute focus, or conflict with Krabnet's design goals.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Query language (Cypher, GQL, Gremlin) | Every graph DB has one. Users expect declarative queries. | Krabnet is not a database — it is a runtime that pre-materializes traversals. Adding a query language implies query-time computation, which contradicts the core value (zero query-time traversal). Query language design is a multi-year effort (see GQL standardization). | Frames are defined programmatically via Rust API. The "query" is the frame pattern definition, not a runtime query string. |
| Distributed / multi-node execution | Timely Dataflow and Differential Dataflow scale to multiple workers. Flink and Spark are distributed by default. | Distribution adds massive complexity: partitioning the graph, routing events across nodes, distributed compaction, consensus on epoch boundaries. This is a PoC — prove the physics first. | Single-node, single-threaded (with correct atomics for future multi-producer). The ring buffer and atomics are designed to scale, but distribution is a v2+ concern. |
| Disk persistence / durability | Databases persist data. Users expect crash recovery. | Persistence changes the entire memory model. Krabnet's zero-allocation hot path assumes everything is in memory. Adding WAL, checkpointing, or disk-backed storage is a different system. | In-memory only for PoC. Snapshot export could be added later as a serialization feature, not a persistence engine. |
| General-purpose graph algorithms (PageRank, community detection, shortest path) | Memgraph MAGE provides 30+ graph algorithms. Neo4j GDS has a full algorithm library. | Krabnet is not a graph analytics platform. Its purpose is pre-materialized traversal for context delivery, not general graph computation. Each algorithm would need its own incremental maintenance strategy. | Frame patterns cover the traversal patterns needed. If a specific algorithm is needed, it can be implemented as a custom frame pattern, but a generic algorithm library is out of scope. |
| Kafka / Pulsar / external stream connectors | Memgraph has native Kafka/Pulsar connectors. Flink is built on stream connectors. | External connectors add dependency surface and protocol complexity. The ring buffer is the ingestion interface — callers push events into it. How they get those events (Kafka, HTTP, file) is their concern. | Ring buffer API is the integration point. A thin Kafka-to-ring-buffer adapter could exist outside the crate, but it is not part of the runtime. |
| Multi-threaded event processing | Modern systems parallelize event processing. Timely Dataflow uses multiple workers per machine. | Multi-threaded graph mutation with lock-free semantics and correct differential math is extremely hard to get right. The PoC must prove correctness first. Concurrency bugs in delta computation are silent and catastrophic. | Single-threaded processing with correct atomic ordering for future multi-producer ingestion. The atomics are designed right so the path to multi-threaded is open, but not in PoC. |
| Async background compaction | Production systems compact in background threads to avoid pausing processing. | Async compaction requires careful synchronization with the MVCC timeline. Readers must not see partially compacted state. The interface is isolated for future async, but the PoC does synchronous compaction. | Synchronous compaction with an isolated interface. When a compaction point is reached, compact inline. The interface boundary means swapping to async later is a refactor, not a redesign. |
| REST / gRPC API layer | Databases expose network APIs. Users expect HTTP endpoints or RPC interfaces. | Krabnet is a library crate, not a server. Adding a network layer changes the deployment model and introduces serialization, connection management, and API versioning concerns. | Rust API only. A server binary that wraps the crate could be built separately, but the crate itself is embeddable, not network-accessible. |

## Feature Dependencies

```
[Ring Buffer Ingestion]
    └──requires──> [Monotonic Epoch Sequencer]
                       └──feeds──> [Differential MVCC Engine]
                                       └──requires──> [+1/-1 Delta Semantics]
                                       └──requires──> [Compaction]
                                       └──requires──> [Temporal Snapshots]

[Property Graph + Topological Indexing]
    └──required-by──> [Frame Materialization]
                          └──requires──> [Differential MVCC Engine]
                          └──requires──> [Signal-to-Frame Routing (Inverted Index)]

[Frame Materialization]
    └──required-by──> [Adaptive Tiering (Hot/Warm/Cold)]
    └──required-by──> [Two-Tier Interpretation]
    └──required-by──> [Embryonic Frame Discovery]

[Signal-to-Frame Routing]
    └──requires──> [Property Graph + Topological Indexing]
    └──requires──> [Frame Materialization]

[Embryonic Frame Discovery]
    └──requires──> [Frame Materialization]
    └──requires──> [Ring Buffer Ingestion] (watches mutation stream)
    └──requires──> [Signal-to-Frame Routing] (promoted frames need routing)

[String Interning]
    └──enhances──> [Property Graph] (integer IDs on hot path)
    └──enhances──> [Zero-Allocation Hot Path]

[Zero-Allocation Hot Path]
    └──constrains──> [All components] (no heap alloc after init)
```

### Dependency Notes

- **Ring Buffer requires Monotonic Epoch Sequencer:** Events must be ordered before they enter the differential engine. The sequencer assigns monotonic epochs that become the time dimension in (data, time, diff) triples.
- **Differential MVCC requires Property Graph:** The delta semantics operate on graph mutations — you cannot compute +1/-1 deltas without a graph to mutate.
- **Frame Materialization requires both MVCC and Graph:** Frames are the intersection of graph traversal (needs the graph) and incremental maintenance (needs the delta engine).
- **Signal-to-Frame Routing requires Frames:** You cannot route signals to frames that do not exist. The inverted index is built as frames are created.
- **Embryonic Discovery requires Frames + Ingestion:** Discovery watches the mutation stream (from ingestion) and promotes embryos to frames (needs the frame system).
- **Adaptive Tiering enhances Frame Materialization:** Tiering is an optimization on top of frames — it decides which frames stay hot. Can be deferred without breaking core functionality.
- **Two-Tier Interpretation enhances Frame Materialization:** An optimization that avoids unnecessary structural analysis. Frames work without it (just slower).
- **String Interning enhances hot path:** Not functionally required but critical for the zero-allocation constraint. Must be in place before hot-path benchmarking.

## MVP Definition

### Launch With (v1 — Proof of Concept)

Minimum viable system that proves the physics of differential MVCC for graph traversals.

- [ ] **Ring buffer with monotonic epoch sequencing** — ingestion path for events; without it nothing enters the system
- [ ] **In-memory property graph with topological indexing** — the data structure everything operates on
- [ ] **Differential MVCC engine (+1/-1 deltas, compaction, snapshots)** — the mathematical core; must be correct or everything downstream is wrong
- [ ] **Frame materialization with multi-hop patterns** — the core value proposition; pre-computed traversal results
- [ ] **Signal-to-frame routing via inverted index** — efficient event-to-frame mapping; without it, every event scans all frames
- [ ] **String interning** — required for zero-allocation hot path constraint
- [ ] **Top-level Engine struct** — wires all components into a coherent ingest pipeline

### Add After Validation (v1.x)

Features to add once the core is proven correct and performant.

- [ ] **Two-tier interpretation** — add when benchmarks show structural analysis is a bottleneck on the hot path
- [ ] **Adaptive frame tiering** — add when frame count grows large enough that memory pressure becomes measurable
- [ ] **Embryonic Frame Discovery** — add when the system is stable enough to trust autonomous frame creation; requires confidence in delta correctness

### Future Consideration (v2+)

Features to defer until the PoC has validated the core hypothesis.

- [ ] **Async background compaction** — swap synchronous compaction for background thread when processing throughput demands it
- [ ] **Multi-producer ingestion** — enable multiple threads to push into the ring buffer; atomics are already correct for this
- [ ] **Snapshot export / serialization** — allow persisting system state for restart or inspection
- [ ] **External connector adapters** — thin wrappers (Kafka, etc.) that push into the ring buffer

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Ring buffer + epoch sequencer | HIGH | MEDIUM | P1 |
| Property graph + topological indexing | HIGH | MEDIUM | P1 |
| Differential MVCC engine | HIGH | HIGH | P1 |
| Frame materialization | HIGH | HIGH | P1 |
| Signal-to-frame routing | HIGH | MEDIUM | P1 |
| String interning | MEDIUM | LOW | P1 |
| Engine struct (top-level wiring) | HIGH | LOW | P1 |
| Two-tier interpretation | MEDIUM | LOW | P2 |
| Adaptive frame tiering | MEDIUM | MEDIUM | P2 |
| Embryonic Frame Discovery | HIGH | HIGH | P2 |
| Async compaction | LOW | MEDIUM | P3 |
| Multi-producer ingestion | LOW | LOW | P3 |
| Snapshot export | LOW | MEDIUM | P3 |

**Priority key:**
- P1: Must have for PoC launch — proves the core hypothesis
- P2: Should have — adds value but core works without them
- P3: Nice to have — future consideration after validation

## Competitor Feature Analysis

| Feature | Differential Dataflow | Materialize | Memgraph | MV4PG (Research) | Krabnet |
|---------|----------------------|-------------|----------|-------------------|---------|
| Incremental computation | Yes — core capability | Yes — via DD | Yes — dynamic algorithms | Yes — templated maintenance | Yes — +1/-1 deltas on graph traversals |
| Property graph model | No — relational collections | No — SQL tables | Yes — native property graph | Yes — extends Cypher/GQL | Yes — in-memory with adjacency-on-node |
| Pre-materialized results | Arrangements (indexed views) | Materialized views (SQL) | No — query-time computation | Yes — materialized views on graph patterns | Yes — parked frames with multi-hop patterns |
| Streaming ingestion | Yes — timely dataflow streams | Yes — Kafka sources | Yes — Kafka/Pulsar connectors | No — batch maintenance | Yes — lock-free ring buffer |
| Delta semantics | Yes — (data, time, diff) | Yes — via DD | No — recomputation | No — template-based maintenance | Yes — +1/-1 multiset |
| Compaction | Yes — timestamp coalescing | Yes — via DD | N/A | N/A | Yes — synchronous, interface-isolated |
| Temporal snapshots | Yes — multi-versioned traces | Yes — temporal queries | No | No | Yes — MVCC with epoch-based versioning |
| Signal routing to views | N/A — dataflow graph handles | N/A — SQL layer handles | N/A | No — query-time optimization | Yes — inverted index posting lists |
| Autonomous pattern discovery | No | No | No | No | Yes — Embryonic Frame Discovery |
| Adaptive tiering of results | No | No | No | No | Yes — hot/warm/cold frame tiering |
| Zero-allocation hot path | No — allocates during processing | No — JVM-based components | No — C++ with standard allocation | N/A — database extension | Yes — pre-allocated everything |
| Query language | No — Rust API | Yes — SQL | Yes — Cypher | Yes — GQL/Cypher | No — Rust API (deliberate) |
| Distributed execution | Yes — multi-worker | Yes — clustered | Yes — replication | N/A | No — single-node (deliberate) |

**Key insight:** No existing system combines all of: property graph model + differential delta semantics + pre-materialized traversal results + signal routing + autonomous pattern discovery. Materialize comes closest conceptually (differential dataflow + materialized views) but operates on SQL, not graph traversals. MV4PG is the closest in the graph world but uses template-based maintenance rather than differential semantics, and does not do streaming ingestion.

## Sources

- [Differential Dataflow GitHub](https://github.com/TimelyDataflow/differential-dataflow) — core DD implementation and documentation (HIGH confidence)
- [Differential Dataflow Arrangements](https://timelydataflow.github.io/differential-dataflow/chapter_5/chapter_5.html) — arrangements, compaction, sharing (HIGH confidence)
- [Materialize: Building DD from Scratch](https://materialize.com/blog/differential-from-scratch/) — DD concepts explained (HIGH confidence)
- [Materialize: IVM Replicas](https://materialize.com/blog/ivm-database-replica/) — incremental view maintenance patterns (HIGH confidence)
- [MV4PG: Materialized Views for Property Graphs](https://arxiv.org/abs/2411.18847) — graph-specific materialized views with templated maintenance (MEDIUM confidence — Nov 2024 paper, not yet widely validated)
- [Memgraph Streaming Features](https://memgraph.com/docs/data-streams) — real-time graph processing with Kafka/Pulsar (HIGH confidence)
- [Flink Gelly Streaming](https://github.com/vasia/gelly-streaming) — streaming graph API for Apache Flink (MEDIUM confidence — experimental)
- [VeilGraph: Incremental Graph Stream Processing](https://journalofbigdata.springeropen.com/articles/10.1186/s40537-022-00565-8) — incremental graph processing patterns (MEDIUM confidence)
- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/user-guide/index.html) — ring buffer patterns for high-performance event processing (HIGH confidence)
- [Ferrous Systems: Lock-free Ring Buffer](https://ferrous-systems.com/blog/lock-free-ring-buffer/) — Rust-specific lock-free ring buffer design (HIGH confidence)
- [EPDA: Emergent Pattern Detection Algorithm](https://hal.science/hal-02558083/document) — closest analog to Embryonic Frame Discovery in stream processing (LOW confidence — different domain)
- [Elasticsearch Data Tiers](https://www.elastic.co/docs/manage-data/lifecycle/data-tiers) — hot/warm/cold tiering patterns (HIGH confidence — different domain but established pattern)
- [Incremental Pattern Discovery on Streams, Graphs and Tensors](http://reports-archive.adm.cs.cmu.edu/anon/2007/CMU-CS-07-149.pdf) — academic foundation for pattern discovery in graph streams (MEDIUM confidence — 2007, foundational but older)

---
*Feature research for: Streaming graph runtime with differential MVCC*
*Researched: 2026-02-24*
