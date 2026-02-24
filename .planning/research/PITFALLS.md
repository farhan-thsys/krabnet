# Pitfalls Research

**Domain:** Streaming graph runtime with differential MVCC in Rust (lock-free concurrency + graph data structures + differential dataflow)
**Researched:** 2026-02-24
**Confidence:** HIGH (multiple authoritative sources: Rustonomicon, crossbeam docs, Materialize engineering blog, differential-dataflow issues, Ferrous Systems)

## Critical Pitfalls

### Pitfall 1: Unsound `unsafe impl Send/Sync` on Types Containing `UnsafeCell`

**What goes wrong:**
Implementing `Send` or `Sync` on a struct containing `UnsafeCell` (or raw pointers to shared mutable state) without ensuring the type's internal synchronization is actually thread-safe. This causes undefined behavior: data races, torn reads/writes, and memory corruption that manifests nondeterministically and often only on ARM or under load.

**Why it happens:**
Rust's auto-trait derivation opts out of `Send`/`Sync` for types containing `UnsafeCell` or raw pointers. Developers add `unsafe impl Sync for MyType {}` to "make the compiler happy" without proving the synchronization contract. The ring buffer's slot array and the MVCC version chains both require interior mutability that the compiler cannot verify.

**How to avoid:**
- Document the synchronization invariant in a `// SAFETY:` comment above every `unsafe impl Send` and `unsafe impl Sync`. The comment must state *which* mechanism (atomic CAS, epoch sequencing, exclusive ownership transfer) makes the impl sound.
- Never implement `Sync` on a type that allows `&self` to mutate non-atomic data. If mutation through `&self` is required, the mutation must go through `AtomicU64`/`AtomicBool`/`AtomicPtr` or be protected by an external synchronization protocol that is documented.
- Rule of thumb: if a struct has `UnsafeCell<T>` where `T` is not an atomic type, that struct should almost certainly NOT be `Sync` unless all access goes through a single-writer protocol enforced by the ring buffer's index sequencing.

**Warning signs:**
- `unsafe impl Sync` without a `// SAFETY:` comment
- `UnsafeCell` wrapping a non-atomic type (e.g., `UnsafeCell<Option<Event>>`) in a struct that is shared across threads
- Tests pass on x86 but fail intermittently on CI with different core counts
- Miri reports "data race" errors when running `cargo +nightly miri test`

**Phase to address:**
Ring buffer module (Phase 1). This is the first module that introduces `unsafe` concurrency. Establish the safety documentation pattern here and enforce it for all subsequent modules.

---

### Pitfall 2: Incorrect Atomic Memory Ordering (Acquire/Release Misuse)

**What goes wrong:**
Using `Ordering::Relaxed` where `Acquire/Release` is needed, or using `Acquire/Release` where `SeqCst` is needed for a total order. The most common failure mode: a producer writes event data into a ring buffer slot, then updates the write index with `Relaxed` ordering. A consumer sees the updated index but reads stale/uninitialized slot data because the CPU reordered the data write after the index write. This is invisible on x86 (which has strong memory ordering) but crashes on ARM.

**Why it happens:**
x86 has a Total Store Order that masks most ordering bugs. Developers test on x86, everything passes, and the code ships with `Relaxed` everywhere for "performance." The Rustonomicon warns: "having your program run a bit slower is certainly better than it running incorrectly." Additionally, `SeqCst` is often used as a blanket "safe" default, which obscures the actual synchronization protocol and makes code review harder -- reviewers cannot tell which orderings form paired Acquire/Release relationships.

**How to avoid:**
- **Rule: every atomic store that "publishes" data (makes previously written data visible to other threads) MUST use `Release`. Every atomic load that "consumes" published data MUST use `Acquire`.** This is the fundamental Acquire/Release protocol.
- Use `SeqCst` ONLY when you need a total order across multiple independent atomic variables (rare in Krabnet -- the ring buffer's monotonic epoch sequencer may be the only case).
- For the ring buffer: the write index update is `Release`, the read of that index by the consumer is `Acquire`. The epoch counter increment is `Release`, the snapshot read is `Acquire`.
- For compare-and-swap operations (CAS), use `AcqRel` for the success case and `Acquire` for the failure case.
- Comment every atomic operation with its pairing partner: `// Release: pairs with Acquire in consumer_read()`.

**Warning signs:**
- Any `Ordering::Relaxed` on a variable that gates access to non-atomic data
- `SeqCst` used everywhere without documented rationale (indicates developer didn't analyze ordering requirements)
- Tests pass on x86 but fail on ARM CI or under `loom` model checking
- Atomic operations without comments explaining the synchronization pair

**Phase to address:**
Ring buffer module (Phase 1) and MVCC engine (Phase 3). These modules have the highest density of atomics. The ordering protocol established in Phase 1 must be validated with `loom` tests before proceeding.

---

### Pitfall 3: Ring Buffer ABA Problem and Slot Reuse Races

**What goes wrong:**
A thread reads the ring buffer head index as `i`, gets preempted, another thread(s) enqueue and dequeue enough entries to wrap the buffer so the head is `i` again, then the first thread's CAS succeeds on stale data. The thread believes nothing changed, but the slot now contains completely different data. In Krabnet's context, this could cause an event to be processed twice or a different event to be silently dropped.

**Why it happens:**
Ring buffers reuse slots by design (the write index wraps modulo capacity). With power-of-2 capacity and a raw index comparison, the ABA condition is structurally possible whenever the buffer wraps. This is especially dangerous during bursts where the buffer is near full and wrapping is frequent.

**How to avoid:**
- **Use monotonic 64-bit counters that never wrap in practice** (a `u64` counter incrementing at 1 GHz takes ~584 years to overflow). The actual slot index is `counter % capacity`, but the CAS operates on the full 64-bit counter value, making ABA impossible because the counter is always strictly increasing.
- Pre-condition: buffer capacity MUST be a power of 2 so that `counter & (capacity - 1)` gives the slot index without expensive modulo.
- For multi-producer scenarios (future): use a reserve-commit protocol where the producer (1) atomically increments a "reserved" counter via CAS, (2) writes data to the slot, (3) atomically updates a "committed" counter. Consumers only read up to the committed counter.
- Never compare raw slot indices -- always compare full monotonic counter values.

**Warning signs:**
- CAS comparisons using `index % capacity` instead of the full monotonic counter
- Buffer capacity that is not a power of 2
- Wrap-around arithmetic using `wrapping_add` on the slot index rather than on a 64-bit epoch
- Stress tests that occasionally produce duplicated or missing events under high contention

**Phase to address:**
Ring buffer module (Phase 1). The monotonic counter design must be baked into the initial implementation. Retrofitting ABA protection is a rewrite.

---

### Pitfall 4: Differential Math Edge Cases (Negative Deltas, Zero-Sum Annihilation, Compaction Errors)

**What goes wrong:**
Three distinct failure modes in the differential MVCC engine:

1. **Negative multiplicity below zero:** A retraction (-1) arrives for a tuple that was never asserted (+1), producing a multiplicity of -1. If the system treats multiplicity as unsigned or does not check for negative results after compaction, it silently corrupts the materialized view.

2. **Premature annihilation:** Compaction sums `+1 + (-1) = 0` and removes the tuple, but the retraction logically belonged to a different version than the assertion. If compaction crosses version boundaries incorrectly, it destroys tuples that should still be visible at intermediate versions.

3. **Compaction frontier advancement error:** The compaction frontier advances past versions that still have outstanding reads/snapshots, destroying data that active queries still need. This is the bug documented in [differential-dataflow Issue #242](https://github.com/TimelyDataflow/differential-dataflow/issues/242) where trace wrappers inaccurately summarized frontiers.

**Why it happens:**
Differential dataflow's correctness depends on precise frontier tracking. The frontier defines "which versions are still distinguishable." Compaction may only merge tuples at versions that are *indistinguishable* given the current frontier. Developers often implement compaction as "sum all deltas for the same key" without respecting version ordering, which silently violates the MVCC contract.

**How to avoid:**
- **Use signed integers (`i64`) for multiplicities.** Assert `multiplicity >= 0` after compaction and treat violations as hard errors (panic in debug, return Error in release).
- **Compaction must respect the frontier:** only merge deltas at versions `v1` and `v2` if `v1` and `v2` are indistinguishable according to the current compaction frontier. Two versions are indistinguishable if no outstanding capability can produce a future version that distinguishes them.
- **Test the "retract before assert" case explicitly:** ingest a `-1` delta for a key that has no prior `+1`. The system must either reject it or track the negative multiplicity and resolve it when the matching `+1` arrives.
- **Test cross-version compaction:** create versions [1, 2, 3], assert at v1, retract at v3, compact at frontier v2. The tuple must still be visible at v2.

**Warning signs:**
- Multiplicity stored as `u64` instead of `i64`
- Compaction function that does not take a frontier parameter
- Tests that only test the happy path (`+1` then `-1`) but never test out-of-order or orphaned retractions
- Snapshot reads returning empty results for tuples that should be visible at intermediate versions

**Phase to address:**
MVCC engine (Phase 3). This is the mathematical heart of Krabnet. The compaction frontier logic must be proven correct with exhaustive property-based tests before the frame materialization layer (Phase 4) is built on top of it.

---

### Pitfall 5: Graph Adjacency Inconsistency on Node/Edge Removal

**What goes wrong:**
Removing a node leaves dangling entries in other nodes' adjacency lists (incoming/outgoing edge references). Removing an edge updates the source node's outgoing list but not the target node's incoming list (or vice versa). Subsequent traversals follow dangling references, producing incorrect paths or panicking on invalid IDs.

**Why it happens:**
Krabnet stores edges directly on Node structs (outgoing + incoming adjacency lists). This means every edge has TWO representations: one in the source node's outgoing list and one in the target node's incoming list. Deletion must update both atomically. With pre-allocated Vec storage and integer IDs (not pointers), a "dangling reference" is an ID pointing to a slot that has been reused for a different node -- a logical ABA problem at the graph level.

**How to avoid:**
- **Edge removal must be a two-phase operation:** (1) remove from source's outgoing list, (2) remove from target's incoming list. If the operation is interrupted between steps, the graph is inconsistent.
- **Node removal must first remove all incident edges** (both outgoing and incoming), then mark the node slot as free. Never reuse a node ID slot until all references have been cleared.
- **Generation counters on node/edge slots:** each slot has a generation counter that increments on reuse. References store `(slot_index, generation)`. A lookup checks that the generation matches; if not, the reference is stale. This is the graph-level equivalent of ABA protection.
- **Validate adjacency symmetry in debug builds:** after every mutation, assert that for every edge `(u, v)` in `u.outgoing`, `v.incoming` also contains the corresponding entry.

**Warning signs:**
- Node removal function that does not iterate `incoming` edges from other nodes
- Edge ID reuse without generation counters
- Traversals that silently skip missing nodes instead of treating them as errors
- Tests that add and query but never remove and re-query

**Phase to address:**
Property graph module (Phase 2). The adjacency consistency invariant must be established here with debug assertions. Frame materialization (Phase 4) depends entirely on traversal correctness.

---

### Pitfall 6: Frame Materialization DFS Traversal Errors

**What goes wrong:**
The cold-start DFS traversal for frame materialization produces incorrect paths. Common failure modes: (1) cycles in the graph cause infinite traversal because visited tracking uses the wrong scope (global vs per-path), (2) multi-hop patterns miss valid paths because the hop filter is applied too early or too late, (3) directed vs undirected edge traversal is confused when following "incoming" edges in reverse.

**Why it happens:**
Krabnet's frame system performs multi-hop DFS from an anchor node following a hop pattern. DFS cycle detection requires tracking nodes on the *current path* (recursion stack), not just globally visited nodes. A globally-visited set prevents finding multiple paths through the same node, which is wrong for path enumeration. But a purely per-path visited set can cause exponential blowup in dense graphs. The correct approach depends on the frame's semantics.

**How to avoid:**
- **Distinguish "path enumeration" from "reachability":** if the frame needs all distinct paths, use per-path visited tracking. If it needs reachable nodes, use global visited. Document which semantics each frame type uses.
- **For directed graphs, explicitly track edge direction in the hop pattern:** "hop 1: follow outgoing edges of type X, hop 2: follow incoming edges of type Y." Never default to "follow any edge."
- **Cap traversal depth and path count** with configurable limits. A frame pattern that matches 10,000 paths in a dense graph will blow the pre-allocated buffer. Detect this and degrade gracefully (truncate or error) rather than silently producing incomplete results.
- **Test with diamond graphs and cycles:** a diamond graph (A->B, A->C, B->D, C->D) with anchor A and depth 2 must find exactly 2 paths to D. A graph with a cycle (A->B->C->A) must terminate.

**Warning signs:**
- DFS implementation using `HashSet<NodeId>` as a global visited set for path collection
- No maximum depth or path count limit
- Tests only on tree-shaped graphs (no diamonds, no cycles)
- Frame materialization returns different result counts between cold-start and incremental update for the same graph state

**Phase to address:**
Frame materialization module (Phase 4). Must have comprehensive graph topology test fixtures (trees, diamonds, cycles, disconnected components) before implementing.

---

### Pitfall 7: Zero-Allocation Hot Path vs Rust Ownership -- The Pre-allocation Trap

**What goes wrong:**
The constraint "zero heap allocation after initialization" conflicts with Rust's ownership model in three ways: (1) pre-allocated `Vec`s used as object pools require unsafe index-based access that bypasses borrow checking, (2) returning references to pool-allocated objects requires lifetime gymnastics that either forces `unsafe` or makes the API unusable, (3) temporary scratch buffers for DFS traversal or delta accumulation must be pre-allocated and reused, but the borrow checker prevents holding a mutable reference to the scratch buffer while also reading the graph.

**Why it happens:**
Rust's borrow checker enforces "readers OR writer" at compile time. A pre-allocated pool that hands out `&mut T` references cannot also be iterated or queried. The typical workaround is `unsafe` index-based access, which puts the correctness burden on the developer. Additionally, `Vec::push` may reallocate even if `capacity > len`, since Rust does not guarantee that `push` on a `Vec` with spare capacity is allocation-free (it is in practice, but it is not contractual).

**How to avoid:**
- **Use index-based APIs (arena pattern) throughout.** Functions return `NodeId` / `EdgeId` / `FrameId` (which are just `u64` indices), never `&Node` / `&Edge`. All access goes through `graph.node(id)` which does bounds-checked indexing. This eliminates borrow conflicts entirely.
- **Pre-allocate all Vecs at startup with `Vec::with_capacity(N)` and NEVER exceed N.** Use `debug_assert!(vec.len() < vec.capacity())` before every push to catch capacity violations early. In release, if the assertion would fire, drop the oldest entry or return an error -- never silently allocate.
- **Scratch buffers: use a `ScratchPool` struct** that owns all reusable buffers (DFS stack, delta accumulator, path collector). Borrow the specific buffer needed for each operation. The pool pattern avoids the "borrow self and self.scratch simultaneously" problem.
- **Measure: use `#[global_allocator]` with a counting allocator in integration tests** to assert that zero allocations occur after initialization.

**Warning signs:**
- `Vec::push` without a preceding capacity check
- Functions returning `&T` from a mutable pool (lifetime issues)
- `unsafe` blocks for "the borrow checker doesn't understand this" without an index-based alternative
- No allocation-counting test in the test suite

**Phase to address:**
Every phase, starting from Phase 1 (ring buffer). The arena/index pattern and scratch pool pattern must be established in Phase 1 and used consistently. A counting allocator test should be added in Phase 1 and run in CI.

---

### Pitfall 8: Bitvec Completion Tracking Off-by-One in Embryonic Frame Discovery

**What goes wrong:**
The embryonic frame discovery system uses bitvec bit-vectors to track per-hop completion of forming patterns. Off-by-one errors in bit indexing cause: (1) a frame is promoted before all hops are satisfied (false positive -- triggers with incomplete data), (2) a frame never promotes because the final bit is never checked (false negative -- useful patterns are silently dropped), (3) bit index calculation for multi-hop patterns maps hop N to bit position N-1 but the check uses `bits.all()` which includes an uninitialized trailing bit.

**Why it happens:**
Hop patterns are 1-indexed conceptually ("hop 1, hop 2, hop 3") but bitvec is 0-indexed. A 3-hop pattern needs a 3-bit vector with bits at positions 0, 1, 2. Developers naturally write `bits.set(hop_number, true)` instead of `bits.set(hop_number - 1, true)`. The bitvec crate's strong type-system prevents out-of-bounds access, but an off-by-one that stays in-bounds produces silently wrong results.

**How to avoid:**
- **Define a `HopIndex` newtype that is always 0-based internally.** The pattern definition uses 1-based hop numbers for readability, but the conversion to `HopIndex` happens exactly once, in a single function, with an explicit `-1`.
- **Assert `bitvec.len() == pattern.hop_count()`** when creating the completion tracker. Any mismatch is a bug.
- **Test the boundary:** a 1-hop pattern must promote when exactly 1 bit is set. A 3-hop pattern must NOT promote when only 2 of 3 bits are set. Test with `all()` and `count_ones()` both.
- **Use bitvec's `BitSlice::all()` rather than manual iteration** to check completion. Manual `for i in 0..len` loops are where off-by-one errors hide.

**Warning signs:**
- Raw integer arithmetic on bit indices without a newtype
- Bitvec allocated with `len + 1` or `len - 1` (fudge factors are a code smell)
- Tests that only check "all hops complete" but never check "N-1 hops complete should NOT promote"
- Pattern hop numbering in docs is 1-based but code is 0-based without an explicit conversion layer

**Phase to address:**
Embryonic frame discovery module (Phase 6). The `HopIndex` newtype should be defined in the types module (Phase 1) and used from the start.

---

### Pitfall 9: Epoch-Based Compaction Frontier Desynchronization

**What goes wrong:**
The compaction frontier advances past the oldest active snapshot's epoch, destroying version data that an in-flight read still needs. The read then returns incorrect results (missing tuples that should be visible at its snapshot epoch) or panics on missing version data. This is the Krabnet-specific analog of the [differential-dataflow Issue #242](https://github.com/TimelyDataflow/differential-dataflow/issues/242) compaction/frontier composition bug.

**Why it happens:**
Krabnet uses a monotonic epoch from the ring buffer as the version clock for MVCC. Compaction runs synchronously and advances the "since" frontier. If the frontier advancement does not account for all active snapshots (reads in progress), it over-compacts. The typical bug: the compaction function reads the current epoch and compacts everything before it, but a concurrent snapshot was taken at epoch E-2 and is still being read. The compaction destroys versions at E-2.

**How to avoid:**
- **Maintain an explicit "active snapshots" registry.** Every snapshot read registers its epoch in an atomic min-heap or sorted list. The compaction frontier is `min(active_snapshot_epochs) - 1`. Compaction MUST NOT advance past this.
- **For the single-threaded PoC:** even without concurrent readers, the discipline of tracking active snapshots is essential because the engine's `ingest -> route -> interpret` pipeline may hold intermediate snapshot references across multiple operations in a single tick.
- **Test: take a snapshot at epoch 5, ingest events up to epoch 10, run compaction, read the snapshot at epoch 5.** The read must return the same results as before compaction.
- **Defensive: compaction should return the actual frontier it advanced to**, not assume it advanced to the requested target. Callers assert the returned frontier matches expectations.

**Warning signs:**
- Compaction function that takes no "minimum safe epoch" parameter
- No snapshot registration mechanism
- Tests that never interleave snapshot reads with compaction
- Compaction always advances to `current_epoch - 1` without checking active reads

**Phase to address:**
MVCC engine (Phase 3). The active snapshot registry must be designed alongside the version chain, not bolted on after. Compaction correctness tests must be written before the frame materialization layer depends on snapshots.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| `SeqCst` everywhere instead of analyzed Acquire/Release | Faster development, fewer ordering bugs | Performance overhead on ARM; obscures actual synchronization protocol making future optimization risky | PoC only. Must document "upgrade to Acquire/Release" for each atomic before multi-threaded mode. |
| `unsafe` index access instead of arena newtype IDs | Faster prototyping, fewer types | Every callsite is a potential out-of-bounds UB; refactoring requires touching every access | Never. The newtype costs zero runtime and prevents a class of bugs. |
| Re-traverse for frame maintenance (not incremental) | Correctness is easier to verify | O(hops * edges) per event instead of O(affected paths) | PoC explicitly. Isolated behind trait interface for future incremental impl. |
| `clone()` on delta collections during compaction | Avoids borrow checker fights with in-place mutation | Heap allocation on hot path; violates zero-alloc constraint | Only during initial MVCC development. Must be replaced with in-place merge before benchmarking. |
| Synchronous compaction blocking ingestion | Simpler single-threaded model | Latency spikes on compaction; cannot scale to real-time workloads | PoC explicitly. Interface isolated for async migration. |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| False sharing on ring buffer head/tail indices | Throughput plateaus with 2+ threads; perf counter shows high cache-line invalidation | Use `crossbeam_utils::CachePadded` on head, tail, and epoch atomics. Cache lines are 128 bytes on x86-64/aarch64 | Immediately with multi-producer; measurable even with single-producer if head/tail are on same cache line |
| Linear scan of adjacency list for edge lookup | Graph mutations slow down as node degree increases | Maintain sorted adjacency lists or secondary edge-type index. Pre-allocate adjacency capacity based on expected max degree | Nodes with >100 edges (hub nodes in agent context graphs) |
| Full DFS re-traversal on every mutation event | Frame maintenance dominates CPU time; ingestion throughput drops with more parked frames | Inverted index (signal-to-frame routing) limits re-traversal to affected frames only. Still O(hops * edges) per affected frame, but only affected frames re-traverse | >50 active parked frames, or frames with >3 hops on dense subgraphs |
| Bitvec allocation per embryonic candidate | Heap allocation per candidate creation violates zero-alloc; GC pressure under pattern discovery bursts | Pre-allocate a pool of bitvec trackers at startup. Recycle on candidate completion/eviction | >1000 active embryonic candidates |
| String comparison in hot-path type/property checks | Type checking becomes bottleneck; shows as high instruction count in flamegraph | String interning at ingestion boundary. All hot-path comparisons use integer type IDs and property key IDs (u32) | Any workload; strings on hot path is always wrong |

## "Looks Done But Isn't" Checklist

- [ ] **Ring buffer:** Appears to work in single-threaded tests but uses `Relaxed` ordering -- will fail under multi-producer. Verify: run under `loom` or Miri with `--cfg loom` / `cargo +nightly miri test`
- [ ] **Graph removal:** Add/remove tests pass but only test removing leaf nodes -- verify: remove a node with both incoming and outgoing edges, then traverse from a neighbor. All adjacency lists must be consistent.
- [ ] **Differential compaction:** Compaction "works" (reduces tuple count) but was only tested with monotonically increasing versions -- verify: test with interleaved versions, concurrent snapshots, and out-of-order retractions.
- [ ] **Frame cold start:** DFS produces correct paths on test tree -- verify: test on diamond graph, cycle graph, disconnected graph, and graph where anchor node has zero outgoing edges.
- [ ] **Embryonic promotion:** Candidates promote when threshold met -- verify: test that candidates do NOT promote when threshold is one bit short, and that candidates are evicted when their tracked nodes are removed.
- [ ] **Zero allocation:** No `Vec::push` panics in tests -- verify: use a counting global allocator and assert zero allocations in the hot-path benchmark.
- [ ] **String interning:** Property lookups work -- verify: ensure two different interning calls with the same string return the same integer ID, and that IDs from one interner are never used with a different interner instance.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Incorrect Send/Sync | HIGH | Audit every `unsafe impl`, add Miri to CI, potentially redesign type to avoid sharing UnsafeCell |
| Wrong atomic ordering | HIGH | Re-analyze every atomic pair, add loom tests, potentially rewrite ring buffer protocol |
| Ring buffer ABA | HIGH | Requires redesigning index scheme to monotonic counters -- touches every producer/consumer function |
| Differential math errors | MEDIUM | Fix compaction logic, add property tests, revalidate all downstream frame materializations |
| Graph adjacency inconsistency | MEDIUM | Add generation counters to slots, fix removal to be two-phase, add debug assertion sweep |
| DFS traversal errors | LOW | Fix visited tracking, add topology test fixtures, re-materialize affected frames |
| Zero-alloc violation | MEDIUM | Profile with counting allocator, replace allocating code with pre-allocated pools, may require API redesign |
| Bitvec off-by-one | LOW | Fix index conversion, add boundary tests, re-evaluate all embryonic candidates |
| Compaction frontier desync | HIGH | Add snapshot registry, rewrite compaction to respect frontier, re-validate all MVCC tests |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Unsound Send/Sync | Phase 1 (ring buffer) | `cargo +nightly miri test` passes; every `unsafe impl` has `// SAFETY:` doc |
| Atomic ordering errors | Phase 1 (ring buffer) | `loom` test suite covers all producer/consumer interleavings |
| Ring buffer ABA | Phase 1 (ring buffer) | Monotonic 64-bit counters used; stress test with 10M enqueue/dequeue cycles shows zero duplicates/drops |
| Differential math errors | Phase 3 (MVCC engine) | Property-based tests with `proptest`: random assertion/retraction sequences produce non-negative multiplicities after compaction |
| Graph adjacency inconsistency | Phase 2 (property graph) | Debug-mode adjacency symmetry assertion after every mutation; removal test suite covers all topologies |
| DFS traversal errors | Phase 4 (frame materialization) | Test fixtures for tree, diamond, cycle, disconnected, and zero-edge-anchor topologies |
| Zero-alloc violations | Phase 1+ (all phases) | Counting allocator integration test asserts zero allocations on hot path; runs in CI |
| Bitvec off-by-one | Phase 6 (embryonic discovery) | Boundary tests for 1-hop, N-1 of N hops, and exact-N hops completion |
| Compaction frontier desync | Phase 3 (MVCC engine) | Snapshot-interleaved-with-compaction test; active snapshot registry design reviewed before coding |

## Sources

- [The Rustonomicon: Send and Sync](https://doc.rust-lang.org/nomicon/send-and-sync.html) -- HIGH confidence
- [Mara Bos: Rust Atomics and Locks, Chapter 3: Memory Ordering](https://mara.nl/atomics/memory-ordering.html) -- HIGH confidence
- [SeqCst as default considered harmful (Nomicon Issue #166)](https://github.com/rust-lang/nomicon/issues/166) -- HIGH confidence
- [nyanpasu64: An Unsafe Tour of Rust's Send and Sync](https://nyanpasu64.gitlab.io/blog/an-unsafe-tour-of-rust-s-send-and-sync/) -- MEDIUM confidence
- [kmdreko: A Simple Lock-Free Ring Buffer](https://kmdreko.github.io/posts/20191003/a-simple-lock-free-ring-buffer/) -- MEDIUM confidence
- [Lock-Free Rust: How to Build a Rollercoaster While It's on Fire](https://yeet.cx/blog/lock-free-rust) -- MEDIUM confidence
- [Ferrous Systems: Lock-free ring-buffer with contiguous reservations](https://ferrous-systems.com/blog/lock-free-ring-buffer/) -- HIGH confidence
- [differential-dataflow Issue #242: Compaction and trace wrappers do not compose](https://github.com/TimelyDataflow/differential-dataflow/issues/242) -- HIGH confidence
- [Materialize: Building Differential Dataflow from Scratch](https://materialize.com/blog/differential-from-scratch/) -- HIGH confidence
- [Materialize: Managing Memory with Differential Dataflow](https://materialize.com/blog/managing-memory-with-differential-dataflow/) -- HIGH confidence
- [crossbeam-utils CachePadded documentation](https://docs.rs/crossbeam-utils/latest/crossbeam_utils/struct.CachePadded.html) -- HIGH confidence
- [Loom: Concurrency permutation testing tool](https://docs.rs/loom/latest/loom/) -- HIGH confidence
- [spacejam/tla-rust: Writing correct lock-free systems in Rust](https://github.com/spacejam/tla-rust) -- MEDIUM confidence
- [Graphs and Arena Allocation in Rust](https://aminb.gitbooks.io/rust-for-c/content/graphs/index.html) -- MEDIUM confidence
- [matklad: Fast and Simple Rust Interner](https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html) -- MEDIUM confidence
- [Cargo cyclic dep graph detection bug (PR #9075)](https://github.com/rust-lang/cargo/pull/9075) -- MEDIUM confidence (illustrates DFS visited-tracking bugs in production Rust code)
- [bitvec crate documentation](https://docs.rs/bitvec/latest/bitvec/) -- HIGH confidence

---
*Pitfalls research for: Streaming graph runtime with differential MVCC in Rust*
*Researched: 2026-02-24*
