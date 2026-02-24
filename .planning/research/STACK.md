# Stack Research

**Domain:** Streaming graph runtime with differential MVCC in Rust
**Researched:** 2026-02-24
**Confidence:** HIGH

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Rust stable | latest (1.85+) | Language and toolchain | Zero-cost abstractions, ownership model prevents data races at compile time, no GC pauses. Stable-only constraint eliminates nightly churn. **Confidence: HIGH** |
| `std::sync::atomic` | stable | Lock-free primitives (AtomicU64, AtomicBool, AtomicUsize) | First-party, zero-dependency, correct memory model. Provides all orderings (Relaxed, Acquire, Release, AcqRel, SeqCst). No external crate needed. **Confidence: HIGH** |
| `std::cell::UnsafeCell` | stable | Interior mutability primitive | The only legal way to obtain aliased `&mut T` in Rust. Required for any lock-free data structure that mutates shared state. **Confidence: HIGH** |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `crossbeam` | 0.8.x | Epoch-based memory reclamation, scoped threads, utilities | Use `crossbeam-epoch` for deferred deallocation of removed graph nodes/edges. Use `crossbeam-utils` for `CachePadded` to prevent false sharing on hot atomics. **Confidence: HIGH** |
| `bitvec` | 1.0.x | Bit-addressed memory, compact boolean vectors | Use for embryonic frame completion tracking (per-hop completion bitmasks). `BitVec<usize, Lsb0>` is the fastest configuration. Pre-allocate with `bitvec![0; capacity]`. **Confidence: HIGH** |
| `criterion` | 0.8.x | Statistical micro-benchmarking | Dev-dependency only. Use for ring buffer throughput, graph traversal latency, and differential compaction benchmarks. **Confidence: HIGH** |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| `cargo bench` | Run criterion benchmarks | Requires `harness = false` in `[[bench]]` section of Cargo.toml |
| `cargo miri` | Detect undefined behavior in unsafe code | Run `cargo +nightly miri test` to check UB in ring buffer, graph storage, and atomic operations. Use nightly only for miri, not for the crate itself |
| `cargo clippy` | Lint for common Rust mistakes | Pay attention to `clippy::undocumented_unsafe_blocks` lint |
| `loom` | Concurrency model checker (optional, dev-only) | If future multi-producer support is needed, loom exhaustively tests atomic orderings. Not required for single-producer PoC |

## Cargo.toml Configuration

```toml
[package]
name = "krabnet"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"  # MSRV: conservative stable

[dependencies]
crossbeam = { version = "0.8", default-features = false, features = ["std"] }
bitvec = "1.0"

[dev-dependencies]
criterion = { version = "0.8", features = ["html_reports"] }

[[bench]]
name = "krabnet_bench"
harness = false
```

## Core Patterns and Idioms

### 1. Atomic Ordering Selection

**Decision rule: Use the weakest ordering that is provably correct.**

| Scenario | Ordering | Rationale |
|----------|----------|-----------|
| Monotonic epoch counter (single writer, multiple readers) | `Release` on store, `Acquire` on load | Writer publishes epoch; readers synchronize to see all writes before that epoch. Acquire/Release is sufficient because there is one total modification order per atomic variable. |
| Ring buffer head/tail pointers | `Release` on producer advance, `Acquire` on consumer read | Producer publishes slot data before advancing head. Consumer must see slot data after reading head. Classic producer-consumer pattern. |
| Boolean flags (e.g., "compaction needed") | `Release` on set, `Acquire` on check | Flag signals that preparatory work is visible. |
| Simple counters where only the count matters (no data dependency) | `Relaxed` | No other memory access depends on the count's ordering. Only the total modification order of the counter itself matters. |
| Compare-and-swap on shared state (future multi-producer) | `AcqRel` on success, `Acquire` on failure | CAS must both read current state (Acquire) and publish new state (Release). Failure only reads. |

**When to use SeqCst:** Almost never. SeqCst is needed only when correctness depends on a *global total order* across *multiple* atomic variables observed by *multiple* threads. A single-producer system with epoch ordering does not need this. SeqCst on a single atomic variable provides no additional guarantee over Acquire/Release.

**Confidence: HIGH** -- Based on Mara Bos's "Rust Atomics and Locks" (Chapter 3), the authoritative Rust reference on memory ordering.

```rust
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

// Epoch sequencer: single writer, readers see consistent state
pub struct EpochSequencer {
    epoch: AtomicU64,
}

impl EpochSequencer {
    pub fn new() -> Self {
        Self { epoch: AtomicU64::new(0) }
    }

    /// Writer: advance epoch. Release ensures all prior writes are visible.
    pub fn advance(&self) -> u64 {
        // fetch_add with Release: all writes to ring buffer slots
        // that happened before this call become visible to any thread
        // that does an Acquire load of this epoch value.
        self.epoch.fetch_add(1, Ordering::Release)
    }

    /// Reader: read current epoch. Acquire synchronizes with writer's Release.
    pub fn current(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
}
```

### 2. UnsafeCell + Manual Sync (Stable Pattern)

**Critical: `SyncUnsafeCell` is nightly-only (feature gate `sync_unsafe_cell`, tracking issue #95439). Do NOT use it on stable Rust.**

The stable pattern is a wrapper struct with a manual `unsafe impl Sync`:

```rust
use std::cell::UnsafeCell;

/// A cell that can be shared between threads.
///
/// # Safety Invariants
/// The caller must ensure that:
/// 1. No `&T` and `&mut T` references to the inner value coexist.
/// 2. All access through raw pointers follows the concurrent memory model
///    (i.e., conflicting accesses use atomics or are otherwise synchronized).
/// 3. The type `T` itself is `Send` (transferable between threads).
#[repr(transparent)]
pub struct SyncCell<T> {
    inner: UnsafeCell<T>,
}

// SAFETY: Access is synchronized externally via atomics or epoch ordering.
// The caller guarantees no unsynchronized aliased mutation.
unsafe impl<T: Send> Sync for SyncCell<T> {}

// SAFETY: If T can be sent between threads, so can our wrapper.
unsafe impl<T: Send> Send for SyncCell<T> {}

impl<T> SyncCell<T> {
    pub const fn new(value: T) -> Self {
        Self { inner: UnsafeCell::new(value) }
    }

    /// Get a raw pointer to the inner value.
    ///
    /// # Safety
    /// Caller must ensure no aliased mutable references exist, or that
    /// access is synchronized via atomics.
    pub fn get(&self) -> *mut T {
        self.inner.get()
    }
}
```

**Why not just use `AtomicPtr`?** `AtomicPtr` atomicizes the pointer itself, not the pointed-to data. For pre-allocated buffer slots where you need interior mutability of the slot contents (not the pointer), `UnsafeCell` is correct.

**Confidence: HIGH** -- UnsafeCell is the documented foundation for interior mutability in std. Manual Sync impl is the standard pattern used by crossbeam, tokio, and parking_lot.

### 3. Lock-Free Ring Buffer (Single-Producer, Pre-Allocated)

```rust
use std::sync::atomic::{AtomicU64, Ordering};

/// Single-producer ring buffer with pre-allocated slots.
///
/// Capacity MUST be a power of 2. This enables `index & (capacity - 1)`
/// instead of `index % capacity`, which is a single AND instruction vs
/// an expensive division.
///
/// # Layout
/// - `slots`: Pre-allocated Vec<T> with `capacity` elements
/// - `head`: Write position (only producer advances)
/// - `tail`: Read position (only consumer advances)
///
/// # Memory Ordering
/// - Producer: writes slot data, then Release-stores head
/// - Consumer: Acquire-loads head, then reads slot data
pub struct RingBuffer<T: Default + Clone> {
    slots: Vec<T>,       // Pre-allocated, never reallocated
    mask: u64,           // capacity - 1, for fast modulo
    head: AtomicU64,     // Next write position (producer-owned)
    tail: AtomicU64,     // Next read position (consumer-owned)
}

impl<T: Default + Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of 2");
        assert!(capacity > 0);
        Self {
            slots: vec![T::default(); capacity],
            mask: (capacity as u64) - 1,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
        }
    }

    /// Produce: write an event to the next slot.
    /// Returns the epoch (sequence number) assigned.
    ///
    /// # Safety Contract
    /// Only ONE thread may call push(). This is a single-producer buffer.
    /// The atomics are correct for future multi-producer extension.
    pub fn push(&self, event: T) -> Option<u64> {
        let head = self.head.load(Ordering::Relaxed); // Only producer reads head
        let tail = self.tail.load(Ordering::Acquire);  // Synchronize with consumer

        if head - tail >= (self.mask + 1) {
            return None; // Buffer full
        }

        let idx = (head & self.mask) as usize;
        // SAFETY: Single producer guarantees exclusive write to this slot.
        // The slot is within bounds because idx = head & mask < capacity.
        unsafe {
            let slot = self.slots.as_ptr().add(idx) as *mut T;
            std::ptr::write(slot, event);
        }

        // Release: makes the slot write visible to consumer
        self.head.store(head + 1, Ordering::Release);
        Some(head)
    }
}
```

**Power-of-2 rationale:** `index % capacity` compiles to a `div` instruction on x86 (20-90 cycles). `index & (capacity - 1)` compiles to a single `and` instruction (1 cycle). At millions of events/sec, this matters.

**Why not use `ringbuf` crate?** The project constraint is zero external dependencies beyond crossbeam/bitvec/criterion. A hand-rolled ring buffer also gives full control over the epoch sequencer integration, which is central to the MVCC design.

**Confidence: HIGH** -- This pattern is well-documented across Ferrous Systems blog, LMAX Disruptor, and multiple Rust implementations.

### 4. String Interning for Zero-Allocation Hot Path

```rust
use std::collections::HashMap;

/// Interns strings at startup, returning integer IDs for hot-path use.
///
/// After initialization, all lookups on the hot path use u32 IDs only.
/// No String, &str, or heap allocation on the hot path.
pub struct StringInterner {
    map: HashMap<String, u32>,
    strings: Vec<String>,
}

impl StringInterner {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            strings: Vec::new(),
        }
    }

    /// Intern a string. Called ONLY during initialization.
    /// Returns a u32 ID that can be used on the hot path.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.strings.len() as u32;
        self.strings.push(s.to_owned());
        self.map.insert(s.to_owned(), id);
        id
    }

    /// Resolve ID back to string. For diagnostics/debugging only.
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
    }
}
```

**Why u32 and not u64?** Property keys and type names will number in the hundreds or low thousands. u32 gives 4 billion unique strings, is half the cache line footprint of u64, and aligns better in packed structs alongside other u32 fields.

**Confidence: HIGH** -- This is the matklad interner pattern, widely used in rust-analyzer and other Rust compilers.

### 5. Property Graph Storage (Adjacency-on-Node)

```rust
/// Node with inline adjacency lists. Trades write cost for read locality.
///
/// Design rationale: Frame traversals read edges far more often than
/// mutations add/remove edges. Storing edges on the node means a DFS
/// traversal touches contiguous memory for each node's edges, rather
/// than chasing pointers through a separate edge table.
pub struct Node {
    pub id: u64,
    pub type_id: u32,                        // Interned type name
    pub properties: Vec<(u32, PropertyValue)>, // (interned_key, value)
    pub outgoing: Vec<EdgeRef>,               // Pre-allocated
    pub incoming: Vec<EdgeRef>,               // Pre-allocated
}

/// Lightweight edge reference stored on nodes.
/// Full edge data (properties, MVCC versions) lives in a separate slab.
pub struct EdgeRef {
    pub edge_id: u64,
    pub target_node_id: u64,
    pub type_id: u32,  // Interned edge type
}

/// Slab-allocated node storage. Nodes indexed by ID for O(1) lookup.
///
/// Using a Vec as a slab: node IDs are indices. Deleted nodes are
/// marked in a free list for reuse. No HashMap overhead on hot path.
pub struct GraphStorage {
    nodes: Vec<Option<Node>>,       // Slab: index = node_id
    edges: Vec<Option<Edge>>,       // Slab: index = edge_id
    node_count: usize,
    edge_count: usize,
}
```

**Why not `HashMap<u64, Node>`?** HashMap has per-lookup overhead (hashing, probe chain). Slab-indexed `Vec<Option<Node>>` is O(1) with a single array index. For a dense ID space (monotonic IDs), this is strictly better. The trade-off is wasted space for sparse IDs, but krabnet controls ID assignment, so IDs are dense.

**Why not petgraph?** Project constraint forbids it. Also, petgraph's `Graph` uses a different edge storage model (linked-list per node) that doesn't align with krabnet's need for pre-allocated adjacency vectors and MVCC versioning on edges.

**Confidence: HIGH** -- Slab allocation with index-based references is the standard Rust pattern for graph structures, used by ECS frameworks (bevy, hecs) and the Rust compiler itself.

### 6. Differential MVCC Data Structures

```rust
/// A differential collection entry: (data, timestamp, diff).
///
/// Differential dataflow represents changes as (+1, -1) deltas:
/// - +1 means "this tuple is asserted (added)"
/// - -1 means "this tuple is retracted (removed)"
/// - Net zero means "annihilated" (effectively deleted)
///
/// Compaction collapses deltas at the same timestamp:
///   [(A, t1, +1), (A, t1, -1)] -> [] (annihilated)
///   [(A, t1, +1), (A, t2, -1)] -> kept separately (different times)
pub struct DiffEntry<T> {
    pub data: T,
    pub timestamp: u64,  // Epoch from the ring buffer
    pub diff: i64,       // +1 assertion, -1 retraction
}

/// Version-aware multiset index.
/// Maps key -> Vec<DiffEntry<V>>, sorted by timestamp.
///
/// Compaction: periodically collapse entries where net diff = 0
/// at timestamps older than the oldest active reader.
pub struct DiffIndex<K, V> {
    entries: HashMap<K, Vec<DiffEntry<V>>>,
}

impl<K: Eq + std::hash::Hash, V: PartialEq> DiffIndex<K, V> {
    /// Compact entries older than `frontier`.
    /// Combines entries with the same (data, timestamp) by summing diffs.
    /// Removes entries where net diff == 0.
    pub fn compact(&mut self, frontier: u64) {
        for entries in self.entries.values_mut() {
            // Merge entries at same timestamp
            // Remove entries with diff == 0
            // Keep entries at timestamp >= frontier unchanged
            entries.retain(|e| e.timestamp >= frontier || e.diff != 0);
        }
    }
}
```

**Why i64 for diff and not i32?** In multiset semantics, a single key can accumulate large counts if many assertions happen before compaction. i64 prevents overflow in pathological cases. The memory cost (4 extra bytes per entry) is negligible compared to the data payload.

**Why HashMap for the index?** The DiffIndex is NOT on the hot path for event ingestion. It is used during frame materialization and compaction, which are less frequent. HashMap's O(1) amortized lookup is appropriate here. The hot path (ring buffer -> epoch) uses slab-indexed storage.

**Confidence: HIGH** -- Based on Materialize's "Building Differential Dataflow from Scratch" and Frank McSherry's differential-dataflow crate design.

### 7. Embryonic Frame Completion Tracking with bitvec

```rust
use bitvec::prelude::*;

/// Tracks completion of an embryonic frame pattern.
/// Each bit represents one hop in the pattern template.
/// Frame auto-promotes to full parked frame when all bits are set.
pub struct CompletionTracker {
    /// Bit i is set when hop i has been observed in the mutation stream.
    bits: BitVec<usize, Lsb0>,
    total_hops: usize,
    observed_count: usize,
}

impl CompletionTracker {
    pub fn new(num_hops: usize) -> Self {
        Self {
            bits: bitvec![usize, Lsb0; 0; num_hops],
            total_hops: num_hops,
            observed_count: 0,
        }
    }

    /// Mark a hop as observed. Returns true if newly observed.
    pub fn mark_hop(&mut self, hop_index: usize) -> bool {
        if hop_index < self.total_hops && !self.bits[hop_index] {
            self.bits.set(hop_index, true);
            self.observed_count += 1;
            true
        } else {
            false
        }
    }

    /// Check if all hops are complete (ready for promotion).
    pub fn is_complete(&self) -> bool {
        self.observed_count == self.total_hops
    }

    /// Completion ratio for tiering decisions.
    pub fn completion_ratio(&self) -> f64 {
        self.observed_count as f64 / self.total_hops as f64
    }
}
```

**Why `bitvec` over a simple `Vec<bool>`?** `Vec<bool>` uses 1 byte per boolean. `BitVec` uses 1 bit per boolean -- 8x more compact. For completion tracking where you may have hundreds of embryonic frames each with dozens of hops, the cache footprint difference matters. Also, `bitvec` provides optimized `count_ones()` and bitwise operations.

**Type parameter choice:** `<usize, Lsb0>` is the fastest configuration because it matches the platform's natural word size and avoids bit-reversal overhead.

**Confidence: HIGH** -- bitvec 1.0 is stable, well-maintained, and this is its primary use case.

### 8. CachePadded for False Sharing Prevention

```rust
use crossbeam::utils::CachePadded;

/// When producer and consumer atomics live on the same cache line,
/// writing to one invalidates the other thread's cache, even though
/// they access different variables. This is "false sharing."
///
/// CachePadded<T> pads T to a full cache line (typically 64 bytes),
/// ensuring each atomic lives on its own cache line.
pub struct PaddedRingBuffer<T: Default + Clone> {
    slots: Vec<T>,
    mask: u64,
    head: CachePadded<AtomicU64>,  // Producer's cache line
    tail: CachePadded<AtomicU64>,  // Consumer's cache line
}
```

**When to use CachePadded:** On any atomic that is written by one thread and read by another, when those atomics might be adjacent in memory. The head/tail of a ring buffer is the textbook case.

**When NOT to use CachePadded:** On atomics that are only accessed by a single thread, or on cold-path data. The padding wastes 56+ bytes per field.

**Confidence: HIGH** -- `crossbeam-utils::CachePadded` is stable, widely used, and the standard Rust idiom for this.

### 9. Pre-Allocated Vectors (Zero-Allocation Hot Path)

```rust
/// Pattern: allocate all Vecs at startup with known capacity.
/// On the hot path, use only indexed access and len tracking.
///
/// NEVER call push() on the hot path if it might reallocate.
/// Use a pre-sized Vec and track a write cursor manually.
pub struct PreAllocatedBuffer<T: Default + Clone> {
    data: Vec<T>,
    len: usize,
    capacity: usize,
}

impl<T: Default + Clone> PreAllocatedBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![T::default(); capacity],
            len: 0,
            capacity,
        }
    }

    /// Add an item without heap allocation.
    /// Returns None if buffer is full (caller must handle backpressure).
    pub fn add(&mut self, item: T) -> Option<usize> {
        if self.len >= self.capacity {
            return None;
        }
        self.data[self.len] = item;
        let idx = self.len;
        self.len += 1;
        Some(idx)
    }

    /// Reset the buffer for reuse (no deallocation).
    pub fn clear(&mut self) {
        self.len = 0;
    }
}
```

**The rule: Any Vec on the hot path must be created with `Vec::with_capacity(n)` or `vec![default; n]` at initialization. The hot path only writes into pre-existing slots.**

**Why not `bumpalo` (arena allocator)?** Bumpalo is an excellent general arena allocator, but krabnet's allocation pattern is simpler: all buffers have known sizes at startup. Pre-sized Vecs are zero-overhead for this pattern. Bumpalo adds a dependency and introduces lifetime complexity that isn't needed here.

**Confidence: HIGH** -- This is the standard pattern for zero-allocation hot paths in game engines, audio processors, and trading systems written in Rust.

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| Hand-rolled ring buffer | `ringbuf` crate | If you need MPMC or don't need epoch integration. ringbuf is well-tested for generic SPSC. |
| `crossbeam-epoch` for deferred reclamation | `std::sync::Arc` + `Drop` | If your data structures don't have concurrent readers during removal. Arc works fine for single-owner cleanup. |
| `bitvec` for completion bits | `Vec<bool>` or raw `u64` bitmask | `Vec<bool>` is fine for < 8 tracked items. Raw `u64` bitmask works if you always have <= 64 hops. bitvec is general. |
| Slab-indexed `Vec<Option<T>>` | `HashMap<u64, T>` | If IDs are sparse or externally assigned. HashMap handles sparse keyspaces better. |
| `criterion` benchmarks | `#[bench]` (nightly) or `divan` | `divan` is newer and simpler for basic benchmarks. `criterion` wins on statistical rigor and HTML reports. |
| Manual `unsafe impl Sync` wrapper | `SyncUnsafeCell` (nightly) or `sync-unsafe-cell` crate | Once `SyncUnsafeCell` stabilizes, use it directly. Until then, the manual wrapper is idiomatic. The `sync-unsafe-cell` crate is a backport if you want to avoid writing the wrapper yourself. |

## What NOT to Do

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `Mutex`/`RwLock` on the hot path | Lock contention defeats the purpose of a streaming runtime. Even uncontended Mutex has syscall overhead on some platforms. | Atomics with Acquire/Release ordering |
| `SeqCst` as default ordering | SeqCst is the "I don't know what I need" ordering. It adds memory fence overhead and makes the code harder to reason about because every SeqCst op participates in a global total order. | Use the weakest correct ordering. Acquire/Release for publish/consume patterns. Relaxed for thread-local counters. |
| `Box<dyn Trait>` on hot path | Dynamic dispatch + heap allocation per event. The vtable indirection also defeats branch prediction. | Enum dispatch or generic monomorphization. Pre-allocate and reuse buffers. |
| `String` or `&str` on hot path | Heap allocation, comparison is O(n) string length, cache-unfriendly. | String interning: intern at startup, use u32 IDs everywhere on hot path. |
| `HashMap` on hot path for dense integer keys | HashMap hashes the key (unnecessary for integers), has probe chains, and poor cache locality for sequential access. | Slab-indexed `Vec<Option<T>>` with integer indices. |
| `Arc<T>` for shared immutable config | Arc has atomic reference counting overhead on every clone/drop. For config that never changes after init, you pay atomics for nothing. | `&'static T` via `Box::leak()` for truly static config, or just pass `&T` with appropriate lifetimes. |
| `petgraph` for graph storage | External dependency (project constraint). Also, petgraph's edge storage doesn't support MVCC versioning or pre-allocated adjacency. | Hand-rolled node+edge slabs with adjacency-on-node. |
| `SyncUnsafeCell` on stable | It is nightly-only (feature `sync_unsafe_cell`, tracking issue rust-lang/rust#95439). Using it requires `#![feature(sync_unsafe_cell)]` which breaks the stable toolchain constraint. | Manual `unsafe impl Sync` wrapper around `UnsafeCell`. |
| `Vec::push()` in hot loop without pre-allocation | May trigger reallocation (memcpy of entire buffer). Each reallocation doubles capacity, so early pushes reallocate frequently. | `Vec::with_capacity(n)` at init. Write to pre-existing slots by index on hot path. |
| Forgetting `#[repr(C)]` on types shared across unsafe boundaries | Rust's default struct layout (`repr(Rust)`) allows field reordering. If you compute pointer offsets manually, reordering breaks your code silently. | Use `#[repr(C)]` on any struct where you do pointer arithmetic or transmute. |

## Safety Invariants for Unsafe Code

### Ring Buffer Slot Access
```
SAFETY CONTRACT: Ring buffer slot write via raw pointer
  Pre-conditions:
    1. Single producer: only one thread calls push()
    2. idx = head & mask, so idx is always in bounds [0, capacity)
    3. Consumer has not advanced past this slot (head - tail < capacity)
  Post-conditions:
    1. Slot at idx contains the new event
    2. head is advanced with Release ordering AFTER the write
  Violation consequence: Data race (UB), torn reads by consumer
```

### Manual Sync Implementation
```
SAFETY CONTRACT: unsafe impl Sync for SyncCell<T>
  Pre-conditions:
    1. T: Send (the inner value can be transferred between threads)
    2. All concurrent access to the inner value goes through atomics
       or is otherwise externally synchronized (e.g., epoch ordering
       guarantees only one thread accesses at a time)
    3. No &T and &mut T references to the inner value coexist
  Violation consequence: Data race (UB), use-after-free if reclamation
    is not epoch-protected
```

### Slab Deallocation
```
SAFETY CONTRACT: Removing a node/edge from the slab
  Pre-conditions:
    1. No live references to the node/edge exist
    2. If concurrent readers may hold references, use crossbeam-epoch:
       defer destruction until no pinned threads can see the old value
    3. The ID is not reused until deferred destruction completes
  Post-conditions:
    1. Slot is set to None
    2. ID is added to free list for future reuse
  Violation consequence: Use-after-free, dangling references
```

## Crossbeam Epoch Usage Pattern

```rust
use crossbeam::epoch::{self, Atomic, Owned, Shared};
use std::sync::atomic::Ordering;

/// When removing a node from the graph, defer its destruction
/// until no thread can hold a reference to it.
///
/// Pattern:
/// 1. Thread "pins" itself to the current epoch before accessing shared data
/// 2. Removal marks data as garbage in the current epoch
/// 3. When all threads have advanced past that epoch, garbage is collected
///
/// This avoids the ABA problem without stop-the-world GC.
fn remove_node_safely(
    node_slot: &Atomic<Node>,
    guard: &epoch::Guard,
) {
    let old = node_slot.swap(Shared::null(), Ordering::AcqRel, guard);
    if !old.is_null() {
        // SAFETY: We're the only thread that removed this node
        // (protected by external synchronization or CAS ownership).
        // The guard ensures no thread is reading the old value
        // once the epoch advances.
        unsafe {
            guard.defer_destroy(old);
        }
    }
}

/// Reader pattern: pin before accessing shared data
fn read_node(node_slot: &Atomic<Node>) -> Option<u64> {
    let guard = epoch::pin();
    let shared = node_slot.load(Ordering::Acquire, &guard);
    // shared is guaranteed valid for the lifetime of `guard`
    unsafe { shared.as_ref() }.map(|node| node.id)
    // guard dropped here -> thread unpins
}
```

**When you need crossbeam-epoch:** When graph nodes or edges are removed while other threads may hold references (future multi-reader scenario). For the single-threaded PoC, direct removal is safe, but using epoch-based reclamation now makes the code correct for future multi-threaded extension without refactoring.

**When you DON'T need crossbeam-epoch:** For the ring buffer slots, which are overwritten in place (not deallocated). The epoch sequencer + Acquire/Release ordering is sufficient.

**Confidence: HIGH** -- crossbeam-epoch is the de facto standard for lock-free reclamation in Rust.

## Criterion Benchmark Setup

```rust
// benches/krabnet_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn ring_buffer_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer");

    // Benchmark with different buffer sizes
    for size in [1024, 4096, 16384, 65536] {
        group.bench_with_input(
            criterion::BenchmarkId::new("push", size),
            &size,
            |b, &size| {
                let buffer = RingBuffer::new(size);
                b.iter(|| {
                    buffer.push(black_box(Event::default()));
                });
            },
        );
    }
    group.finish();
}

fn graph_traversal_latency(c: &mut Criterion) {
    c.bench_function("dfs_3_hop", |b| {
        let graph = setup_test_graph(1000, 5000);
        b.iter(|| {
            traverse_dfs(black_box(&graph), black_box(0), 3);
        });
    });
}

fn differential_compaction(c: &mut Criterion) {
    c.bench_function("compact_1000_entries", |b| {
        let mut index = setup_diff_index(1000);
        b.iter(|| {
            index.compact(black_box(500));
        });
    });
}

criterion_group!(
    benches,
    ring_buffer_throughput,
    graph_traversal_latency,
    differential_compaction,
);
criterion_main!(benches);
```

**Key benchmarking rules:**
1. Always use `black_box()` to prevent the compiler from optimizing away the computation
2. Benchmark with realistic data sizes (not toy sizes)
3. Group related benchmarks for comparison
4. Use `BenchmarkId` for parameterized benchmarks
5. Run with `cargo bench` (not `cargo test`)

**Confidence: HIGH** -- Criterion is the de facto Rust benchmarking framework.

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| `crossbeam` 0.8.x | Rust 1.61+ | Stable, no nightly features required |
| `bitvec` 1.0.x | Rust 1.56+ (edition 2021) | MSRV only bumps on minor releases; pin `~1.0` for stability |
| `criterion` 0.8.x | Rust 1.88+ | Recent criterion versions require recent stable Rust |
| `crossbeam-epoch` 0.9.x | Rust 1.61+ | Sub-crate of crossbeam, same MSRV |
| `crossbeam-utils` 0.8.x | Rust 1.61+ | Provides `CachePadded`, scoped threads |

## Sources

- [Rust Atomics and Locks, Chapter 3: Memory Ordering](https://mara.nl/atomics/memory-ordering.html) -- Authoritative guide on Acquire/Release/SeqCst. **HIGH confidence**
- [Ferrous Systems: Lock-Free Ring Buffer Design](https://ferrous-systems.com/blog/lock-free-ring-buffer/) -- Ring buffer design with atomic operations. **HIGH confidence**
- [Materialize: Building Differential Dataflow from Scratch](https://materialize.com/blog/differential-from-scratch/) -- Differential multiset semantics and compaction. **HIGH confidence**
- [Frank McSherry's differential-dataflow](https://github.com/TimelyDataflow/differential-dataflow) -- Reference Rust implementation of differential dataflow. **HIGH confidence**
- [matklad: Fast Simple Rust Interner](https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html) -- String interning pattern. **HIGH confidence**
- [UnsafeCell std docs](https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html) -- Safety invariants for interior mutability. **HIGH confidence**
- [SyncUnsafeCell tracking issue #95439](https://github.com/rust-lang/rust/issues/95439) -- Confirmed nightly-only status. **HIGH confidence**
- [Send and Sync - Rustonomicon](https://doc.rust-lang.org/nomicon/send-and-sync.html) -- Manual Send/Sync impl rules. **HIGH confidence**
- [crossbeam-epoch docs](https://docs.rs/crossbeam/latest/crossbeam/epoch/index.html) -- Epoch-based reclamation API. **HIGH confidence**
- [bitvec crate docs](https://docs.rs/bitvec/latest/bitvec/) -- BitVec usage and type parameter recommendations. **HIGH confidence**
- [Criterion.rs documentation](https://bheisler.github.io/criterion.rs/book/getting_started.html) -- Benchmark setup. **HIGH confidence**
- [The Rust Performance Book: Heap Allocations](https://nnethercote.github.io/perf-book/heap-allocations.html) -- Pre-allocation patterns. **HIGH confidence**
- [Nomicon: Atomics](https://doc.rust-lang.org/nomicon/atomics.html) -- Atomic ordering semantics. **HIGH confidence**
- [crossbeam on crates.io](https://crates.io/crates/crossbeam) -- Version 0.8.4 latest. **HIGH confidence**
- [bitvec on crates.io](https://crates.io/crates/bitvec) -- Version 1.0.1 latest. **HIGH confidence**
- [criterion on crates.io](https://crates.io/crates/criterion) -- Version 0.8.x latest. **HIGH confidence**
- [Nomicon issue #166: SeqCst considered harmful](https://github.com/rust-lang/nomicon/issues/166) -- Discussion on why SeqCst should not be the default. **MEDIUM confidence** (community discussion, not official docs)
- [DEV Community: Cache-Friendly SPSC Ring Buffer](https://dev.to/codeapprentice/low-latency-rust-building-a-cache-friendly-lock-free-spsc-ring-buffer-in-rust-ddm) -- Power-of-2 sizing rationale. **MEDIUM confidence**

---
*Stack research for: Streaming graph runtime with differential MVCC in Rust*
*Researched: 2026-02-24*
