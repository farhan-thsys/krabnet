# Phase 1: Core Types and String Interning - Research

**Researched:** 2026-02-24
**Domain:** Rust newtype definitions, enum modeling, and string interning for a streaming graph runtime
**Confidence:** HIGH

## Summary

Phase 1 establishes the foundation layer for the entire Krabnet crate: a `types` module defining all shared newtypes and enums, and an `interner` module providing bidirectional string-to-u32 interning. This is a greenfield Rust project -- no source code exists yet. The phase must also create the Cargo.toml, lib.rs, and the initial crate structure.

The types module is straightforward Rust newtype and enum definitions with zero runtime cost. The interner follows the matklad pattern: a `HashMap<String, u32>` for string-to-ID lookup and a `Vec<String>` for ID-to-string reverse lookup. The interner is populated exclusively at initialization time; after initialization, it becomes effectively immutable, satisfying the "no heap allocation after initialization" constraint. No external crates are needed for this phase -- only `std` types.

**Primary recommendation:** Define all newtypes as tuple structs wrapping their inner integer types. Define all enums exhaustively per the requirements. Implement the string interner as a simple `HashMap` + `Vec` with `&mut self` for `intern()` and `&self` for `resolve()`. The API boundary enforces the init-time-only invariant: `intern()` takes `&mut self`, so once the interner is shared as `&Interner`, no further interning is possible without `unsafe`.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| INFRA-01 | System defines core types (PropertyValue, PropertySet, Direction, Filter, HopSpec, Event, DiffTuple, InterpretationTier, FrameTier) shared across all modules | Types module with newtypes (NodeId, EdgeId, TypeId, Epoch, Delta) and enums (PropertyValue, Direction, Filter, HopSpec, Event, DiffTuple, InterpretationTier, FrameTier). All defined in a single `types.rs` module, re-exported from lib.rs. PropertySet is `Vec<(u32, PropertyValue)>` using interned keys. |
| INFRA-02 | String interner maps bidirectionally between String and u32 for property keys and type names at initialization | Interner module using the matklad pattern: `HashMap<String, u32>` for forward mapping, `Vec<String>` for reverse mapping. Insert-once semantics enforced by `&mut self` on `intern()`. Idempotent: interning the same string twice returns the same u32 ID. Resolve via index lookup in O(1). |
</phase_requirements>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Rust stable | 1.85+ | Language and toolchain | Project constraint: stable only, no nightly features. Zero-cost abstractions, ownership model for correctness |
| `std::collections::HashMap` | stable | Forward map (string -> u32) in interner | First-party, zero-dependency. Adequate for init-time-only lookups (not on hot path) |
| `std::vec::Vec` | stable | Reverse map (u32 -> string) in interner, plus arena for string storage | O(1) index-based lookup. ID is the index. Standard pattern for dense integer-keyed storage |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `crossbeam` | 0.8.x | Listed in Cargo.toml but NOT used in Phase 1 | Phase 2+ (ring buffer, CachePadded). Declare dependency now so Cargo.toml is ready |
| `bitvec` | 1.0.x | Listed in Cargo.toml but NOT used in Phase 1 | Phase 8 (embryonic completion tracking). Declare dependency now |
| `criterion` | 0.8.x | Dev-dependency, NOT used in Phase 1 | Phase 10 (benchmarks). Declare dependency now |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Hand-rolled interner | `lasso` crate (Rodeo) | lasso provides thread-safe interning, O(1) resolution, and memory-efficient storage. But project constraint is "only crossbeam, bitvec, criterion" -- no other dependencies allowed. Hand-rolled is simple enough (< 50 lines) that a crate is unnecessary. |
| Hand-rolled interner | `string-interner` crate | Same constraint: not an allowed dependency. Also, the init-only pattern (no runtime interning) is simpler than what these crates optimize for. |
| `HashMap<String, u32>` | `HashMap<&str, u32>` with arena-backed strings | Arena-backed `&str` avoids double-storing strings (once in HashMap key, once in Vec). But this requires lifetime management that complicates the API. For < 10,000 strings at init time, the double-store is negligible. Keep it simple. |
| `Vec<String>` for reverse | `Vec<Box<str>>` for reverse | `Box<str>` is slightly smaller (no capacity field). But the difference is 8 bytes per string, irrelevant for < 10,000 interned strings. `String` is more ergonomic. |

**Installation:**
```bash
cargo init --lib --name krabnet
# Then edit Cargo.toml to add dependencies
```

## Architecture Patterns

### Recommended Project Structure
```
krabnet/
├── Cargo.toml          # Package manifest with all dependencies declared
├── src/
│   ├── lib.rs          # Crate root: module declarations, public re-exports
│   ├── types.rs        # All shared newtypes and enums (INFRA-01)
│   └── interner.rs     # StringInterner: string <-> u32 mapping (INFRA-02)
└── tests/              # Integration tests (empty for now, placeholder)
```

### Pattern 1: Newtypes for Type Safety

**What:** Wrap primitive integer types in single-field tuple structs to prevent mixing up IDs of different domains (e.g., passing a NodeId where an EdgeId is expected).

**When to use:** Every domain-specific integer identifier. NodeId, EdgeId, TypeId, Epoch, and Delta are all newtypes.

**Why:** Zero runtime cost (the newtype compiles away). The compiler catches cross-domain ID mixups at compile time. This is Rust's primary idiom for domain modeling.

**Example:**
```rust
// Source: Standard Rust newtype pattern (Rust API Guidelines C-NEWTYPE)
/// Unique identifier for a node in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Unique identifier for an edge in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeId(pub u64);

/// Interned type identifier (property key or type name).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeId(pub u32);

/// Monotonic epoch from the sequencer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Epoch(pub u64);

/// Differential delta: +1 for assertion, -1 for retraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Delta(pub i64);
```

### Pattern 2: Exhaustive Enums for Domain Modeling

**What:** Use Rust enums (algebraic data types) to model all finite domain values. Derive standard traits for ergonomic use.

**When to use:** Every domain value with a known, finite set of variants. PropertyValue, Direction, Filter, Event, HopSpec, DiffTuple, InterpretationTier, FrameTier.

**Example:**
```rust
/// A property value that can be stored on a node.
/// Uses interned u32 for string-typed properties (zero allocation on hot path).
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Integer(i64),
    Float(f64),
    Text(u32),      // Interned string ID -- no String on hot path
    Boolean(bool),
}

/// Traversal direction for neighbor queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Outgoing,
    Incoming,
    Any,
}

/// A graph mutation event entering through the ring buffer.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    NodeAdded { node_id: NodeId, type_id: TypeId },
    NodeRemoved { node_id: NodeId },
    EdgeAdded { edge_id: EdgeId, source: NodeId, target: NodeId, type_id: TypeId },
    EdgeRemoved { edge_id: EdgeId, source: NodeId, target: NodeId },
    PropertyChanged { node_id: NodeId, key: u32, value: PropertyValue },
}
```

### Pattern 3: Matklad Interner (HashMap + Vec)

**What:** A string interner that maps strings to sequential u32 IDs at initialization time. Forward lookup via HashMap, reverse lookup via Vec index. After initialization, the interner is immutable.

**When to use:** At the boundary between human-readable strings and machine-efficient integer IDs. Called during initialization (when loading config, defining types and property keys). Never called on the hot path.

**Source:** [matklad: Fast and Simple Rust Interner](https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html)

**Example:**
```rust
use std::collections::HashMap;

/// Interns strings at startup, returning integer IDs for hot-path use.
///
/// After initialization, all lookups on the hot path use u32 IDs only.
/// No String, &str, or heap allocation on the hot path.
///
/// # Initialization Contract
/// - `intern()` takes `&mut self` -- can only be called before sharing
/// - `resolve()` takes `&self` -- safe to call from any context
/// - Once the interner is shared as `&Interner`, no new strings can be added
pub struct Interner {
    map: HashMap<String, u32>,
    strings: Vec<String>,
}

impl Interner {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            strings: Vec::new(),
        }
    }

    /// Pre-allocate capacity for expected number of strings.
    /// Call before interning to avoid reallocations during init.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            strings: Vec::with_capacity(capacity),
        }
    }

    /// Intern a string. Returns a stable u32 ID.
    /// Idempotent: interning the same string twice returns the same ID.
    ///
    /// # Panics
    /// Panics if more than u32::MAX strings are interned.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.strings.len() as u32;
        assert!(
            (self.strings.len() as u64) < u32::MAX as u64,
            "interner capacity exceeded: cannot intern more than {} strings",
            u32::MAX
        );
        self.strings.push(s.to_owned());
        self.map.insert(s.to_owned(), id);
        id
    }

    /// Resolve an ID back to the interned string.
    /// Returns None if the ID was never interned.
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
    }

    /// Number of interned strings.
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Whether the interner is empty.
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}
```

### Anti-Patterns to Avoid

- **String on hot path:** Never use `String` or `&str` for type comparisons or property lookups after initialization. Always use the interned u32 ID. The interner is the boundary between human-readable and machine-efficient representations.

- **Generic type parameters on newtypes:** Do not make `NodeId<T>` or `Epoch<T>`. The inner types are fixed by the architecture (u64 for IDs/epochs, u32 for interned IDs, i64 for deltas). Generics add complexity with no benefit.

- **Deriving `Default` on newtypes without intent:** `NodeId(0)` as a default is semantically wrong -- ID 0 may be a valid node. Only derive `Default` on types where a zero/empty value is semantically meaningful (e.g., `Delta(0)` means "no change").

- **`pub` fields on enums that should be opaque:** Event variants use named fields (not positional) for self-documenting API. But the fields should be `pub` since downstream modules need to destructure events in match arms.

- **Implementing `Eq` on `f64`-containing types:** `PropertyValue` contains `Float(f64)`. Do NOT derive `Eq` on `PropertyValue` because `f64` does not implement `Eq` (NaN != NaN). Derive only `PartialEq`. If you need `PropertyValue` in a `HashMap` key, you need a wrapper that handles NaN, but this is unlikely for property values.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| String-to-ID mapping | Trie-based interner, custom hash table | `HashMap<String, u32>` + `Vec<String>` | HashMap is well-optimized in std. A trie is slower and more complex for < 10K strings (per matklad's benchmarks). The init-only pattern means HashMap performance is irrelevant -- it is never called on the hot path. |
| Hash function for interner | Custom hash, FNV, FxHash | `std::collections::HashMap` (default SipHash) | SipHash is fine for init-time-only usage. FxHash would be faster but requires `rustc-hash` dependency (not allowed). The performance difference is unmeasurable at init time with < 10K strings. |
| Type-safe IDs | Raw `u64`/`u32` passed everywhere | Newtype tuple structs | Newtypes are zero-cost at runtime. They prevent an entire class of bugs (passing NodeId where EdgeId is expected) at compile time. The derive macros generate all necessary trait impls. |

**Key insight:** Phase 1 types are purely compile-time constructs (newtypes, enums). The only runtime logic is the interner, which is ~50 lines of straightforward HashMap + Vec code. Do not over-engineer this phase. The complexity lives in later phases.

## Common Pitfalls

### Pitfall 1: Forgetting `Copy` on Newtypes
**What goes wrong:** Newtypes wrapping `u64`/`u32`/`i64` that don't derive `Copy` force unnecessary `.clone()` calls everywhere they are used. Since these types are passed by value throughout the crate (in function arguments, match arms, struct fields), missing `Copy` causes borrow checker friction and code noise.
**Why it happens:** Developers derive `Clone` but forget `Copy`. The compiler does not warn about this.
**How to avoid:** Always derive both `Clone` and `Copy` on newtypes wrapping `Copy` primitives. The derive order should be `Clone, Copy` (Clone is required by Copy).
**Warning signs:** `.clone()` calls on NodeId, EdgeId, TypeId, Epoch, or Delta values.

### Pitfall 2: PropertyValue Containing Heap-Allocated String
**What goes wrong:** Defining `PropertyValue::Text(String)` instead of `PropertyValue::Text(u32)` puts a heap-allocated String inside every text property value. This violates the zero-allocation hot path constraint and makes PropertyValue non-Copy.
**Why it happens:** Natural Rust modeling would use `String` for text. The interning strategy requires replacing the String with its interned u32 ID.
**How to avoid:** `PropertyValue::Text(u32)` where the u32 is the interned string ID from the `Interner`. The actual string content lives in the interner's Vec, not in the property value. Document this clearly: "u32 is an interned string ID, not a raw integer."
**Warning signs:** `PropertyValue` not being `Clone` cheaply, `String` anywhere in the hot-path type definitions.

### Pitfall 3: Interner ID Collision Across Instances
**What goes wrong:** Two `Interner` instances intern the same strings but assign different u32 IDs. A u32 ID obtained from one interner is used with a different interner, returning the wrong string or `None`.
**Why it happens:** The interner assigns IDs sequentially starting from 0. If two interners exist and intern strings in different order, the same string maps to different IDs.
**How to avoid:** Design the system so there is exactly ONE interner instance, created at initialization and shared (by reference) with all modules. The `Engine` struct should own the `Interner` and pass `&Interner` to subsystems. Document: "IDs from one interner instance MUST NOT be used with another."
**Warning signs:** Multiple `Interner::new()` calls in the codebase. Tests creating their own interners instead of using a shared fixture.

### Pitfall 4: Missing Derive Traits Causing Downstream Compilation Failures
**What goes wrong:** A type in `types.rs` does not derive `Hash`, causing a compilation error in a later phase when that type is used as a HashMap key. Or a type does not derive `PartialOrd`/`Ord`, causing errors when it is used in a BTreeMap or sorted context.
**Why it happens:** Phase 1 defines types used by all 12 subsequent modules. The required trait impls depend on how the type is used downstream, which is not always obvious during Phase 1.
**How to avoid:** Derive the full standard set on every newtype: `Debug, Clone, Copy, PartialEq, Eq, Hash`. Add `PartialOrd, Ord` on types that represent ordered values (Epoch, NodeId, EdgeId). For enums, derive what the variants allow (no `Eq` or `Hash` on enums containing `f64`).
**Warning signs:** Compilation errors in later phases that trace back to missing trait derives on types.rs types.

### Pitfall 5: Interner Capacity Overflow
**What goes wrong:** Interning more than `u32::MAX` (4,294,967,295) strings causes a silent overflow when casting `usize` to `u32`, wrapping back to 0 and overwriting existing mappings.
**Why it happens:** `let id = self.strings.len() as u32` silently truncates on overflow in release mode.
**How to avoid:** Add an explicit assertion: `assert!((self.strings.len() as u64) < u32::MAX as u64)` before the cast. This will never fire in practice (Krabnet will have hundreds of interned strings, not billions), but it prevents silent corruption if the invariant is violated.
**Warning signs:** Using `as u32` without a bounds check.

## Code Examples

Verified patterns from the project's architecture research and standard Rust idioms.

### Complete types.rs Skeleton

```rust
// Source: ARCHITECTURE.md component table + REQUIREMENTS.md INFRA-01
//! Core type definitions shared across all Krabnet modules.
//!
//! This module defines the newtypes, enums, and type aliases that form
//! the vocabulary of the entire crate. All types are designed for zero-cost
//! abstraction: newtypes compile to their inner primitives, enums use
//! no heap allocation.

/// Unique identifier for a node in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Unique identifier for an edge in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeId(pub u64);

/// Interned type/property-key identifier.
/// Obtained from `Interner::intern()` at initialization time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeId(pub u32);

/// Monotonic epoch from the sequencer. Total ordering of all events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Epoch(pub u64);

/// Differential delta: +1 for assertion, -1 for retraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Delta(pub i64);

/// A property value stored on a node.
/// Text variant uses interned u32 ID, not String (zero-alloc hot path).
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Integer(i64),
    Float(f64),
    Text(u32),       // Interned string ID
    Boolean(bool),
}

/// A set of properties: pairs of (interned_key, value).
pub type PropertySet = Vec<(u32, PropertyValue)>;

/// Traversal direction for neighbor queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Outgoing,
    Incoming,
    Any,
}

/// Property filter for hop-level traversal constraints.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// No filter -- accept all.
    None,
    /// Property key must exist with matching value.
    PropertyEquals { key: u32, value: PropertyValue },
    /// Property key must exist (any value).
    HasProperty { key: u32 },
}

/// One hop in a multi-hop traversal pattern.
#[derive(Debug, Clone, PartialEq)]
pub struct HopSpec {
    /// Direction to traverse edges.
    pub direction: Direction,
    /// Optional edge type filter (interned TypeId).
    pub edge_type: Option<TypeId>,
    /// Optional target node type filter (interned TypeId).
    pub target_type: Option<TypeId>,
    /// Optional property filter on the target node.
    pub filter: Filter,
}

/// A graph mutation event entering through the ring buffer.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    NodeAdded { node_id: NodeId, type_id: TypeId },
    NodeRemoved { node_id: NodeId },
    EdgeAdded {
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        type_id: TypeId,
    },
    EdgeRemoved {
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
    },
    PropertyChanged {
        node_id: NodeId,
        key: u32,
        value: PropertyValue,
    },
}

/// A differential tuple: (payload, epoch, delta).
/// Represents a single assertion (+1) or retraction (-1) at a given epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct DiffTuple<T> {
    pub data: T,
    pub epoch: Epoch,
    pub delta: Delta,
}

/// Interpretation tier for frame analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InterpretationTier {
    /// Fast binary delta-sum check (O(1)).
    Tier1,
    /// Full structural path analysis (expensive).
    Tier2,
}

/// Frame temperature tier for adaptive tiering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameTier {
    /// High priority: fully materialized, interpreted every cycle.
    Hot,
    /// Medium priority: materialized but not always interpreted.
    Warm,
    /// Low priority: may be evicted or stored compactly.
    Cold,
}
```

### Complete interner.rs Skeleton

```rust
// Source: matklad interner pattern + STACK.md string interning section
//! String interner for property keys and type names.
//!
//! Converts human-readable strings to compact u32 integer IDs at
//! initialization time. After initialization, all hot-path operations
//! use integer IDs exclusively -- zero String allocation on the hot path.
//!
//! # Usage
//!
//! ```rust
//! let mut interner = Interner::new();
//! let person = interner.intern("Person");
//! let name = interner.intern("name");
//! let person2 = interner.intern("Person");
//! assert_eq!(person, person2);           // Same string -> same ID
//! assert_eq!(interner.resolve(person), Some("Person"));
//! ```

use std::collections::HashMap;

/// Bidirectional string-to-u32 interner.
///
/// # Initialization Contract
/// - `intern()` requires `&mut self` -- only callable before sharing
/// - `resolve()` requires `&self` -- callable from any context after init
/// - The interner is populated during system initialization and then
///   shared immutably for the lifetime of the system
///
/// # Guarantees
/// - Interning the same string twice returns the same u32 ID
/// - IDs are assigned sequentially starting from 0
/// - `resolve(id)` returns `Some(&str)` for all IDs returned by `intern()`
/// - No heap allocation occurs after initialization is complete
pub struct Interner {
    /// Forward map: string -> ID.
    map: HashMap<String, u32>,
    /// Reverse map: ID -> string. Index is the ID.
    strings: Vec<String>,
}

impl Interner {
    /// Create a new empty interner.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            strings: Vec::new(),
        }
    }

    /// Create a new interner with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            strings: Vec::with_capacity(capacity),
        }
    }

    /// Intern a string, returning its stable u32 ID.
    ///
    /// Idempotent: calling `intern("foo")` twice returns the same ID.
    ///
    /// # Panics
    /// Panics if more than `u32::MAX` strings are interned.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.strings.len();
        assert!(
            (id as u64) < u32::MAX as u64,
            "interner capacity exceeded"
        );
        let id = id as u32;
        self.strings.push(s.to_owned());
        self.map.insert(s.to_owned(), id);
        id
    }

    /// Resolve a u32 ID back to the original string.
    ///
    /// Returns `None` if the ID was never returned by `intern()`.
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
    }

    /// Number of interned strings.
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Whether the interner contains no strings.
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}
```

### Cargo.toml

```toml
# Source: STACK.md Cargo.toml configuration
[package]
name = "krabnet"
version = "0.1.0"
edition = "2021"
description = "Streaming graph runtime with differential MVCC and pre-materialized traversals"

[dependencies]
crossbeam = { version = "0.8", default-features = false, features = ["std"] }
bitvec = "1.0"

[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "krabnet_bench"
harness = false
```

### lib.rs

```rust
//! Krabnet: a streaming graph runtime with differential MVCC.
//!
//! Pre-materializes graph traversal results for AI agent context systems.
//! When a signal arrives, decision-relevant context is already materialized --
//! zero query-time graph traversal.

pub mod types;
pub mod interner;

// Re-export core types for ergonomic use
pub use types::*;
pub use interner::Interner;
```

### Test Patterns for Phase 1

```rust
// tests/test_types.rs or within types.rs as #[cfg(test)] mod tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newtype_ids_are_distinct_types() {
        // This test is a compile-time check more than a runtime check.
        // If NodeId and EdgeId were both u64, you could accidentally
        // pass one where the other is expected. Newtypes prevent this.
        let node = NodeId(1);
        let edge = EdgeId(1);
        // These are different types even though inner values are equal:
        assert_ne!(std::mem::discriminant(&node), std::mem::discriminant(&node));
        // The real test is that this does NOT compile:
        // fn takes_node(id: NodeId) {}
        // takes_node(edge);  // <-- compilation error
    }

    #[test]
    fn newtypes_are_copy() {
        let id = NodeId(42);
        let id2 = id;  // Copy, not move
        assert_eq!(id, id2);  // Both still valid
    }

    #[test]
    fn epoch_ordering() {
        assert!(Epoch(1) < Epoch(2));
        assert!(Epoch(0) < Epoch(u64::MAX));
    }

    #[test]
    fn direction_variants() {
        // Ensure all variants exist and are distinct
        let dirs = [Direction::Outgoing, Direction::Incoming, Direction::Any];
        for (i, a) in dirs.iter().enumerate() {
            for (j, b) in dirs.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn property_value_text_uses_interned_id() {
        // Text variant holds u32 (interned ID), not String
        let val = PropertyValue::Text(42);
        assert_eq!(std::mem::size_of_val(&val), std::mem::size_of::<PropertyValue>());
        // No heap allocation from creating a Text property value
    }
}
```

```rust
// tests/test_interner.rs or within interner.rs as #[cfg(test)] mod tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_returns_stable_id() {
        let mut interner = Interner::new();
        let id1 = interner.intern("Person");
        let id2 = interner.intern("Person");
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_strings_get_different_ids() {
        let mut interner = Interner::new();
        let a = interner.intern("alpha");
        let b = interner.intern("beta");
        assert_ne!(a, b);
    }

    #[test]
    fn resolve_returns_original_string() {
        let mut interner = Interner::new();
        let id = interner.intern("hello");
        assert_eq!(interner.resolve(id), Some("hello"));
    }

    #[test]
    fn resolve_unknown_id_returns_none() {
        let interner = Interner::new();
        assert_eq!(interner.resolve(999), None);
    }

    #[test]
    fn ids_are_sequential_from_zero() {
        let mut interner = Interner::new();
        assert_eq!(interner.intern("first"), 0);
        assert_eq!(interner.intern("second"), 1);
        assert_eq!(interner.intern("third"), 2);
    }

    #[test]
    fn len_tracks_unique_strings() {
        let mut interner = Interner::new();
        assert_eq!(interner.len(), 0);
        interner.intern("a");
        assert_eq!(interner.len(), 1);
        interner.intern("a");  // Duplicate
        assert_eq!(interner.len(), 1);  // Still 1
        interner.intern("b");
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn with_capacity_does_not_affect_behavior() {
        let mut interner = Interner::with_capacity(100);
        let id = interner.intern("test");
        assert_eq!(interner.resolve(id), Some("test"));
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn all_interned_strings_resolvable() {
        let mut interner = Interner::new();
        let strings = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let ids: Vec<u32> = strings.iter().map(|s| interner.intern(s)).collect();
        for (id, expected) in ids.iter().zip(strings.iter()) {
            assert_eq!(interner.resolve(*id), Some(*expected));
        }
    }

    #[test]
    fn empty_string_can_be_interned() {
        let mut interner = Interner::new();
        let id = interner.intern("");
        assert_eq!(interner.resolve(id), Some(""));
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `SyncUnsafeCell` for interior mutability | Manual `unsafe impl Sync` around `UnsafeCell` | `SyncUnsafeCell` is still nightly-only as of Rust 1.85 (tracking issue #95439) | Phase 1 does not need `UnsafeCell` or `Sync`. This matters in Phase 2 (ring buffer). |
| `string-interner` crate | Hand-rolled or `lasso` crate | Both actively maintained as of 2025 | Project constraint forbids external interner crates. Hand-rolled is appropriate. |
| `#[derive(Eq)]` on f64-containing types | Manually impl `PartialEq` only | Always | `PropertyValue` contains `f64`. Cannot derive `Eq`. This is a permanent Rust language constraint. |

**Deprecated/outdated:**
- Nothing in this phase uses deprecated features. All patterns are stable Rust idioms.

## Open Questions

1. **Should `DiffTuple<T>` be generic or concrete?**
   - What we know: The requirements list `DiffTuple` as a shared type. The differential engine (Phase 4) will use it with specific payload types (path tuples).
   - What's unclear: Whether making it generic now helps or hinders. Generic types require `T: Clone + PartialEq` bounds that may need adjustment in Phase 4.
   - Recommendation: Define it as generic `DiffTuple<T>` now. The bounds can be added where needed (on impl blocks, not on the struct definition). This avoids defining the payload type prematurely.

2. **Should `Event` carry an `Epoch` field or is epoch assigned separately?**
   - What we know: The architecture shows the sequencer assigning epochs before events enter the ring buffer. The ring buffer stores events with assigned epochs.
   - What's unclear: Whether `Event` should contain an `epoch: Epoch` field, or whether epoch assignment happens via a wrapper struct in the ring buffer module.
   - Recommendation: Define `Event` WITHOUT an epoch field. The epoch is assigned by the sequencer when the event enters the ring buffer. The ring buffer module (Phase 2) will define a wrapper like `StampedEvent { event: Event, epoch: Epoch }`. This keeps the separation of concerns clean -- events are domain mutations, epochs are infrastructure.

3. **Should `PropertyValue::Float` use `ordered_float::OrderedFloat<f64>` for Eq/Hash?**
   - What we know: `f64` does not implement `Eq` or `Hash`. This prevents `PropertyValue` from being used as a HashMap key or in sets.
   - What's unclear: Whether PropertyValue will ever need to be a HashMap key. It is used as a value in property storage, not typically as a key.
   - Recommendation: Keep `f64` raw. Do not add `ordered_float` dependency (not in allowed list). If `Eq`/`Hash` is needed on `PropertyValue` in a later phase, add a custom impl at that time that handles NaN. For now, `PartialEq` and `Clone` are sufficient.

## Sources

### Primary (HIGH confidence)
- [matklad: Fast and Simple Rust Interner](https://matklad.github.io/2020/03/22/fast-simple-rust-interner.html) - Interner design pattern: HashMap + Vec, arena allocation, bidirectional lookup
- [Rust API Guidelines: C-NEWTYPE](https://rust-lang.github.io/api-guidelines/type-safety.html) - Newtype pattern for type safety
- ARCHITECTURE.md (project-internal) - Module structure, component responsibilities, types.rs contents
- STACK.md (project-internal) - String interning pattern with code, Cargo.toml configuration
- REQUIREMENTS.md (project-internal) - INFRA-01 and INFRA-02 requirement definitions

### Secondary (MEDIUM confidence)
- [String interners in Rust (DEV Community)](https://dev.to/cad97/string-interners-in-rust-797) - Comparison of interner approaches in Rust ecosystem
- PITFALLS.md (project-internal) - Pitfall 7 (zero-allocation hot path), Pitfall on string-based hot path

### Tertiary (LOW confidence)
- None. Phase 1 uses entirely standard Rust patterns with no speculative components.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - Only std types (HashMap, Vec) are needed. No external crates for Phase 1 logic.
- Architecture: HIGH - Flat module layout with types.rs + interner.rs is the textbook Rust pattern. Well-documented in project's own ARCHITECTURE.md.
- Pitfalls: HIGH - All pitfalls (missing derives, f64 Eq, capacity overflow, interned ID collision) are well-known Rust patterns with clear mitigations.

**Research date:** 2026-02-24
**Valid until:** Indefinite -- Phase 1 uses only stable Rust std library features that do not change.
