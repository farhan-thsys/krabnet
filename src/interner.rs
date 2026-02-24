//! String interner for property keys and type names.
//!
//! Converts human-readable strings to compact `u32` integer IDs at
//! initialization time. After initialization, all hot-path operations
//! use integer IDs exclusively -- zero `String` allocation on the hot path.
//!
//! # Initialization Contract
//!
//! - [`Interner::intern()`] requires `&mut self` -- only callable before sharing
//! - [`Interner::resolve()`] requires `&self` -- callable from any context after init
//! - Once the interner is shared as `&Interner`, no new strings can be added
//!
//! # Usage
//!
//! ```
//! use krabnet::Interner;
//!
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
/// Maps human-readable strings to compact `u32` integer IDs at initialization
/// time. The interner is populated during system startup and then shared
/// immutably for the lifetime of the system.
///
/// # Guarantees
///
/// - Interning the same string twice returns the same `u32` ID
/// - IDs are assigned sequentially starting from 0
/// - [`resolve(id)`](Interner::resolve) returns `Some(&str)` for all IDs
///   returned by [`intern()`](Interner::intern)
/// - No heap allocation occurs after initialization is complete
///
/// # Capacity
///
/// The interner supports up to [`u32::MAX`] unique strings. Attempting to
/// intern more will panic. In practice, Krabnet uses hundreds of interned
/// strings (type names, property keys), well within this limit.
pub struct Interner {
    /// Forward map: string -> ID.
    map: HashMap<String, u32>,
    /// Reverse map: ID -> string. The index is the ID.
    strings: Vec<String>,
}

impl Interner {
    /// Creates a new empty interner.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Interner;
    ///
    /// let interner = Interner::new();
    /// assert!(interner.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            strings: Vec::new(),
        }
    }

    /// Creates a new interner with pre-allocated capacity for the expected
    /// number of unique strings.
    ///
    /// Call before interning to avoid reallocations during initialization.
    /// Does not affect the behavior of the interner, only its initial
    /// memory allocation.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Interner;
    ///
    /// let mut interner = Interner::with_capacity(100);
    /// let id = interner.intern("test");
    /// assert_eq!(interner.resolve(id), Some("test"));
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            strings: Vec::with_capacity(capacity),
        }
    }

    /// Interns a string, returning its stable `u32` ID.
    ///
    /// Idempotent: calling `intern("foo")` twice returns the same ID.
    /// IDs are assigned sequentially starting from 0.
    ///
    /// The `&mut self` signature enforces the initialization-only invariant:
    /// once the interner is shared as `&Interner`, no new strings can be
    /// interned without `unsafe`.
    ///
    /// # Panics
    ///
    /// Panics if more than [`u32::MAX`] unique strings are interned.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Interner;
    ///
    /// let mut interner = Interner::new();
    /// let id1 = interner.intern("hello");
    /// let id2 = interner.intern("hello");
    /// assert_eq!(id1, id2); // Same string -> same ID
    /// ```
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = self.strings.len();
        assert!(
            (id as u64) < u32::MAX as u64,
            "interner capacity exceeded: cannot intern more than {} strings",
            u32::MAX
        );
        let id = id as u32;
        self.strings.push(s.to_owned());
        self.map.insert(s.to_owned(), id);
        id
    }

    /// Resolves a `u32` ID back to the original interned string.
    ///
    /// Returns `None` if the ID was never returned by [`intern()`](Interner::intern).
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Interner;
    ///
    /// let mut interner = Interner::new();
    /// let id = interner.intern("world");
    /// assert_eq!(interner.resolve(id), Some("world"));
    /// assert_eq!(interner.resolve(999), None);
    /// ```
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(|s| s.as_str())
    }

    /// Returns the number of unique interned strings.
    ///
    /// Duplicates are not counted: interning the same string multiple times
    /// does not increase the length.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Interner;
    ///
    /// let mut interner = Interner::new();
    /// assert_eq!(interner.len(), 0);
    /// interner.intern("a");
    /// assert_eq!(interner.len(), 1);
    /// interner.intern("a"); // duplicate
    /// assert_eq!(interner.len(), 1); // still 1
    /// ```
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Returns `true` if the interner contains no interned strings.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Interner;
    ///
    /// let mut interner = Interner::new();
    /// assert!(interner.is_empty());
    /// interner.intern("x");
    /// assert!(!interner.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}

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
        interner.intern("a"); // Duplicate
        assert_eq!(interner.len(), 1); // Still 1
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

    #[test]
    fn is_empty_reflects_state() {
        let mut interner = Interner::new();
        assert!(interner.is_empty());
        interner.intern("something");
        assert!(!interner.is_empty());
    }

    #[test]
    fn default_creates_empty() {
        let interner = Interner::default();
        assert!(interner.is_empty());
        assert_eq!(interner.len(), 0);
        assert_eq!(interner.resolve(0), None);
    }
}
