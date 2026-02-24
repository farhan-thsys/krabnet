//! String interner for property keys and type names.
//!
//! Placeholder module -- full implementation in Task 2.

use std::collections::HashMap;

/// Bidirectional string-to-u32 interner.
pub struct Interner {
    map: HashMap<String, u32>,
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
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}
