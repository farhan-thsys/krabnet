//! Set-Trie data structure for efficient set containment and intersection queries.
//!
//! A trie over sorted sets that enables O(|pattern|) lookups instead of O(n)
//! HashMap-based posting list scans. Each path in the trie represents a sorted
//! set of element IDs, and terminal nodes store associated values (frame IDs).
//!
//! # Operations
//!
//! - **Insert:** Walk/create trie path for each element in a sorted set,
//!   store the value (frame ID) at the terminal node.
//! - **Remove:** Walk trie path, remove value from terminal, prune empty nodes.
//! - **Query containing:** Find all values whose registered set CONTAINS all
//!   given query elements (intersection of posting lists).
//! - **Query intersecting:** Find all values whose registered set shares at
//!   least one element with the query (union of posting lists).
//!
//! # Example
//!
//! ```
//! use krabnet::set_trie::SetTrie;
//!
//! let mut trie = SetTrie::new();
//! trie.insert(&[1, 3, 5], 100); // frame 100 covers elements {1, 3, 5}
//! trie.insert(&[1, 2, 3], 200); // frame 200 covers elements {1, 2, 3}
//!
//! // Frames whose set intersects with {1} => both 100 and 200
//! let result = trie.query_intersecting(&[1]);
//! assert!(result.contains(&100));
//! assert!(result.contains(&200));
//!
//! // Frames whose set contains all of {1, 3} => both 100 and 200
//! let result = trie.query_containing(&[1, 3]);
//! assert!(result.contains(&100));
//! assert!(result.contains(&200));
//! ```

use std::collections::{HashMap, HashSet};

/// A node in the Set-Trie.
///
/// Each node may have children keyed by element IDs and a set of values
/// (frame IDs) associated with sets that include the path to this node.
#[derive(Debug, Clone)]
struct SetTrieNode {
    /// Children keyed by element ID.
    children: HashMap<u64, SetTrieNode>,
    /// Values (frame IDs) stored at this node.
    values: HashSet<u64>,
}

impl SetTrieNode {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            values: HashSet::new(),
        }
    }

    /// Returns true if this node has no children and no values.
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.values.is_empty()
    }
}

/// A trie over sorted sets for efficient containment and intersection queries.
///
/// Stores associations between sorted element sets and values (frame IDs).
/// Supports O(|pattern|) lookups for both containment and intersection semantics.
///
/// Elements must be sorted in ascending order before insertion or query.
/// The trie stores values at terminal nodes (the last element in the sorted set path).
#[derive(Debug, Clone)]
pub struct SetTrie {
    root: SetTrieNode,
}

impl SetTrie {
    /// Creates a new, empty Set-Trie.
    pub fn new() -> Self {
        Self {
            root: SetTrieNode::new(),
        }
    }

    /// Inserts a sorted set of elements associated with a value (frame ID).
    ///
    /// Walks or creates the trie path for each element in the sorted set,
    /// and stores the value at the terminal node (last element).
    ///
    /// # Arguments
    ///
    /// * `elements` - A sorted (ascending) slice of element IDs.
    /// * `value` - The value (frame ID) to associate with this set.
    ///
    /// # Panics
    ///
    /// Does not panic. Empty element slices are no-ops (value stored at root).
    pub fn insert(&mut self, elements: &[u64], value: u64) {
        let mut node = &mut self.root;
        for &elem in elements {
            node = node.children.entry(elem).or_insert_with(SetTrieNode::new);
        }
        node.values.insert(value);
    }

    /// Removes a value from the set path defined by the given sorted elements.
    ///
    /// Walks the trie path, removes the value from the terminal node, and
    /// prunes empty nodes on the way back up.
    ///
    /// # Arguments
    ///
    /// * `elements` - A sorted (ascending) slice of element IDs (same as used during insert).
    /// * `value` - The value (frame ID) to remove.
    pub fn remove(&mut self, elements: &[u64], value: u64) {
        Self::remove_recursive(&mut self.root, elements, 0, value);
    }

    /// Recursive helper for remove that prunes empty nodes after removal.
    fn remove_recursive(node: &mut SetTrieNode, elements: &[u64], depth: usize, value: u64) -> bool {
        if depth == elements.len() {
            node.values.remove(&value);
            return node.is_empty();
        }

        let elem = elements[depth];
        let should_remove = if let Some(child) = node.children.get_mut(&elem) {
            Self::remove_recursive(child, elements, depth + 1, value)
        } else {
            return false;
        };

        if should_remove {
            node.children.remove(&elem);
        }

        node.is_empty()
    }

    /// Finds all values whose registered set CONTAINS all given query elements.
    ///
    /// Returns the intersection of posting lists: frame IDs that appear at
    /// terminal nodes reachable via paths that include ALL query elements.
    ///
    /// # Arguments
    ///
    /// * `elements` - A sorted (ascending) slice of query element IDs.
    ///
    /// # Returns
    ///
    /// A `HashSet<u64>` of values (frame IDs) whose registered element sets
    /// contain all of the query elements.
    pub fn query_containing(&self, elements: &[u64]) -> HashSet<u64> {
        if elements.is_empty() {
            return self.collect_all_values(&self.root);
        }

        // For containment: we need all values whose stored set includes ALL query elements.
        // Strategy: for each query element, collect all values reachable through that element's
        // subtree, then intersect the result sets.
        let mut result: Option<HashSet<u64>> = None;

        for &elem in elements {
            let mut elem_values = HashSet::new();
            // Collect all values from any path that goes through this element
            self.collect_values_through_element(&self.root, elem, &mut elem_values);

            result = Some(match result {
                None => elem_values,
                Some(existing) => existing.intersection(&elem_values).copied().collect(),
            });
        }

        result.unwrap_or_default()
    }

    /// Collects all values reachable from paths that include the given element.
    ///
    /// Searches the trie for any node keyed by `element`, then collects all
    /// values in that subtree (including the element node itself).
    fn collect_values_through_element(&self, node: &SetTrieNode, element: u64, result: &mut HashSet<u64>) {
        // If this node has a child for the element, collect all values in that subtree
        if let Some(child) = node.children.get(&element) {
            self.collect_all_values_into(child, result);
        }

        // Also search deeper: the element might appear further down the trie
        for child in node.children.values() {
            self.collect_values_through_element(child, element, result);
        }
    }

    /// Finds all values whose registered set INTERSECTS with the given elements.
    ///
    /// Returns the union of posting lists: frame IDs that appear at terminal
    /// nodes reachable via paths that include ANY of the query elements.
    ///
    /// # Arguments
    ///
    /// * `elements` - A sorted (ascending) slice of query element IDs.
    ///
    /// # Returns
    ///
    /// A `HashSet<u64>` of values (frame IDs) whose registered element sets
    /// share at least one element with the query.
    pub fn query_intersecting(&self, elements: &[u64]) -> HashSet<u64> {
        let mut result = HashSet::new();

        for &elem in elements {
            self.collect_values_through_element(&self.root, elem, &mut result);
        }

        result
    }

    /// Counts the total number of unique values stored across all nodes.
    pub fn len(&self) -> usize {
        let values = self.collect_all_values(&self.root);
        values.len()
    }

    /// Returns true if the trie contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Collects all values in the subtree rooted at `node`.
    fn collect_all_values(&self, node: &SetTrieNode) -> HashSet<u64> {
        let mut result = HashSet::new();
        self.collect_all_values_into(node, &mut result);
        result
    }

    /// Collects all values in the subtree rooted at `node` into `result`.
    fn collect_all_values_into(&self, node: &SetTrieNode, result: &mut HashSet<u64>) {
        result.extend(&node.values);
        for child in node.children.values() {
            self.collect_all_values_into(child, result);
        }
    }
}

impl Default for SetTrie {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query_containing() {
        let mut trie = SetTrie::new();
        // Insert 5 sets with associated frame IDs
        trie.insert(&[1, 2, 3], 100);     // frame 100: {1, 2, 3}
        trie.insert(&[1, 3, 5], 200);     // frame 200: {1, 3, 5}
        trie.insert(&[2, 3, 4], 300);     // frame 300: {2, 3, 4}
        trie.insert(&[1, 2, 3, 4], 400);  // frame 400: {1, 2, 3, 4}
        trie.insert(&[5, 6, 7], 500);     // frame 500: {5, 6, 7}

        // Query: sets containing {1, 3}
        // Should return 100 ({1,2,3}), 200 ({1,3,5}), 400 ({1,2,3,4})
        let result = trie.query_containing(&[1, 3]);
        assert!(result.contains(&100), "frame 100 should contain {{1,3}}");
        assert!(result.contains(&200), "frame 200 should contain {{1,3}}");
        assert!(!result.contains(&300), "frame 300 should NOT contain {{1,3}}");
        assert!(result.contains(&400), "frame 400 should contain {{1,3}}");
        assert!(!result.contains(&500), "frame 500 should NOT contain {{1,3}}");

        // Query: sets containing {2, 3, 4}
        // Should return 300 ({2,3,4}), 400 ({1,2,3,4})
        let result = trie.query_containing(&[2, 3, 4]);
        assert!(!result.contains(&100));
        assert!(!result.contains(&200));
        assert!(result.contains(&300));
        assert!(result.contains(&400));
        assert!(!result.contains(&500));

        // Query: sets containing {5}
        // Should return 200 ({1,3,5}), 500 ({5,6,7})
        let result = trie.query_containing(&[5]);
        assert!(!result.contains(&100));
        assert!(result.contains(&200));
        assert!(!result.contains(&300));
        assert!(!result.contains(&400));
        assert!(result.contains(&500));
    }

    #[test]
    fn test_query_intersecting() {
        let mut trie = SetTrie::new();
        trie.insert(&[1, 2, 3], 100);
        trie.insert(&[3, 4, 5], 200);
        trie.insert(&[6, 7, 8], 300);

        // Query: intersecting with {3} => 100 and 200 (both have 3)
        let result = trie.query_intersecting(&[3]);
        assert!(result.contains(&100));
        assert!(result.contains(&200));
        assert!(!result.contains(&300));

        // Query: intersecting with {1, 6} => 100 (has 1) and 300 (has 6)
        let result = trie.query_intersecting(&[1, 6]);
        assert!(result.contains(&100));
        assert!(!result.contains(&200));
        assert!(result.contains(&300));

        // Query: intersecting with {9} => none
        let result = trie.query_intersecting(&[9]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_remove() {
        let mut trie = SetTrie::new();
        trie.insert(&[1, 2, 3], 100);
        trie.insert(&[1, 2, 3], 200); // two values on same set

        assert_eq!(trie.len(), 2);

        // Remove frame 100 from set {1,2,3}
        trie.remove(&[1, 2, 3], 100);

        // Frame 100 should no longer appear in queries
        let result = trie.query_intersecting(&[1]);
        assert!(!result.contains(&100));
        assert!(result.contains(&200));

        assert_eq!(trie.len(), 1);

        // Remove frame 200
        trie.remove(&[1, 2, 3], 200);
        assert_eq!(trie.len(), 0);

        // Everything should be empty now
        let result = trie.query_intersecting(&[1, 2, 3]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_trie() {
        let trie = SetTrie::new();

        assert_eq!(trie.len(), 0);
        assert!(trie.is_empty());
        assert!(trie.query_containing(&[1, 2, 3]).is_empty());
        assert!(trie.query_intersecting(&[1, 2, 3]).is_empty());
    }

    #[test]
    fn test_duplicate_insert() {
        let mut trie = SetTrie::new();
        trie.insert(&[1, 2, 3], 100);
        trie.insert(&[1, 2, 3], 100); // duplicate

        // Should be idempotent -- still only one value
        assert_eq!(trie.len(), 1);

        let result = trie.query_intersecting(&[1]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&100));
    }
}
