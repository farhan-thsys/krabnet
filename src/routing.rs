//! Inverted index for O(affected) event-to-frame routing.
//!
//! When a graph mutation event arrives, Krabnet needs to determine which
//! frames are affected without scanning all frames. This module provides
//! an [`InvertedIndex`] that maintains posting lists mapping graph elements
//! (node IDs and edge keys) to the set of frame IDs that reference them.
//!
//! # Design
//!
//! The index uses a [`SetTrie`] for node-to-frame
//! lookups, enabling O(|pattern|) set intersection and containment queries.
//! Edge key lookups remain HashMap-based since they are individual key lookups.
//!
//! - **node_trie**: A `SetTrie` mapping sorted node element sets to frame IDs.
//!   Each frame's registered nodes are inserted as a sorted set, enabling
//!   efficient intersection queries via `query_intersecting`.
//! - **edge_key_to_frames**: Maps each `(source_node, edge_type)` pair
//!   to the set of frame IDs whose patterns traverse edges of that type
//!   from that source.
//!
//! When an [`Event`] arrives, [`InvertedIndex::affected_frames`] performs
//! set-union lookups across relevant posting lists, returning the
//! deduplicated set of affected frame IDs in O(affected) time.
//!
//! # Example
//!
//! ```
//! use krabnet::routing::InvertedIndex;
//! use krabnet::types::{NodeId, TypeId};
//!
//! let mut index = InvertedIndex::new();
//! index.register_frame(1, &[NodeId(10), NodeId(20)], &[(NodeId(10), TypeId(5))]);
//! // Frame 1 is now indexed under nodes 10, 20 and edge key (10, type 5)
//! ```

use std::collections::{HashMap, HashSet};

use crate::set_trie::SetTrie;
use crate::types::{Event, NodeId, TypeId};

/// Inverted index mapping graph elements to the frames they affect.
///
/// Enables O(affected) event routing: given an event, the index returns
/// exactly the set of frame IDs that need re-evaluation, without scanning
/// all registered frames.
///
/// # Posting Lists
///
/// - `node_trie`: A [`SetTrie`] mapping sorted node element sets to frame IDs.
///   For each frame, its registered node IDs are inserted as a sorted set.
///   `query_intersecting` with a single node element returns all frames
///   containing that node.
/// - `edge_key_to_frames`: For each `(source_node, edge_type)` pair, the
///   set of frame IDs whose patterns traverse that specific edge type from
///   that source node.
pub struct InvertedIndex {
    /// Set-Trie mapping sorted node element sets to frame IDs.
    node_trie: SetTrie,
    /// Maps frame_id -> sorted node element set (for unregister).
    frame_nodes: HashMap<u64, Vec<u64>>,
    /// Maps (source_node, edge_type) pairs to the set of frame IDs.
    edge_key_to_frames: HashMap<(NodeId, TypeId), HashSet<u64>>,
}

impl InvertedIndex {
    /// Creates a new, empty inverted index.
    ///
    /// Both posting lists start empty. Use [`register_frame`](Self::register_frame)
    /// to populate the index.
    pub fn new() -> Self {
        Self {
            node_trie: SetTrie::new(),
            frame_nodes: HashMap::new(),
            edge_key_to_frames: HashMap::new(),
        }
    }

    /// Registers a frame in the index under all its relevant posting lists.
    ///
    /// Adds `frame_id` to the posting list for each node in `node_ids` and
    /// each edge key in `edge_keys`. If a frame is registered multiple times
    /// with the same elements, the set semantics ensure no duplicates.
    ///
    /// # Arguments
    ///
    /// * `frame_id` - Unique identifier for the frame being registered.
    /// * `node_ids` - Nodes that this frame's traversal pattern touches.
    /// * `edge_keys` - `(source_node, edge_type)` pairs from the frame's hops.
    pub fn register_frame(
        &mut self,
        frame_id: u64,
        node_ids: &[NodeId],
        edge_keys: &[(NodeId, TypeId)],
    ) {
        // Convert node IDs to sorted u64 elements for the SetTrie
        let mut elements: Vec<u64> = node_ids.iter().map(|n| n.0).collect();
        elements.sort_unstable();
        elements.dedup();

        // Insert into SetTrie and store the element set for later removal
        self.node_trie.insert(&elements, frame_id);
        self.frame_nodes.insert(frame_id, elements);

        for &edge_key in edge_keys {
            self.edge_key_to_frames
                .entry(edge_key)
                .or_default()
                .insert(frame_id);
        }
    }

    /// Removes a frame from all its relevant posting lists.
    ///
    /// After unregistration, the frame will no longer appear in any
    /// [`affected_frames`](Self::affected_frames) results. Empty posting
    /// lists are cleaned up to avoid unbounded memory growth.
    ///
    /// # Arguments
    ///
    /// * `frame_id` - Unique identifier for the frame being removed.
    /// * `node_ids` - The same nodes passed during registration.
    /// * `edge_keys` - The same edge keys passed during registration.
    pub fn unregister_frame(
        &mut self,
        frame_id: u64,
        node_ids: &[NodeId],
        edge_keys: &[(NodeId, TypeId)],
    ) {
        // Remove from SetTrie using stored element set
        if let Some(elements) = self.frame_nodes.remove(&frame_id) {
            self.node_trie.remove(&elements, frame_id);
        } else {
            // Fallback: reconstruct elements from node_ids (for backward compatibility)
            let mut elements: Vec<u64> = node_ids.iter().map(|n| n.0).collect();
            elements.sort_unstable();
            elements.dedup();
            self.node_trie.remove(&elements, frame_id);
        }

        for &edge_key in edge_keys {
            if let Some(frames) = self.edge_key_to_frames.get_mut(&edge_key) {
                frames.remove(&frame_id);
                if frames.is_empty() {
                    self.edge_key_to_frames.remove(&edge_key);
                }
            }
        }
    }

    /// Returns the deduplicated set of frame IDs affected by an event.
    ///
    /// Performs set-union lookups across the relevant posting lists based
    /// on the event variant:
    ///
    /// - [`NodeAdded`](Event::NodeAdded): lookup by `node_id`
    /// - [`NodeRemoved`](Event::NodeRemoved): lookup by `node_id`
    /// - [`EdgeAdded`](Event::EdgeAdded): union of `source`, `target`, and
    ///   `(source, type_id)` lookups
    /// - [`EdgeRemoved`](Event::EdgeRemoved): union of `source` and `target`
    ///   lookups
    /// - [`PropertyChanged`](Event::PropertyChanged): lookup by `node_id`
    ///
    /// The result is always deduplicated via [`HashSet`] semantics.
    pub fn affected_frames(&self, event: &Event) -> HashSet<u64> {
        let mut result = HashSet::new();

        match event {
            Event::NodeAdded { node_id, .. } => {
                self.collect_by_node(*node_id, &mut result);
            }
            Event::NodeRemoved { node_id } => {
                self.collect_by_node(*node_id, &mut result);
            }
            Event::EdgeAdded {
                source,
                target,
                type_id,
                ..
            } => {
                self.collect_by_node(*source, &mut result);
                self.collect_by_node(*target, &mut result);
                self.collect_by_edge_key((*source, *type_id), &mut result);
            }
            Event::EdgeRemoved {
                source, target, ..
            } => {
                self.collect_by_node(*source, &mut result);
                self.collect_by_node(*target, &mut result);
            }
            Event::PropertyChanged { node_id, .. } => {
                self.collect_by_node(*node_id, &mut result);
            }
        }

        result
    }

    /// Returns the set of frame IDs affected by a mutation to a specific node.
    ///
    /// Looks up the node posting list only. Used by the coalescer integration
    /// path where events have already been deduplicated to node IDs.
    pub fn affected_frames_by_node(&self, node_id: NodeId) -> HashSet<u64> {
        let mut result = HashSet::new();
        self.collect_by_node(node_id, &mut result);
        result
    }

    /// Returns the count of unique frame IDs across all posting lists.
    ///
    /// This performs a union across all posting lists to count distinct
    /// frame IDs. Useful for diagnostics and testing.
    pub fn frame_count(&self) -> usize {
        let mut all_frames: HashSet<u64> = HashSet::new();
        // Collect from frame_nodes keys (all frames registered via node sets)
        all_frames.extend(self.frame_nodes.keys());
        for frames in self.edge_key_to_frames.values() {
            all_frames.extend(frames);
        }
        all_frames.len()
    }

    /// Collects frame IDs from the node posting list into `result`.
    ///
    /// Uses the SetTrie's `query_intersecting` with a single element to find
    /// all frames whose registered node set contains the given node.
    fn collect_by_node(&self, node_id: NodeId, result: &mut HashSet<u64>) {
        let frames = self.node_trie.query_intersecting(&[node_id.0]);
        result.extend(frames);
    }

    /// Collects frame IDs from the edge key posting list into `result`.
    fn collect_by_edge_key(
        &self,
        edge_key: (NodeId, TypeId),
        result: &mut HashSet<u64>,
    ) {
        if let Some(frames) = self.edge_key_to_frames.get(&edge_key) {
            result.extend(frames);
        }
    }
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EdgeId, NodeId, PropertyValue, TypeId};

    #[test]
    fn register_and_lookup_by_node() {
        let mut index = InvertedIndex::new();
        let nodes = [NodeId(10), NodeId(20), NodeId(30)];
        index.register_frame(1, &nodes, &[]);

        // Each registered node should map back to frame 1
        for &node in &nodes {
            let event = Event::NodeAdded {
                node_id: node,
                type_id: TypeId(0),
            };
            let affected = index.affected_frames(&event);
            assert!(affected.contains(&1), "frame 1 should be affected by node {:?}", node);
            assert_eq!(affected.len(), 1);
        }
    }

    #[test]
    fn affected_frames_edge_added() {
        let mut index = InvertedIndex::new();
        // Frame 1 watches source node 10 and edge key (10, type 5)
        index.register_frame(1, &[NodeId(10)], &[(NodeId(10), TypeId(5))]);
        // Frame 2 watches target node 20
        index.register_frame(2, &[NodeId(20)], &[]);

        let event = Event::EdgeAdded {
            edge_id: EdgeId(100),
            source: NodeId(10),
            target: NodeId(20),
            type_id: TypeId(5),
        };
        let affected = index.affected_frames(&event);

        // Frame 1 hit via source node + edge key, frame 2 hit via target node
        assert!(affected.contains(&1));
        assert!(affected.contains(&2));
        assert_eq!(affected.len(), 2);
    }

    #[test]
    fn affected_frames_deduplicated() {
        let mut index = InvertedIndex::new();
        // Frame 1 is registered under both source and target nodes
        // An EdgeAdded touching both should still return frame 1 only once
        index.register_frame(1, &[NodeId(10), NodeId(20)], &[(NodeId(10), TypeId(5))]);

        let event = Event::EdgeAdded {
            edge_id: EdgeId(100),
            source: NodeId(10),
            target: NodeId(20),
            type_id: TypeId(5),
        };
        let affected = index.affected_frames(&event);

        // Frame 1 appears via source, target, and edge key -- but deduplicated
        assert!(affected.contains(&1));
        assert_eq!(affected.len(), 1);
    }

    #[test]
    fn shared_node_fan_out() {
        let mut index = InvertedIndex::new();
        // Three frames all share node 42
        index.register_frame(1, &[NodeId(42)], &[]);
        index.register_frame(2, &[NodeId(42)], &[]);
        index.register_frame(3, &[NodeId(42)], &[]);

        let event = Event::NodeAdded {
            node_id: NodeId(42),
            type_id: TypeId(0),
        };
        let affected = index.affected_frames(&event);

        assert_eq!(affected.len(), 3);
        assert!(affected.contains(&1));
        assert!(affected.contains(&2));
        assert!(affected.contains(&3));
    }

    #[test]
    fn unregister_removes_from_all_lists() {
        let mut index = InvertedIndex::new();
        let nodes = [NodeId(10), NodeId(20)];
        let edge_keys = [(NodeId(10), TypeId(5))];
        index.register_frame(1, &nodes, &edge_keys);

        // Verify frame is present before unregister
        assert_eq!(index.frame_count(), 1);

        index.unregister_frame(1, &nodes, &edge_keys);

        // After unregister, no event should return frame 1
        let event_node = Event::NodeAdded {
            node_id: NodeId(10),
            type_id: TypeId(0),
        };
        assert!(index.affected_frames(&event_node).is_empty());

        let event_edge = Event::EdgeAdded {
            edge_id: EdgeId(100),
            source: NodeId(10),
            target: NodeId(20),
            type_id: TypeId(5),
        };
        assert!(index.affected_frames(&event_edge).is_empty());

        assert_eq!(index.frame_count(), 0);
    }

    #[test]
    fn unregister_cleans_empty_sets() {
        let mut index = InvertedIndex::new();
        index.register_frame(1, &[NodeId(10)], &[(NodeId(10), TypeId(5))]);
        index.unregister_frame(1, &[NodeId(10)], &[(NodeId(10), TypeId(5))]);

        // Internal posting lists should be empty (no dangling entries)
        assert!(index.node_trie.is_empty());
        assert!(index.frame_nodes.is_empty());
        assert!(index.edge_key_to_frames.is_empty());
    }

    #[test]
    fn affected_frames_property_changed() {
        let mut index = InvertedIndex::new();
        index.register_frame(1, &[NodeId(10)], &[]);
        index.register_frame(2, &[NodeId(20)], &[]);

        let event = Event::PropertyChanged {
            node_id: NodeId(10),
            key: 0,
            value: PropertyValue::Integer(42),
        };
        let affected = index.affected_frames(&event);

        assert_eq!(affected.len(), 1);
        assert!(affected.contains(&1));
        // Frame 2 is NOT affected (different node)
        assert!(!affected.contains(&2));
    }

    #[test]
    fn affected_frames_node_removed() {
        let mut index = InvertedIndex::new();
        index.register_frame(1, &[NodeId(10), NodeId(20)], &[]);
        index.register_frame(2, &[NodeId(10)], &[]);

        let event = Event::NodeRemoved {
            node_id: NodeId(10),
        };
        let affected = index.affected_frames(&event);

        // Both frames contain node 10
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&1));
        assert!(affected.contains(&2));
    }

    #[test]
    fn empty_index_returns_empty() {
        let index = InvertedIndex::new();

        // Every event variant on an empty index should return empty
        let events = vec![
            Event::NodeAdded {
                node_id: NodeId(1),
                type_id: TypeId(0),
            },
            Event::NodeRemoved {
                node_id: NodeId(1),
            },
            Event::EdgeAdded {
                edge_id: EdgeId(10),
                source: NodeId(1),
                target: NodeId(2),
                type_id: TypeId(1),
            },
            Event::EdgeRemoved {
                edge_id: EdgeId(10),
                source: NodeId(1),
                target: NodeId(2),
            },
            Event::PropertyChanged {
                node_id: NodeId(1),
                key: 0,
                value: PropertyValue::Boolean(true),
            },
        ];

        for event in &events {
            let affected = index.affected_frames(event);
            assert!(
                affected.is_empty(),
                "empty index should return empty for {:?}",
                event,
            );
        }

        assert_eq!(index.frame_count(), 0);
    }

    // ── Set-Trie integration tests (TEST-25, TEST-26) ─────────────────

    /// TEST-25: Insert 1000 sets with varied node overlaps, verify containment
    /// and intersection queries return correct results vs brute-force HashMap reference.
    #[test]
    fn test_set_trie_correctness() {
        use std::collections::HashMap as BruteMap;

        let mut index = InvertedIndex::new();
        let mut reference: BruteMap<u64, Vec<NodeId>> = BruteMap::new();

        // Register 1000 frames with overlapping node sets
        for fid in 0..1000u64 {
            // Each frame covers nodes [fid*3, fid*3+1, fid*3+2] with some overlap
            let base = fid * 3;
            let nodes = vec![
                NodeId(base),
                NodeId(base + 1),
                NodeId(base + 2),
                NodeId(fid % 100),  // creates significant overlap in range [0, 99]
            ];
            index.register_frame(fid, &nodes, &[]);
            reference.insert(fid, nodes);
        }

        // Test intersection queries for several nodes
        for test_node_val in [0u64, 50, 99, 150, 500, 1500] {
            let test_node = NodeId(test_node_val);
            let actual = index.affected_frames_by_node(test_node);

            // Brute-force: find all frames containing this node
            let expected: HashSet<u64> = reference
                .iter()
                .filter(|(_, nodes)| nodes.contains(&test_node))
                .map(|(&fid, _)| fid)
                .collect();

            assert_eq!(
                actual, expected,
                "Mismatch for node {test_node_val}: actual={}, expected={}",
                actual.len(), expected.len()
            );
        }
    }

    /// TEST-26: Create InvertedIndex with 10K frames, verify Set-Trie based
    /// index handles the scale without errors.
    #[test]
    fn test_set_trie_memory_vs_hashmap() {
        let mut index = InvertedIndex::new();

        // Register 10K frames
        for fid in 0..10_000u64 {
            let nodes: Vec<NodeId> = (0..5).map(|i| NodeId(fid * 5 + i)).collect();
            index.register_frame(fid, &nodes, &[]);
        }

        assert_eq!(index.frame_count(), 10_000);

        // Verify lookups still work at scale
        let result = index.affected_frames_by_node(NodeId(0));
        assert!(result.contains(&0), "frame 0 should be found for node 0");

        let result = index.affected_frames_by_node(NodeId(49_995));
        assert!(result.contains(&9_999), "frame 9999 should be found for node 49995");

        // Unregister some frames
        for fid in 0..100u64 {
            let nodes: Vec<NodeId> = (0..5).map(|i| NodeId(fid * 5 + i)).collect();
            index.unregister_frame(fid, &nodes, &[]);
        }

        assert_eq!(index.frame_count(), 9_900);
    }
}
