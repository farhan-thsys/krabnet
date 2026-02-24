//! Inverted index for O(affected) event-to-frame routing.
//!
//! When a graph mutation event arrives, Krabnet needs to determine which
//! frames are affected without scanning all frames. This module provides
//! an [`InvertedIndex`] that maintains posting lists mapping graph elements
//! (node IDs and edge keys) to the set of frame IDs that reference them.
//!
//! # Design
//!
//! The index maintains two posting lists:
//! - **node_to_frames**: Maps each [`NodeId`] to the set of frame IDs
//!   whose traversal patterns touch that node.
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

use crate::types::{Event, NodeId, TypeId};

/// Inverted index mapping graph elements to the frames they affect.
///
/// Enables O(affected) event routing: given an event, the index returns
/// exactly the set of frame IDs that need re-evaluation, without scanning
/// all registered frames.
///
/// # Posting Lists
///
/// - `node_to_frames`: For each [`NodeId`], the set of frame IDs whose
///   traversal patterns include that node (as anchor, intermediate, or leaf).
/// - `edge_key_to_frames`: For each `(source_node, edge_type)` pair, the
///   set of frame IDs whose patterns traverse that specific edge type from
///   that source node.
pub struct InvertedIndex {
    /// Maps node IDs to the set of frame IDs containing that node.
    node_to_frames: HashMap<NodeId, HashSet<u64>>,
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
            node_to_frames: HashMap::new(),
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
        for &node_id in node_ids {
            self.node_to_frames
                .entry(node_id)
                .or_default()
                .insert(frame_id);
        }
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
        for &node_id in node_ids {
            if let Some(frames) = self.node_to_frames.get_mut(&node_id) {
                frames.remove(&frame_id);
                if frames.is_empty() {
                    self.node_to_frames.remove(&node_id);
                }
            }
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

    /// Returns the count of unique frame IDs across all posting lists.
    ///
    /// This performs a union across all posting lists to count distinct
    /// frame IDs. Useful for diagnostics and testing.
    pub fn frame_count(&self) -> usize {
        let mut all_frames: HashSet<u64> = HashSet::new();
        for frames in self.node_to_frames.values() {
            all_frames.extend(frames);
        }
        for frames in self.edge_key_to_frames.values() {
            all_frames.extend(frames);
        }
        all_frames.len()
    }

    /// Collects frame IDs from the node posting list into `result`.
    fn collect_by_node(&self, node_id: NodeId, result: &mut HashSet<u64>) {
        if let Some(frames) = self.node_to_frames.get(&node_id) {
            result.extend(frames);
        }
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
