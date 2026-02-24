//! Parked traversers with multi-hop DFS materialization.
//!
//! Frames are the core value proposition of Krabnet: pre-materialized graph
//! traversal results that are incrementally maintained via differential
//! operations. A frame holds a multi-hop pattern ([`Vec<HopSpec>`]), materializes
//! by DFS from an anchor node, stores complete paths as +1 assertions in a
//! [`DiffCollection`], and supports delta application, eviction, and
//! re-materialization.
//!
//! # Design
//!
//! Each frame is anchored at a single [`NodeId`] and defines a traversal pattern
//! as a sequence of [`HopSpec`] steps. Materialization performs a depth-first
//! search from the anchor, following each hop's direction and edge-type filter,
//! then checking target-type and property filters on reached nodes. Complete
//! paths (length == hops + 1, including the anchor) are asserted into the
//! frame's [`DiffCollection`].
//!
//! Incremental maintenance is achieved via `apply_delta`, which asserts or
//! retracts individual paths without re-traversing the graph. Eviction clears
//! state for memory pressure relief, and re-materialization restores the same
//! paths from the current graph.
//!
//! # Usage
//!
//! ```
//! use krabnet::{Frame, Graph, NodeId, TypeId, Direction, HopSpec, Filter, Epoch};
//!
//! let mut g = Graph::new();
//! g.add_node(NodeId(1), TypeId(10));
//! g.add_node(NodeId(2), TypeId(20));
//! g.add_edge(NodeId(1), NodeId(2), TypeId(100));
//!
//! let pattern = vec![HopSpec {
//!     direction: Direction::Outgoing,
//!     edge_type: Some(TypeId(100)),
//!     target_type: Some(TypeId(20)),
//!     filter: Filter::None,
//! }];
//!
//! let mut frame = Frame::new(1, NodeId(1), pattern);
//! frame.materialize(&g, Epoch(1));
//! assert_eq!(frame.query().len(), 1);
//! ```

use crate::diff::{CompactionResult, DiffCollection};
use crate::graph::Graph;
use crate::types::{Delta, Epoch, Filter, FrameTier, HopSpec, NodeId};

/// A parked traverser that materializes and maintains multi-hop graph patterns.
///
/// Frames hold a traversal pattern anchored at a specific node. On
/// materialization, the frame performs DFS from the anchor following the
/// pattern hops, asserting each complete path into its [`DiffCollection`].
/// Subsequent mutations are applied incrementally via [`apply_delta`](Frame::apply_delta).
///
/// # Invariants
///
/// - [`query_count`](Frame::query_count) is incremented only by [`query`](Frame::query),
///   never by [`snapshot`](Frame::snapshot).
/// - [`net_delta`](Frame::net_delta) always equals the aggregate net delta of
///   the underlying [`DiffCollection`].
/// - After [`evict`](Frame::evict), the state is empty and tier is [`FrameTier::Cold`].
#[derive(Debug)]
pub struct Frame {
    /// Unique frame identifier.
    id: u64,
    /// Root node for materialization.
    anchor: NodeId,
    /// Multi-hop traversal pattern.
    pattern: Vec<HopSpec>,
    /// Materialized paths stored as differential tuples.
    state: DiffCollection<Vec<NodeId>>,
    /// Current temperature tier.
    tier: FrameTier,
    /// Number of times this frame has been queried.
    query_count: u64,
    /// Number of deltas applied to this frame.
    mutation_count: u64,
    /// Last epoch at which state was modified.
    last_epoch: Option<Epoch>,
    /// Cached aggregate net delta from DiffCollection.
    net_delta: i64,
}

impl Frame {
    /// Creates a new empty frame with the given pattern, anchored at the
    /// specified node. The frame starts in [`FrameTier::Cold`] with no
    /// materialized state.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Frame, NodeId, HopSpec, Direction, Filter, FrameTier};
    ///
    /// let pattern = vec![HopSpec {
    ///     direction: Direction::Outgoing,
    ///     edge_type: None,
    ///     target_type: None,
    ///     filter: Filter::None,
    /// }];
    /// let frame = Frame::new(1, NodeId(42), pattern);
    /// ```
    pub fn new(id: u64, anchor: NodeId, pattern: Vec<HopSpec>) -> Self {
        Self {
            id,
            anchor,
            pattern,
            state: DiffCollection::new(),
            tier: FrameTier::Cold,
            query_count: 0,
            mutation_count: 0,
            last_epoch: None,
            net_delta: 0,
        }
    }

    /// Materializes the frame by DFS from the anchor node following the
    /// hop pattern.
    ///
    /// For each complete path (length == hops + 1, including anchor),
    /// asserts the path into the [`DiffCollection`] at the given epoch.
    /// Each hop filters by direction, edge type, target node type, and
    /// property filter.
    ///
    /// This method does **not** clear existing state; call [`evict`](Frame::evict)
    /// first if a clean materialization is desired.
    pub fn materialize(&mut self, graph: &Graph, epoch: Epoch) {
        let mut paths: Vec<Vec<NodeId>> = Vec::new();
        let initial_path = vec![self.anchor];
        self.dfs_collect(graph, &initial_path, 0, &mut paths);

        for path in paths {
            self.state.assert_tuple(path, epoch);
        }
        self.last_epoch = Some(epoch);
        self.net_delta = self.state.aggregate_net_delta();
    }

    /// Recursive DFS helper that collects complete paths matching the pattern.
    ///
    /// At each hop level, retrieves neighbors filtered by direction and edge
    /// type, then checks target type and property filters on each candidate.
    /// When all hops are exhausted, the accumulated path is added to results.
    fn dfs_collect(
        &self,
        graph: &Graph,
        current_path: &[NodeId],
        hop_index: usize,
        results: &mut Vec<Vec<NodeId>>,
    ) {
        // Base case: all hops consumed, path is complete.
        if hop_index >= self.pattern.len() {
            results.push(current_path.to_vec());
            return;
        }

        let hop = &self.pattern[hop_index];
        let current_node = *current_path.last().expect("path must be non-empty");

        // Get neighbors filtered by direction and edge type.
        let neighbors = graph.neighbors(current_node, hop.direction, hop.edge_type);

        for (_edge_id, neighbor_id) in neighbors {
            // Check target type filter.
            if let Some(target_type) = hop.target_type {
                if graph.get_node_type(neighbor_id) != Some(target_type) {
                    continue;
                }
            }

            // Check property filter.
            match &hop.filter {
                Filter::None => {}
                Filter::PropertyEquals { key, value } => {
                    if graph.get_property(neighbor_id, *key) != Some(value) {
                        continue;
                    }
                }
                Filter::HasProperty { key } => {
                    if graph.get_property(neighbor_id, *key).is_none() {
                        continue;
                    }
                }
            }

            // Extend path and recurse to the next hop.
            let mut next_path = current_path.to_vec();
            next_path.push(neighbor_id);
            self.dfs_collect(graph, &next_path, hop_index + 1, results);
        }
    }

    /// Applies a delta (+1 or -1) to a specific path in the frame's state.
    ///
    /// Updates `mutation_count`, `last_epoch`, and `net_delta` accordingly.
    pub fn apply_delta(&mut self, path: Vec<NodeId>, epoch: Epoch, delta: Delta) {
        if delta.0 >= 0 {
            self.state.assert_tuple(path, epoch);
        } else {
            self.state.retract_tuple(path, epoch);
        }
        self.mutation_count += 1;
        self.last_epoch = Some(epoch);
        self.net_delta = self.state.aggregate_net_delta();
    }

    /// Returns the current state: paths with positive net delta.
    ///
    /// Increments `query_count` on each call. This is the primary read
    /// path for frames.
    pub fn query(&mut self) -> Vec<&Vec<NodeId>> {
        self.query_count += 1;
        self.state.current_state()
    }

    /// Returns a temporal snapshot of paths at the given epoch.
    ///
    /// Does **not** increment `query_count`. Useful for historical queries
    /// and debugging.
    pub fn snapshot(&self, epoch: Epoch) -> Vec<&Vec<NodeId>> {
        self.state.snapshot(epoch)
    }

    /// Compacts the underlying [`DiffCollection`] at the given frontier epoch.
    ///
    /// Delegates directly to [`DiffCollection::compact`] and updates the
    /// cached `net_delta`.
    pub fn compact(&mut self, frontier: Epoch) -> CompactionResult {
        let result = self.state.compact(frontier);
        self.net_delta = self.state.aggregate_net_delta();
        result
    }

    /// Evicts the frame: clears all state and sets tier to [`FrameTier::Cold`].
    ///
    /// After eviction, the frame contains no materialized paths and
    /// `net_delta` is 0.
    pub fn evict(&mut self) {
        self.state = DiffCollection::new();
        self.tier = FrameTier::Cold;
        self.net_delta = 0;
    }

    /// Evicts then re-materializes the frame from the current graph.
    ///
    /// Combines [`evict`](Frame::evict) and [`materialize`](Frame::materialize)
    /// into a single operation.
    pub fn rematerialize(&mut self, graph: &Graph, epoch: Epoch) {
        self.evict();
        self.materialize(graph, epoch);
    }

    // ── Accessors ──────────────────────────────────────────────────────

    /// Returns the frame's unique identifier.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the frame's anchor node.
    pub fn anchor(&self) -> NodeId {
        self.anchor
    }

    /// Returns a reference to the frame's traversal pattern.
    pub fn pattern(&self) -> &[HopSpec] {
        &self.pattern
    }

    /// Returns the frame's current temperature tier.
    pub fn tier(&self) -> FrameTier {
        self.tier
    }

    /// Sets the frame's temperature tier.
    pub fn set_tier(&mut self, tier: FrameTier) {
        self.tier = tier;
    }

    /// Returns the number of times this frame has been queried.
    pub fn query_count(&self) -> u64 {
        self.query_count
    }

    /// Returns the number of deltas applied to this frame.
    pub fn mutation_count(&self) -> u64 {
        self.mutation_count
    }

    /// Returns the last epoch at which state was modified.
    pub fn last_epoch(&self) -> Option<Epoch> {
        self.last_epoch
    }

    /// Returns the cached aggregate net delta.
    pub fn net_delta(&self) -> i64 {
        self.net_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Direction, Filter, PropertyValue, TypeId};
    use std::collections::HashSet;

    /// Helper: builds a simple linear graph A -> B -> C with the given types.
    fn build_two_hop_graph() -> Graph {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10)); // A
        g.add_node(NodeId(2), TypeId(20)); // B
        g.add_node(NodeId(3), TypeId(30)); // C

        g.add_edge(NodeId(1), NodeId(2), TypeId(100)); // A -> B
        g.add_edge(NodeId(2), NodeId(3), TypeId(200)); // B -> C
        g
    }

    /// Helper: builds a two-hop pattern matching the two_hop_graph.
    fn two_hop_pattern() -> Vec<HopSpec> {
        vec![
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(100)),
                target_type: Some(TypeId(20)),
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(200)),
                target_type: Some(TypeId(30)),
                filter: Filter::None,
            },
        ]
    }

    #[test]
    fn materialize_two_hop_pattern() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(1, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        let paths = frame.query();
        assert_eq!(paths.len(), 1);
        assert_eq!(*paths[0], vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(frame.net_delta(), 1);
    }

    #[test]
    fn materialize_no_matching_path() {
        let g = build_two_hop_graph();

        // Pattern requires edge type 999 which does not exist.
        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(999)),
            target_type: None,
            filter: Filter::None,
        }];

        let mut frame = Frame::new(2, NodeId(1), pattern);
        frame.materialize(&g, Epoch(1));

        let paths = frame.query();
        assert!(paths.is_empty());
        assert_eq!(frame.net_delta(), 0);
    }

    #[test]
    fn materialize_multiple_paths() {
        // A -> B -> C and A -> B -> D
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10)); // A
        g.add_node(NodeId(2), TypeId(20)); // B
        g.add_node(NodeId(3), TypeId(30)); // C
        g.add_node(NodeId(4), TypeId(30)); // D (same type as C)

        g.add_edge(NodeId(1), NodeId(2), TypeId(100)); // A -> B
        g.add_edge(NodeId(2), NodeId(3), TypeId(200)); // B -> C
        g.add_edge(NodeId(2), NodeId(4), TypeId(200)); // B -> D

        let mut frame = Frame::new(3, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        let paths = frame.query();
        assert_eq!(paths.len(), 2);

        let path_set: HashSet<&Vec<NodeId>> = paths.into_iter().collect();
        assert!(path_set.contains(&vec![NodeId(1), NodeId(2), NodeId(3)]));
        assert!(path_set.contains(&vec![NodeId(1), NodeId(2), NodeId(4)]));
        assert_eq!(frame.net_delta(), 2);
    }

    #[test]
    fn materialize_filters_by_edge_type() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(20));

        g.add_edge(NodeId(1), NodeId(2), TypeId(100)); // matches
        g.add_edge(NodeId(1), NodeId(3), TypeId(999)); // wrong edge type

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: None,
            filter: Filter::None,
        }];

        let mut frame = Frame::new(4, NodeId(1), pattern);
        frame.materialize(&g, Epoch(1));

        let paths = frame.query();
        assert_eq!(paths.len(), 1);
        assert_eq!(*paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn materialize_filters_by_target_type() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20)); // matches
        g.add_node(NodeId(3), TypeId(99)); // wrong node type

        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
        g.add_edge(NodeId(1), NodeId(3), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let mut frame = Frame::new(5, NodeId(1), pattern);
        frame.materialize(&g, Epoch(1));

        let paths = frame.query();
        assert_eq!(paths.len(), 1);
        assert_eq!(*paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn materialize_filters_by_property() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(20));

        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
        g.add_edge(NodeId(1), NodeId(3), TypeId(100));

        // Set property on node 2 (matches), not on node 3.
        g.set_property(NodeId(2), 0, PropertyValue::Integer(42));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::PropertyEquals {
                key: 0,
                value: PropertyValue::Integer(42),
            },
        }];

        let mut frame = Frame::new(6, NodeId(1), pattern);
        frame.materialize(&g, Epoch(1));

        let paths = frame.query();
        assert_eq!(paths.len(), 1);
        assert_eq!(*paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn apply_delta_retraction() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(7, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        assert_eq!(frame.net_delta(), 1);
        assert_eq!(frame.mutation_count(), 0);

        // Retract the path.
        frame.apply_delta(
            vec![NodeId(1), NodeId(2), NodeId(3)],
            Epoch(2),
            Delta(-1),
        );

        assert_eq!(frame.net_delta(), 0);
        assert_eq!(frame.mutation_count(), 1);
        assert_eq!(frame.last_epoch(), Some(Epoch(2)));

        // Current state should be empty (net zero).
        let paths = frame.query();
        assert!(paths.is_empty());
    }

    #[test]
    fn query_increments_count() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(8, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        assert_eq!(frame.query_count(), 0);

        frame.query();
        assert_eq!(frame.query_count(), 1);

        frame.query();
        assert_eq!(frame.query_count(), 2);

        frame.query();
        assert_eq!(frame.query_count(), 3);
    }

    #[test]
    fn snapshot_does_not_increment_count() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(9, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        assert_eq!(frame.query_count(), 0);

        let snap = frame.snapshot(Epoch(1));
        assert_eq!(snap.len(), 1);
        assert_eq!(frame.query_count(), 0);

        // Snapshot at epoch 0 should be empty (path was asserted at epoch 1).
        let snap0 = frame.snapshot(Epoch(0));
        assert!(snap0.is_empty());
        assert_eq!(frame.query_count(), 0);
    }

    #[test]
    fn evict_clears_state() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(10, NodeId(1), two_hop_pattern());
        frame.set_tier(FrameTier::Hot);
        frame.materialize(&g, Epoch(1));

        assert_eq!(frame.net_delta(), 1);
        assert_eq!(frame.tier(), FrameTier::Hot);

        frame.evict();

        assert_eq!(frame.net_delta(), 0);
        assert_eq!(frame.tier(), FrameTier::Cold);

        let paths = frame.query();
        assert!(paths.is_empty());
    }

    #[test]
    fn rematerialize_restores_state() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(11, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        // Record paths before eviction.
        let paths_before: Vec<Vec<NodeId>> =
            frame.query().into_iter().cloned().collect();

        // Evict and rematerialize.
        frame.rematerialize(&g, Epoch(2));

        let paths_after: Vec<Vec<NodeId>> =
            frame.query().into_iter().cloned().collect();

        assert_eq!(paths_before, paths_after);
        assert_eq!(frame.tier(), FrameTier::Cold); // evict sets Cold
    }

    #[test]
    fn compact_delegates_to_diff() {
        let g = build_two_hop_graph();
        let mut frame = Frame::new(12, NodeId(1), two_hop_pattern());
        frame.materialize(&g, Epoch(1));

        // Apply a retraction at epoch 2.
        frame.apply_delta(
            vec![NodeId(1), NodeId(2), NodeId(3)],
            Epoch(2),
            Delta(-1),
        );

        // Compact up to epoch 2 -- the path was asserted at 1 and retracted at 2,
        // so it should be annihilated.
        let result = frame.compact(Epoch(2));
        assert_eq!(result.annihilated, 1);
        assert_eq!(result.collapsed, 0);
        assert!(result.warnings.is_empty());
        assert_eq!(frame.net_delta(), 0);
    }
}
