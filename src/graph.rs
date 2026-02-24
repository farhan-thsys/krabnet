//! In-memory property graph with adjacency-on-node storage.
//!
//! Provides the primary storage layer that frames materialize from. Supports
//! O(1) node lookup via [`HashMap`], efficient neighbor queries by direction
//! and edge type, cascading node removal, and property storage with interned
//! `u32` keys.
//!
//! # Design
//!
//! Each node stores its own adjacency lists (outgoing and incoming edges),
//! avoiding a separate adjacency matrix. This co-locates traversal data with
//! node data for cache-friendly neighbor iteration.
//!
//! Property keys are interned `u32` IDs from [`crate::Interner`], ensuring
//! zero allocation on the hot path. Edge IDs are auto-assigned by an
//! incrementing counter within the graph.
//!
//! # Usage
//!
//! ```
//! use krabnet::{Graph, NodeId, TypeId, Direction};
//!
//! let mut g = Graph::new();
//! g.add_node(NodeId(1), TypeId(0));
//! g.add_node(NodeId(2), TypeId(0));
//! let eid = g.add_edge(NodeId(1), NodeId(2), TypeId(1)).unwrap();
//! let neighbors = g.neighbors(NodeId(1), Direction::Outgoing, None);
//! assert_eq!(neighbors.len(), 1);
//! ```

use std::collections::HashMap;

use crate::types::{Direction, EdgeId, NodeId, PropertyValue, TypeId};

/// Per-node data including type, properties, and adjacency lists.
///
/// Adjacency lists store `(EdgeId, NodeId, TypeId)` tuples for efficient
/// filtered neighbor queries without accessing the edge map.
#[derive(Debug)]
struct NodeData {
    /// The interned type of this node.
    type_id: TypeId,
    /// Properties stored on this node, keyed by interned `u32` property key IDs.
    properties: HashMap<u32, PropertyValue>,
    /// Outgoing edges: `(edge_id, target_node, edge_type)`.
    outgoing: Vec<(EdgeId, NodeId, TypeId)>,
    /// Incoming edges: `(edge_id, source_node, edge_type)`.
    incoming: Vec<(EdgeId, NodeId, TypeId)>,
}

/// Per-edge data: endpoints and type.
///
/// Fields `edge_id` and `type_id` are retained for structural completeness
/// and future use (e.g., edge property storage, serialization). Current
/// neighbor queries read type information from the adjacency list tuples.
#[derive(Debug)]
#[allow(dead_code)]
struct EdgeData {
    /// The unique identifier for this edge.
    edge_id: EdgeId,
    /// The source node of this edge.
    source: NodeId,
    /// The target node of this edge.
    target: NodeId,
    /// The interned type of this edge.
    type_id: TypeId,
}

/// In-memory property graph with adjacency-on-node storage.
///
/// Supports O(1) node and edge lookup, directional neighbor queries with
/// optional edge-type filtering, cascading node removal, and property
/// upsert with interned `u32` keys.
///
/// # Invariants
///
/// - [`node_count()`](Graph::node_count) always equals the number of entries
///   in the internal node map.
/// - [`edge_count()`](Graph::edge_count) always equals the number of entries
///   in the internal edge map.
/// - Every edge in the edge map has corresponding entries in both its source
///   node's `outgoing` list and its target node's `incoming` list.
/// - Removing a node cascades to remove all connected edges and their
///   adjacency entries from all neighbor nodes.
#[derive(Debug)]
pub struct Graph {
    /// All nodes, keyed by [`NodeId`].
    nodes: HashMap<NodeId, NodeData>,
    /// All edges, keyed by [`EdgeId`].
    edges: HashMap<EdgeId, EdgeData>,
    /// Auto-incrementing edge ID counter.
    next_edge_id: u64,
}

impl Graph {
    /// Creates a new empty graph.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::Graph;
    ///
    /// let g = Graph::new();
    /// assert_eq!(g.node_count(), 0);
    /// assert_eq!(g.edge_count(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            next_edge_id: 0,
        }
    }

    // ── Node operations ────────────────────────────────────────────────

    /// Adds a node with the given ID and type. No-op if the node already exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// assert!(g.has_node(NodeId(1)));
    /// ```
    pub fn add_node(&mut self, node_id: NodeId, type_id: TypeId) {
        self.nodes.entry(node_id).or_insert_with(|| NodeData {
            type_id,
            properties: HashMap::new(),
            outgoing: Vec::new(),
            incoming: Vec::new(),
        });
    }

    /// Removes a node and cascades removal of all connected edges.
    ///
    /// All edges where this node is either source or target are removed.
    /// The adjacency lists of all neighbor nodes are updated accordingly.
    ///
    /// Returns `true` if the node existed and was removed, `false` if the
    /// node did not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// assert!(g.remove_node(NodeId(1)));
    /// assert!(!g.has_node(NodeId(1)));
    /// ```
    pub fn remove_node(&mut self, node_id: NodeId) -> bool {
        let node_data = match self.nodes.remove(&node_id) {
            Some(data) => data,
            None => return false,
        };

        // Collect all edge IDs connected to this node (both directions).
        // We need to collect them first because we'll mutate adjacency lists.
        let mut edge_ids_to_remove: Vec<EdgeId> =
            node_data.outgoing.iter().map(|(eid, _, _)| *eid).collect();
        for (eid, _, _) in &node_data.incoming {
            // Avoid duplicates for self-loops.
            if !edge_ids_to_remove.contains(eid) {
                edge_ids_to_remove.push(*eid);
            }
        }

        // Remove each connected edge from the edge map and from neighbor
        // adjacency lists.
        for eid in edge_ids_to_remove {
            if let Some(edge) = self.edges.remove(&eid) {
                // Clean the other endpoint's adjacency list (not the removed node).
                if edge.source != node_id {
                    if let Some(src) = self.nodes.get_mut(&edge.source) {
                        src.outgoing.retain(|(e, _, _)| *e != eid);
                    }
                }
                if edge.target != node_id {
                    if let Some(tgt) = self.nodes.get_mut(&edge.target) {
                        tgt.incoming.retain(|(e, _, _)| *e != eid);
                    }
                }
            }
        }

        true
    }

    /// Returns the type of the given node, or `None` if the node does not exist.
    ///
    /// This is an O(1) lookup via [`HashMap`].
    pub fn get_node_type(&self, node_id: NodeId) -> Option<TypeId> {
        self.nodes.get(&node_id).map(|n| n.type_id)
    }

    /// Returns `true` if the graph contains the given node.
    pub fn has_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }

    /// Returns the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    // ── Edge operations ────────────────────────────────────────────────

    /// Adds a directed edge from `source` to `target` with the given type.
    ///
    /// Returns `Some(EdgeId)` with the auto-assigned edge ID on success.
    /// Returns `None` if either the source or target node does not exist.
    ///
    /// Both the source node's outgoing adjacency list and the target node's
    /// incoming adjacency list are updated.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// g.add_node(NodeId(2), TypeId(0));
    /// let eid = g.add_edge(NodeId(1), NodeId(2), TypeId(1));
    /// assert!(eid.is_some());
    /// ```
    pub fn add_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        type_id: TypeId,
    ) -> Option<EdgeId> {
        // Both endpoints must exist.
        if !self.nodes.contains_key(&source) || !self.nodes.contains_key(&target) {
            return None;
        }

        let edge_id = EdgeId(self.next_edge_id);
        self.next_edge_id += 1;

        self.edges.insert(
            edge_id,
            EdgeData {
                edge_id,
                source,
                target,
                type_id,
            },
        );

        // Update source outgoing.
        // SAFETY: we checked `contains_key` above — unwrap is safe.
        self.nodes
            .get_mut(&source)
            .unwrap()
            .outgoing
            .push((edge_id, target, type_id));

        // Update target incoming.
        self.nodes
            .get_mut(&target)
            .unwrap()
            .incoming
            .push((edge_id, source, type_id));

        Some(edge_id)
    }

    /// Removes an edge by ID. Updates both endpoint adjacency lists.
    ///
    /// Returns `true` if the edge existed and was removed, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// g.add_node(NodeId(2), TypeId(0));
    /// let eid = g.add_edge(NodeId(1), NodeId(2), TypeId(1)).unwrap();
    /// assert!(g.remove_edge(eid));
    /// assert_eq!(g.edge_count(), 0);
    /// ```
    pub fn remove_edge(&mut self, edge_id: EdgeId) -> bool {
        let edge = match self.edges.remove(&edge_id) {
            Some(e) => e,
            None => return false,
        };

        // Clean source outgoing.
        if let Some(src) = self.nodes.get_mut(&edge.source) {
            src.outgoing.retain(|(e, _, _)| *e != edge_id);
        }

        // Clean target incoming.
        if let Some(tgt) = self.nodes.get_mut(&edge.target) {
            tgt.incoming.retain(|(e, _, _)| *e != edge_id);
        }

        true
    }

    /// Returns the number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    // ── Neighbor queries ───────────────────────────────────────────────

    /// Returns neighbors of a node filtered by direction and optional edge type.
    ///
    /// Each result is an `(EdgeId, NodeId)` pair: the edge connecting to
    /// the neighbor and the neighbor's ID.
    ///
    /// - [`Direction::Outgoing`]: nodes reachable via outgoing edges.
    /// - [`Direction::Incoming`]: nodes reachable via incoming edges.
    /// - [`Direction::Any`]: union of both directions.
    ///
    /// If `edge_type` is `Some(t)`, only edges with type `t` are included.
    ///
    /// Returns an empty `Vec` if the node does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId, Direction};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// g.add_node(NodeId(2), TypeId(0));
    /// g.add_edge(NodeId(1), NodeId(2), TypeId(1));
    /// let out = g.neighbors(NodeId(1), Direction::Outgoing, None);
    /// assert_eq!(out.len(), 1);
    /// assert_eq!(out[0].1, NodeId(2));
    /// ```
    pub fn neighbors(
        &self,
        node_id: NodeId,
        direction: Direction,
        edge_type: Option<TypeId>,
    ) -> Vec<(EdgeId, NodeId)> {
        let node = match self.nodes.get(&node_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let filter = |entries: &[(EdgeId, NodeId, TypeId)]| -> Vec<(EdgeId, NodeId)> {
            entries
                .iter()
                .filter(|(_, _, t)| edge_type.is_none_or(|et| *t == et))
                .map(|(eid, nid, _)| (*eid, *nid))
                .collect()
        };

        match direction {
            Direction::Outgoing => filter(&node.outgoing),
            Direction::Incoming => filter(&node.incoming),
            Direction::Any => {
                let mut results = filter(&node.outgoing);
                results.extend(filter(&node.incoming));
                results
            }
        }
    }

    // ── Property operations ────────────────────────────────────────────

    /// Sets (upserts) a property on a node.
    ///
    /// If the node exists, inserts or updates the property and returns `true`.
    /// If the node does not exist, returns `false` without any modification.
    ///
    /// Property keys are interned `u32` IDs from [`crate::Interner`].
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId, PropertyValue};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// assert!(g.set_property(NodeId(1), 0, PropertyValue::Integer(42)));
    /// ```
    pub fn set_property(&mut self, node_id: NodeId, key: u32, value: PropertyValue) -> bool {
        match self.nodes.get_mut(&node_id) {
            Some(node) => {
                node.properties.insert(key, value);
                true
            }
            None => false,
        }
    }

    /// Returns a reference to the property value for the given node and key.
    ///
    /// Returns `None` if the node does not exist or the property key is not set.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::{Graph, NodeId, TypeId, PropertyValue};
    ///
    /// let mut g = Graph::new();
    /// g.add_node(NodeId(1), TypeId(0));
    /// g.set_property(NodeId(1), 0, PropertyValue::Boolean(true));
    /// assert_eq!(g.get_property(NodeId(1), 0), Some(&PropertyValue::Boolean(true)));
    /// ```
    pub fn get_property(&self, node_id: NodeId, key: u32) -> Option<&PropertyValue> {
        self.nodes
            .get(&node_id)
            .and_then(|n| n.properties.get(&key))
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get_node() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        assert!(g.has_node(NodeId(1)));
        assert_eq!(g.get_node_type(NodeId(1)), Some(TypeId(10)));
        assert_eq!(g.node_count(), 1);
    }

    #[test]
    fn remove_node_cascades_edges() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));
        g.add_node(NodeId(3), TypeId(0));

        // 1 -> 2, 3 -> 1
        g.add_edge(NodeId(1), NodeId(2), TypeId(1));
        g.add_edge(NodeId(3), NodeId(1), TypeId(1));
        assert_eq!(g.edge_count(), 2);

        // Remove node 1 -- should cascade both edges.
        assert!(g.remove_node(NodeId(1)));
        assert!(!g.has_node(NodeId(1)));
        assert_eq!(g.edge_count(), 0);

        // Node 2 should have no incoming edges from node 1.
        let n2_incoming = g.neighbors(NodeId(2), Direction::Incoming, None);
        assert!(n2_incoming.is_empty());

        // Node 3 should have no outgoing edges to node 1.
        let n3_outgoing = g.neighbors(NodeId(3), Direction::Outgoing, None);
        assert!(n3_outgoing.is_empty());
    }

    #[test]
    fn add_edge_updates_both_endpoints() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));

        let eid = g.add_edge(NodeId(1), NodeId(2), TypeId(5)).unwrap();

        // Source's outgoing should contain the edge.
        let out = g.neighbors(NodeId(1), Direction::Outgoing, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], (eid, NodeId(2)));

        // Target's incoming should contain the edge.
        let inc = g.neighbors(NodeId(2), Direction::Incoming, None);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0], (eid, NodeId(1)));
    }

    #[test]
    fn remove_edge_updates_both_endpoints() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));

        let eid = g.add_edge(NodeId(1), NodeId(2), TypeId(5)).unwrap();
        assert!(g.remove_edge(eid));

        // Both adjacency lists should be empty.
        assert!(g.neighbors(NodeId(1), Direction::Outgoing, None).is_empty());
        assert!(g.neighbors(NodeId(2), Direction::Incoming, None).is_empty());
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn neighbors_outgoing() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));
        g.add_node(NodeId(3), TypeId(0));

        g.add_edge(NodeId(1), NodeId(2), TypeId(1));
        g.add_edge(NodeId(3), NodeId(1), TypeId(1)); // incoming to 1

        let out = g.neighbors(NodeId(1), Direction::Outgoing, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, NodeId(2));
    }

    #[test]
    fn neighbors_incoming() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));
        g.add_node(NodeId(3), TypeId(0));

        g.add_edge(NodeId(1), NodeId(2), TypeId(1)); // outgoing from 1
        g.add_edge(NodeId(3), NodeId(1), TypeId(1)); // incoming to 1

        let inc = g.neighbors(NodeId(1), Direction::Incoming, None);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].1, NodeId(3));
    }

    #[test]
    fn neighbors_any() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));
        g.add_node(NodeId(3), TypeId(0));

        g.add_edge(NodeId(1), NodeId(2), TypeId(1));
        g.add_edge(NodeId(3), NodeId(1), TypeId(1));

        let any = g.neighbors(NodeId(1), Direction::Any, None);
        assert_eq!(any.len(), 2);
    }

    #[test]
    fn neighbors_filtered_by_edge_type() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));
        g.add_node(NodeId(3), TypeId(0));

        g.add_edge(NodeId(1), NodeId(2), TypeId(10)); // type 10
        g.add_edge(NodeId(1), NodeId(3), TypeId(20)); // type 20

        let filtered = g.neighbors(NodeId(1), Direction::Outgoing, Some(TypeId(10)));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].1, NodeId(2));

        let filtered2 = g.neighbors(NodeId(1), Direction::Outgoing, Some(TypeId(20)));
        assert_eq!(filtered2.len(), 1);
        assert_eq!(filtered2[0].1, NodeId(3));

        // No edges of type 99.
        let none = g.neighbors(NodeId(1), Direction::Outgoing, Some(TypeId(99)));
        assert!(none.is_empty());
    }

    #[test]
    fn property_upsert() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));

        // Set initial value.
        assert!(g.set_property(NodeId(1), 0, PropertyValue::Integer(10)));
        assert_eq!(
            g.get_property(NodeId(1), 0),
            Some(&PropertyValue::Integer(10))
        );

        // Update to new value.
        assert!(g.set_property(NodeId(1), 0, PropertyValue::Integer(20)));
        assert_eq!(
            g.get_property(NodeId(1), 0),
            Some(&PropertyValue::Integer(20))
        );
    }

    #[test]
    fn property_on_missing_node_returns_false() {
        let mut g = Graph::new();
        assert!(!g.set_property(NodeId(999), 0, PropertyValue::Boolean(true)));
        assert_eq!(g.get_property(NodeId(999), 0), None);
    }

    #[test]
    fn node_count_and_edge_count_consistent() {
        let mut g = Graph::new();
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);

        g.add_node(NodeId(1), TypeId(0));
        g.add_node(NodeId(2), TypeId(0));
        g.add_node(NodeId(3), TypeId(0));
        assert_eq!(g.node_count(), 3);

        g.add_edge(NodeId(1), NodeId(2), TypeId(1));
        g.add_edge(NodeId(2), NodeId(3), TypeId(1));
        assert_eq!(g.edge_count(), 2);

        // Remove one edge.
        g.remove_node(NodeId(2));
        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 0); // Both edges cascaded.

        g.remove_node(NodeId(1));
        assert_eq!(g.node_count(), 1);

        g.remove_node(NodeId(3));
        assert_eq!(g.node_count(), 0);
    }

    #[test]
    fn add_edge_to_missing_node_returns_none() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(0));

        // Missing target.
        assert!(g.add_edge(NodeId(1), NodeId(999), TypeId(1)).is_none());

        // Missing source.
        assert!(g.add_edge(NodeId(999), NodeId(1), TypeId(1)).is_none());

        // Both missing.
        assert!(g.add_edge(NodeId(100), NodeId(200), TypeId(1)).is_none());

        assert_eq!(g.edge_count(), 0);
    }
}
