//! Incremental path extension for EdgeAdded events.
//!
//! Given a frame's anchor and hop pattern, computes new complete paths
//! produced by a newly added edge without full DFS re-traverse. The algorithm
//! decomposes into three steps:
//!
//! 1. **Edge-to-hop matching**: For each hop in the pattern, check whether the
//!    new edge could satisfy it (direction, edge type, target type, property filter).
//! 2. **Backward prefix resolution**: Find all partial paths from the anchor through
//!    hops 0..K-1 that end at the node from which the new edge originates.
//! 3. **Forward extension**: Continue DFS from the reached node through remaining
//!    hops K+1..N-1 to produce complete paths.
//!
//! The function is stateless: it takes read-only references to the graph and
//! pattern, returning path deltas that the engine applies via [`crate::Frame::apply_delta`].
//!
//! # Correctness
//!
//! The filter logic in backward prefix and forward extension replicates
//! [`crate::Frame::dfs_collect`] exactly: direction, edge type, target type,
//! and property filter (None, PropertyEquals, HasProperty). Paths are
//! deduplicated via [`std::collections::HashSet`] to prevent double-counting
//! when an edge satisfies multiple hop positions.

use std::collections::HashSet;

use crate::graph::Graph;
use crate::types::{Direction, Filter, HopSpec, NodeId, TypeId};

/// Result of incremental edge-added path extension.
///
/// Contains the new complete paths that should be asserted as +1 deltas
/// into the affected frame's [`crate::diff::DiffCollection`].
#[derive(Debug)]
pub struct EdgeAddedDeltas {
    /// New complete paths to assert as +1 deltas.
    pub new_paths: Vec<Vec<NodeId>>,
}

/// Computes new paths for a frame produced by a newly added edge.
///
/// For each hop position in the pattern that the new edge could satisfy,
/// performs backward prefix resolution and forward extension to produce
/// complete paths. Paths are deduplicated before returning.
///
/// # Arguments
///
/// * `anchor` - The frame's anchor node.
/// * `pattern` - The frame's hop pattern (sequence of [`HopSpec`]).
/// * `graph` - The current graph state (edge already added).
/// * `source` - Source node of the new edge.
/// * `target` - Target node of the new edge.
/// * `edge_type` - Type of the new edge.
///
/// # Returns
///
/// [`EdgeAddedDeltas`] containing deduplicated new paths.
pub fn extend_edge_added(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    source: NodeId,
    target: NodeId,
    edge_type: TypeId,
) -> EdgeAddedDeltas {
    if pattern.is_empty() {
        return EdgeAddedDeltas {
            new_paths: Vec::new(),
        };
    }

    let mut all_paths: Vec<Vec<NodeId>> = Vec::new();

    for (hop_idx, hop) in pattern.iter().enumerate() {
        match hop.direction {
            Direction::Outgoing => {
                // For outgoing hop: origin=source, reached=target.
                if !edge_matches_hop_directed(hop, target, edge_type, graph) {
                    continue;
                }
                let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, source);
                for prefix in prefixes {
                    extend_forward(graph, prefix, target, pattern, hop_idx, &mut all_paths);
                }
            }
            Direction::Incoming => {
                // For incoming hop at node N: follows edges where N is the target,
                // reaching the source. So origin=target, reached=source.
                if !edge_matches_hop_directed(hop, source, edge_type, graph) {
                    continue;
                }
                let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, target);
                for prefix in prefixes {
                    extend_forward(graph, prefix, source, pattern, hop_idx, &mut all_paths);
                }
            }
            Direction::Any => {
                // Try outgoing interpretation: origin=source, reached=target.
                if edge_matches_hop_directed(hop, target, edge_type, graph) {
                    let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, source);
                    for prefix in prefixes {
                        extend_forward(graph, prefix, target, pattern, hop_idx, &mut all_paths);
                    }
                }
                // Try incoming interpretation: origin=target, reached=source.
                if edge_matches_hop_directed(hop, source, edge_type, graph) {
                    let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, target);
                    for prefix in prefixes {
                        extend_forward(graph, prefix, source, pattern, hop_idx, &mut all_paths);
                    }
                }
            }
        }
    }

    // Deduplicate paths (Pitfall 4: same path from multiple hop positions).
    let mut seen = HashSet::new();
    all_paths.retain(|path| seen.insert(path.clone()));

    EdgeAddedDeltas {
        new_paths: all_paths,
    }
}

/// Checks if a new edge could satisfy the given hop specification for
/// a specific traversal direction. Tests edge type filter, target node type
/// filter, and property filter on the reached node.
///
/// This is called after the caller has already determined which node is the
/// "reached" node based on direction (Outgoing: target, Incoming: source).
fn edge_matches_hop_directed(
    hop: &HopSpec,
    reached_node: NodeId,
    edge_type: TypeId,
    graph: &Graph,
) -> bool {
    // Check edge type filter.
    if let Some(required_type) = hop.edge_type {
        if edge_type != required_type {
            return false;
        }
    }

    // Check target node type filter on the reached node.
    if let Some(target_type) = hop.target_type {
        if graph.get_node_type(reached_node) != Some(target_type) {
            return false;
        }
    }

    // Check property filter on the reached node.
    match &hop.filter {
        Filter::None => true,
        Filter::PropertyEquals { key, value } => {
            graph.get_property(reached_node, *key) == Some(value)
        }
        Filter::HasProperty { key } => graph.get_property(reached_node, *key).is_some(),
    }
}

/// Finds all partial paths from `anchor` through hops 0..`hop_idx`-1
/// that end at `required_end`.
///
/// Special case: `hop_idx == 0` means the new edge starts at the anchor,
/// so the backward prefix is `[anchor]` if `anchor == required_end`, else empty.
///
/// For `hop_idx > 0`, performs a partial DFS from the anchor through
/// intermediate hops, filtering to paths that terminate at `required_end`.
fn backward_prefixes(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    hop_idx: usize,
    required_end: NodeId,
) -> Vec<Vec<NodeId>> {
    // Special case: hop_idx == 0, the edge starts right from anchor.
    if hop_idx == 0 {
        if anchor == required_end {
            return vec![vec![anchor]];
        } else {
            return Vec::new();
        }
    }

    // Partial DFS from anchor through hops 0..hop_idx-1.
    let mut results = Vec::new();
    let initial = vec![anchor];
    partial_dfs(
        graph,
        &initial,
        pattern,
        0,
        hop_idx,
        required_end,
        &mut results,
    );
    results
}

/// Recursive partial DFS that collects paths of exactly `target_depth` hops
/// from the anchor, ending at `required_end`.
///
/// Filter logic replicates [`crate::Frame::dfs_collect`]: direction,
/// edge_type, target_type, and property filter.
fn partial_dfs(
    graph: &Graph,
    current_path: &[NodeId],
    pattern: &[HopSpec],
    current_hop: usize,
    target_depth: usize,
    required_end: NodeId,
    results: &mut Vec<Vec<NodeId>>,
) {
    // Base case: reached target depth.
    if current_hop == target_depth {
        if *current_path.last().expect("path must be non-empty") == required_end {
            results.push(current_path.to_vec());
        }
        return;
    }

    let hop = &pattern[current_hop];
    let current_node = *current_path.last().expect("path must be non-empty");

    // Get neighbors filtered by direction and edge type -- same as Frame::dfs_collect.
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

        // Extend path and recurse.
        let mut next_path = current_path.to_vec();
        next_path.push(neighbor_id);
        partial_dfs(
            graph,
            &next_path,
            pattern,
            current_hop + 1,
            target_depth,
            required_end,
            results,
        );
    }
}

/// Appends `reached_node` to the prefix and extends forward through
/// remaining hops. If `hop_idx` was the last hop, the path is already
/// complete. Otherwise, continues DFS from `reached_node` through
/// hops `hop_idx+1..N-1`.
fn extend_forward(
    graph: &Graph,
    mut prefix: Vec<NodeId>,
    reached_node: NodeId,
    pattern: &[HopSpec],
    hop_idx: usize,
    results: &mut Vec<Vec<NodeId>>,
) {
    prefix.push(reached_node);

    if hop_idx == pattern.len() - 1 {
        // Last hop: path is complete.
        results.push(prefix);
    } else {
        // Continue DFS from reached_node through remaining hops.
        forward_dfs(graph, &prefix, pattern, hop_idx + 1, results);
    }
}

/// Recursive forward DFS from `start_hop` through end of pattern.
///
/// Filter logic replicates [`crate::Frame::dfs_collect`] exactly.
fn forward_dfs(
    graph: &Graph,
    current_path: &[NodeId],
    pattern: &[HopSpec],
    hop_index: usize,
    results: &mut Vec<Vec<NodeId>>,
) {
    // Base case: all hops consumed, path is complete.
    if hop_index >= pattern.len() {
        results.push(current_path.to_vec());
        return;
    }

    let hop = &pattern[hop_index];
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
        forward_dfs(graph, &next_path, pattern, hop_index + 1, results);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PropertyValue;

    #[test]
    fn test_single_hop_outgoing_edge_added() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_two_hop_second_edge_added() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(30));

        // First hop edge already exists.
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
        // Second hop edge -- the newly added one.
        g.add_edge(NodeId(2), NodeId(3), TypeId(200));

        let pattern = vec![
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
        ];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(2), NodeId(3), TypeId(200));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn test_incoming_direction() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));

        // Edge goes 2->1. For an incoming hop at node 1, this means
        // following the incoming edge from 1 to reach source=2.
        g.add_edge(NodeId(2), NodeId(1), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Incoming,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        // For incoming hop: origin=target=1, reached=source=2.
        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(2), NodeId(1), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_no_matching_hop() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(999)), // Wrong edge type.
            target_type: None,
            filter: Filter::None,
        }];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert!(result.new_paths.is_empty());
    }

    #[test]
    fn test_empty_pattern() {
        let g = Graph::new();
        let pattern: Vec<HopSpec> = Vec::new();

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert!(result.new_paths.is_empty());
    }

    #[test]
    fn test_multi_hop_diamond_dedup() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(20));
        g.add_node(NodeId(4), TypeId(30));

        // Edges: 1->2 type 100, 1->3 type 100, 2->4 type 200 (already exists).
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
        g.add_edge(NodeId(1), NodeId(3), TypeId(100));
        g.add_edge(NodeId(2), NodeId(4), TypeId(200));
        // New edge: 3->4 type 200.
        g.add_edge(NodeId(3), NodeId(4), TypeId(200));

        let pattern = vec![
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
        ];

        // Only the new edge 3->4 should produce new paths.
        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(3), NodeId(4), TypeId(200));

        // Should produce [1, 3, 4] only. NOT [1, 2, 4] because that path
        // doesn't traverse the new edge 3->4.
        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(3), NodeId(4)]);
    }

    #[test]
    fn test_property_filter() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.set_property(NodeId(2), 5, PropertyValue::Integer(42));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::PropertyEquals {
                key: 5,
                value: PropertyValue::Integer(42),
            },
        }];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);

        // Now test with wrong property value -- should return empty.
        let mut g2 = Graph::new();
        g2.add_node(NodeId(1), TypeId(10));
        g2.add_node(NodeId(2), TypeId(20));
        g2.set_property(NodeId(2), 5, PropertyValue::Integer(99)); // Wrong value.
        g2.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let result2 =
            extend_edge_added(NodeId(1), &pattern, &g2, NodeId(1), NodeId(2), TypeId(100));

        assert!(result2.new_paths.is_empty());
    }

    #[test]
    fn test_any_direction() {
        // Test Direction::Any: edge 1->2, hop says Any, anchor=1.
        // The edge can be traversed as outgoing (1->2) or incoming (2->1).
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Any,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        // Outgoing interpretation: origin=1, reached=2 (type 20 matches).
        // Incoming interpretation: origin=2, reached=1 (type 10 != 20, fails).
        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_any_direction_both_orientations() {
        // Both orientations produce valid paths -- no target_type filter.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(10)); // Same type as node 1.
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Any,
            edge_type: Some(TypeId(100)),
            target_type: None, // No type filter, both orientations valid.
            filter: Filter::None,
        }];

        // Anchor=1. Outgoing: origin=1==anchor, reached=2. Path: [1,2].
        // Incoming: origin=2!=anchor, reached=1. No backward prefix (2 != 1 for hop 0).
        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);

        // Now with anchor=2: Outgoing: origin=1!=2. Incoming: origin=2==anchor, reached=1.
        let result2 =
            extend_edge_added(NodeId(2), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result2.new_paths.len(), 1);
        assert_eq!(result2.new_paths[0], vec![NodeId(2), NodeId(1)]);
    }

    #[test]
    fn test_has_property_filter() {
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.set_property(NodeId(2), 7, PropertyValue::Boolean(true));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::HasProperty { key: 7 },
        }];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);

        // Node without the property -- should return empty.
        let mut g2 = Graph::new();
        g2.add_node(NodeId(1), TypeId(10));
        g2.add_node(NodeId(2), TypeId(20));
        // No property set on node 2.
        g2.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let result2 =
            extend_edge_added(NodeId(1), &pattern, &g2, NodeId(1), NodeId(2), TypeId(100));

        assert!(result2.new_paths.is_empty());
    }

    #[test]
    fn test_three_hop_first_edge_added() {
        // 3-hop pattern, new edge satisfies hop 0. Forward extension must
        // traverse hops 1 and 2 to complete the path.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(30));
        g.add_node(NodeId(4), TypeId(40));

        // New edge at hop 0: 1->2 type 100.
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
        // Existing edges for hops 1 and 2.
        g.add_edge(NodeId(2), NodeId(3), TypeId(200));
        g.add_edge(NodeId(3), NodeId(4), TypeId(300));

        let pattern = vec![
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
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(300)),
                target_type: Some(TypeId(40)),
                filter: Filter::None,
            },
        ];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(100));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(
            result.new_paths[0],
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
        );
    }

    #[test]
    fn test_anchor_mismatch_hop_zero() {
        // Edge source != anchor for hop 0, so no backward prefix.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(20));
        g.add_edge(NodeId(2), NodeId(3), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        // Anchor=1, but edge source=2. No backward prefix for hop 0.
        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(2), NodeId(3), TypeId(100));

        assert!(result.new_paths.is_empty());
    }

    #[test]
    fn test_wildcard_edge_type() {
        // hop.edge_type is None -- any edge type matches.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_edge(NodeId(1), NodeId(2), TypeId(999));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: None, // Wildcard: any edge type.
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let result = extend_edge_added(NodeId(1), &pattern, &g, NodeId(1), NodeId(2), TypeId(999));

        assert_eq!(result.new_paths.len(), 1);
        assert_eq!(result.new_paths[0], vec![NodeId(1), NodeId(2)]);
    }
}
