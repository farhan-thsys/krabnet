//! Incremental path extension and retraction for edge/node/property mutations.
//!
//! This module provides the algorithmic core for incremental path maintenance:
//!
//! - **[`extend_edge_added`]**: Computes new complete paths produced by a newly
//!   added edge without full DFS re-traverse.
//! - **[`retract_edge_removed`]**: Identifies materialized paths broken by a
//!   removed edge, with parallel-edge survival checks to avoid over-retraction.
//! - **[`retract_node_removed`]**: Identifies materialized paths containing a
//!   removed node at any position.
//! - **[`reevaluate_property_changed`]**: Re-evaluates hop filters when a node's
//!   property changes, computing both retracted paths (-1) and newly valid paths (+1).
//!
//! ## Edge-Added Extension
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
//! ## Edge-Removed Retraction
//!
//! Scans a frame's materialized paths to find those traversing the removed edge
//! at any hop position. For each matched hop, a parallel-edge survival check
//! queries [`Graph::neighbors`] on the post-removal graph state: if another edge
//! still connects the same (from, to) pair with the correct direction and type,
//! the path survives. Only paths with no surviving parallel edge are retracted.
//!
//! ## Node-Removed Retraction
//!
//! Scans materialized paths for the removed node's presence at any position
//! (anchor, intermediate, or terminal). All matching paths are retracted.
//!
//! All functions are stateless: they take read-only references to the graph and
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

/// Result of incremental edge-removed retraction.
///
/// Contains the materialized paths that should be retracted as -1 deltas
/// from the affected frame's [`crate::diff::DiffCollection`] because they
/// traversed the removed edge and no parallel edge survives.
#[derive(Debug)]
pub struct EdgeRemovedDeltas {
    /// Paths to retract as -1 deltas.
    pub retracted_paths: Vec<Vec<NodeId>>,
}

/// Result of incremental node-removed retraction.
///
/// Contains the materialized paths that should be retracted as -1 deltas
/// from the affected frame's [`crate::diff::DiffCollection`] because they
/// contain the removed node at some position.
#[derive(Debug)]
pub struct NodeRemovedDeltas {
    /// Paths to retract as -1 deltas.
    pub retracted_paths: Vec<Vec<NodeId>>,
}

/// Identifies materialized paths broken by a removed edge.
///
/// For each path in `current_paths`, checks whether any hop traverses the
/// removed edge (source, target). If a hop matches, performs a parallel-edge
/// survival check via [`Graph::neighbors`] on the post-removal graph state.
/// Only paths with no surviving parallel edge are retracted.
///
/// Paths are deduplicated to prevent double-retraction.
///
/// # Arguments
///
/// * `pattern` - The frame's hop pattern (sequence of [`HopSpec`]).
/// * `graph` - The current graph state (edge already removed).
/// * `current_paths` - References to the frame's currently materialized paths.
/// * `source` - Source node of the removed edge.
/// * `target` - Target node of the removed edge.
///
/// # Returns
///
/// [`EdgeRemovedDeltas`] containing deduplicated retracted paths.
pub fn retract_edge_removed(
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    source: NodeId,
    target: NodeId,
) -> EdgeRemovedDeltas {
    if pattern.is_empty() || current_paths.is_empty() {
        return EdgeRemovedDeltas {
            retracted_paths: Vec::new(),
        };
    }

    let mut retracted_paths: Vec<Vec<NodeId>> = Vec::new();

    for path in current_paths {
        if path_broken_by_edge_removal(path, pattern, graph, source, target) {
            retracted_paths.push(path.to_vec());
        }
    }

    // Deduplicate via HashSet to prevent double-retraction.
    let mut seen = HashSet::new();
    retracted_paths.retain(|p| seen.insert(p.clone()));

    EdgeRemovedDeltas { retracted_paths }
}

/// Checks whether a path is broken by the removal of edge (removed_source, removed_target).
///
/// For each hop in the pattern, determines the (from, to) nodes in the path and
/// checks whether the removed edge matches this hop based on direction. If matched,
/// performs a parallel-edge survival check: queries `graph.neighbors()` to see if
/// any remaining edge still connects from -> to with the hop's direction and type.
///
/// Returns `true` if the path is broken (should be retracted).
fn path_broken_by_edge_removal(
    path: &[NodeId],
    pattern: &[HopSpec],
    graph: &Graph,
    removed_source: NodeId,
    removed_target: NodeId,
) -> bool {
    // Guard: invalid path length.
    if path.len() != pattern.len() + 1 {
        return false;
    }

    for (hop_idx, hop) in pattern.iter().enumerate() {
        let from = path[hop_idx];
        let to = path[hop_idx + 1];

        // Check if the removed edge matches this hop based on direction.
        let matches = match hop.direction {
            Direction::Outgoing => from == removed_source && to == removed_target,
            Direction::Incoming => from == removed_target && to == removed_source,
            Direction::Any => {
                (from == removed_source && to == removed_target)
                    || (from == removed_target && to == removed_source)
            }
        };

        if matches {
            // Parallel edge survival check: does any remaining neighbor still
            // connect from -> to with the hop's direction and edge_type?
            let neighbors = graph.neighbors(from, hop.direction, hop.edge_type);
            let surviving = neighbors.iter().any(|(_eid, n)| *n == to);

            if !surviving {
                // No parallel edge survives -- path is broken.
                return true;
            }
            // A parallel edge survives for this hop, continue checking remaining hops.
        }
    }

    false
}

/// Identifies materialized paths containing a removed node.
///
/// Scans each path in `current_paths` for the presence of `removed_node`
/// at any position (anchor, intermediate, or terminal). All matching paths
/// are collected and returned for retraction as -1 deltas.
///
/// No deduplication is needed: each path reference in `current_paths` is
/// unique (from [`crate::Frame::snapshot`] which returns `&Vec<NodeId>` refs).
///
/// # Arguments
///
/// * `current_paths` - References to the frame's currently materialized paths.
/// * `removed_node` - The node that was removed from the graph.
///
/// # Returns
///
/// [`NodeRemovedDeltas`] containing retracted paths.
pub fn retract_node_removed(
    current_paths: &[&Vec<NodeId>],
    removed_node: NodeId,
) -> NodeRemovedDeltas {
    let retracted_paths: Vec<Vec<NodeId>> = current_paths
        .iter()
        .filter(|path| path.contains(&removed_node))
        .map(|path| path.to_vec())
        .collect();

    NodeRemovedDeltas { retracted_paths }
}

/// Result of incremental property-change re-evaluation.
///
/// Contains paths to retract (-1 deltas) because a hop filter no longer
/// passes after the property change, and new paths to assert (+1 deltas)
/// because a hop filter now passes where it previously did not.
#[derive(Debug)]
pub struct PropertyChangedDeltas {
    /// Paths to retract as -1 deltas (no longer satisfy filters).
    pub retracted_paths: Vec<Vec<NodeId>>,
    /// New paths to assert as +1 deltas (newly satisfy filters).
    pub new_paths: Vec<Vec<NodeId>>,
}

/// Re-evaluates hop filters for a frame when a node's property changes.
///
/// Scans existing materialized paths for those containing the affected
/// node at any hop position with a property filter. Paths where the
/// filter no longer passes are retracted. New paths where the filter
/// now passes are discovered via backward prefix resolution and forward
/// extension (reusing Phase 18 DFS helpers).
///
/// # Arguments
///
/// * `anchor` - The frame's anchor node.
/// * `pattern` - The frame's hop pattern (sequence of [`HopSpec`]).
/// * `graph` - The current graph state (property already changed).
/// * `current_paths` - References to the frame's currently materialized paths.
/// * `changed_node` - The node whose property changed.
///
/// # Returns
///
/// [`PropertyChangedDeltas`] containing both retracted and new paths.
pub fn reevaluate_property_changed(
    anchor: NodeId,
    pattern: &[HopSpec],
    graph: &Graph,
    current_paths: &[&Vec<NodeId>],
    changed_node: NodeId,
) -> PropertyChangedDeltas {
    // Early exit: empty pattern or no hop has a property filter.
    if pattern.is_empty() || !pattern.iter().any(|hop| !matches!(hop.filter, Filter::None)) {
        return PropertyChangedDeltas {
            retracted_paths: Vec::new(),
            new_paths: Vec::new(),
        };
    }

    // Step 1: Retract newly-invalid paths.
    let mut retracted_paths: Vec<Vec<NodeId>> = Vec::new();
    for path in current_paths {
        if path.len() != pattern.len() + 1 {
            continue;
        }
        if path_invalidated_by_property_change(path, pattern, graph, changed_node) {
            retracted_paths.push(path.to_vec());
        }
    }
    // Deduplicate retracted paths.
    let mut seen = HashSet::new();
    retracted_paths.retain(|p| seen.insert(p.clone()));

    // Step 2: Assert newly-valid paths.
    let existing: HashSet<&Vec<NodeId>> = current_paths.iter().copied().collect();
    let retracted_set: HashSet<Vec<NodeId>> = retracted_paths.iter().cloned().collect();

    let mut new_paths: Vec<Vec<NodeId>> = Vec::new();

    for (hop_idx, hop) in pattern.iter().enumerate() {
        // Only check hops with property filters.
        if matches!(hop.filter, Filter::None) {
            continue;
        }

        // Check if the changed node satisfies this hop's full constraint
        // (target_type + property filter).
        if !node_passes_hop(hop, changed_node, graph) {
            continue;
        }

        // The changed node satisfies this hop's filter. Find all complete
        // paths that pass through the changed node at position hop_idx+1.
        let origins = find_hop_origins(graph, hop, changed_node);

        for origin in origins {
            let prefixes = backward_prefixes(anchor, pattern, graph, hop_idx, origin);
            for prefix in prefixes {
                extend_forward(graph, prefix, changed_node, pattern, hop_idx, &mut new_paths);
            }
        }
    }

    // Deduplicate new paths.
    let mut seen_new = HashSet::new();
    new_paths.retain(|p| seen_new.insert(p.clone()));

    // Remove paths already in current materialized set (avoid double-assertion).
    new_paths.retain(|p| !existing.contains(p));

    // Remove paths in the retracted set (defensive).
    new_paths.retain(|p| !retracted_set.contains(p));

    PropertyChangedDeltas {
        retracted_paths,
        new_paths,
    }
}

/// Checks whether a materialized path is invalidated because the changed
/// node's property no longer satisfies a hop filter.
///
/// For each hop K in the pattern, if `path[K+1] == changed_node` and hop K
/// has a property filter, re-evaluates the full hop constraint (target_type
/// and property filter). If any hop's constraint no longer passes, the path
/// is invalid.
fn path_invalidated_by_property_change(
    path: &[NodeId],
    pattern: &[HopSpec],
    graph: &Graph,
    changed_node: NodeId,
) -> bool {
    for (hop_idx, hop) in pattern.iter().enumerate() {
        let reached_node = path[hop_idx + 1];

        // Only check hops where the changed node is the reached node.
        if reached_node != changed_node {
            continue;
        }

        // Only relevant if this hop has a property filter.
        if matches!(hop.filter, Filter::None) {
            continue;
        }

        // Re-evaluate the full hop constraint on the changed node.
        if !node_passes_hop(hop, changed_node, graph) {
            return true; // Path invalidated by this hop.
        }
    }
    false
}

/// Checks if a node satisfies a hop's node-side constraints: target_type
/// and property filter. Does NOT check edge_type (that is verified by the
/// edge, not the node).
///
/// Returns `true` if the node passes both the target_type and property
/// filter checks.
fn node_passes_hop(hop: &HopSpec, node_id: NodeId, graph: &Graph) -> bool {
    // Check target type (if specified).
    if let Some(target_type) = hop.target_type {
        if graph.get_node_type(node_id) != Some(target_type) {
            return false;
        }
    }

    // Check property filter.
    match &hop.filter {
        Filter::None => true,
        Filter::PropertyEquals { key, value } => {
            graph.get_property(node_id, *key) == Some(value)
        }
        Filter::HasProperty { key } => graph.get_property(node_id, *key).is_some(),
    }
}

/// Finds all nodes that have an edge TO `reached_node` matching the hop's
/// direction and edge type. These are the "origin" nodes that could reach
/// `reached_node` via this hop.
///
/// - For Outgoing hop: origin has outgoing edge to reached_node, so query
///   `graph.neighbors(reached_node, Incoming, edge_type)` to find origins.
/// - For Incoming hop: origin follows incoming edge, reached_node is the
///   source, so query `graph.neighbors(reached_node, Outgoing, edge_type)`.
/// - For Any: query both directions, deduplicate.
fn find_hop_origins(graph: &Graph, hop: &HopSpec, reached_node: NodeId) -> Vec<NodeId> {
    let mut origins = Vec::new();

    match hop.direction {
        Direction::Outgoing => {
            // Origin->reached via outgoing edge => reached has incoming from origin.
            let neighbors = graph.neighbors(reached_node, Direction::Incoming, hop.edge_type);
            for (_eid, neighbor) in neighbors {
                origins.push(neighbor);
            }
        }
        Direction::Incoming => {
            // Origin follows incoming edge from reached_node. The traversal
            // direction is "Incoming" meaning the origin node has incoming edges,
            // reached_node is the source of the edge (origin is target).
            // So reached_node has outgoing edge to origin.
            let neighbors = graph.neighbors(reached_node, Direction::Outgoing, hop.edge_type);
            for (_eid, neighbor) in neighbors {
                origins.push(neighbor);
            }
        }
        Direction::Any => {
            // Try both directions, deduplicate.
            let incoming = graph.neighbors(reached_node, Direction::Incoming, hop.edge_type);
            for (_eid, neighbor) in incoming {
                origins.push(neighbor);
            }
            let outgoing = graph.neighbors(reached_node, Direction::Outgoing, hop.edge_type);
            for (_eid, neighbor) in outgoing {
                if !origins.contains(&neighbor) {
                    origins.push(neighbor);
                }
            }
        }
    }

    origins
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

    // ── Edge removal retraction tests ─────────────────────────────────

    #[test]
    fn test_retract_edge_removed_single_hop() {
        // 1-hop frame (A->B). Remove edge A->B. Path [A, B] should be retracted.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        // Edge A->B is removed -- so NOT in the graph when we call retract.

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(1), NodeId(2));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(result.retracted_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_retract_edge_removed_no_match() {
        // 1-hop frame (A->B). Remove edge C->D (unrelated). No retraction.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(30));
        g.add_node(NodeId(4), TypeId(40));
        // Edge A->B still exists in the graph.
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        // Remove edge C->D (source=3, target=4) -- unrelated to path.
        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(3), NodeId(4));

        assert!(result.retracted_paths.is_empty());
    }

    #[test]
    fn test_retract_edge_removed_parallel_edge_survives() {
        // Two outgoing edges from A to B (both matching hop type).
        // Remove one. graph.neighbors(A, Outgoing, edge_type) still returns B
        // via the surviving edge. Path should NOT be retracted.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        // Add two edges A->B with same type.
        let eid1 = g.add_edge(NodeId(1), NodeId(2), TypeId(100)).unwrap();
        let _eid2 = g.add_edge(NodeId(1), NodeId(2), TypeId(100)).unwrap();
        // Remove one edge (simulate post-removal state).
        g.remove_edge(eid1);

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(1), NodeId(2));

        // Surviving parallel edge -- path NOT retracted.
        assert!(result.retracted_paths.is_empty());
    }

    #[test]
    fn test_retract_edge_removed_multi_hop_middle() {
        // 3-hop frame (A->B->C->D). Remove edge B->C. Path retracted.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(30));
        g.add_node(NodeId(4), TypeId(40));
        // Edge A->B and C->D still exist; B->C removed.
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
        // B->C NOT in graph (removed).
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

        let paths = vec![vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(2), NodeId(3));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(
            result.retracted_paths[0],
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
        );
    }

    #[test]
    fn test_retract_edge_removed_direction_incoming() {
        // 1-hop frame with Direction::Incoming. Path is [B, A] meaning B has
        // an incoming edge from A (edge A->B). Remove edge A->B (source=A,
        // target=B). Incoming hop: from=B=removed_target, to=A=removed_source.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10)); // A
        g.add_node(NodeId(2), TypeId(20)); // B
        // Edge A->B removed -- NOT in graph.

        let pattern = vec![HopSpec {
            direction: Direction::Incoming,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(10)),
            filter: Filter::None,
        }];

        // Path [B, A]: anchor=B, follows incoming edge from A.
        let paths = vec![vec![NodeId(2), NodeId(1)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        // Removed edge: source=A(1), target=B(2).
        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(1), NodeId(2));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(result.retracted_paths[0], vec![NodeId(2), NodeId(1)]);
    }

    #[test]
    fn test_retract_edge_removed_direction_any() {
        // 1-hop frame with Direction::Any. Path [A, B]. Remove edge A->B.
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        // Edge A->B removed -- NOT in graph.

        let pattern = vec![HopSpec {
            direction: Direction::Any,
            edge_type: Some(TypeId(100)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }];

        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(1), NodeId(2));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(result.retracted_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_retract_edge_removed_empty_paths() {
        // Empty current_paths. No retraction.
        let g = Graph::new();

        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: None,
            filter: Filter::None,
        }];

        let path_refs: Vec<&Vec<NodeId>> = Vec::new();

        let result = retract_edge_removed(&pattern, &g, &path_refs, NodeId(1), NodeId(2));

        assert!(result.retracted_paths.is_empty());
    }

    // ── Node removal retraction tests ─────────────────────────────────

    #[test]
    fn test_retract_node_removed_single_hop() {
        // 1-hop frame (A->B). Remove node B. Path [A, B] retracted.
        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_node_removed(&path_refs, NodeId(2));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(result.retracted_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_retract_node_removed_anchor() {
        // 1-hop frame (A->B). Remove anchor node A. Path [A, B] retracted.
        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_node_removed(&path_refs, NodeId(1));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(result.retracted_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_retract_node_removed_middle_of_multi_hop() {
        // 3-hop path [A, B, C, D]. Remove node B. Path retracted.
        let paths = vec![vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_node_removed(&path_refs, NodeId(2));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(
            result.retracted_paths[0],
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
        );
    }

    #[test]
    fn test_retract_node_removed_no_match() {
        // Path [A, B]. Remove node C (not in path). No retraction.
        let paths = vec![vec![NodeId(1), NodeId(2)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_node_removed(&path_refs, NodeId(3));

        assert!(result.retracted_paths.is_empty());
    }

    #[test]
    fn test_retract_node_removed_multiple_paths() {
        // Two paths: [A, B] and [A, C]. Remove node B.
        // Only [A, B] should be retracted.
        let paths = vec![vec![NodeId(1), NodeId(2)], vec![NodeId(1), NodeId(3)]];
        let path_refs: Vec<&Vec<NodeId>> = paths.iter().collect();

        let result = retract_node_removed(&path_refs, NodeId(2));

        assert_eq!(result.retracted_paths.len(), 1);
        assert_eq!(result.retracted_paths[0], vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_retract_node_removed_empty_paths() {
        // Empty current_paths. No retraction.
        let path_refs: Vec<&Vec<NodeId>> = Vec::new();

        let result = retract_node_removed(&path_refs, NodeId(1));

        assert!(result.retracted_paths.is_empty());
    }
}
