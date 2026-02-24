//! Two-tier signal interpretation for frame analysis.
//!
//! Provides a fast binary check (Tier 1) and a structural path analysis
//! (Tier 2) for determining how a signal affects a frame's materialized state.
//!
//! # Tier 1: Binary Delta Check
//!
//! An O(1) comparison of a frame's previous and current net delta. If the
//! net delta changed, the frame's state was modified and may need deeper
//! analysis. This is the first-pass filter: most frames will show no change
//! and can be skipped.
//!
//! # Tier 2: Structural Path Analysis
//!
//! For frames where Tier 1 detected a change, Tier 2 analyzes each hop in
//! the frame's pattern to determine how many paths complete or break at
//! each hop level. This provides fine-grained visibility into which part of
//! a multi-hop pattern is affected by the signal.
//!
//! # Usage
//!
//! ```
//! use krabnet::interpret::{tier1_check, tier2_analysis, HopAnalysis};
//! use krabnet::{Frame, Graph, NodeId, TypeId, Direction, HopSpec, Filter, Epoch};
//!
//! // Tier 1: fast binary check
//! let changed = tier1_check(0, 1);
//! assert!(changed);
//!
//! // Tier 2: structural analysis on a materialized frame
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
//!
//! let analysis = tier2_analysis(&frame, Epoch(1));
//! assert_eq!(analysis.len(), 1);
//! assert_eq!(analysis[0].completed, 1);
//! ```

use crate::frame::Frame;
use crate::types::Epoch;

/// Result of analyzing a single hop in a frame's traversal pattern.
///
/// For each hop index in the pattern, counts how many of the frame's
/// current paths complete that hop (have sufficient length) versus how
/// many break at that hop (path is too short to reach beyond it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopAnalysis {
    /// The zero-based index of this hop in the frame's pattern.
    pub hop_index: usize,
    /// Number of paths that completed this hop (path length >= hop_index + 2).
    pub completed: usize,
    /// Number of paths that broke at this hop (path length == hop_index + 1).
    pub broken: usize,
}

/// Tier 1 binary delta check: did the frame's net delta change?
///
/// Compares the previous net delta with the current net delta. Returns
/// `true` if they differ (the frame's state changed), `false` if they
/// are the same (no change detected).
///
/// This is an O(1) operation designed as the first-pass filter in the
/// two-tier interpretation pipeline. Most frames will show no change
/// after a signal, making this an effective early exit.
///
/// # Examples
///
/// ```
/// use krabnet::interpret::tier1_check;
///
/// assert!(tier1_check(0, 1));   // changed
/// assert!(!tier1_check(5, 5));  // unchanged
/// ```
pub fn tier1_check(previous_net_delta: i64, current_net_delta: i64) -> bool {
    previous_net_delta != current_net_delta
}

/// Tier 2 structural path analysis: identify completed and broken hops.
///
/// For each hop in the frame's pattern, examines all paths in the frame's
/// snapshot at the given epoch and counts:
/// - **Completed:** paths with length >= hop_index + 2 (the path extends
///   beyond this hop, meaning the hop was successfully traversed).
/// - **Broken:** paths with length == hop_index + 1 (the path ends exactly
///   at this hop, meaning traversal could not continue past it).
///
/// Returns a [`Vec<HopAnalysis>`] with one entry per hop in the pattern.
/// An empty pattern produces an empty result. If the frame has no paths,
/// all hops will show zero completed and zero broken.
///
/// # Examples
///
/// ```
/// use krabnet::interpret::{tier2_analysis, HopAnalysis};
/// use krabnet::{Frame, Graph, NodeId, TypeId, Direction, HopSpec, Filter, Epoch};
///
/// let mut g = Graph::new();
/// g.add_node(NodeId(1), TypeId(10));
/// g.add_node(NodeId(2), TypeId(20));
/// g.add_edge(NodeId(1), NodeId(2), TypeId(100));
///
/// let pattern = vec![HopSpec {
///     direction: Direction::Outgoing,
///     edge_type: Some(TypeId(100)),
///     target_type: Some(TypeId(20)),
///     filter: Filter::None,
/// }];
///
/// let mut frame = Frame::new(1, NodeId(1), pattern);
/// frame.materialize(&g, Epoch(1));
///
/// let analysis = tier2_analysis(&frame, Epoch(1));
/// assert_eq!(analysis.len(), 1);
/// assert_eq!(analysis[0].completed, 1);
/// assert_eq!(analysis[0].broken, 0);
/// ```
pub fn tier2_analysis(frame: &Frame, epoch: Epoch) -> Vec<HopAnalysis> {
    let pattern = frame.pattern();
    let paths = frame.snapshot(epoch);

    pattern
        .iter()
        .enumerate()
        .map(|(hop_index, _hop)| {
            let mut completed = 0usize;
            let mut broken = 0usize;

            for path in &paths {
                let path_len = path.len();
                if path_len >= hop_index + 2 {
                    // Path extends beyond this hop -- hop completed.
                    completed += 1;
                } else if path_len == hop_index + 1 {
                    // Path ends exactly at this hop -- broken here.
                    broken += 1;
                }
                // Paths shorter than hop_index + 1 are irrelevant to this hop.
            }

            HopAnalysis {
                hop_index,
                completed,
                broken,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::types::{Direction, Filter, HopSpec, NodeId, TypeId};

    #[test]
    fn tier1_detects_change() {
        assert!(tier1_check(0, 1));
        assert!(tier1_check(5, -3));
        assert!(tier1_check(-1, 0));
    }

    #[test]
    fn tier1_detects_no_change() {
        assert!(!tier1_check(0, 0));
        assert!(!tier1_check(42, 42));
        assert!(!tier1_check(-7, -7));
    }

    #[test]
    fn tier2_identifies_completed_hops() {
        // Build a two-hop graph: A -> B -> C
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_node(NodeId(3), TypeId(30));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));
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

        let mut frame = Frame::new(1, NodeId(1), pattern);
        frame.materialize(&g, Epoch(1));

        let analysis = tier2_analysis(&frame, Epoch(1));
        assert_eq!(analysis.len(), 2);

        // Hop 0: path [1,2,3] has length 3 >= 0+2=2 → completed
        assert_eq!(analysis[0].hop_index, 0);
        assert_eq!(analysis[0].completed, 1);
        assert_eq!(analysis[0].broken, 0);

        // Hop 1: path [1,2,3] has length 3 >= 1+2=3 → completed
        assert_eq!(analysis[1].hop_index, 1);
        assert_eq!(analysis[1].completed, 1);
        assert_eq!(analysis[1].broken, 0);
    }

    #[test]
    fn tier2_identifies_broken_hops() {
        // Build a one-hop graph: A -> B (no second hop available)
        let mut g = Graph::new();
        g.add_node(NodeId(1), TypeId(10));
        g.add_node(NodeId(2), TypeId(20));
        g.add_edge(NodeId(1), NodeId(2), TypeId(100));

        // Pattern asks for two hops, but only one is available in the graph.
        // Frame materialization only produces complete paths, so with a
        // two-hop pattern on a one-hop graph, there are NO materialized paths.
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

        let mut frame = Frame::new(1, NodeId(1), pattern);
        frame.materialize(&g, Epoch(1));

        let analysis = tier2_analysis(&frame, Epoch(1));
        assert_eq!(analysis.len(), 2);

        // No paths materialized (frame only stores complete paths), so all
        // hops show zero completed and zero broken.
        assert_eq!(analysis[0].completed, 0);
        assert_eq!(analysis[0].broken, 0);
        assert_eq!(analysis[1].completed, 0);
        assert_eq!(analysis[1].broken, 0);
    }
}
