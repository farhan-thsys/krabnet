//! Core type definitions shared across all Krabnet modules.
//!
//! This module defines the newtypes, enums, and type aliases that form
//! the vocabulary of the entire crate. All types are designed for zero-cost
//! abstraction: newtypes compile to their inner primitives, enums use
//! no heap allocation.
//!
//! # Design Principles
//!
//! - **Type safety**: Newtypes prevent mixing up IDs from different domains
//!   (e.g., passing a [`NodeId`] where an [`EdgeId`] is expected).
//! - **Zero allocation**: [`PropertyValue::Text`] uses an interned `u32` ID
//!   instead of `String`, eliminating heap allocation on the hot path.
//! - **Exhaustive modeling**: All domain values are modeled as enums with
//!   known, finite variant sets.

/// Unique identifier for a node in the graph.
///
/// Wraps a `u64` for type safety. Two `NodeId` values are equal if and only
/// if their inner values are equal. Supports total ordering for use in
/// sorted collections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

/// Unique identifier for an edge in the graph.
///
/// Wraps a `u64` for type safety. Edges connect a source [`NodeId`] to a
/// target [`NodeId`] with a [`TypeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeId(pub u64);

/// Interned type or property-key identifier.
///
/// Obtained from [`crate::Interner::intern()`] at initialization time.
/// All runtime comparisons use this `u32` ID instead of string comparison,
/// ensuring zero allocation on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypeId(pub u32);

/// Monotonic epoch from the sequencer.
///
/// Represents a globally unique, strictly increasing timestamp assigned to
/// each event as it enters the ring buffer. Supports total ordering for
/// temporal queries and snapshot operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Epoch(pub u64);

/// Differential delta: +1 for assertion, -1 for retraction.
///
/// Used in the differential MVCC engine to represent additions (+1) and
/// removals (-1) of tuples. The multiset semantics allow multiple assertions
/// of the same tuple (multiplicity > 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Delta(pub i64);

/// A property value stored on a node.
///
/// Supports four value types: integers, floats, interned text, and booleans.
///
/// # Zero-Allocation Constraint
///
/// The [`Text`](PropertyValue::Text) variant holds an interned `u32` string ID
/// obtained from [`crate::Interner::intern()`], **not** a `String`. This ensures
/// zero heap allocation when reading or comparing property values on the hot path.
/// The actual string content lives in the interner's backing storage.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    /// A 64-bit signed integer value.
    Integer(i64),
    /// A 64-bit floating point value.
    ///
    /// Note: `PropertyValue` does not derive `Eq` because `f64` does not
    /// implement `Eq` (NaN != NaN).
    Float(f64),
    /// An interned string ID. Resolve to the original string via
    /// [`crate::Interner::resolve()`].
    ///
    /// This is a `u32` interned ID, **not** a heap-allocated `String`.
    Text(u32),
    /// A boolean value.
    Boolean(bool),
}

/// A set of properties: pairs of (interned key ID, value).
///
/// Property keys are interned `u32` IDs obtained from [`crate::Interner::intern()`],
/// not raw strings. This ensures zero allocation when iterating or querying
/// property sets on the hot path.
pub type PropertySet = Vec<(u32, PropertyValue)>;

/// Traversal direction for neighbor queries.
///
/// Used in [`HopSpec`] to specify which edges to follow when traversing
/// the graph from a given node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Follow edges where the current node is the source.
    Outgoing,
    /// Follow edges where the current node is the target.
    Incoming,
    /// Follow edges in either direction.
    Any,
}

/// Property filter for hop-level traversal constraints.
///
/// Applied at each hop during graph traversal to constrain which nodes
/// are included in the materialized result.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// No filter -- accept all nodes.
    None,
    /// Property key must exist with a matching value.
    PropertyEquals {
        /// Interned property key ID.
        key: u32,
        /// Expected property value.
        value: PropertyValue,
    },
    /// Property key must exist (any value accepted).
    HasProperty {
        /// Interned property key ID.
        key: u32,
    },
}

/// One hop in a multi-hop traversal pattern.
///
/// A [`HopSpec`] defines the constraints for a single step in a parked
/// traverser's pattern. Multiple hops form a path pattern that is
/// materialized from an anchor node.
#[derive(Debug, Clone, PartialEq)]
pub struct HopSpec {
    /// Direction to traverse edges at this hop.
    pub direction: Direction,
    /// Optional edge type filter. If `Some`, only edges with this type
    /// are followed. If `None`, all edge types are accepted.
    pub edge_type: Option<TypeId>,
    /// Optional target node type filter. If `Some`, only nodes with this
    /// type are included. If `None`, all node types are accepted.
    pub target_type: Option<TypeId>,
    /// Optional property filter applied to the target node.
    pub filter: Filter,
}

/// A graph mutation event entering through the ring buffer.
///
/// Events represent atomic mutations to the property graph. Each event
/// is assigned an [`Epoch`] by the sequencer before being stored in the
/// ring buffer. The epoch is assigned externally (not part of this enum)
/// to maintain separation of concerns between domain events and
/// infrastructure timestamps.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A new node was added to the graph.
    NodeAdded {
        /// The unique identifier for the new node.
        node_id: NodeId,
        /// The interned type of the node.
        type_id: TypeId,
    },
    /// A node was removed from the graph.
    ///
    /// Removing a node should cascade to remove all connected edges.
    NodeRemoved {
        /// The unique identifier of the removed node.
        node_id: NodeId,
    },
    /// A new edge was added between two nodes.
    EdgeAdded {
        /// The unique identifier for the new edge.
        edge_id: EdgeId,
        /// The source node of the edge.
        source: NodeId,
        /// The target node of the edge.
        target: NodeId,
        /// The interned type of the edge.
        type_id: TypeId,
    },
    /// An edge was removed from the graph.
    EdgeRemoved {
        /// The unique identifier of the removed edge.
        edge_id: EdgeId,
        /// The source node of the edge (needed for adjacency cleanup).
        source: NodeId,
        /// The target node of the edge (needed for adjacency cleanup).
        target: NodeId,
    },
    /// A property on a node was changed (added or updated).
    PropertyChanged {
        /// The node whose property changed.
        node_id: NodeId,
        /// Interned property key ID.
        key: u32,
        /// The new property value.
        value: PropertyValue,
    },
}

/// A differential tuple: payload with epoch and delta.
///
/// Represents a single assertion (+1) or retraction (-1) of a data payload
/// at a given epoch. Collections of `DiffTuple` values form the basis of
/// the differential MVCC engine's multiset semantics.
///
/// # Type Parameter
///
/// `T` is the payload type. In practice this will be path tuples or other
/// materialized traversal data. Trait bounds are applied on impl blocks
/// rather than on the struct definition for maximum flexibility.
#[derive(Debug, Clone, PartialEq)]
pub struct DiffTuple<T> {
    /// The data payload.
    pub data: T,
    /// The epoch at which this assertion or retraction occurred.
    pub epoch: Epoch,
    /// The differential delta: +1 for assertion, -1 for retraction.
    pub delta: Delta,
}

/// Interpretation tier for frame analysis.
///
/// Determines the depth of analysis applied to a frame when a signal
/// arrives. Tier 1 is a fast binary check; Tier 2 is a full structural
/// analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InterpretationTier {
    /// Fast binary delta-sum check (O(1)).
    /// Returns whether the frame's net delta changed since last check.
    Tier1,
    /// Full structural path analysis.
    /// Identifies completed and broken hops in frame paths.
    Tier2,
}

/// Frame temperature tier for adaptive tiering.
///
/// Frames are classified by their access pattern and importance.
/// The tier determines materialization and interpretation priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameTier {
    /// High priority: fully materialized, interpreted every cycle.
    /// Score > 0.7 based on query frequency, mutation rate, and recency.
    Hot,
    /// Medium priority: materialized but not always interpreted.
    /// Score between 0.2 and 0.7.
    Warm,
    /// Low priority: may be evicted or stored compactly.
    /// Score < 0.2.
    Cold,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newtypes_are_copy() {
        // NodeId
        let node = NodeId(42);
        let node2 = node; // Copy, not move
        assert_eq!(node, node2);

        // EdgeId
        let edge = EdgeId(99);
        let edge2 = edge;
        assert_eq!(edge, edge2);

        // TypeId
        let tid = TypeId(7);
        let tid2 = tid;
        assert_eq!(tid, tid2);

        // Epoch
        let epoch = Epoch(100);
        let epoch2 = epoch;
        assert_eq!(epoch, epoch2);

        // Delta
        let delta = Delta(1);
        let delta2 = delta;
        assert_eq!(delta, delta2);
    }

    #[test]
    fn epoch_has_correct_ordering() {
        assert!(Epoch(1) < Epoch(2));
        assert!(Epoch(0) < Epoch(u64::MAX));
        assert_eq!(Epoch(5), Epoch(5));
        assert!(Epoch(10) > Epoch(3));
    }

    #[test]
    fn direction_variants_are_distinct() {
        let dirs = [Direction::Outgoing, Direction::Incoming, Direction::Any];
        for (i, a) in dirs.iter().enumerate() {
            for (j, b) in dirs.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn property_value_text_holds_u32() {
        // Text variant holds u32 (interned ID), not String
        let val = PropertyValue::Text(42);
        if let PropertyValue::Text(id) = val {
            assert_eq!(id, 42u32);
        } else {
            panic!("expected Text variant");
        }
    }

    #[test]
    fn event_variants_can_be_constructed_and_matched() {
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
                value: PropertyValue::Integer(42),
            },
        ];

        // Verify each variant can be pattern-matched
        for event in &events {
            match event {
                Event::NodeAdded { node_id, type_id } => {
                    assert_eq!(*node_id, NodeId(1));
                    assert_eq!(*type_id, TypeId(0));
                }
                Event::NodeRemoved { node_id } => {
                    assert_eq!(*node_id, NodeId(1));
                }
                Event::EdgeAdded {
                    edge_id,
                    source,
                    target,
                    type_id,
                } => {
                    assert_eq!(*edge_id, EdgeId(10));
                    assert_eq!(*source, NodeId(1));
                    assert_eq!(*target, NodeId(2));
                    assert_eq!(*type_id, TypeId(1));
                }
                Event::EdgeRemoved {
                    edge_id,
                    source,
                    target,
                } => {
                    assert_eq!(*edge_id, EdgeId(10));
                    assert_eq!(*source, NodeId(1));
                    assert_eq!(*target, NodeId(2));
                }
                Event::PropertyChanged {
                    node_id,
                    key,
                    value,
                } => {
                    assert_eq!(*node_id, NodeId(1));
                    assert_eq!(*key, 0);
                    assert_eq!(*value, PropertyValue::Integer(42));
                }
            }
        }
    }

    #[test]
    fn diff_tuple_can_hold_different_payload_types() {
        // DiffTuple with u64 payload
        let dt_u64 = DiffTuple {
            data: 42u64,
            epoch: Epoch(1),
            delta: Delta(1),
        };
        assert_eq!(dt_u64.data, 42u64);
        assert_eq!(dt_u64.epoch, Epoch(1));
        assert_eq!(dt_u64.delta, Delta(1));

        // DiffTuple with String payload
        let dt_string = DiffTuple {
            data: String::from("path"),
            epoch: Epoch(2),
            delta: Delta(-1),
        };
        assert_eq!(dt_string.data, "path");
        assert_eq!(dt_string.delta, Delta(-1));

        // DiffTuple with tuple payload
        let dt_tuple = DiffTuple {
            data: (NodeId(1), NodeId(2)),
            epoch: Epoch(3),
            delta: Delta(1),
        };
        assert_eq!(dt_tuple.data, (NodeId(1), NodeId(2)));
    }
}
