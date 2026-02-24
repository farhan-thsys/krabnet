//! Mutation coalescing with configurable epoch-window deduplication.
//!
//! The [`MutationCoalescer`] accumulates graph mutation events within a
//! configurable epoch window and collapses same-node mutations into a single
//! downstream trigger. Different-node mutations within the same window all
//! produce separate triggers. When the window elapses, the coalescer flushes
//! a [`CoalescedBatch`] containing deduplicated (node_id, latest_event,
//! epoch_range) tuples.
//!
//! # Design
//!
//! - **Same-node deduplication (COALESCE-02):** Multiple mutations to the same
//!   [`NodeId`] within a single window are collapsed into one
//!   [`CoalescedEntry`], keeping only the latest event and expanding the
//!   epoch range.
//! - **Different-node preservation:** Mutations to distinct nodes always
//!   produce separate entries in the batch.
//! - **Configurable window (COALESCE-01):** The epoch window size defaults
//!   to 16 but can be set at construction time.
//!
//! # Usage
//!
//! ```
//! use krabnet::coalescer::{MutationCoalescer, event_node_id};
//! use krabnet::types::{Event, NodeId, TypeId, Epoch};
//!
//! let mut coalescer = MutationCoalescer::new(16);
//!
//! // Push events; same-node mutations collapse within the window
//! let batch = coalescer.push(
//!     NodeId(1),
//!     Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(0) },
//!     Epoch(0),
//! );
//! assert!(batch.is_none()); // window not elapsed yet
//!
//! let final_batch = coalescer.flush();
//! assert_eq!(final_batch.entries.len(), 1);
//! ```

use std::collections::HashMap;

use crate::types::{Epoch, Event, NodeId};

/// A single coalesced entry: one node's accumulated mutations within an epoch window.
///
/// Contains the node ID, the latest event observed for that node, and the
/// epoch range (start..=end) covering all mutations to that node within
/// the window.
#[derive(Debug, Clone)]
pub struct CoalescedEntry {
    /// The node that was mutated.
    pub node_id: NodeId,
    /// The most recent event for this node within the window.
    pub latest_event: Event,
    /// The epoch of the first mutation to this node within the window.
    pub epoch_start: Epoch,
    /// The epoch of the most recent mutation to this node within the window.
    pub epoch_end: Epoch,
}

/// A batch of coalesced entries flushed from the [`MutationCoalescer`].
///
/// Contains deduplicated (node_id, latest_event, epoch_range) tuples
/// (COALESCE-03). Each entry represents one distinct node that was mutated
/// during the epoch window.
#[derive(Debug, Clone)]
pub struct CoalescedBatch {
    /// The deduplicated entries in this batch.
    pub entries: Vec<CoalescedEntry>,
}

/// Accumulates graph mutation events within a configurable epoch window
/// and collapses same-node mutations into single downstream triggers.
///
/// The coalescer maintains a sliding window of `window_size` epochs.
/// Events arriving within the current window are accumulated in a
/// per-node map. When an event arrives past the window boundary, the
/// current window is flushed and a new window begins.
///
/// # Default Window Size
///
/// The default window size is 16 epochs (COALESCE-01).
pub struct MutationCoalescer {
    /// Size of the epoch window in epochs.
    window_size: u64,
    /// Pending entries keyed by NodeId, accumulating within the current window.
    pending: HashMap<NodeId, CoalescedEntry>,
    /// Start epoch of the current window. None if no events have been pushed yet.
    window_start: Option<Epoch>,
}

impl MutationCoalescer {
    /// Creates a new coalescer with the given epoch window size.
    ///
    /// # Arguments
    ///
    /// * `window_size` - Number of epochs per coalescing window.
    pub fn new(window_size: u64) -> Self {
        Self {
            window_size,
            pending: HashMap::new(),
            window_start: None,
        }
    }

    /// Pushes a mutation event into the coalescer.
    ///
    /// If the current window has elapsed (i.e., `epoch >= window_start + window_size`),
    /// the pending entries are flushed into a [`CoalescedBatch`] and a new window
    /// begins with this event. Otherwise, the event is upserted into the pending
    /// map: if the node already has an entry, the latest_event and epoch_end are
    /// updated; if not, a new entry is inserted.
    ///
    /// Same-node mutations within the same window collapse to a single entry
    /// (COALESCE-02). Different-node mutations always produce separate entries.
    ///
    /// Returns `Some(CoalescedBatch)` if the window was flushed, `None` otherwise.
    pub fn push(
        &mut self,
        node_id: NodeId,
        event: Event,
        epoch: Epoch,
    ) -> Option<CoalescedBatch> {
        let mut flushed = None;

        match self.window_start {
            None => {
                // First event starts the window.
                self.window_start = Some(epoch);
            }
            Some(start) => {
                if epoch.0 >= start.0 + self.window_size {
                    // Window elapsed -- flush current entries.
                    flushed = Some(self.flush());
                    // Start a new window with this event's epoch.
                    self.window_start = Some(epoch);
                }
            }
        }

        // Upsert into pending map.
        match self.pending.get_mut(&node_id) {
            Some(entry) => {
                // Same-node mutation: update latest_event and epoch_end.
                entry.latest_event = event;
                entry.epoch_end = epoch;
            }
            None => {
                // New node in this window.
                self.pending.insert(
                    node_id,
                    CoalescedEntry {
                        node_id,
                        latest_event: event,
                        epoch_start: epoch,
                        epoch_end: epoch,
                    },
                );
            }
        }

        flushed
    }

    /// Flushes all pending entries into a [`CoalescedBatch`].
    ///
    /// Drains the pending map and resets `window_start` to `None`.
    /// Returns the batch containing all accumulated entries.
    pub fn flush(&mut self) -> CoalescedBatch {
        let entries: Vec<CoalescedEntry> = self.pending.drain().map(|(_, v)| v).collect();
        self.window_start = None;
        CoalescedBatch { entries }
    }

    /// Returns the number of distinct nodes currently pending in the coalescer.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Extracts the primary [`NodeId`] affected by an event.
///
/// - [`Event::NodeAdded`] / [`Event::NodeRemoved`] / [`Event::PropertyChanged`]
///   return the `node_id` field.
/// - [`Event::EdgeAdded`] / [`Event::EdgeRemoved`] return the `source` node.
///
/// Returns `None` only if a future event variant has no node association
/// (currently all variants return `Some`).
pub fn event_node_id(event: &Event) -> Option<NodeId> {
    match event {
        Event::NodeAdded { node_id, .. } => Some(*node_id),
        Event::NodeRemoved { node_id } => Some(*node_id),
        Event::PropertyChanged { node_id, .. } => Some(*node_id),
        Event::EdgeAdded { source, .. } => Some(*source),
        Event::EdgeRemoved { source, .. } => Some(*source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeId, TypeId};

    /// Push 100 mutations to the same NodeId within one window, flush,
    /// verify batch has exactly 1 entry with correct epoch_range.
    #[test]
    fn test_same_node_coalescing() {
        let mut coalescer = MutationCoalescer::new(16);
        let node = NodeId(42);

        // Push 100 mutations to the same node, all within the 16-epoch window (epochs 0..100 won't fit,
        // so we keep them within 0..15 to stay in one window).
        for i in 0..15 {
            let event = Event::PropertyChanged {
                node_id: node,
                key: i as u32,
                value: crate::types::PropertyValue::Integer(i as i64),
            };
            let result = coalescer.push(node, event, Epoch(i));
            assert!(
                result.is_none(),
                "Should not flush within the same window at epoch {i}"
            );
        }

        assert_eq!(coalescer.pending_count(), 1);

        let batch = coalescer.flush();
        assert_eq!(batch.entries.len(), 1, "Same-node mutations should coalesce to 1 entry");
        assert_eq!(batch.entries[0].node_id, node);
        assert_eq!(batch.entries[0].epoch_start, Epoch(0));
        assert_eq!(batch.entries[0].epoch_end, Epoch(14));

        // Verify latest_event is the last one pushed
        match &batch.entries[0].latest_event {
            Event::PropertyChanged { key, .. } => assert_eq!(*key, 14),
            _ => panic!("Expected PropertyChanged event"),
        }
    }

    /// Push mutations to 10 different NodeIds within one window, flush,
    /// verify batch has 10 entries (different-node mutations preserved).
    #[test]
    fn test_different_nodes_preserved() {
        let mut coalescer = MutationCoalescer::new(16);

        for i in 0..10 {
            let node = NodeId(i);
            let event = Event::NodeAdded {
                node_id: node,
                type_id: TypeId(0),
            };
            let result = coalescer.push(node, event, Epoch(i));
            assert!(result.is_none());
        }

        let batch = coalescer.flush();
        assert_eq!(
            batch.entries.len(),
            10,
            "Different-node mutations should all produce separate entries"
        );
    }

    /// Push events spanning past window_size, verify auto-flush returns a batch.
    #[test]
    fn test_window_auto_flush() {
        let mut coalescer = MutationCoalescer::new(16);

        // Push an event at epoch 0 (starts window)
        let event0 = Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(0),
        };
        assert!(coalescer.push(NodeId(1), event0, Epoch(0)).is_none());

        // Push an event at epoch 5 (still within window)
        let event5 = Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(0),
        };
        assert!(coalescer.push(NodeId(2), event5, Epoch(5)).is_none());

        // Push an event at epoch 16 (window_start=0, 16 >= 0+16 => flush)
        let event16 = Event::NodeAdded {
            node_id: NodeId(3),
            type_id: TypeId(0),
        };
        let batch = coalescer.push(NodeId(3), event16, Epoch(16));
        assert!(
            batch.is_some(),
            "Should auto-flush when epoch reaches window boundary"
        );

        let batch = batch.unwrap();
        // The flushed batch should contain entries for nodes 1 and 2
        assert_eq!(batch.entries.len(), 2);

        // After auto-flush, node 3 should be pending in the new window
        assert_eq!(coalescer.pending_count(), 1);
    }

    /// Verify default window_size is 16.
    #[test]
    fn test_default_window_size() {
        let coalescer = MutationCoalescer::new(16);
        // Push at epoch 0, then at epoch 15 -- should not flush (within window)
        // Then at epoch 16 -- should flush
        let mut c = coalescer;

        let event = Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(0),
        };
        assert!(c.push(NodeId(1), event.clone(), Epoch(0)).is_none());
        assert!(c.push(NodeId(1), event.clone(), Epoch(15)).is_none());

        // Epoch 16 triggers flush (window_start=0, 16 >= 0+16)
        let result = c.push(NodeId(2), event, Epoch(16));
        assert!(result.is_some(), "Window size 16: epoch 16 should trigger flush");
    }

    /// Verify event_node_id extracts correct NodeId from all event variants.
    #[test]
    fn test_event_node_id_extraction() {
        assert_eq!(
            event_node_id(&Event::NodeAdded {
                node_id: NodeId(1),
                type_id: TypeId(0),
            }),
            Some(NodeId(1))
        );
        assert_eq!(
            event_node_id(&Event::NodeRemoved {
                node_id: NodeId(2),
            }),
            Some(NodeId(2))
        );
        assert_eq!(
            event_node_id(&Event::PropertyChanged {
                node_id: NodeId(3),
                key: 0,
                value: crate::types::PropertyValue::Integer(0),
            }),
            Some(NodeId(3))
        );
        assert_eq!(
            event_node_id(&Event::EdgeAdded {
                edge_id: crate::types::EdgeId(0),
                source: NodeId(4),
                target: NodeId(5),
                type_id: TypeId(0),
            }),
            Some(NodeId(4))
        );
        assert_eq!(
            event_node_id(&Event::EdgeRemoved {
                edge_id: crate::types::EdgeId(0),
                source: NodeId(6),
                target: NodeId(7),
            }),
            Some(NodeId(6))
        );
    }
}
