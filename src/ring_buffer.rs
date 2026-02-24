//! Lock-free pre-allocated ring buffer for event ingestion.
//!
//! Stores [`Event`] values in a fixed-capacity circular buffer with
//! power-of-2 slot addressing. Each pushed event is assigned a globally
//! unique [`Epoch`] via an embedded [`EpochSequencer`], and the slot
//! index is computed by bitwise AND against a pre-computed mask --
//! O(1) with no branch or modulo.
//!
//! # Design
//!
//! - **Pre-allocated:** All slots are allocated once at construction.
//!   No heap allocation occurs on the hot path (push/get).
//! - **Power-of-2 masking:** Slot index = `epoch & mask` where
//!   `mask = capacity - 1`. This replaces modulo with a single bitwise AND.
//! - **Wrap-around:** When the buffer is full, new events overwrite the
//!   oldest slot. Reading an overwritten epoch returns `None` because
//!   the stored epoch no longer matches the requested epoch.
//! - **Epoch verification:** Each slot stores its assigned epoch alongside
//!   the event. On read, the stored epoch is compared against the
//!   requested epoch to detect overwrites.
//!
//! # Usage
//!
//! ```
//! use krabnet::ring_buffer::RingBuffer;
//! use krabnet::types::{Event, NodeId, TypeId};
//!
//! let mut rb = RingBuffer::new(8); // capacity must be power of 2
//! let epoch = rb.push(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(0) });
//! assert_eq!(rb.get(epoch).unwrap(), &Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(0) });
//! ```

use crate::sequencer::EpochSequencer;
use crate::types::{Epoch, Event};

/// A pre-allocated circular buffer for event ingestion.
///
/// Events are pushed into the buffer and assigned a monotonically
/// increasing [`Epoch`] by the embedded [`EpochSequencer`]. The buffer
/// has a fixed capacity (must be a power of 2) and wraps around when
/// full, overwriting the oldest events.
///
/// # Slot Addressing
///
/// For a capacity of `N` (power of 2), the mask is `N - 1`. The slot
/// for a given epoch is `epoch.0 as usize & mask`. This is equivalent
/// to `epoch % capacity` but uses a single bitwise AND instruction.
///
/// # Overwrite Detection
///
/// Each slot stores both the event and its assigned epoch. When reading
/// by epoch, the stored epoch is compared against the requested epoch.
/// If they differ (because a newer event overwrote the slot), `None`
/// is returned.
///
/// # Thread Safety
///
/// `RingBuffer` is `Send` and `Sync`:
/// - `Send`: The buffer can be transferred between threads. All fields
///   are `Send` (`Vec<Option<(Epoch, Event)>>`, `usize`, `EpochSequencer`).
/// - `Sync`: Shared read access via `get()` is safe because it only reads
///   immutable slot data. Mutation via `push()` requires `&mut self`,
///   which the borrow checker ensures is exclusive.
///
/// These traits are derived automatically because all constituent types
/// implement them. No `unsafe impl` is needed.
pub struct RingBuffer {
    /// Pre-allocated slot storage. Each slot holds `None` (unwritten) or
    /// `Some((epoch, event))` for overwrite detection.
    slots: Vec<Option<(Epoch, Event)>>,
    /// Total number of slots. Always a power of 2.
    capacity: usize,
    /// Bitmask for slot index computation: `capacity - 1`.
    mask: usize,
    /// Epoch sequencer for assigning monotonic epochs to pushed events.
    sequencer: EpochSequencer,
    /// Next write position (unbounded, wraps via mask).
    write_pos: usize,
}

impl RingBuffer {
    /// Creates a new ring buffer with the given capacity.
    ///
    /// All slots are pre-allocated as `None`. The capacity **must** be a
    /// power of 2 to enable bitwise slot addressing.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0 or not a power of 2.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::ring_buffer::RingBuffer;
    ///
    /// let rb = RingBuffer::new(16);
    /// assert_eq!(rb.capacity(), 16);
    /// assert_eq!(rb.len(), 0);
    /// ```
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0 && capacity.is_power_of_two(),
            "ring buffer capacity must be a power of 2, got {}",
            capacity
        );

        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);

        Self {
            slots,
            capacity,
            mask: capacity - 1,
            sequencer: EpochSequencer::new(),
            write_pos: 0,
        }
    }

    /// Pushes an event into the buffer, assigning it a unique epoch.
    ///
    /// The event is stored in the slot at `epoch & mask`, overwriting
    /// whatever was previously in that slot. The assigned epoch is
    /// returned to the caller.
    ///
    /// # Zero Allocation
    ///
    /// This method performs no heap allocation. The slot is pre-allocated
    /// and only the `Option` value is overwritten in place.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::ring_buffer::RingBuffer;
    /// use krabnet::types::{Event, NodeId, TypeId, Epoch};
    ///
    /// let mut rb = RingBuffer::new(4);
    /// let e0 = rb.push(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(0) });
    /// assert_eq!(e0, Epoch(0));
    /// let e1 = rb.push(Event::NodeRemoved { node_id: NodeId(1) });
    /// assert_eq!(e1, Epoch(1));
    /// ```
    pub fn push(&mut self, event: Event) -> Epoch {
        let epoch = self.sequencer.next();
        let slot = epoch.0 as usize & self.mask;
        self.slots[slot] = Some((epoch, event));
        self.write_pos += 1;
        epoch
    }

    /// Reads an event by its assigned epoch.
    ///
    /// Returns `Some(&Event)` if the slot still contains the event for
    /// the requested epoch, or `None` if:
    /// - The epoch was never written (slot is `None`)
    /// - The slot was overwritten by a newer event (stored epoch differs)
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::ring_buffer::RingBuffer;
    /// use krabnet::types::{Event, NodeId, TypeId, Epoch};
    ///
    /// let mut rb = RingBuffer::new(4);
    /// let epoch = rb.push(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(0) });
    /// assert!(rb.get(epoch).is_some());
    /// assert!(rb.get(Epoch(999)).is_none()); // never written
    /// ```
    pub fn get(&self, epoch: Epoch) -> Option<&Event> {
        let slot = epoch.0 as usize & self.mask;
        match &self.slots[slot] {
            Some((stored_epoch, event)) if *stored_epoch == epoch => Some(event),
            _ => None,
        }
    }

    /// Returns the number of events currently stored in the buffer.
    ///
    /// This is `min(write_pos, capacity)` -- once the buffer wraps
    /// around, `len()` stays at `capacity` because older events are
    /// overwritten, not removed.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::ring_buffer::RingBuffer;
    /// use krabnet::types::{Event, NodeId, TypeId};
    ///
    /// let mut rb = RingBuffer::new(4);
    /// assert_eq!(rb.len(), 0);
    /// rb.push(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(0) });
    /// assert_eq!(rb.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.write_pos.min(self.capacity)
    }

    /// Returns `true` if no events have been pushed to the buffer.
    pub fn is_empty(&self) -> bool {
        self.write_pos == 0
    }

    /// Returns the total slot capacity of the buffer.
    ///
    /// This is always the power-of-2 value passed to [`new()`](RingBuffer::new).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeId, TypeId};

    /// Helper to create a simple NodeAdded event.
    fn node_added(id: u64) -> Event {
        Event::NodeAdded {
            node_id: NodeId(id),
            type_id: TypeId(0),
        }
    }

    #[test]
    fn push_returns_sequential_epochs() {
        let mut rb = RingBuffer::new(8);
        for i in 0..5 {
            let epoch = rb.push(node_added(i));
            assert_eq!(epoch, Epoch(i));
        }
    }

    #[test]
    fn get_returns_pushed_event() {
        let mut rb = RingBuffer::new(4);
        let event = Event::NodeAdded {
            node_id: NodeId(42),
            type_id: TypeId(7),
        };
        let epoch = rb.push(event.clone());
        let retrieved = rb.get(epoch);
        assert_eq!(retrieved, Some(&event));
    }

    #[test]
    fn get_unwritten_returns_none() {
        let rb = RingBuffer::new(4);
        assert_eq!(rb.get(Epoch(0)), None);
        assert_eq!(rb.get(Epoch(100)), None);
        assert_eq!(rb.get(Epoch(u64::MAX)), None);
    }

    #[test]
    fn wraparound_overwrites_oldest() {
        let capacity = 4;
        let mut rb = RingBuffer::new(capacity);

        // Fill the buffer: epochs 0, 1, 2, 3
        for i in 0..capacity as u64 {
            rb.push(node_added(i));
        }

        // All should be readable
        for i in 0..capacity as u64 {
            assert!(
                rb.get(Epoch(i)).is_some(),
                "epoch {} should be readable before wraparound",
                i
            );
        }

        // Push one more (epoch 4), which overwrites slot 0 (epoch 0)
        let epoch4 = rb.push(node_added(100));
        assert_eq!(epoch4, Epoch(4));

        // Epoch 0 is now overwritten -- should return None
        assert_eq!(
            rb.get(Epoch(0)),
            None,
            "epoch 0 should be overwritten after wraparound"
        );

        // Epoch 4 should be readable
        assert_eq!(
            rb.get(Epoch(4)),
            Some(&node_added(100)),
            "epoch 4 should be readable"
        );

        // Epochs 1, 2, 3 should still be readable
        for i in 1..capacity as u64 {
            assert!(
                rb.get(Epoch(i)).is_some(),
                "epoch {} should still be readable",
                i
            );
        }
    }

    #[test]
    #[should_panic(expected = "power of 2")]
    fn capacity_must_be_power_of_two() {
        RingBuffer::new(3);
    }

    #[test]
    #[should_panic(expected = "power of 2")]
    fn capacity_zero_panics() {
        RingBuffer::new(0);
    }

    #[test]
    fn len_tracks_events() {
        let mut rb = RingBuffer::new(4);
        assert_eq!(rb.len(), 0);
        assert!(rb.is_empty());

        rb.push(node_added(1));
        assert_eq!(rb.len(), 1);
        assert!(!rb.is_empty());

        rb.push(node_added(2));
        assert_eq!(rb.len(), 2);

        rb.push(node_added(3));
        rb.push(node_added(4));
        assert_eq!(rb.len(), 4); // at capacity

        // After wraparound, len stays at capacity
        rb.push(node_added(5));
        assert_eq!(rb.len(), 4);
        rb.push(node_added(6));
        assert_eq!(rb.len(), 4);
    }

    #[test]
    fn capacity_returns_initial_value() {
        let rb = RingBuffer::new(16);
        assert_eq!(rb.capacity(), 16);
        let rb2 = RingBuffer::new(1024);
        assert_eq!(rb2.capacity(), 1024);
    }

    #[test]
    fn ring_buffer_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<RingBuffer>();
        assert_sync::<RingBuffer>();
    }

    #[test]
    fn multiple_wraparounds() {
        let mut rb = RingBuffer::new(2);
        // Push 10 events into a buffer of size 2
        for i in 0..10u64 {
            let epoch = rb.push(node_added(i));
            assert_eq!(epoch, Epoch(i));
        }
        // Only the last 2 should be readable
        assert_eq!(rb.get(Epoch(8)), Some(&node_added(8)));
        assert_eq!(rb.get(Epoch(9)), Some(&node_added(9)));
        // Earlier epochs should all be overwritten
        for i in 0..8u64 {
            assert_eq!(rb.get(Epoch(i)), None, "epoch {} should be overwritten", i);
        }
    }

    #[test]
    fn different_event_types_stored_correctly() {
        let mut rb = RingBuffer::new(4);

        let e0 = rb.push(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(0),
        });
        let e1 = rb.push(Event::NodeRemoved {
            node_id: NodeId(1),
        });
        let e2 = rb.push(Event::PropertyChanged {
            node_id: NodeId(1),
            key: 42,
            value: crate::types::PropertyValue::Integer(100),
        });

        assert!(matches!(rb.get(e0), Some(Event::NodeAdded { .. })));
        assert!(matches!(rb.get(e1), Some(Event::NodeRemoved { .. })));
        assert!(matches!(rb.get(e2), Some(Event::PropertyChanged { .. })));
    }
}
