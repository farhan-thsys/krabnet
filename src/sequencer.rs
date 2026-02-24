//! Global monotonic epoch sequencer.
//!
//! Produces strictly increasing [`Epoch`] values with no gaps under
//! sequential calls. The sequencer uses [`AtomicU64`] with [`SeqCst`](std::sync::atomic::Ordering::SeqCst)
//! ordering to guarantee global visibility of epoch assignments across
//! all threads.
//!
//! # Design
//!
//! - Epochs start at 0 and increment by 1 on each [`next()`](EpochSequencer::next) call
//! - [`current()`](EpochSequencer::current) is a read-only observation that never modifies state
//! - The sequencer is `Send + Sync` automatically (composed entirely of atomic types)
//! - No heap allocation after construction
//!
//! # Usage
//!
//! ```
//! use krabnet::sequencer::EpochSequencer;
//! use krabnet::Epoch;
//!
//! let seq = EpochSequencer::new();
//! assert_eq!(seq.next(), Epoch(0));
//! assert_eq!(seq.next(), Epoch(1));
//! assert_eq!(seq.current(), Epoch(2)); // next value, not yet assigned
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use crate::Epoch;

/// A monotonic epoch counter backed by [`AtomicU64`].
///
/// Each call to [`next()`](EpochSequencer::next) atomically increments
/// the counter and returns the previous value wrapped in an [`Epoch`].
/// This guarantees strictly increasing, gap-free epoch assignment even
/// under concurrent access (though the current ring buffer design uses
/// `&mut self` for push, so concurrent sequencing is a future concern).
///
/// # Thread Safety
///
/// `EpochSequencer` is `Send + Sync` automatically because it is
/// composed entirely of [`AtomicU64`], which itself implements both
/// traits. No unsafe code is needed.
pub struct EpochSequencer {
    /// The next epoch value to be assigned.
    counter: AtomicU64,
}

impl EpochSequencer {
    /// Creates a new sequencer starting at epoch 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::sequencer::EpochSequencer;
    /// use krabnet::Epoch;
    ///
    /// let seq = EpochSequencer::new();
    /// assert_eq!(seq.current(), Epoch(0));
    /// ```
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    /// Assigns and returns the next epoch.
    ///
    /// Atomically increments the internal counter and returns the
    /// **previous** value as an [`Epoch`]. This means the first call
    /// returns `Epoch(0)`, the second returns `Epoch(1)`, and so on.
    ///
    /// Uses [`SeqCst`](Ordering::SeqCst) ordering to ensure all threads
    /// observe epoch assignments in a single total order.
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::sequencer::EpochSequencer;
    /// use krabnet::Epoch;
    ///
    /// let seq = EpochSequencer::new();
    /// assert_eq!(seq.next(), Epoch(0));
    /// assert_eq!(seq.next(), Epoch(1));
    /// assert_eq!(seq.next(), Epoch(2));
    /// ```
    pub fn next(&self) -> Epoch {
        let prev = self.counter.fetch_add(1, Ordering::SeqCst);
        Epoch(prev)
    }

    /// Returns the current counter value without incrementing.
    ///
    /// This is the epoch that **will** be assigned on the next call to
    /// [`next()`](EpochSequencer::next). Calling `current()` multiple
    /// times without intervening `next()` calls returns the same value.
    ///
    /// Uses [`SeqCst`](Ordering::SeqCst) ordering for consistency with
    /// [`next()`](EpochSequencer::next).
    ///
    /// # Examples
    ///
    /// ```
    /// use krabnet::sequencer::EpochSequencer;
    /// use krabnet::Epoch;
    ///
    /// let seq = EpochSequencer::new();
    /// assert_eq!(seq.current(), Epoch(0));
    /// assert_eq!(seq.current(), Epoch(0)); // unchanged
    /// seq.next();
    /// assert_eq!(seq.current(), Epoch(1));
    /// ```
    pub fn current(&self) -> Epoch {
        let val = self.counter.load(Ordering::SeqCst);
        Epoch(val)
    }
}

impl Default for EpochSequencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_zero() {
        let seq = EpochSequencer::new();
        assert_eq!(seq.next(), Epoch(0));
    }

    #[test]
    fn sequential_epochs_are_increasing() {
        let seq = EpochSequencer::new();
        let mut prev = seq.next();
        for _ in 1..100 {
            let curr = seq.next();
            assert!(curr.0 == prev.0 + 1, "expected {}, got {}", prev.0 + 1, curr.0);
            prev = curr;
        }
    }

    #[test]
    fn current_does_not_increment() {
        let seq = EpochSequencer::new();
        let c1 = seq.current();
        let c2 = seq.current();
        let c3 = seq.current();
        assert_eq!(c1, c2);
        assert_eq!(c2, c3);
        assert_eq!(c1, Epoch(0));
    }

    #[test]
    fn current_reflects_next_calls() {
        let seq = EpochSequencer::new();
        assert_eq!(seq.current(), Epoch(0));
        seq.next();
        assert_eq!(seq.current(), Epoch(1));
        seq.next();
        seq.next();
        assert_eq!(seq.current(), Epoch(3));
    }

    #[test]
    fn default_starts_at_zero() {
        let seq = EpochSequencer::default();
        assert_eq!(seq.next(), Epoch(0));
    }

    #[test]
    fn sequencer_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<EpochSequencer>();
        assert_sync::<EpochSequencer>();
    }
}
