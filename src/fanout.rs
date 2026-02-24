//! Fan-out limiting with priority-based deferred evaluation.
//!
//! The [`FanOutLimiter`] caps the number of immediate frame evaluations
//! triggered by a single event. When a super-node mutation affects thousands
//! of frames, only the top `max_fanout` frames (by priority score) are
//! evaluated immediately. The remainder are queued in the
//! [`DeferredEvalQueue`], sorted by priority score descending, for later
//! batch processing.
//!
//! # Design
//!
//! - **MAX_FANOUT cap (FANOUT-01):** Configurable limit (default 1000) on
//!   immediate evaluations per event.
//! - **Priority-based deferral (FANOUT-02):** Excess frames are queued by
//!   priority score, highest first, ensuring the most important frames are
//!   always processed immediately.
//! - **Batch drain:** Deferred frames can be drained in batches for
//!   background or idle-time processing.
//!
//! # Usage
//!
//! ```
//! use krabnet::fanout::FanOutLimiter;
//!
//! let mut limiter = FanOutLimiter::new(1000);
//!
//! // 500 frames affected, all processed immediately
//! let frames: Vec<(u64, f64)> = (0..500).map(|i| (i, i as f64 / 500.0)).collect();
//! let (immediate, deferred_count) = limiter.limit(frames);
//! assert_eq!(immediate.len(), 500);
//! assert_eq!(deferred_count, 0);
//! ```

/// A single entry in the deferred evaluation queue.
///
/// Holds the frame ID and its priority score for ordering.
#[derive(Debug, Clone)]
pub struct DeferredEvalEntry {
    /// The frame to be evaluated.
    pub frame_id: u64,
    /// The priority score used for ordering (higher = more important).
    pub priority_score: f64,
}

/// A priority queue of deferred frame evaluations, sorted by priority
/// score descending (highest priority first).
///
/// Uses a sorted `Vec` with binary search insertion to maintain order.
/// This is efficient for the expected usage pattern: bulk insertion
/// followed by batch draining.
#[derive(Debug, Clone)]
pub struct DeferredEvalQueue {
    /// Entries sorted by priority_score descending.
    queue: Vec<DeferredEvalEntry>,
}

impl DeferredEvalQueue {
    /// Creates a new empty deferred evaluation queue.
    pub fn new() -> Self {
        Self { queue: Vec::new() }
    }

    /// Inserts a frame into the queue, maintaining descending sort order
    /// by priority score.
    ///
    /// Uses `binary_search_by` to find the insertion point in O(log n),
    /// then inserts in O(n) due to Vec shifting. This is acceptable because
    /// bulk insertions happen infrequently (only when fan-out exceeds limit).
    pub fn push(&mut self, frame_id: u64, priority_score: f64) {
        // We want descending order: higher scores first.
        // binary_search_by returns Ok(pos) if found, Err(pos) if not found.
        // We compare in reverse (b.cmp(a)) for descending order.
        let pos = self
            .queue
            .binary_search_by(|entry| {
                entry
                    .priority_score
                    .partial_cmp(&priority_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .reverse()
            })
            .unwrap_or_else(|pos| pos);

        self.queue.insert(
            pos,
            DeferredEvalEntry {
                frame_id,
                priority_score,
            },
        );
    }

    /// Pops up to `count` highest-priority frame IDs from the queue.
    ///
    /// Since the queue is sorted descending, this drains from the front.
    /// Returns the frame IDs in priority order (highest first).
    pub fn pop_batch(&mut self, count: usize) -> Vec<u64> {
        let take = count.min(self.queue.len());
        self.queue
            .drain(..take)
            .map(|entry| entry.frame_id)
            .collect()
    }

    /// Returns the number of entries currently in the queue.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl Default for DeferredEvalQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Limits fan-out of frame evaluations per event, deferring excess by priority.
///
/// When a mutation event affects more frames than `max_fanout`,
/// only the highest-priority frames are evaluated immediately. The rest are
/// queued in a [`DeferredEvalQueue`] for later processing.
///
/// # Default MAX_FANOUT
///
/// The default maximum fan-out is 1000 (FANOUT-01).
pub struct FanOutLimiter {
    /// Maximum number of frames to evaluate immediately per event.
    max_fanout: usize,
    /// Queue for deferred (excess) frame evaluations.
    deferred: DeferredEvalQueue,
}

impl FanOutLimiter {
    /// Creates a new fan-out limiter with the given maximum immediate evaluations.
    ///
    /// # Arguments
    ///
    /// * `max_fanout` - Maximum number of frames evaluated immediately per event.
    pub fn new(max_fanout: usize) -> Self {
        Self {
            max_fanout,
            deferred: DeferredEvalQueue::new(),
        }
    }

    /// Splits affected frames into immediate and deferred sets.
    ///
    /// Takes a list of `(frame_id, priority_score)` pairs. If the count is
    /// within `max_fanout`, all frames are returned as immediate. Otherwise,
    /// the frames are sorted by priority score descending, the top
    /// `max_fanout` are returned as immediate, and the remainder are pushed
    /// into the [`DeferredEvalQueue`] (FANOUT-02).
    ///
    /// Returns `(immediate_frame_ids, count_deferred)`.
    pub fn limit(&mut self, mut affected_frame_ids: Vec<(u64, f64)>) -> (Vec<u64>, usize) {
        if affected_frame_ids.len() <= self.max_fanout {
            // All frames fit within the limit.
            let immediate: Vec<u64> = affected_frame_ids.iter().map(|(id, _)| *id).collect();
            return (immediate, 0);
        }

        // Sort descending by priority_score (highest first).
        affected_frame_ids.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Take the top max_fanout as immediate.
        let immediate: Vec<u64> = affected_frame_ids[..self.max_fanout]
            .iter()
            .map(|(id, _)| *id)
            .collect();

        // Push the remainder into the deferred queue.
        let deferred_count = affected_frame_ids.len() - self.max_fanout;
        for &(frame_id, priority_score) in &affected_frame_ids[self.max_fanout..] {
            self.deferred.push(frame_id, priority_score);
        }

        (immediate, deferred_count)
    }

    /// Drains up to `count` deferred frames for later processing.
    ///
    /// Returns frame IDs in priority order (highest first).
    pub fn drain_deferred(&mut self, count: usize) -> Vec<u64> {
        self.deferred.pop_batch(count)
    }

    /// Returns the number of frames currently in the deferred queue.
    pub fn deferred_count(&self) -> usize {
        self.deferred.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 500 frames with max_fanout=1000: all returned as immediate.
    #[test]
    fn test_under_max_fanout() {
        let mut limiter = FanOutLimiter::new(1000);
        let frames: Vec<(u64, f64)> = (0..500).map(|i| (i, i as f64 / 500.0)).collect();

        let (immediate, deferred_count) = limiter.limit(frames);
        assert_eq!(immediate.len(), 500);
        assert_eq!(deferred_count, 0);
        assert_eq!(limiter.deferred_count(), 0);
    }

    /// 2000 frames with max_fanout=1000: top 1000 by priority returned
    /// immediate, 1000 deferred.
    #[test]
    fn test_over_max_fanout() {
        let mut limiter = FanOutLimiter::new(1000);
        // Create 2000 frames with priority scores 0.0 to 1999.0
        let frames: Vec<(u64, f64)> = (0..2000).map(|i| (i, i as f64)).collect();

        let (immediate, deferred_count) = limiter.limit(frames);
        assert_eq!(immediate.len(), 1000);
        assert_eq!(deferred_count, 1000);
        assert_eq!(limiter.deferred_count(), 1000);

        // The immediate set should contain the top 1000 by priority (frames 1000..2000)
        for &id in &immediate {
            assert!(
                id >= 1000,
                "Immediate frame {id} should be in the top 1000 by priority"
            );
        }
    }

    /// Verify deferred frames are popped in priority order (highest first).
    #[test]
    fn test_deferred_queue_priority_order() {
        let mut queue = DeferredEvalQueue::new();

        // Insert in random order
        queue.push(1, 0.5);
        queue.push(2, 0.9);
        queue.push(3, 0.1);
        queue.push(4, 0.7);
        queue.push(5, 0.3);

        // Pop all -- should come out in descending priority order
        let batch = queue.pop_batch(5);
        assert_eq!(batch, vec![2, 4, 1, 5, 3]);
    }

    /// Push excess frames, drain_deferred returns correct batch in priority order.
    #[test]
    fn test_drain_deferred() {
        let mut limiter = FanOutLimiter::new(2);

        // 5 frames, max_fanout=2, so 3 deferred
        let frames = vec![
            (10, 0.1),
            (20, 0.5),
            (30, 0.9),
            (40, 0.3),
            (50, 0.7),
        ];

        let (immediate, deferred_count) = limiter.limit(frames);
        assert_eq!(immediate.len(), 2);
        assert_eq!(deferred_count, 3);

        // Immediate should be highest priority: 30 (0.9) and 50 (0.7)
        assert_eq!(immediate[0], 30);
        assert_eq!(immediate[1], 50);

        // Drain 2 deferred -- should get next highest priority
        let drained = limiter.drain_deferred(2);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0], 20); // 0.5
        assert_eq!(drained[1], 40); // 0.3

        // 1 remaining
        assert_eq!(limiter.deferred_count(), 1);

        let rest = limiter.drain_deferred(10);
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0], 10); // 0.1
    }
}
