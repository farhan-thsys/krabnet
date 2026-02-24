//! Background compaction worker with double-buffer strategy.
//!
//! The [`CompactionWorker`] runs on a dedicated `std::thread` and processes
//! compaction requests via a crossbeam unbounded channel. Compaction uses
//! double-buffering: the worker acquires a read lock to clone the
//! `DiffCollection`, compacts the clone off-lock (the expensive step),
//! then acquires a write lock only to swap the compacted collection back.
//! This ensures readers are blocked only during the brief swap, never
//! during the full compaction.
//!
//! # Usage
//!
//! ```no_run
//! use krabnet::compaction::CompactionWorker;
//! use krabnet::frame::Frame;
//! use krabnet::types::{Epoch, NodeId};
//! use std::sync::{Arc, RwLock};
//!
//! let worker = CompactionWorker::new(10_000);
//!
//! let frame = Frame::new(0, NodeId(1), vec![]);
//! let frame_ref = Arc::new(RwLock::new(frame));
//!
//! if worker.should_compact(15_000) {
//!     worker.request_compaction(frame_ref.clone(), Epoch(5));
//! }
//!
//! let stats = worker.stats();
//! worker.shutdown();
//! ```

use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crossbeam::channel::{self, Sender};

use crate::frame::Frame;
use crate::types::Epoch;

/// Statistics tracked by the compaction worker.
///
/// Provides a snapshot of compaction activity: how many compactions have
/// completed, total tuples before and after compaction, and cumulative
/// compaction time in microseconds.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of compaction operations completed.
    pub compactions_completed: u64,
    /// Total tuples seen before compaction (cumulative across all compactions).
    pub tuples_before: u64,
    /// Total tuples remaining after compaction (cumulative across all compactions).
    pub tuples_after: u64,
    /// Total compaction time in microseconds (cumulative).
    pub total_compaction_time_us: u64,
}

/// A request to compact a specific frame at a given frontier epoch.
///
/// Holds an `Arc<RwLock<Frame>>` for the frame to compact and the frontier
/// [`Epoch`] below which tuples are compacted. The worker uses double-buffering:
/// read lock to clone, compact off-lock, write lock only to swap back.
pub struct CompactionRequest {
    /// The frame to compact, wrapped in Arc<RwLock<>> for thread-safe access.
    pub frame: Arc<RwLock<Frame>>,
    /// The frontier epoch: compact tuples at or before this epoch.
    pub frontier: Epoch,
}

/// Background compaction worker running on a dedicated std::thread.
///
/// Receives [`CompactionRequest`]s via a crossbeam unbounded channel and
/// processes them using double-buffering to minimize lock contention:
///
/// 1. Acquire READ lock, clone the DiffCollection, release read lock.
/// 2. Compact the cloned DiffCollection off-lock (expensive step).
/// 3. Acquire WRITE lock, swap the compacted collection back, release write lock.
///
/// The write lock is held ONLY during the swap, not during the full compaction.
pub struct CompactionWorker {
    /// Sender end of the request channel.
    sender: Option<Sender<CompactionRequest>>,
    /// Shared compaction statistics.
    stats: Arc<Mutex<CompactionStats>>,
    /// Tuple count threshold for triggering compaction.
    threshold: usize,
    /// Handle to the background worker thread.
    handle: Option<std::thread::JoinHandle<()>>,
}

impl CompactionWorker {
    /// Creates a new compaction worker with the given tuple count threshold.
    ///
    /// Spawns a dedicated background thread named "compaction-worker" that
    /// processes compaction requests from the channel.
    ///
    /// # Arguments
    ///
    /// * `threshold` - Tuple count at or above which compaction should be triggered.
    pub fn new(threshold: usize) -> Self {
        let (sender, receiver) = channel::unbounded::<CompactionRequest>();
        let stats: Arc<Mutex<CompactionStats>> = Arc::new(Mutex::new(CompactionStats::default()));
        let stats_clone: Arc<Mutex<CompactionStats>> = Arc::clone(&stats);

        let handle = std::thread::Builder::new()
            .name("compaction-worker".to_string())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let start = Instant::now();

                    // Step 1: Acquire READ lock, clone the DiffCollection, read tuple count.
                    let (cloned_diff, tuples_before) = {
                        let frame_read = request.frame.read().expect("RwLock poisoned");
                        let diff = frame_read.clone_diff_collection();
                        let count = frame_read.tuple_count();
                        (diff, count)
                    };
                    // Read lock released here.

                    // Step 2: Compact the clone off-lock (expensive step, no lock held).
                    let mut compacted_diff: crate::diff::DiffCollection<Vec<crate::types::NodeId>> =
                        cloned_diff;
                    compacted_diff.compact(request.frontier);

                    // Step 3: Acquire WRITE lock, swap compacted collection back.
                    let tuples_after = {
                        let mut frame_write = request.frame.write().expect("RwLock poisoned");
                        frame_write.swap_diff_collection(compacted_diff);
                        frame_write.tuple_count()
                    };
                    // Write lock released here.

                    // Step 4: Update stats.
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    let mut s = stats_clone.lock().expect("Mutex poisoned");
                    s.compactions_completed += 1;
                    s.tuples_before += tuples_before as u64;
                    s.tuples_after += tuples_after as u64;
                    s.total_compaction_time_us += elapsed_us;
                }
            })
            .expect("Failed to spawn compaction-worker thread");

        Self {
            sender: Some(sender),
            stats,
            threshold,
            handle: Some(handle),
        }
    }

    /// Sends a compaction request to the background worker.
    ///
    /// Non-blocking (unbounded channel). The worker will process the request
    /// asynchronously using double-buffering.
    pub fn request_compaction(&self, frame: Arc<RwLock<Frame>>, frontier: Epoch) {
        if let Some(ref sender) = self.sender {
            let _ = sender.send(CompactionRequest { frame, frontier });
        }
    }

    /// Returns whether the given tuple count meets or exceeds the compaction threshold.
    pub fn should_compact(&self, tuple_count: usize) -> bool {
        tuple_count >= self.threshold
    }

    /// Returns a clone of the current compaction statistics.
    pub fn stats(&self) -> CompactionStats {
        self.stats.lock().expect("Mutex poisoned").clone()
    }

    /// Shuts down the compaction worker by dropping the sender and joining the thread.
    ///
    /// Dropping the sender causes the receiver to return `Err`, breaking the
    /// worker loop. The thread is then joined to ensure clean shutdown.
    pub fn shutdown(mut self) {
        self.sender.take(); // Drop sender -> breaks recv loop
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for CompactionWorker {
    fn drop(&mut self) {
        self.sender.take(); // Drop sender -> breaks recv loop
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Delta, Epoch, NodeId};

    /// Helper: creates a frame with N assert+retract pairs to produce 2*N tuples.
    fn frame_with_pairs(n: usize) -> Frame {
        let mut frame = Frame::new(0, NodeId(1), vec![]);
        for i in 0..n {
            let path = vec![NodeId(1), NodeId((i + 2) as u64)];
            frame.apply_delta(path.clone(), Epoch(i as u64), Delta(1));
            frame.apply_delta(path, Epoch((i + n) as u64), Delta(-1));
        }
        frame
    }

    #[test]
    fn test_compaction_worker_compacts_on_request() {
        let frame = frame_with_pairs(100);
        assert_eq!(frame.tuple_count(), 200); // 100 asserts + 100 retracts

        let frame_ref = Arc::new(RwLock::new(frame));
        let worker = CompactionWorker::new(100);

        // Verify readers are not blocked: we can acquire a read lock right now
        {
            let _read_guard = frame_ref.read().expect("RwLock poisoned");
            // Read lock acquired successfully -- not blocked
        }

        // Send compaction request with frontier that covers all tuples
        worker.request_compaction(Arc::clone(&frame_ref), Epoch(200));

        // Wait for compaction to complete
        std::thread::sleep(std::time::Duration::from_millis(200));

        // After compaction, all 100 assert+retract pairs should be annihilated
        let guard = frame_ref.read().expect("RwLock poisoned");
        assert_eq!(
            guard.tuple_count(),
            0,
            "All assert+retract pairs should be annihilated after compaction"
        );
    }

    #[test]
    fn test_compaction_stats_tracking() {
        let frame = frame_with_pairs(50);
        let tuples_before = frame.tuple_count(); // 100
        assert_eq!(tuples_before, 100);

        let frame_ref = Arc::new(RwLock::new(frame));
        let worker = CompactionWorker::new(50);

        worker.request_compaction(Arc::clone(&frame_ref), Epoch(200));

        // Wait for compaction to complete
        std::thread::sleep(std::time::Duration::from_millis(200));

        let stats = worker.stats();
        assert_eq!(stats.compactions_completed, 1);
        assert_eq!(stats.tuples_before, 100);
        assert_eq!(stats.tuples_after, 0); // All annihilated
        assert!(stats.total_compaction_time_us > 0);
    }

    #[test]
    fn test_should_compact_threshold() {
        let worker = CompactionWorker::new(10_000);

        assert!(!worker.should_compact(0));
        assert!(!worker.should_compact(9_999));
        assert!(worker.should_compact(10_000));
        assert!(worker.should_compact(10_001));
        assert!(worker.should_compact(100_000));
    }
}
