//! Differential MVCC collection with +1/-1 multiset semantics.
//!
//! This module implements the mathematical core of Krabnet's differential engine.
//! Frames store their materialized paths as differential tuples. The +1/-1 math
//! is provably exact: assertion + retraction = annihilation, double-assert =
//! multiplicity 2.
//!
//! # Concepts
//!
//! - **Assert (+1):** Records the presence of a payload at a given epoch.
//! - **Retract (-1):** Records the removal of a payload at a given epoch.
//! - **Net delta:** The algebraic sum of deltas for a given payload. A positive
//!   net delta means the payload is "present" with that multiplicity.
//! - **Temporal snapshot:** At epoch E, returns payloads with positive net delta
//!   considering only tuples at or before E.
//! - **Compaction:** Below a frontier epoch, collapses multiple tuples per payload
//!   into one, annihilates net-zero payloads, and warns on negative net deltas.

use std::collections::HashMap;
use std::hash::Hash;

use crate::types::{Delta, DiffTuple, Epoch};

/// Result of a compaction operation.
///
/// Reports how many payloads were annihilated (net-zero removed),
/// how many were collapsed into single tuples, and any warnings
/// about negative net deltas encountered.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionResult {
    /// Count of payloads that netted to zero and were removed.
    pub annihilated: usize,
    /// Count of payloads collapsed to a single tuple.
    pub collapsed: usize,
    /// Warnings for payloads with negative net deltas.
    pub warnings: Vec<String>,
}

/// A differential MVCC collection with mathematically exact multiset semantics.
///
/// Stores differential tuples (payload + epoch + delta) and supports:
/// - Assertion (+1) and retraction (-1) of payloads
/// - Per-payload and aggregate net delta computation
/// - Temporal snapshots at any epoch
/// - Compaction below a frontier epoch
///
/// # Type Parameter
///
/// `T` must implement `Clone + PartialEq + Eq + Hash` for grouping and lookup
/// operations. In practice, `T` will be path tuples or other materialized
/// traversal data.
#[derive(Debug, Clone)]
pub struct DiffCollection<T>
where
    T: Clone + PartialEq + Eq + Hash,
{
    /// All differential tuples in insertion order.
    tuples: Vec<DiffTuple<T>>,
    /// Cached aggregate net delta (sum of all delta values).
    net_delta: i64,
}

impl<T> DiffCollection<T>
where
    T: Clone + PartialEq + Eq + Hash,
{
    /// Creates an empty differential collection.
    pub fn new() -> Self {
        Self {
            tuples: Vec::new(),
            net_delta: 0,
        }
    }

    /// Asserts a payload at the given epoch (delta = +1).
    ///
    /// Records the presence of `data` at `epoch`. Multiple assertions of the
    /// same payload produce true multiset multiplicity (e.g., two asserts = 2).
    pub fn assert_tuple(&mut self, data: T, epoch: Epoch) {
        self.tuples.push(DiffTuple {
            data,
            epoch,
            delta: Delta(1),
        });
        self.net_delta += 1;
    }

    /// Retracts a payload at the given epoch (delta = -1).
    ///
    /// Records the removal of `data` at `epoch`. Retraction without a prior
    /// assertion produces a negative net delta (detected during compaction).
    pub fn retract_tuple(&mut self, data: T, epoch: Epoch) {
        self.tuples.push(DiffTuple {
            data,
            epoch,
            delta: Delta(-1),
        });
        self.net_delta -= 1;
    }

    /// Computes the net delta for a specific payload.
    ///
    /// Sums all delta values across tuples matching `data`. This is
    /// mathematically exact for arbitrary assertion/retraction sequences.
    pub fn net_delta_for(&self, data: &T) -> i64 {
        self.tuples
            .iter()
            .filter(|t| t.data == *data)
            .map(|t| t.delta.0)
            .sum()
    }

    /// Returns the cached aggregate net delta across all tuples.
    ///
    /// This is the sum of all delta values, maintained incrementally as
    /// tuples are added. O(1) access.
    pub fn aggregate_net_delta(&self) -> i64 {
        self.net_delta
    }

    /// Returns a temporal snapshot at the given epoch.
    ///
    /// Considers only tuples with epoch <= the given epoch. Returns unique
    /// payloads that have a positive net delta at that point in time.
    pub fn snapshot(&self, epoch: Epoch) -> Vec<&T> {
        let mut deltas: HashMap<&T, i64> = HashMap::new();
        for tuple in &self.tuples {
            if tuple.epoch <= epoch {
                *deltas.entry(&tuple.data).or_insert(0) += tuple.delta.0;
            }
        }
        deltas
            .into_iter()
            .filter(|(_, delta)| *delta > 0)
            .map(|(data, _)| data)
            .collect()
    }

    /// Returns the current state: snapshot at the maximum possible epoch.
    ///
    /// Equivalent to `snapshot(Epoch(u64::MAX))` -- considers all tuples.
    pub fn current_state(&self) -> Vec<&T> {
        self.snapshot(Epoch(u64::MAX))
    }

    /// Compacts tuples at or before the frontier epoch.
    ///
    /// For tuples at or before `frontier`:
    /// - Groups by payload and sums deltas per payload.
    /// - **Annihilates** payloads with net-zero delta (removes all tuples).
    /// - **Collapses** payloads with positive delta to a single tuple at
    ///   the frontier epoch.
    /// - **Warns** on payloads with negative net delta (keeps collapsed).
    ///
    /// Tuples with epoch > frontier are left unchanged.
    pub fn compact(&mut self, frontier: Epoch) -> CompactionResult {
        let mut result = CompactionResult {
            annihilated: 0,
            collapsed: 0,
            warnings: Vec::new(),
        };

        // Partition tuples: at-or-before frontier vs after frontier
        let mut after_frontier: Vec<DiffTuple<T>> = Vec::new();
        let mut groups: HashMap<T, i64> = HashMap::new();

        for tuple in self.tuples.drain(..) {
            if tuple.epoch <= frontier {
                *groups.entry(tuple.data).or_insert(0) += tuple.delta.0;
            } else {
                after_frontier.push(tuple);
            }
        }

        // Process groups
        let mut compacted: Vec<DiffTuple<T>> = Vec::new();
        for (data, net) in groups {
            if net == 0 {
                result.annihilated += 1;
            } else {
                if net < 0 {
                    result.warnings.push(format!(
                        "Negative net delta ({net}) for payload during compaction"
                    ));
                }
                result.collapsed += 1;
                compacted.push(DiffTuple {
                    data,
                    epoch: frontier,
                    delta: Delta(net),
                });
            }
        }

        // Rebuild tuples: compacted first, then after-frontier
        compacted.extend(after_frontier);
        self.tuples = compacted;

        // Recalculate net_delta from scratch to maintain exactness
        self.net_delta = self.tuples.iter().map(|t| t.delta.0).sum();

        result
    }

    /// Returns the total number of tuples stored.
    pub fn tuple_count(&self) -> usize {
        self.tuples.len()
    }

    /// Returns `true` if the collection contains no tuples.
    pub fn is_empty(&self) -> bool {
        self.tuples.is_empty()
    }
}

impl<T> Default for DiffCollection<T>
where
    T: Clone + PartialEq + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}
