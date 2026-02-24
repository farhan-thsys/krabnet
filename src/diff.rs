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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Assert then retract the same payload at the same epoch produces
    /// net-zero, and compaction removes the annihilated tuple entirely.
    #[test]
    fn assert_retract_annihilation() {
        let mut coll = DiffCollection::new();
        coll.assert_tuple("Alice", Epoch(1));
        coll.retract_tuple("Alice", Epoch(1));

        assert_eq!(coll.net_delta_for(&"Alice"), 0);
        assert_eq!(coll.aggregate_net_delta(), 0);
        assert_eq!(coll.tuple_count(), 2);

        let result = coll.compact(Epoch(1));
        assert_eq!(result.annihilated, 1);
        assert_eq!(result.collapsed, 0);
        assert!(result.warnings.is_empty());
        assert_eq!(coll.tuple_count(), 0);
        assert!(coll.is_empty());
    }

    /// Double-assert of the same payload produces multiplicity 2, not 1
    /// (true multiset semantics).
    #[test]
    fn double_assert_multiplicity() {
        let mut coll = DiffCollection::new();
        coll.assert_tuple(42u64, Epoch(1));
        coll.assert_tuple(42u64, Epoch(2));

        assert_eq!(coll.net_delta_for(&42u64), 2);
        assert_eq!(coll.aggregate_net_delta(), 2);
        assert_eq!(coll.tuple_count(), 2);
    }

    /// Arbitrary sequence of asserts/retracts produces mathematically exact
    /// per-payload and aggregate deltas.
    #[test]
    fn net_delta_exact_for_sequence() {
        let mut coll = DiffCollection::new();

        // "x": +1 +1 +1 -1 = net 2
        coll.assert_tuple("x", Epoch(1));
        coll.assert_tuple("x", Epoch(2));
        coll.assert_tuple("x", Epoch(3));
        coll.retract_tuple("x", Epoch(4));

        // "y": +1 -1 -1 = net -1
        coll.assert_tuple("y", Epoch(1));
        coll.retract_tuple("y", Epoch(2));
        coll.retract_tuple("y", Epoch(3));

        // "z": +1 = net 1
        coll.assert_tuple("z", Epoch(5));

        assert_eq!(coll.net_delta_for(&"x"), 2);
        assert_eq!(coll.net_delta_for(&"y"), -1);
        assert_eq!(coll.net_delta_for(&"z"), 1);

        // Aggregate: 2 + (-1) + 1 = 2
        assert_eq!(coll.aggregate_net_delta(), 2);
    }

    /// Snapshot at a middle epoch returns only payloads with positive net
    /// delta from tuples at-or-before that epoch.
    #[test]
    fn snapshot_at_epoch() {
        let mut coll = DiffCollection::new();

        // "a" asserted at epoch 1
        coll.assert_tuple("a", Epoch(1));
        // "b" asserted at epoch 2
        coll.assert_tuple("b", Epoch(2));
        // "c" asserted at epoch 3
        coll.assert_tuple("c", Epoch(3));
        // "a" retracted at epoch 4
        coll.retract_tuple("a", Epoch(4));

        // Snapshot at epoch 2: "a" (+1), "b" (+1) -- "c" not yet, "a" not yet retracted
        let snap2: HashSet<&&str> = coll.snapshot(Epoch(2)).into_iter().collect();
        assert_eq!(snap2.len(), 2);
        assert!(snap2.contains(&&"a"));
        assert!(snap2.contains(&&"b"));

        // Snapshot at epoch 3: "a" (+1), "b" (+1), "c" (+1)
        let snap3: HashSet<&&str> = coll.snapshot(Epoch(3)).into_iter().collect();
        assert_eq!(snap3.len(), 3);

        // Snapshot at epoch 4: "a" retracted (net 0), "b" (+1), "c" (+1)
        let snap4: HashSet<&&str> = coll.snapshot(Epoch(4)).into_iter().collect();
        assert_eq!(snap4.len(), 2);
        assert!(snap4.contains(&&"b"));
        assert!(snap4.contains(&&"c"));
        assert!(!snap4.contains(&&"a"));
    }

    /// current_state() is equivalent to snapshot(Epoch(u64::MAX)).
    #[test]
    fn current_state_is_max_snapshot() {
        let mut coll = DiffCollection::new();
        coll.assert_tuple(10u64, Epoch(1));
        coll.assert_tuple(20u64, Epoch(2));
        coll.retract_tuple(10u64, Epoch(3));

        let current: HashSet<&u64> = coll.current_state().into_iter().collect();
        let max_snap: HashSet<&u64> = coll.snapshot(Epoch(u64::MAX)).into_iter().collect();
        assert_eq!(current, max_snap);

        // Only 20 should be present (10 was retracted)
        assert_eq!(current.len(), 1);
        assert!(current.contains(&20u64));
    }

    /// Compaction removes net-zero payloads entirely (annihilation).
    #[test]
    fn compact_annihilates_net_zero() {
        let mut coll = DiffCollection::new();
        coll.assert_tuple("gone", Epoch(1));
        coll.retract_tuple("gone", Epoch(2));
        coll.assert_tuple("stays", Epoch(1));

        let result = coll.compact(Epoch(2));
        assert_eq!(result.annihilated, 1);
        assert_eq!(result.collapsed, 1);
        assert!(result.warnings.is_empty());

        // Only "stays" should remain as a single tuple
        assert_eq!(coll.tuple_count(), 1);
        assert_eq!(coll.net_delta_for(&"stays"), 1);
        assert_eq!(coll.net_delta_for(&"gone"), 0);
    }

    /// Compaction collapses multiple tuples for a survivor into one with
    /// the correct summed delta.
    #[test]
    fn compact_collapses_survivors() {
        let mut coll = DiffCollection::new();
        // Three asserts of same payload across different epochs
        coll.assert_tuple("multi", Epoch(1));
        coll.assert_tuple("multi", Epoch(2));
        coll.assert_tuple("multi", Epoch(3));

        assert_eq!(coll.tuple_count(), 3);

        let result = coll.compact(Epoch(3));
        assert_eq!(result.collapsed, 1);
        assert_eq!(result.annihilated, 0);
        assert!(result.warnings.is_empty());

        // Should now be a single tuple with delta = 3
        assert_eq!(coll.tuple_count(), 1);
        assert_eq!(coll.net_delta_for(&"multi"), 3);
        assert_eq!(coll.aggregate_net_delta(), 3);
    }

    /// Retract without assert produces a negative net delta warning
    /// during compaction.
    #[test]
    fn compact_warns_on_negative() {
        let mut coll = DiffCollection::new();
        coll.retract_tuple("orphan", Epoch(1));

        let result = coll.compact(Epoch(1));
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("Negative net delta"));
        assert!(result.warnings[0].contains("-1"));
        assert_eq!(result.collapsed, 1);
        assert_eq!(result.annihilated, 0);

        // The collapsed tuple should still exist with negative delta
        assert_eq!(coll.tuple_count(), 1);
        assert_eq!(coll.net_delta_for(&"orphan"), -1);
    }

    /// Tuples with epoch > frontier are left untouched by compaction.
    #[test]
    fn compact_preserves_tuples_after_frontier() {
        let mut coll = DiffCollection::new();
        coll.assert_tuple("before", Epoch(1));
        coll.assert_tuple("before", Epoch(2));
        coll.assert_tuple("after", Epoch(5));
        coll.assert_tuple("after", Epoch(6));

        let result = coll.compact(Epoch(3));

        // "before" collapsed to 1 tuple, "after" untouched (2 tuples)
        assert_eq!(result.collapsed, 1);
        assert_eq!(coll.tuple_count(), 3); // 1 collapsed + 2 after

        // "after" still has original individual tuples
        assert_eq!(coll.net_delta_for(&"after"), 2);
        assert_eq!(coll.net_delta_for(&"before"), 2);
        assert_eq!(coll.aggregate_net_delta(), 4);
    }

    /// tuple_count and is_empty reflect actual state.
    #[test]
    fn tuple_count_and_empty() {
        let mut coll: DiffCollection<u64> = DiffCollection::new();
        assert!(coll.is_empty());
        assert_eq!(coll.tuple_count(), 0);

        coll.assert_tuple(1, Epoch(1));
        assert!(!coll.is_empty());
        assert_eq!(coll.tuple_count(), 1);

        coll.assert_tuple(2, Epoch(2));
        assert_eq!(coll.tuple_count(), 2);

        coll.retract_tuple(1, Epoch(3));
        assert_eq!(coll.tuple_count(), 3);

        // After compaction: 1 annihilated, 1 collapsed
        coll.compact(Epoch(3));
        assert_eq!(coll.tuple_count(), 1);
        assert!(!coll.is_empty());
    }
}
