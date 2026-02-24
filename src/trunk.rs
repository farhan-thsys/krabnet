//! Trunk/leaf detection for identifying structural spines shared across frames.
//!
//! A "trunk" is a sub-path (sequence of [`HopSpec`]s) that appears as a
//! contiguous sub-sequence in multiple frame patterns. Frames sharing trunk
//! paths benefit from Hot pinning since they are structural spines accessed
//! by many queries.
//!
//! # Usage
//!
//! ```
//! use krabnet::trunk::{detect_trunks, classify_frames, pinned_frame_ids};
//! use krabnet::types::{HopSpec, Direction, Filter, TypeId};
//!
//! let frames: Vec<(u64, Vec<HopSpec>)> = vec![
//!     (0, vec![HopSpec { direction: Direction::Outgoing, edge_type: Some(TypeId(1)),
//!              target_type: Some(TypeId(2)), filter: Filter::None }]),
//!     (1, vec![HopSpec { direction: Direction::Outgoing, edge_type: Some(TypeId(1)),
//!              target_type: Some(TypeId(2)), filter: Filter::None }]),
//! ];
//!
//! let trunks = detect_trunks(&frames, 2);
//! assert_eq!(trunks.len(), 1);
//!
//! let pinned = pinned_frame_ids(&trunks);
//! assert!(pinned.contains(&0));
//! assert!(pinned.contains(&1));
//! ```

use std::collections::{HashMap, HashSet};

use crate::types::HopSpec;

/// Result of trunk detection: which sub-paths are trunks and which frames they span.
#[derive(Debug, Clone)]
pub struct TrunkInfo {
    /// Sub-path that qualifies as a trunk.
    pub path: Vec<HopSpec>,
    /// Frame IDs sharing this trunk.
    pub frame_ids: Vec<u64>,
}

/// Generates a canonical string key for a single HopSpec.
///
/// Since HopSpec contains Filter which contains PropertyValue with f64,
/// we cannot derive Hash on HopSpec. Instead we serialize to a canonical
/// string for use as HashMap keys.
fn hop_key(hop: &HopSpec) -> String {
    format!(
        "{:?}|{:?}|{:?}|{:?}",
        hop.direction, hop.edge_type, hop.target_type, hop.filter
    )
}

/// Generates a canonical string key for a sub-path (sequence of HopSpecs).
fn path_key(path: &[HopSpec]) -> String {
    path.iter().map(hop_key).collect::<Vec<_>>().join("/")
}

/// Detects trunk sub-paths shared across multiple frame patterns.
///
/// For each frame pattern, generates all contiguous sub-paths of length >= 1
/// using a sliding window. Counts how many frames share each sub-path and
/// returns those with `frame_ids.len() >= min_shared_count` as [`TrunkInfo`].
///
/// Results are sorted by frame count descending (most shared first).
///
/// # Arguments
///
/// * `frames` - Pairs of `(frame_id, pattern)` to analyze.
/// * `min_shared_count` - Minimum number of frames that must share a sub-path
///   for it to qualify as a trunk.
///
/// # Examples
///
/// ```
/// use krabnet::trunk::detect_trunks;
/// use krabnet::types::{HopSpec, Direction, Filter, TypeId};
///
/// let hop = HopSpec {
///     direction: Direction::Outgoing,
///     edge_type: Some(TypeId(1)),
///     target_type: Some(TypeId(2)),
///     filter: Filter::None,
/// };
/// let frames = vec![
///     (0, vec![hop.clone()]),
///     (1, vec![hop.clone()]),
///     (2, vec![hop.clone()]),
/// ];
/// let trunks = detect_trunks(&frames, 2);
/// assert!(!trunks.is_empty());
/// ```
pub fn detect_trunks(
    frames: &[(u64, Vec<HopSpec>)],
    min_shared_count: usize,
) -> Vec<TrunkInfo> {
    // Map from sub-path key -> (sub-path HopSpecs, set of frame IDs)
    let mut subpath_map: HashMap<String, (Vec<HopSpec>, Vec<u64>)> = HashMap::new();

    for (frame_id, pattern) in frames {
        // Generate all contiguous sub-paths of length >= 1
        let len = pattern.len();
        for start in 0..len {
            for end in (start + 1)..=len {
                let sub = &pattern[start..end];
                let key = path_key(sub);

                let entry = subpath_map
                    .entry(key)
                    .or_insert_with(|| (sub.to_vec(), Vec::new()));
                if !entry.1.contains(frame_id) {
                    entry.1.push(*frame_id);
                }
            }
        }
    }

    // Filter to those meeting min_shared_count and build TrunkInfo
    let mut trunks: Vec<TrunkInfo> = subpath_map
        .into_values()
        .filter(|(_, fids)| fids.len() >= min_shared_count)
        .map(|(path, frame_ids)| TrunkInfo { path, frame_ids })
        .collect();

    // Sort by frame count descending (most shared first)
    trunks.sort_by(|a, b| b.frame_ids.len().cmp(&a.frame_ids.len()));

    trunks
}

/// Classifies frames as trunk frames or leaf frames.
///
/// A frame is a "trunk frame" if it appears in any [`TrunkInfo::frame_ids`].
/// A frame is a "leaf frame" if it does NOT appear in any trunk.
///
/// Returns `(trunk_frame_ids, leaf_frame_ids)`.
///
/// # Examples
///
/// ```
/// use krabnet::trunk::{detect_trunks, classify_frames};
/// use krabnet::types::{HopSpec, Direction, Filter, TypeId};
///
/// let shared_hop = HopSpec {
///     direction: Direction::Outgoing,
///     edge_type: Some(TypeId(1)),
///     target_type: Some(TypeId(2)),
///     filter: Filter::None,
/// };
/// let unique_hop = HopSpec {
///     direction: Direction::Incoming,
///     edge_type: Some(TypeId(99)),
///     target_type: None,
///     filter: Filter::None,
/// };
///
/// let frames = vec![
///     (0, vec![shared_hop.clone()]),
///     (1, vec![shared_hop.clone()]),
///     (2, vec![unique_hop]),
/// ];
/// let trunks = detect_trunks(&frames, 2);
/// let (trunk_ids, leaf_ids) = classify_frames(&frames, &trunks);
/// assert!(trunk_ids.contains(&0));
/// assert!(trunk_ids.contains(&1));
/// assert!(leaf_ids.contains(&2));
/// ```
pub fn classify_frames(
    frames: &[(u64, Vec<HopSpec>)],
    trunk_infos: &[TrunkInfo],
) -> (HashSet<u64>, HashSet<u64>) {
    let trunk_frame_ids = pinned_frame_ids(trunk_infos);

    let all_frame_ids: HashSet<u64> = frames.iter().map(|(fid, _)| *fid).collect();
    let leaf_frame_ids: HashSet<u64> = all_frame_ids
        .difference(&trunk_frame_ids)
        .copied()
        .collect();

    (trunk_frame_ids, leaf_frame_ids)
}

/// Returns the set of all frame IDs that participate in any trunk.
///
/// These frames should be pinned to [`crate::types::FrameTier::Hot`] to prevent eviction
/// of structural spines.
///
/// # Examples
///
/// ```
/// use krabnet::trunk::{detect_trunks, pinned_frame_ids};
/// use krabnet::types::{HopSpec, Direction, Filter, TypeId};
///
/// let hop = HopSpec {
///     direction: Direction::Outgoing,
///     edge_type: Some(TypeId(1)),
///     target_type: Some(TypeId(2)),
///     filter: Filter::None,
/// };
/// let frames = vec![(0, vec![hop.clone()]), (1, vec![hop.clone()])];
/// let trunks = detect_trunks(&frames, 2);
/// let pinned = pinned_frame_ids(&trunks);
/// assert_eq!(pinned.len(), 2);
/// ```
pub fn pinned_frame_ids(trunk_infos: &[TrunkInfo]) -> HashSet<u64> {
    let mut pinned = HashSet::new();
    for info in trunk_infos {
        for fid in &info.frame_ids {
            pinned.insert(*fid);
        }
    }
    pinned
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Direction, Filter, TypeId};

    /// Helper: creates a HopSpec with the given edge and target types.
    fn hop(edge_type: u32, target_type: u32) -> HopSpec {
        HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(edge_type)),
            target_type: Some(TypeId(target_type)),
            filter: Filter::None,
        }
    }

    #[test]
    fn test_detect_trunks_basic() {
        // 5 frames, 3 share first 2 hops
        let shared = vec![hop(1, 2), hop(3, 4)];
        let frames: Vec<(u64, Vec<HopSpec>)> = vec![
            (0, vec![hop(1, 2), hop(3, 4), hop(5, 6)]),
            (1, vec![hop(1, 2), hop(3, 4), hop(7, 8)]),
            (2, vec![hop(1, 2), hop(3, 4)]),
            (3, vec![hop(10, 20), hop(30, 40)]),
            (4, vec![hop(50, 60)]),
        ];

        let trunks = detect_trunks(&frames, 3);

        // The shared 2-hop sub-path should be detected
        let shared_key = path_key(&shared);
        let found = trunks.iter().any(|t| path_key(&t.path) == shared_key);
        assert!(found, "Should detect the shared 2-hop sub-path as trunk");

        // The trunk should span frames 0, 1, 2
        let trunk = trunks
            .iter()
            .find(|t| path_key(&t.path) == shared_key)
            .unwrap();
        assert_eq!(trunk.frame_ids.len(), 3);
        assert!(trunk.frame_ids.contains(&0));
        assert!(trunk.frame_ids.contains(&1));
        assert!(trunk.frame_ids.contains(&2));
    }

    /// TEST-28: 50 frames, 30 share first 2 hops, detected as trunk,
    /// pinned to Hot via pinned_frame_ids.
    #[test]
    fn test_detect_trunks_50_frames() {
        let mut frames: Vec<(u64, Vec<HopSpec>)> = Vec::new();

        // 30 frames share the same first 2 hops
        for i in 0..30u64 {
            frames.push((
                i,
                vec![hop(1, 2), hop(3, 4), hop(i as u32 + 100, i as u32 + 200)],
            ));
        }

        // 20 frames with unique patterns
        for i in 30..50u64 {
            frames.push((
                i,
                vec![hop(i as u32 * 10, i as u32 * 20)],
            ));
        }

        let trunks = detect_trunks(&frames, 2);
        assert!(!trunks.is_empty(), "Should detect at least one trunk");

        // The 2-hop shared prefix should be a trunk
        let shared_key = path_key(&[hop(1, 2), hop(3, 4)]);
        let trunk = trunks
            .iter()
            .find(|t| path_key(&t.path) == shared_key)
            .expect("Should find the 2-hop trunk");
        assert_eq!(trunk.frame_ids.len(), 30, "30 frames should share this trunk");

        // Pinned frame IDs should include all 30 trunk frames
        let pinned = pinned_frame_ids(&trunks);
        for i in 0..30u64 {
            assert!(
                pinned.contains(&i),
                "Frame {i} should be pinned (trunk frame)"
            );
        }
    }

    #[test]
    fn test_classify_frames() {
        let frames: Vec<(u64, Vec<HopSpec>)> = vec![
            (0, vec![hop(1, 2), hop(3, 4)]),
            (1, vec![hop(1, 2), hop(3, 4)]),
            (2, vec![hop(10, 20)]),
        ];

        let trunks = detect_trunks(&frames, 2);
        let (trunk_ids, leaf_ids) = classify_frames(&frames, &trunks);

        assert!(trunk_ids.contains(&0));
        assert!(trunk_ids.contains(&1));
        assert!(leaf_ids.contains(&2));
        assert!(!trunk_ids.contains(&2));
        assert!(!leaf_ids.contains(&0));
    }

    #[test]
    fn test_no_trunks() {
        // All unique patterns, no trunks detected
        let frames: Vec<(u64, Vec<HopSpec>)> = vec![
            (0, vec![hop(1, 2)]),
            (1, vec![hop(3, 4)]),
            (2, vec![hop(5, 6)]),
            (3, vec![hop(7, 8)]),
        ];

        let trunks = detect_trunks(&frames, 2);
        assert!(
            trunks.is_empty(),
            "All unique patterns should produce no trunks"
        );
    }

    #[test]
    fn test_min_shared_count() {
        // 3 frames share a hop, but min_shared_count = 4
        let frames: Vec<(u64, Vec<HopSpec>)> = vec![
            (0, vec![hop(1, 2)]),
            (1, vec![hop(1, 2)]),
            (2, vec![hop(1, 2)]),
        ];

        let trunks_high = detect_trunks(&frames, 4);
        assert!(
            trunks_high.is_empty(),
            "min_shared_count=4 should exclude sub-paths shared by only 3 frames"
        );

        let trunks_low = detect_trunks(&frames, 3);
        assert!(
            !trunks_low.is_empty(),
            "min_shared_count=3 should include sub-paths shared by 3 frames"
        );
    }
}
