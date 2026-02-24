//! Top-level engine orchestrator wiring all Krabnet components.
//!
//! The [`Engine`] owns a [`RingBuffer`], [`Graph`], [`InvertedIndex`],
//! [`HashMap`] of [`Frame`]s, [`EmbryonicDiscovery`], and a [`TierConfig`].
//! It executes the full ingest-update-maintain-interpret pipeline:
//!
//! 1. Push event to ring buffer (epoch assignment)
//! 2. Apply mutation to property graph
//! 3. Query inverted index for affected frames
//! 4. For each affected frame, run Tier 1 interpretation check
//! 5. For EdgeAdded events, trigger embryonic observation
//! 6. Auto-promote candidates that meet threshold to new frames
//!
//! # Usage
//!
//! ```
//! use krabnet::engine::Engine;
//! use krabnet::types::{Event, NodeId, TypeId, EdgeId, Epoch, HopSpec, Direction, Filter};
//!
//! let mut engine = Engine::new(64);
//!
//! // Add nodes and edges via ingest
//! engine.ingest(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(10) });
//! engine.ingest(Event::NodeAdded { node_id: NodeId(2), type_id: TypeId(20) });
//! engine.ingest(Event::EdgeAdded {
//!     edge_id: EdgeId(0), source: NodeId(1), target: NodeId(2), type_id: TypeId(100),
//! });
//!
//! // Register a frame
//! let pattern = vec![HopSpec {
//!     direction: Direction::Outgoing,
//!     edge_type: Some(TypeId(100)),
//!     target_type: Some(TypeId(20)),
//!     filter: Filter::None,
//! }];
//! let frame_id = engine.register_frame(NodeId(1), pattern, Epoch(3));
//!
//! // Query the frame
//! let paths = engine.query_frame(frame_id).unwrap();
//! assert_eq!(paths.len(), 1);
//! ```

use std::collections::HashMap;

use crate::embryonic::{EmbryonicDiscovery, PatternTemplate};
use crate::frame::Frame;
use crate::graph::Graph;
use crate::interpret::tier1_check;
use crate::ring_buffer::RingBuffer;
use crate::routing::InvertedIndex;
use crate::tiering::TierConfig;
use crate::types::{Epoch, Event, FrameTier, HopSpec, NodeId};

/// Aggregate statistics for the engine.
///
/// Provides a snapshot of node/edge/frame counts, tier distribution,
/// tuple count, and embryonic discovery statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineStats {
    /// Total number of nodes in the property graph.
    pub node_count: usize,
    /// Total number of edges in the property graph.
    pub edge_count: usize,
    /// Total number of registered frames.
    pub frame_count: usize,
    /// Number of frames in [`FrameTier::Hot`].
    pub hot_frames: usize,
    /// Number of frames in [`FrameTier::Warm`].
    pub warm_frames: usize,
    /// Number of frames in [`FrameTier::Cold`].
    pub cold_frames: usize,
    /// Total number of differential tuples across all frames.
    pub total_tuples: usize,
    /// Number of active embryonic candidates.
    pub embryonic_candidates: usize,
    /// Number of registered embryonic templates.
    pub embryonic_templates: usize,
}

/// Top-level engine orchestrating all Krabnet components.
///
/// Owns the ring buffer, property graph, inverted index, frame map,
/// embryonic discovery engine, and tier configuration. Exposes the
/// full ingest-update-maintain-interpret pipeline through [`ingest`](Engine::ingest).
pub struct Engine {
    /// Ring buffer for event ingestion with epoch assignment.
    ring_buffer: RingBuffer,
    /// In-memory property graph.
    graph: Graph,
    /// Inverted index for O(affected) event-to-frame routing.
    index: InvertedIndex,
    /// Registered frames keyed by frame ID.
    frames: HashMap<u64, Frame>,
    /// Embryonic frame discovery engine.
    embryonic: EmbryonicDiscovery,
    /// Adaptive tiering configuration (used by external callers for scoring).
    #[allow(dead_code)]
    tier_config: TierConfig,
    /// Previous net_delta per frame for Tier 1 delta comparison.
    previous_deltas: HashMap<u64, i64>,
    /// Next available frame ID.
    next_frame_id: u64,
    /// Current epoch (updated after each ingest).
    current_epoch: Epoch,
}

impl Engine {
    /// Creates a new engine with the given ring buffer capacity.
    ///
    /// Initializes all components to empty/default state. The ring buffer
    /// capacity must be a power of 2.
    ///
    /// # Panics
    ///
    /// Panics if `ring_buffer_capacity` is 0 or not a power of 2.
    pub fn new(ring_buffer_capacity: usize) -> Self {
        Self {
            ring_buffer: RingBuffer::new(ring_buffer_capacity),
            graph: Graph::new(),
            index: InvertedIndex::new(),
            frames: HashMap::new(),
            embryonic: EmbryonicDiscovery::new(),
            tier_config: TierConfig::default(),
            previous_deltas: HashMap::new(),
            next_frame_id: 0,
            current_epoch: Epoch(0),
        }
    }

    /// Ingests an event through the full pipeline.
    ///
    /// Pipeline steps:
    /// 1. Push event to ring buffer, receiving an assigned epoch.
    /// 2. Apply the mutation to the property graph.
    /// 3. Query the inverted index for affected frames.
    /// 4. For each affected frame, run Tier 1 check (delta comparison).
    /// 5. For EdgeAdded events, trigger embryonic observation and
    ///    auto-promote any candidates that meet their threshold.
    /// 6. Update `current_epoch` and return the assigned epoch.
    pub fn ingest(&mut self, event: Event) -> Epoch {
        // Step 1: Push to ring buffer
        let epoch = self.ring_buffer.push(event.clone());

        // Step 2: Apply mutation to graph
        match &event {
            Event::NodeAdded { node_id, type_id } => {
                self.graph.add_node(*node_id, *type_id);
            }
            Event::NodeRemoved { node_id } => {
                self.graph.remove_node(*node_id);
            }
            Event::EdgeAdded {
                source,
                target,
                type_id,
                ..
            } => {
                self.graph.add_edge(*source, *target, *type_id);
            }
            Event::EdgeRemoved { edge_id, .. } => {
                self.graph.remove_edge(*edge_id);
            }
            Event::PropertyChanged {
                node_id,
                key,
                value,
            } => {
                self.graph.set_property(*node_id, *key, value.clone());
            }
        }

        // Step 3: Query inverted index for affected frames
        let affected = self.index.affected_frames(&event);

        // Step 4: For each affected frame, run Tier 1 check
        for frame_id in &affected {
            if let Some(frame) = self.frames.get(frame_id) {
                let previous = self.previous_deltas.get(frame_id).copied().unwrap_or(0);
                let current = frame.net_delta();
                let _changed = tier1_check(previous, current);
                self.previous_deltas.insert(*frame_id, current);
            }
        }

        // Step 5: For EdgeAdded events, trigger embryonic observation
        if let Event::EdgeAdded {
            source,
            target,
            type_id,
            ..
        } = &event
        {
            let promoted = self.embryonic.observe_edge(*source, *target, *type_id, epoch);

            // Auto-promote: create Frame, materialize, register in index
            for promo in promoted {
                let frame_id = self.next_frame_id;
                self.next_frame_id += 1;

                let mut frame = Frame::new(frame_id, promo.anchor, promo.pattern);
                frame.materialize(&self.graph, epoch);

                // Extract node IDs from materialized paths for index registration
                let node_ids = Self::extract_node_ids_from_frame(&mut frame);
                self.index.register_frame(frame_id, &node_ids, &[]);

                self.previous_deltas.insert(frame_id, frame.net_delta());
                self.frames.insert(frame_id, frame);
            }
        }

        // Step 6: Update current epoch
        self.current_epoch = epoch;

        epoch
    }

    /// Registers a new frame with the given anchor, pattern, and epoch.
    ///
    /// Creates the frame, materializes it against the current graph state,
    /// extracts node IDs from materialized paths, registers in the inverted
    /// index, and stores the frame. Returns the assigned frame ID.
    pub fn register_frame(
        &mut self,
        anchor: NodeId,
        pattern: Vec<HopSpec>,
        epoch: Epoch,
    ) -> u64 {
        let frame_id = self.next_frame_id;
        self.next_frame_id += 1;

        let mut frame = Frame::new(frame_id, anchor, pattern);
        frame.materialize(&self.graph, epoch);

        // Extract all unique NodeIds from materialized paths
        let node_ids = Self::extract_node_ids_from_frame(&mut frame);
        self.index.register_frame(frame_id, &node_ids, &[]);

        self.previous_deltas.insert(frame_id, frame.net_delta());
        self.frames.insert(frame_id, frame);

        frame_id
    }

    /// Registers an embryonic pattern template for observation.
    pub fn register_template(&mut self, template: PatternTemplate) {
        self.embryonic.register_template(template);
    }

    /// Compacts all frames below the given frontier epoch.
    ///
    /// Iterates through every frame and calls `compact(frontier)`.
    pub fn compact_all(&mut self, frontier: Epoch) {
        for frame in self.frames.values_mut() {
            frame.compact(frontier);
        }
    }

    /// Queries a frame by ID, returning owned paths.
    ///
    /// Returns `None` if the frame does not exist. The returned paths
    /// are cloned from the frame's internal references.
    pub fn query_frame(&mut self, frame_id: u64) -> Option<Vec<Vec<NodeId>>> {
        self.frames
            .get_mut(&frame_id)
            .map(|frame| frame.query().into_iter().cloned().collect())
    }

    /// Returns a temporal snapshot of a frame at the given epoch.
    ///
    /// Returns `None` if the frame does not exist. The returned paths
    /// are cloned from the frame's internal references.
    pub fn snapshot_frame(&self, frame_id: u64, epoch: Epoch) -> Option<Vec<Vec<NodeId>>> {
        self.frames
            .get(&frame_id)
            .map(|frame| frame.snapshot(epoch).into_iter().cloned().collect())
    }

    /// Collects aggregate statistics from all engine components.
    pub fn stats(&self) -> EngineStats {
        let mut hot_frames = 0usize;
        let mut warm_frames = 0usize;
        let mut cold_frames = 0usize;
        let mut total_tuples = 0usize;

        for frame in self.frames.values() {
            match frame.tier() {
                FrameTier::Hot => hot_frames += 1,
                FrameTier::Warm => warm_frames += 1,
                FrameTier::Cold => cold_frames += 1,
            }
            total_tuples += frame.tuple_count();
        }

        EngineStats {
            node_count: self.graph.node_count(),
            edge_count: self.graph.edge_count(),
            frame_count: self.frames.len(),
            hot_frames,
            warm_frames,
            cold_frames,
            total_tuples,
            embryonic_candidates: self.embryonic.candidate_count(),
            embryonic_templates: self.embryonic.template_count(),
        }
    }

    /// Extracts all unique NodeIds from a frame's current materialized paths.
    ///
    /// Calls `frame.query()` to get current paths, then collects all unique
    /// NodeIds across all paths.
    fn extract_node_ids_from_frame(frame: &mut Frame) -> Vec<NodeId> {
        let paths = frame.query();
        let mut node_ids: Vec<NodeId> = Vec::new();
        for path in paths {
            for node_id in path {
                if !node_ids.contains(node_id) {
                    node_ids.push(*node_id);
                }
            }
        }
        node_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embryonic::PatternTemplate;
    use crate::types::{Delta, Direction, EdgeId, Filter, HopSpec, NodeId, TypeId};
    use std::collections::HashSet;

    /// Helper: creates a one-hop outgoing pattern with given edge and target types.
    fn one_hop_pattern(edge_type: TypeId, target_type: TypeId) -> Vec<HopSpec> {
        vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(edge_type),
            target_type: Some(target_type),
            filter: Filter::None,
        }]
    }

    /// Helper: builds a simple engine with nodes 1,2 and edge 1->2.
    fn engine_with_edge() -> (Engine, Epoch) {
        let mut engine = Engine::new(64);
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });
        let epoch = engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });
        (engine, epoch)
    }

    // ── full_ingest_pipeline ───────────────────────────────────────────

    #[test]
    fn full_ingest_pipeline() {
        let (mut engine, epoch) = engine_with_edge();

        // Register a frame: anchor at node 1, one hop outgoing via edge type 100
        let fid = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );

        let paths = engine.query_frame(fid).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec![NodeId(1), NodeId(2)]);
    }

    // ── retraction_pipeline ────────────────────────────────────────────

    #[test]
    fn retraction_pipeline() {
        let (mut engine, epoch) = engine_with_edge();

        // Register frame that sees path [1, 2]
        let fid = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );

        // Verify the frame has the path
        let paths_before = engine.query_frame(fid).unwrap();
        assert_eq!(paths_before.len(), 1);

        // Now apply a retraction delta directly to simulate edge removal effect
        // In a full system, edge removal would trigger frame maintenance.
        // Here we manually apply a delta to verify the differential math.
        let frame = engine.frames.get_mut(&fid).unwrap();
        frame.apply_delta(
            vec![NodeId(1), NodeId(2)],
            Epoch(epoch.0 + 1),
            Delta(-1),
        );

        let paths_after = engine.query_frame(fid).unwrap();
        assert!(
            paths_after.is_empty(),
            "After retraction, frame should have no active paths"
        );
    }

    // ── shared_node_multi_frame ────────────────────────────────────────

    #[test]
    fn shared_node_multi_frame() {
        let mut engine = Engine::new(64);

        // Build: node 1 -> node 2 (type 100), node 1 -> node 3 (type 200)
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(3),
            type_id: TypeId(30),
        });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });
        let epoch = engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(1),
            source: NodeId(1),
            target: NodeId(3),
            type_id: TypeId(200),
        });

        // Register two frames both anchored at node 1 (shared node)
        let fid1 = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );
        let fid2 = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(200), TypeId(30)),
            epoch,
        );

        // Both frames should have different paths through the shared anchor
        let paths1 = engine.query_frame(fid1).unwrap();
        let paths2 = engine.query_frame(fid2).unwrap();
        assert_eq!(paths1.len(), 1);
        assert_eq!(paths1[0], vec![NodeId(1), NodeId(2)]);
        assert_eq!(paths2.len(), 1);
        assert_eq!(paths2[0], vec![NodeId(1), NodeId(3)]);

        // Ingest an event on the shared node -- both frames should be affected
        // (since both are registered under NodeId(1) in the inverted index)
        let affected = engine.index.affected_frames(&Event::PropertyChanged {
            node_id: NodeId(1),
            key: 0,
            value: crate::types::PropertyValue::Integer(42),
        });
        assert!(
            affected.contains(&fid1) && affected.contains(&fid2),
            "Both frames should be affected by event on shared node"
        );
    }

    // ── embryonic_auto_promotion ───────────────────────────────────────

    #[test]
    fn embryonic_auto_promotion() {
        let mut engine = Engine::new(64);

        // Register embryonic template: two-hop outgoing pattern, 0.5 threshold
        // (one matching edge is enough to promote)
        engine.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                HopSpec {
                    direction: Direction::Outgoing,
                    edge_type: Some(TypeId(100)),
                    target_type: None,
                    filter: Filter::None,
                },
                HopSpec {
                    direction: Direction::Outgoing,
                    edge_type: Some(TypeId(200)),
                    target_type: None,
                    filter: Filter::None,
                },
            ],
            threshold: 0.5, // 1/2 hops = 0.5, triggers promotion
            max_candidates: 100,
            stale_window: 10,
        });

        // Add nodes first
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });

        let stats_before = engine.stats();
        assert_eq!(stats_before.frame_count, 0);

        // Ingest an edge matching the first hop -- should trigger promotion
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });

        let stats_after = engine.stats();
        assert!(
            stats_after.frame_count >= 1,
            "Embryonic auto-promotion should create at least one frame, got {}",
            stats_after.frame_count
        );
    }

    // ── compaction_correctness ─────────────────────────────────────────

    #[test]
    fn compaction_correctness() {
        let (mut engine, epoch) = engine_with_edge();

        let fid = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );

        // Verify frame has a path
        let paths = engine.query_frame(fid).unwrap();
        assert_eq!(paths.len(), 1);

        // Apply retraction at epoch+1
        let retract_epoch = Epoch(epoch.0 + 1);
        let frame = engine.frames.get_mut(&fid).unwrap();
        frame.apply_delta(
            vec![NodeId(1), NodeId(2)],
            retract_epoch,
            Delta(-1),
        );

        // Compact at retraction epoch -- assert + retract should annihilate
        engine.compact_all(retract_epoch);

        // After compaction, the annihilated tuple should be gone
        let paths_after = engine.query_frame(fid).unwrap();
        assert!(
            paths_after.is_empty(),
            "After compaction of assert+retract, frame should be empty"
        );

        // Tuple count should be 0 (annihilated)
        let frame = engine.frames.get(&fid).unwrap();
        assert_eq!(
            frame.tuple_count(),
            0,
            "Annihilated tuples should be removed after compaction"
        );
    }

    // ── temporal_snapshot_consistency ───────────────────────────────────

    #[test]
    fn temporal_snapshot_consistency() {
        let mut engine = Engine::new(64);

        // Build graph at epochs 0-4
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(3),
            type_id: TypeId(20),
        });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });

        // Register frame at epoch 5 (materialize epoch)
        let materialize_epoch = Epoch(5);
        let fid = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            materialize_epoch,
        );

        // Snapshot at epoch 5 should show the original path
        let snap5 = engine.snapshot_frame(fid, materialize_epoch).unwrap();
        assert_eq!(snap5.len(), 1, "Snapshot at materialize epoch should have 1 path");
        assert_eq!(snap5[0], vec![NodeId(1), NodeId(2)]);

        // Add more data at epoch 10
        let frame = engine.frames.get_mut(&fid).unwrap();
        frame.apply_delta(
            vec![NodeId(1), NodeId(3)],
            Epoch(10),
            Delta(1),
        );

        // Snapshot at epoch 5 should still return only the original path
        let snap5_after = engine.snapshot_frame(fid, materialize_epoch).unwrap();
        assert_eq!(
            snap5_after.len(),
            1,
            "Snapshot at epoch 5 should still show only original path"
        );
        assert_eq!(snap5_after[0], vec![NodeId(1), NodeId(2)]);

        // Snapshot at epoch 10 should return both paths
        let snap10 = engine.snapshot_frame(fid, Epoch(10)).unwrap();
        assert_eq!(
            snap10.len(),
            2,
            "Snapshot at epoch 10 should show both paths"
        );
        let snap10_set: HashSet<Vec<NodeId>> = snap10.into_iter().collect();
        assert!(snap10_set.contains(&vec![NodeId(1), NodeId(2)]));
        assert!(snap10_set.contains(&vec![NodeId(1), NodeId(3)]));
    }

    // ── stats_reporting ────────────────────────────────────────────────

    #[test]
    fn stats_reporting() {
        let mut engine = Engine::new(64);

        // Empty engine stats
        let s0 = engine.stats();
        assert_eq!(s0.node_count, 0);
        assert_eq!(s0.edge_count, 0);
        assert_eq!(s0.frame_count, 0);
        assert_eq!(s0.total_tuples, 0);
        assert_eq!(s0.embryonic_candidates, 0);
        assert_eq!(s0.embryonic_templates, 0);

        // Add nodes and edge
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });
        let epoch = engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });

        let s1 = engine.stats();
        assert_eq!(s1.node_count, 2);
        assert_eq!(s1.edge_count, 1);

        // Register a frame
        engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );

        let s2 = engine.stats();
        assert_eq!(s2.frame_count, 1);
        // Frame materialized 1 path = 1 tuple
        assert_eq!(s2.total_tuples, 1);
        // New frames start Cold
        assert_eq!(s2.cold_frames, 1);
        assert_eq!(s2.hot_frames, 0);
        assert_eq!(s2.warm_frames, 0);

        // Register a template
        engine.register_template(PatternTemplate {
            id: 1,
            pattern: vec![HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(999)),
                target_type: None,
                filter: Filter::None,
            }],
            threshold: 1.0,
            max_candidates: 100,
            stale_window: 10,
        });

        let s3 = engine.stats();
        assert_eq!(s3.embryonic_templates, 1);
    }

    // ── ingest_node_added_and_removed ──────────────────────────────────

    #[test]
    fn ingest_node_added_and_removed() {
        let mut engine = Engine::new(64);

        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });

        let s1 = engine.stats();
        assert_eq!(s1.node_count, 2);

        // Add edge
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });
        assert_eq!(engine.stats().edge_count, 1);

        // Remove node 1 -- should cascade edge removal
        engine.ingest(Event::NodeRemoved {
            node_id: NodeId(1),
        });

        let s2 = engine.stats();
        assert_eq!(s2.node_count, 1, "Node 1 should be removed");
        assert_eq!(s2.edge_count, 0, "Edge should cascade-remove with node 1");
    }

    // ── epoch_assignment ───────────────────────────────────────────────

    #[test]
    fn epoch_assignment_is_sequential() {
        let mut engine = Engine::new(64);

        let e0 = engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(0),
        });
        let e1 = engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(0),
        });
        let e2 = engine.ingest(Event::NodeAdded {
            node_id: NodeId(3),
            type_id: TypeId(0),
        });

        assert_eq!(e0, Epoch(0));
        assert_eq!(e1, Epoch(1));
        assert_eq!(e2, Epoch(2));
    }

    // ── query_nonexistent_frame ────────────────────────────────────────

    #[test]
    fn query_nonexistent_frame_returns_none() {
        let mut engine = Engine::new(64);
        assert!(engine.query_frame(999).is_none());
        assert!(engine.snapshot_frame(999, Epoch(0)).is_none());
    }

    // ── compact_all_multiple_frames ────────────────────────────────────

    #[test]
    fn compact_all_compacts_every_frame() {
        let mut engine = Engine::new(64);

        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(3),
            type_id: TypeId(30),
        });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });
        let epoch = engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(1),
            source: NodeId(1),
            target: NodeId(3),
            type_id: TypeId(200),
        });

        let fid1 = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );
        let fid2 = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(200), TypeId(30)),
            epoch,
        );

        // Apply retractions
        let retract_epoch = Epoch(epoch.0 + 1);
        engine
            .frames
            .get_mut(&fid1)
            .unwrap()
            .apply_delta(vec![NodeId(1), NodeId(2)], retract_epoch, Delta(-1));
        engine
            .frames
            .get_mut(&fid2)
            .unwrap()
            .apply_delta(vec![NodeId(1), NodeId(3)], retract_epoch, Delta(-1));

        // Compact all
        engine.compact_all(retract_epoch);

        // Both frames should be empty (annihilated)
        for fid in [fid1, fid2] {
            let frame = engine.frames.get(&fid).unwrap();
            assert_eq!(
                frame.tuple_count(),
                0,
                "Frame {} should be annihilated after compact_all",
                fid
            );
        }
    }

    // ── property_changed_event ─────────────────────────────────────────

    #[test]
    fn property_changed_applies_to_graph() {
        let mut engine = Engine::new(64);

        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::PropertyChanged {
            node_id: NodeId(1),
            key: 42,
            value: crate::types::PropertyValue::Integer(100),
        });

        // Verify the property was applied to the graph
        let val = engine.graph.get_property(NodeId(1), 42);
        assert_eq!(val, Some(&crate::types::PropertyValue::Integer(100)));
    }
}
