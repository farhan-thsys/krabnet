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
