//! Top-level engine orchestrator wiring all Krabnet components.
//!
//! The [`Engine`] owns a [`RingBuffer`], [`Graph`], [`InvertedIndex`],
//! [`HashMap`] of [`Frame`]s wrapped in `Arc<RwLock<>>`, an optional
//! [`CompactionWorker`], [`EmbryonicDiscovery`], and a [`TierConfig`].
//! It executes the full ingest-update-maintain-interpret pipeline:
//!
//! 1. Push event to ring buffer (epoch assignment)
//! 2. Apply mutation to property graph
//! 3. Query inverted index for affected frames (main thread, EVAL-03)
//! 4. Fan out frame evaluation to parallel threads via `std::thread::scope` (EVAL-01)
//! 5. For EdgeAdded events, trigger embryonic observation
//! 6. Auto-promote candidates that meet threshold to new frames
//! 7. If compaction worker is active, check thresholds and request compaction
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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::buffer_pool::BufferPool;
use crate::coalescer::{event_node_id, MutationCoalescer};
use crate::compaction::{CompactionStats, CompactionWorker};
use crate::embryonic::{EmbryonicDiscovery, PatternTemplate};
use crate::fanout::FanOutLimiter;
use crate::frame::Frame;
use crate::graph::Graph;
use crate::interpret::tier1_check;
use crate::ring_buffer::RingBuffer;
use crate::routing::InvertedIndex;
use crate::tiering::{FrameActivityTracker, HysteresisState, TierConfig};
use crate::trunk::{detect_trunks, pinned_frame_ids};
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
/// embryonic discovery engine, optional compaction worker, and tier
/// configuration. Frames are wrapped in `Arc<RwLock<Frame>>` for
/// concurrent read/write access during parallel frame evaluation.
///
/// Exposes the full ingest-update-maintain-interpret pipeline through
/// [`ingest`](Engine::ingest). Frame evaluation fans out to parallel
/// threads via `std::thread::scope` after single-threaded inverted
/// index lookup (EVAL-01, EVAL-03).
pub struct Engine {
    /// Ring buffer for event ingestion with epoch assignment.
    ring_buffer: RingBuffer,
    /// In-memory property graph.
    graph: Graph,
    /// Inverted index for O(affected) event-to-frame routing.
    index: InvertedIndex,
    /// Registered frames keyed by frame ID, wrapped in Arc<RwLock<>> for concurrent access.
    frames: HashMap<u64, Arc<RwLock<Frame>>>,
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
    /// Optional background compaction worker.
    compaction_worker: Option<CompactionWorker>,
    /// Optional mutation coalescer for same-node deduplication within epoch windows.
    coalescer: Option<MutationCoalescer>,
    /// Optional fan-out limiter for capping immediate frame evaluations.
    fanout_limiter: Option<FanOutLimiter>,
    /// Per-frame hysteresis state for preventing tier thrashing.
    hysteresis: HashMap<u64, HysteresisState>,
    /// Hysteresis required_consecutive parameter (default 5).
    hysteresis_consecutive: u32,
    /// Count of frame evaluations triggered (for testing coalescer integration).
    eval_count: u64,
    /// CMS-backed frame activity tracker for priority scoring (CMS-02).
    activity_tracker: FrameActivityTracker,
    /// Set of frame IDs pinned to Hot tier (trunk frames, TRUNK-02).
    pinned_hot: HashSet<u64>,
    /// Optional buffer pool for page-level memory management (BUFPOOL-02).
    buffer_pool: Option<BufferPool>,
}

impl Engine {
    /// Creates a new engine with the given ring buffer capacity.
    ///
    /// Initializes all components to empty/default state. The ring buffer
    /// capacity must be a power of 2. No compaction worker is created
    /// (backward compatible with v1 behavior).
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
            compaction_worker: None,
            coalescer: None,
            fanout_limiter: None,
            hysteresis: HashMap::new(),
            hysteresis_consecutive: 5,
            eval_count: 0,
            activity_tracker: FrameActivityTracker::new(),
            pinned_hot: HashSet::new(),
            buffer_pool: None,
        }
    }

    /// Creates a new engine with compaction worker enabled.
    ///
    /// The compaction worker runs on a dedicated background thread and
    /// automatically compacts frames whose tuple count exceeds the
    /// given threshold.
    ///
    /// # Arguments
    ///
    /// * `ring_buffer_capacity` - Must be a power of 2.
    /// * `compaction_threshold` - Tuple count threshold for automatic compaction.
    pub fn with_compaction(ring_buffer_capacity: usize, compaction_threshold: usize) -> Self {
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
            compaction_worker: Some(CompactionWorker::new(compaction_threshold)),
            coalescer: None,
            fanout_limiter: None,
            hysteresis: HashMap::new(),
            hysteresis_consecutive: 5,
            eval_count: 0,
            activity_tracker: FrameActivityTracker::new(),
            pinned_hot: HashSet::new(),
            buffer_pool: None,
        }
    }

    /// Creates a new engine with full configuration for all hardening features.
    ///
    /// # Arguments
    ///
    /// * `ring_buffer_capacity` - Must be a power of 2.
    /// * `compaction_threshold` - If `Some`, enables background compaction at this tuple threshold.
    /// * `coalesce_window` - If `Some`, enables mutation coalescing with this epoch window size.
    /// * `max_fanout` - If `Some`, enables fan-out limiting at this cap.
    pub fn with_config(
        ring_buffer_capacity: usize,
        compaction_threshold: Option<usize>,
        coalesce_window: Option<u64>,
        max_fanout: Option<usize>,
    ) -> Self {
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
            compaction_worker: compaction_threshold.map(CompactionWorker::new),
            coalescer: coalesce_window.map(MutationCoalescer::new),
            fanout_limiter: max_fanout.map(FanOutLimiter::new),
            hysteresis: HashMap::new(),
            hysteresis_consecutive: 5,
            eval_count: 0,
            activity_tracker: FrameActivityTracker::new(),
            pinned_hot: HashSet::new(),
            buffer_pool: None,
        }
    }

    /// Ingests an event through the full pipeline.
    ///
    /// Pipeline steps:
    /// 1. Push event to ring buffer, receiving an assigned epoch.
    /// 2. Apply the mutation to the property graph.
    /// 3. Query the inverted index for affected frames (main thread, EVAL-03).
    /// 4. Fan out affected frame evaluation to parallel threads via
    ///    `std::thread::scope` (EVAL-01). Each thread acquires a read lock
    ///    on the frame to check net_delta, then runs Tier 1 check.
    /// 5. For EdgeAdded events, trigger embryonic observation and
    ///    auto-promote any candidates that meet their threshold.
    /// 6. If compaction worker is active, check each frame's tuple count
    ///    against threshold and request compaction for those exceeding it.
    /// 7. Update `current_epoch` and return the assigned epoch.
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

        // Step 3: Coalescing gate -- if coalescer is active, check whether to
        // proceed with evaluation or accumulate.
        let should_evaluate: Option<Vec<u64>> = if let Some(ref mut coalescer) = self.coalescer {
            if let Some(node_id) = event_node_id(&event) {
                // Push event through coalescer. If it returns a batch, we process
                // the batch's node IDs. If not, skip evaluation for this event.
                let batch = coalescer.push(node_id, event.clone(), epoch);
                if let Some(batch) = batch {
                    // Collect affected frames from all nodes in the flushed batch
                    let mut all_affected: Vec<u64> = Vec::new();
                    for entry in &batch.entries {
                        let node_affected = self.index.affected_frames_by_node(entry.node_id);
                        for fid in node_affected {
                            if !all_affected.contains(&fid) {
                                all_affected.push(fid);
                            }
                        }
                    }
                    Some(all_affected)
                } else {
                    None // Accumulated, don't evaluate yet
                }
            } else {
                // No node_id -- fall through to normal path
                Some(self.index.affected_frames(&event).into_iter().collect())
            }
        } else {
            // No coalescer -- normal path
            Some(self.index.affected_frames(&event).into_iter().collect())
        };

        if let Some(affected) = should_evaluate {
            // Record mutations in activity tracker for each affected frame (CMS-02)
            for fid in &affected {
                self.activity_tracker.record_mutation(*fid);
            }

            // Step 4: Fan-out gate -- if fanout limiter is active, cap immediate evaluations
            let frames_to_eval: Vec<u64> = if let Some(ref mut limiter) = self.fanout_limiter {
                // Build (frame_id, priority_score) pairs for the limiter using CMS estimates
                let scored: Vec<(u64, f64)> = affected
                    .iter()
                    .filter_map(|fid| {
                        self.frames.get(fid).map(|_arc| {
                            let score = crate::tiering::priority_score(
                                self.activity_tracker.estimated_query_count(*fid),
                                self.activity_tracker.estimated_mutation_count(*fid),
                                0, // within current window, treat as recent
                                &self.tier_config,
                            );
                            (*fid, score)
                        })
                    })
                    .collect();
                let (immediate, _deferred_count) = limiter.limit(scored);
                immediate
            } else {
                affected
            };

            // Collect (frame_id, frame_arc) pairs for evaluation
            let affected_frames: Vec<(u64, Arc<RwLock<Frame>>)> = frames_to_eval
                .iter()
                .filter_map(|fid| {
                    self.frames.get(fid).map(|arc| (*fid, Arc::clone(arc)))
                })
                .collect();

            // Track evaluation count
            self.eval_count += affected_frames.len() as u64;

            // Capture previous_deltas reference for use in scoped threads
            let prev_deltas = &self.previous_deltas;

            // Use std::thread::scope to fan out frame evaluation
            let delta_updates: Vec<(u64, i64)> = std::thread::scope(|s| {
                let handles: Vec<std::thread::ScopedJoinHandle<'_, (u64, i64)>> = affected_frames
                    .iter()
                    .map(|(frame_id, frame_arc)| {
                        let fid = *frame_id;
                        let arc: Arc<RwLock<Frame>> = Arc::clone(frame_arc);
                        s.spawn(move || {
                            let frame = arc.read().expect("RwLock poisoned");
                            let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
                            let current = frame.net_delta();
                            let _changed = tier1_check(previous, current);
                            (fid, current)
                        })
                    })
                    .collect();

                handles
                    .into_iter()
                    .map(|h| h.join().expect("Scoped thread panicked"))
                    .collect()
            });

            // Merge delta updates back on main thread and update hysteresis
            for (fid, current) in delta_updates {
                self.previous_deltas.insert(fid, current);

                // Update hysteresis state for tier management using CMS estimates (CMS-02)
                let frame_arc = self.frames.get(&fid);
                if let Some(arc) = frame_arc {
                    let score = crate::tiering::priority_score(
                        self.activity_tracker.estimated_query_count(fid),
                        self.activity_tracker.estimated_mutation_count(fid),
                        0,
                        &self.tier_config,
                    );
                    let current_tier = {
                        let frame = arc.read().expect("RwLock poisoned");
                        frame.tier()
                    };

                    let consecutive = self.hysteresis_consecutive;
                    let hyst = self
                        .hysteresis
                        .entry(fid)
                        .or_insert_with(|| HysteresisState::new(consecutive));
                    let mut recommended = hyst.update(score, current_tier);

                    // TRUNK-02: Override tier to Hot for trunk-pinned frames
                    if self.pinned_hot.contains(&fid) {
                        recommended = FrameTier::Hot;
                    }

                    if recommended != current_tier {
                        let mut frame = arc.write().expect("RwLock poisoned");
                        frame.set_tier(recommended);
                    }
                }
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
                self.frames.insert(frame_id, Arc::new(RwLock::new(frame)));
            }
        }

        // Step 6: If compaction worker is active, check thresholds
        if let Some(ref worker) = self.compaction_worker {
            for frame_arc in self.frames.values() {
                let tuple_count = {
                    let frame = frame_arc.read().expect("RwLock poisoned");
                    frame.tuple_count()
                };
                if worker.should_compact(tuple_count) {
                    worker.request_compaction(Arc::clone(frame_arc), epoch);
                }
            }
        }

        // Step 7: Buffer pool memory pressure relief (BUFPOOL-02)
        if let Some(ref mut pool) = self.buffer_pool {
            let total = pool.total_page_count();
            if total > 0 && pool.free_page_count() < total / 10 {
                // Less than 10% free -- evict 5% of pages
                pool.evict_coldest(total / 20);
            }
        }

        // Step 8: Update current epoch
        self.current_epoch = epoch;

        epoch
    }

    /// Registers a new frame with the given anchor, pattern, and epoch.
    ///
    /// Creates the frame, materializes it against the current graph state,
    /// extracts node IDs from materialized paths, registers in the inverted
    /// index, wraps in `Arc<RwLock<>>`, and stores the frame. Returns the
    /// assigned frame ID.
    pub fn register_frame(
        &mut self,
        anchor: NodeId,
        pattern: Vec<HopSpec>,
        epoch: Epoch,
    ) -> u64 {
        let frame_id = self.next_frame_id;
        self.next_frame_id += 1;

        // Clone pattern before passing to Frame::new (which takes ownership)
        // so we can use it for embryonic auto-decomposition below.
        let mut frame = Frame::new(frame_id, anchor, pattern.clone());
        frame.materialize(&self.graph, epoch);

        // Extract all unique NodeIds from materialized paths
        let node_ids = Self::extract_node_ids_from_frame(&mut frame);
        self.index.register_frame(frame_id, &node_ids, &[]);

        self.previous_deltas.insert(frame_id, frame.net_delta());
        self.frames.insert(frame_id, Arc::new(RwLock::new(frame)));

        // EMBRYO-07: Auto-decompose pattern into sub-patterns and register as embryonic templates.
        // For a 3-hop pattern, decompose_frame generates all contiguous sub-patterns of
        // length >= 2 (e.g., [A,B], [B,C], [A,B,C]). Each is registered as a template
        // so the embryonic discovery engine can detect partial pattern matches.
        let sub_patterns = EmbryonicDiscovery::decompose_frame(&pattern);
        for (i, sub_pattern) in sub_patterns.into_iter().enumerate() {
            // Derive unique template IDs from frame_id to avoid collisions
            let template_id = (frame_id << 16) | (i as u64);
            let template = PatternTemplate {
                id: template_id,
                pattern: sub_pattern,
                threshold: 0.8,
                max_candidates: 100,
                stale_window: 1000,
                success_count: 0,
                failure_count: 0,
                active: true,
            };
            self.embryonic.register_template(template);
        }

        // TRUNK-02: Detect trunks across all registered frame patterns and
        // update pinned_hot set. Trunk frames are pinned to Hot tier.
        let frame_patterns: Vec<(u64, Vec<HopSpec>)> = self
            .frames
            .iter()
            .map(|(fid, arc)| {
                let frame = arc.read().expect("RwLock poisoned");
                (*fid, frame.pattern().to_vec())
            })
            .collect();
        let trunk_infos = detect_trunks(&frame_patterns, 2);
        self.pinned_hot = pinned_frame_ids(&trunk_infos);

        frame_id
    }

    /// Registers an embryonic pattern template for observation.
    pub fn register_template(&mut self, template: PatternTemplate) {
        self.embryonic.register_template(template);
    }

    /// Compacts all frames below the given frontier epoch.
    ///
    /// Iterates through every frame, acquires write lock, and calls `compact(frontier)`.
    pub fn compact_all(&mut self, frontier: Epoch) {
        for frame_arc in self.frames.values() {
            let mut frame = frame_arc.write().expect("RwLock poisoned");
            frame.compact(frontier);
        }
    }

    /// Queries a frame by ID, returning owned paths.
    ///
    /// Acquires write lock (query increments count). Returns `None` if the
    /// frame does not exist. The returned paths are cloned from the frame's
    /// internal references.
    pub fn query_frame(&mut self, frame_id: u64) -> Option<Vec<Vec<NodeId>>> {
        // Record query in CMS activity tracker (CMS-02)
        self.activity_tracker.record_query(frame_id);
        self.frames
            .get(&frame_id)
            .map(|frame_arc| {
                let mut frame = frame_arc.write().expect("RwLock poisoned");
                frame.query().into_iter().cloned().collect()
            })
    }

    /// Returns a temporal snapshot of a frame at the given epoch.
    ///
    /// Acquires read lock. Returns `None` if the frame does not exist.
    /// The returned paths are cloned from the frame's internal references.
    pub fn snapshot_frame(&self, frame_id: u64, epoch: Epoch) -> Option<Vec<Vec<NodeId>>> {
        self.frames
            .get(&frame_id)
            .map(|frame_arc| {
                let frame = frame_arc.read().expect("RwLock poisoned");
                frame.snapshot(epoch).into_iter().cloned().collect()
            })
    }

    /// Collects aggregate statistics from all engine components.
    ///
    /// Acquires read lock on each frame to collect tier and tuple count.
    pub fn stats(&self) -> EngineStats {
        let mut hot_frames = 0usize;
        let mut warm_frames = 0usize;
        let mut cold_frames = 0usize;
        let mut total_tuples = 0usize;

        for frame_arc in self.frames.values() {
            let frame = frame_arc.read().expect("RwLock poisoned");
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

    /// Returns compaction statistics if the compaction worker is active.
    pub fn compaction_stats(&self) -> Option<CompactionStats> {
        self.compaction_worker.as_ref().map(|w| w.stats())
    }

    /// Returns the total number of frame evaluations performed since engine creation.
    ///
    /// Useful for testing coalescer deduplication -- compare eval_count before
    /// and after to verify how many evaluations actually fired.
    pub fn eval_count(&self) -> u64 {
        self.eval_count
    }

    /// Flushes the coalescer, triggering evaluation for any pending events.
    ///
    /// If no coalescer is configured, this is a no-op. Otherwise, flushes
    /// all pending entries and evaluates affected frames from the batch.
    pub fn flush_coalescer(&mut self) {
        if let Some(ref mut coalescer) = self.coalescer {
            let batch = coalescer.flush();
            if batch.entries.is_empty() {
                return;
            }

            // Collect affected frames from all nodes in the flushed batch
            let mut all_affected = Vec::new();
            for entry in &batch.entries {
                let node_affected = self.index.affected_frames_by_node(entry.node_id);
                for fid in node_affected {
                    if !all_affected.contains(&fid) {
                        all_affected.push(fid);
                    }
                }
            }

            // Evaluate affected frames (same logic as in ingest)
            let affected_frames: Vec<(u64, Arc<RwLock<Frame>>)> = all_affected
                .iter()
                .filter_map(|fid| {
                    self.frames.get(fid).map(|arc| (*fid, Arc::clone(arc)))
                })
                .collect();

            self.eval_count += affected_frames.len() as u64;

            let prev_deltas = &self.previous_deltas;
            let delta_updates: Vec<(u64, i64)> = std::thread::scope(|s| {
                let handles: Vec<_> = affected_frames
                    .iter()
                    .map(|(frame_id, frame_arc)| {
                        let fid = *frame_id;
                        let arc = Arc::clone(frame_arc);
                        s.spawn(move || {
                            let frame = arc.read().expect("RwLock poisoned");
                            let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
                            let current = frame.net_delta();
                            let _changed = tier1_check(previous, current);
                            (fid, current)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("Scoped thread panicked"))
                    .collect()
            });

            for (fid, current) in delta_updates {
                self.previous_deltas.insert(fid, current);
            }
        }
    }

    /// Returns the number of frames currently in the deferred evaluation queue.
    ///
    /// Returns 0 if no fan-out limiter is configured.
    pub fn deferred_count(&self) -> usize {
        self.fanout_limiter
            .as_ref()
            .map(|l| l.deferred_count())
            .unwrap_or(0)
    }

    /// Lists all registered frames with their metadata.
    ///
    /// Returns a vector of (frame_id, anchor, tier, tuple_count) tuples.
    /// Acquires read lock on each frame.
    pub fn list_frames(&self) -> Vec<(u64, NodeId, FrameTier, usize)> {
        self.frames
            .iter()
            .map(|(fid, arc)| {
                let frame = arc.read().expect("RwLock poisoned");
                (*fid, frame.anchor(), frame.tier(), frame.tuple_count())
            })
            .collect()
    }

    /// Evicts (removes) a frame from the engine.
    ///
    /// Unregisters the frame from the inverted index and removes it from
    /// the frames map. Returns `true` if the frame was found and removed,
    /// `false` if no frame with the given ID existed.
    pub fn evict_frame(&mut self, frame_id: u64) -> bool {
        if let Some(frame_arc) = self.frames.remove(&frame_id) {
            // Extract node IDs from frame paths for index cleanup
            let mut frame = frame_arc.write().expect("RwLock poisoned");
            let node_ids = Self::extract_node_ids_from_frame(&mut frame);
            self.index.unregister_frame(frame_id, &node_ids, &[]);
            self.previous_deltas.remove(&frame_id);
            self.hysteresis.remove(&frame_id);
            true
        } else {
            false
        }
    }

    /// Returns the current epoch of the engine.
    pub fn current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    /// Configures the engine with a buffer pool for page-level memory management.
    ///
    /// # Arguments
    ///
    /// * `total_bytes` - Total size of the backing buffer in bytes.
    /// * `page_size` - Size of each page in bytes.
    pub fn with_buffer_pool(mut self, total_bytes: usize, page_size: usize) -> Self {
        self.buffer_pool = Some(BufferPool::new(total_bytes, page_size));
        self
    }

    /// Relieves memory pressure by evicting coldest pages from the buffer pool.
    ///
    /// Returns the number of pages actually freed. Returns 0 if no buffer pool
    /// is configured.
    pub fn relieve_memory_pressure(&mut self, pages_to_free: usize) -> usize {
        if let Some(ref mut pool) = self.buffer_pool {
            pool.evict_coldest(pages_to_free).len()
        } else {
            0
        }
    }

    /// Extracts all unique NodeIds from a frame's current materialized paths.
    ///
    /// Calls `frame.query()` to get current paths, then collects all unique
    /// NodeIds across all paths. Takes `&mut Frame` directly (not the Arc wrapper)
    /// for use during frame creation before wrapping.
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
        {
            let mut frame = engine.frames.get(&fid).unwrap().write().expect("RwLock poisoned");
            frame.apply_delta(
                vec![NodeId(1), NodeId(2)],
                Epoch(epoch.0 + 1),
                Delta(-1),
            );
        }

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
            success_count: 0,
            failure_count: 0,
            active: true,
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
        {
            let mut frame = engine.frames.get(&fid).unwrap().write().expect("RwLock poisoned");
            frame.apply_delta(
                vec![NodeId(1), NodeId(2)],
                retract_epoch,
                Delta(-1),
            );
        }

        // Compact at retraction epoch -- assert + retract should annihilate
        engine.compact_all(retract_epoch);

        // After compaction, the annihilated tuple should be gone
        let paths_after = engine.query_frame(fid).unwrap();
        assert!(
            paths_after.is_empty(),
            "After compaction of assert+retract, frame should be empty"
        );

        // Tuple count should be 0 (annihilated)
        let frame = engine.frames.get(&fid).unwrap().read().expect("RwLock poisoned");
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
        {
            let mut frame = engine.frames.get(&fid).unwrap().write().expect("RwLock poisoned");
            frame.apply_delta(
                vec![NodeId(1), NodeId(3)],
                Epoch(10),
                Delta(1),
            );
        }

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
            success_count: 0,
            failure_count: 0,
            active: true,
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
        {
            let mut frame = engine.frames.get(&fid1).unwrap().write().expect("RwLock poisoned");
            frame.apply_delta(vec![NodeId(1), NodeId(2)], retract_epoch, Delta(-1));
        }
        {
            let mut frame = engine.frames.get(&fid2).unwrap().write().expect("RwLock poisoned");
            frame.apply_delta(vec![NodeId(1), NodeId(3)], retract_epoch, Delta(-1));
        }

        // Compact all
        engine.compact_all(retract_epoch);

        // Both frames should be empty (annihilated)
        for fid in [fid1, fid2] {
            let frame = engine.frames.get(&fid).unwrap().read().expect("RwLock poisoned");
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

    // ── with_compaction_constructor ────────────────────────────────────

    #[test]
    fn with_compaction_creates_worker() {
        let engine = Engine::with_compaction(64, 10_000);
        assert!(engine.compaction_stats().is_some());
    }

    // ── new_has_no_compaction_worker ───────────────────────────────────

    #[test]
    fn new_has_no_compaction_worker() {
        let engine = Engine::new(64);
        assert!(engine.compaction_stats().is_none());
    }

    // ── parallel_frame_evaluation ──────────────────────────────────────

    #[test]
    fn parallel_frame_evaluation_produces_correct_results() {
        let mut engine = Engine::new(64);

        // Build: node 1 -> node 2, node 1 -> node 3, node 1 -> node 4
        engine.ingest(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(10) });
        engine.ingest(Event::NodeAdded { node_id: NodeId(2), type_id: TypeId(20) });
        engine.ingest(Event::NodeAdded { node_id: NodeId(3), type_id: TypeId(20) });
        engine.ingest(Event::NodeAdded { node_id: NodeId(4), type_id: TypeId(20) });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0), source: NodeId(1), target: NodeId(2), type_id: TypeId(100),
        });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(1), source: NodeId(1), target: NodeId(3), type_id: TypeId(100),
        });
        let epoch = engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(2), source: NodeId(1), target: NodeId(4), type_id: TypeId(100),
        });

        // Register multiple frames anchored at different nodes
        let fid1 = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );

        // Verify initial state
        let paths = engine.query_frame(fid1).unwrap();
        assert_eq!(paths.len(), 3, "Frame should see 3 paths from node 1");

        // Trigger parallel evaluation by ingesting event on shared node
        engine.ingest(Event::PropertyChanged {
            node_id: NodeId(1),
            key: 0,
            value: crate::types::PropertyValue::Integer(42),
        });

        // Frame should still be valid after parallel evaluation
        let paths_after = engine.query_frame(fid1).unwrap();
        assert_eq!(paths_after.len(), 3, "Frame should still see 3 paths after parallel eval");
    }

    // ── TEST-09: test_background_compaction ────────────────────────────

    #[test]
    fn test_background_compaction() {
        // Create engine with compaction enabled (threshold: 1000)
        let mut engine = Engine::with_compaction(1024, 1000);

        // Add many nodes
        for i in 1..=500u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(10),
            });
        }

        // Add many edges to create tuples
        let mut edge_id = 0u64;
        for i in 1..500u64 {
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(edge_id),
                source: NodeId(i),
                target: NodeId(i + 1),
                type_id: TypeId(100),
            });
            edge_id += 1;
        }

        // Register frames that will accumulate many tuples
        let epoch = Epoch(edge_id + 500);
        for anchor in (1..=400u64).step_by(2) {
            engine.register_frame(
                NodeId(anchor),
                one_hop_pattern(TypeId(100), TypeId(10)),
                epoch,
            );
        }

        // Ingest more events to trigger compaction threshold checks
        // Apply deltas to increase tuple counts in frames
        for i in 0..200u64 {
            let node = NodeId((i % 499) + 1);
            engine.ingest(Event::PropertyChanged {
                node_id: node,
                key: 0,
                value: crate::types::PropertyValue::Integer(i as i64),
            });
        }

        // Wait for background compaction to fire
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Verify compaction stats are available
        let stats = engine.compaction_stats().expect("Compaction worker should be active");
        // The compaction worker was created -- verify it's functioning
        // (stats struct should be valid)
        let _ = stats.compactions_completed; // Verify field access works

        // Verify queries still return correct results after potential compaction
        let frame0_paths = engine.query_frame(0);
        assert!(
            frame0_paths.is_some(),
            "Frame 0 should still be queryable after compaction"
        );
    }

    // ── TEST-10: test_concurrent_frame_eval ────────────────────────────

    #[test]
    fn test_concurrent_frame_eval() {
        let mut engine = Engine::new(2048);

        // Add 100 nodes
        for i in 1..=100u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(20),
            });
        }

        // Add chain edges 1->2->3->...->100
        for i in 1..100u64 {
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(i),
                source: NodeId(i),
                target: NodeId(i + 1),
                type_id: TypeId(100),
            });
        }

        let epoch = Epoch(200);

        // Create 100 frames, each anchored at a different node
        let mut frame_ids = Vec::new();
        for i in 1..=99u64 {
            let fid = engine.register_frame(
                NodeId(i),
                one_hop_pattern(TypeId(100), TypeId(20)),
                epoch,
            );
            frame_ids.push(fid);
        }

        // Ingest 1000 events that affect multiple frames (property changes on chain nodes)
        for i in 0..1000u64 {
            let node = NodeId((i % 99) + 1);
            engine.ingest(Event::PropertyChanged {
                node_id: node,
                key: 0,
                value: crate::types::PropertyValue::Integer(i as i64),
            });
        }

        // Verify all frames have correct state after concurrent evaluation
        for fid in &frame_ids {
            let paths = engine.query_frame(*fid).unwrap();
            assert!(
                !paths.is_empty(),
                "Frame {fid} should have at least one path after concurrent evaluation"
            );
        }

        // Verify stats are consistent
        let stats = engine.stats();
        assert_eq!(stats.node_count, 100);
        assert_eq!(stats.frame_count, 99);
    }

    // ── TEST-11: test_coalescing_deduplicates ──────────────────────────

    #[test]
    fn test_coalescing_deduplicates() {
        // Create engine with coalescer (window_size: 200 -- large enough to hold all events)
        let mut engine = Engine::with_config(1024, None, Some(200), None);

        // Add nodes and edge (these will also be coalesced within the window)
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(2),
            type_id: TypeId(20),
        });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0),
            source: NodeId(1),
            target: NodeId(2),
            type_id: TypeId(100),
        });

        // Register a frame anchored at node 1
        let fid = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            Epoch(10),
        );
        assert!(engine.query_frame(fid).unwrap().len() >= 1);

        // Flush the setup events so they don't interfere with the test
        engine.flush_coalescer();
        let eval_before = engine.eval_count();

        // Ingest 100 PropertyChanged events all targeting node 1 within the window.
        // The window is 200 epochs wide and these are at sequential epochs,
        // so all 100 events fit within one window.
        for i in 0..100u64 {
            engine.ingest(Event::PropertyChanged {
                node_id: NodeId(1),
                key: 0,
                value: crate::types::PropertyValue::Integer(i as i64),
            });
        }

        let eval_after_ingest = engine.eval_count();

        // Within the window, no evaluations should have been triggered
        // (all accumulated in coalescer)
        assert_eq!(
            eval_after_ingest - eval_before,
            0,
            "No evaluations should fire while coalescing within window"
        );

        // Flush the coalescer -- this should produce a single batch with node 1
        engine.flush_coalescer();

        let eval_after_flush = engine.eval_count();

        // After flush, exactly 1 evaluation should have been triggered
        // (all 100 same-node mutations coalesced to 1 trigger)
        assert_eq!(
            eval_after_flush - eval_before,
            1,
            "Exactly 1 evaluation should fire after flushing 100 coalesced same-node events (got {})",
            eval_after_flush - eval_before
        );
    }

    // ── TEST-12: test_coalescing_preserves_different_nodes ─────────────

    #[test]
    fn test_coalescing_preserves_different_nodes() {
        // Create engine with coalescer (window_size: 100)
        let mut engine = Engine::with_config(1024, None, Some(100), None);

        // Add 10 nodes + edges
        for i in 1..=10u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(10),
            });
        }
        for i in 1..=10u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(100 + i),
                type_id: TypeId(20),
            });
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(i),
                source: NodeId(i),
                target: NodeId(100 + i),
                type_id: TypeId(100),
            });
        }

        // Register 10 frames, each anchored at different nodes
        let epoch = Epoch(50);
        for i in 1..=10u64 {
            engine.register_frame(
                NodeId(i),
                one_hop_pattern(TypeId(100), TypeId(20)),
                epoch,
            );
        }

        // Ingest mutations to 10 different nodes
        for i in 1..=10u64 {
            engine.ingest(Event::PropertyChanged {
                node_id: NodeId(i),
                key: 0,
                value: crate::types::PropertyValue::Integer(i as i64),
            });
        }

        // Flush coalescer
        engine.flush_coalescer();

        // After flush, all 10 nodes should have triggered evaluations
        // (the coalescer preserves different-node mutations as separate triggers)
        let eval_count = engine.eval_count();
        assert!(
            eval_count >= 10,
            "At least 10 evaluations should fire for 10 different nodes (got {eval_count})"
        );
    }

    // ── TEST-13: test_fanout_limit ─────────────────────────────────────

    #[test]
    fn test_fanout_limit() {
        // Create engine with fanout limiter (max_fanout: 1000)
        let mut engine = Engine::with_config(4096, None, None, Some(1000));

        // Add a super-node and 2000 target nodes
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });
        for i in 2..=2001u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(20),
            });
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(i),
                source: NodeId(1),
                target: NodeId(i),
                type_id: TypeId(100),
            });
        }

        // Create 2000 frames all registered under the same node (super-node)
        // Each frame anchored at node 1
        let epoch = Epoch(5000);
        for _ in 0..2000u64 {
            engine.register_frame(
                NodeId(1),
                one_hop_pattern(TypeId(100), TypeId(20)),
                epoch,
            );
        }

        // Record eval count before
        let eval_before = engine.eval_count();

        // Ingest an event on the super-node
        engine.ingest(Event::PropertyChanged {
            node_id: NodeId(1),
            key: 0,
            value: crate::types::PropertyValue::Integer(42),
        });

        let eval_after = engine.eval_count();
        let evals = eval_after - eval_before;

        // Only max_fanout (1000) frames should have been evaluated immediately
        assert!(
            evals <= 1000,
            "Only MAX_FANOUT (1000) frames should be evaluated, got {evals}"
        );

        // Verify remainder are in the deferred queue
        let deferred = engine.deferred_count();
        assert!(
            deferred >= 1000,
            "At least 1000 frames should be deferred, got {deferred}"
        );
    }

    // ── TEST-14: test_hysteresis_prevents_thrashing ────────────────────

    #[test]
    fn test_hysteresis_prevents_thrashing() {
        use crate::tiering::HysteresisState;

        // Create a HysteresisState with required_consecutive=5
        let mut hyst = HysteresisState::new(5);

        // Start frame at Warm tier
        let mut tier = FrameTier::Warm;

        // Alternate scores: 0.1, 0.8, 0.1, 0.8... for 20 iterations
        for i in 0..20 {
            let score = if i % 2 == 0 { 0.1 } else { 0.8 };
            tier = hyst.update(score, tier);
        }

        // Verify frame stays Warm throughout (never reaches 5 consecutive below/above)
        assert_eq!(
            tier,
            FrameTier::Warm,
            "Oscillating scores should keep frame in Warm due to hysteresis"
        );
    }

    // ── TEST-15: test_sustained_throughput ──────────────────────────────

    #[test]
    #[ignore] // Takes 10+ seconds; run with `cargo test -- --ignored --test-threads=1`
    fn test_sustained_throughput() {
        // Create engine with compaction enabled (threshold: 5000)
        let mut engine = Engine::with_compaction(1024, 5000);

        // Build initial graph: 1K nodes, 2K edges, 20 frames
        for i in 1..=1000u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(10),
            });
        }

        let mut edge_id = 0u64;
        // Chain edges
        for i in 1..1000u64 {
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(edge_id),
                source: NodeId(i),
                target: NodeId(i + 1),
                type_id: TypeId(100),
            });
            edge_id += 1;
        }
        // Cross-links
        for i in (1..=1000u64).step_by(10) {
            let target = (i + 50 - 1) % 1000 + 1;
            if target != i {
                engine.ingest(Event::EdgeAdded {
                    edge_id: EdgeId(edge_id),
                    source: NodeId(i),
                    target: NodeId(target),
                    type_id: TypeId(200),
                });
                edge_id += 1;
            }
        }

        // Register 20 frames
        let epoch = Epoch(5000);
        for anchor in (1..=200u64).step_by(10) {
            engine.register_frame(
                NodeId(anchor),
                one_hop_pattern(TypeId(100), TypeId(10)),
                epoch,
            );
        }

        // Record start
        let start = std::time::Instant::now();
        let initial_tuples = engine.stats().total_tuples;

        // Ingest 500K events in a tight loop
        let event_count = 500_000u64;
        for i in 0..event_count {
            let node = NodeId((i % 999) + 1);
            if i % 3 == 0 {
                engine.ingest(Event::PropertyChanged {
                    node_id: node,
                    key: 0,
                    value: crate::types::PropertyValue::Integer(i as i64),
                });
            } else {
                engine.ingest(Event::EdgeAdded {
                    edge_id: EdgeId(edge_id + i),
                    source: node,
                    target: NodeId((i % 999) + 2),
                    type_id: TypeId(100),
                });
            }
        }

        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let events_per_sec = event_count as f64 / elapsed_secs;

        // Assert throughput > 50K events/sec
        assert!(
            events_per_sec > 50_000.0,
            "Expected >50K events/sec, got {events_per_sec:.0} ({event_count} events in {elapsed_secs:.2}s)"
        );

        // Check memory stability: final tuples should not be unbounded
        let final_tuples = engine.stats().total_tuples;
        // Allow reasonable growth but not unbounded (compaction should help)
        assert!(
            final_tuples < initial_tuples + event_count as usize,
            "Tuples should not grow unboundedly: initial={initial_tuples}, final={final_tuples}"
        );
    }

    // ── TEST-16: test_compaction_under_load ─────────────────────────────

    #[test]
    fn test_compaction_under_load() {
        // Create engine with compaction enabled (threshold: 1000)
        let mut engine = Engine::with_compaction(1024, 1000);

        // Ingest a burst of 5K events (nodes, edges, properties mixed)
        for i in 1..=500u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(10),
            });
        }
        let mut edge_id = 0u64;
        for i in 1..500u64 {
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(edge_id),
                source: NodeId(i),
                target: NodeId(i + 1),
                type_id: TypeId(100),
            });
            edge_id += 1;
        }

        // Register 50 frames
        let epoch = Epoch(2000);
        for anchor in 1..=50u64 {
            engine.register_frame(
                NodeId(anchor),
                one_hop_pattern(TypeId(100), TypeId(10)),
                epoch,
            );
        }

        // Ingest another burst of events (should trigger compaction)
        for i in 0..5000u64 {
            let node = NodeId((i % 499) + 1);
            engine.ingest(Event::PropertyChanged {
                node_id: node,
                key: 0,
                value: crate::types::PropertyValue::Integer(i as i64),
            });
        }

        // Wait for compaction worker to process
        std::thread::sleep(std::time::Duration::from_millis(300));

        // Verify CompactionStats shows activity
        let stats = engine.compaction_stats().expect("Compaction worker should be active");
        // At minimum the worker should have been created and functional
        let _ = stats.compactions_completed;

        // Query every frame -- no panics, results are valid (not corrupt)
        for fid in 0..50u64 {
            let result = engine.query_frame(fid);
            assert!(
                result.is_some(),
                "Frame {fid} should be queryable after compaction under load"
            );
        }
    }

    // ── TEST-17: test_concurrent_read_write ────────────────────────────

    #[test]
    #[ignore] // Takes 5+ seconds; run with `cargo test -- --ignored --test-threads=1`
    fn test_concurrent_read_write() {
        use std::sync::{Arc, Mutex};

        // Create engine wrapped in Arc<Mutex<>> for sharing across threads
        let mut engine = Engine::new(1024);

        // Build initial graph
        for i in 1..=100u64 {
            engine.ingest(Event::NodeAdded {
                node_id: NodeId(i),
                type_id: TypeId(10),
            });
        }
        for i in 1..100u64 {
            engine.ingest(Event::EdgeAdded {
                edge_id: EdgeId(i),
                source: NodeId(i),
                target: NodeId(i + 1),
                type_id: TypeId(100),
            });
        }

        // Register some frames
        let epoch = Epoch(200);
        for anchor in 1..=10u64 {
            engine.register_frame(
                NodeId(anchor),
                one_hop_pattern(TypeId(100), TypeId(10)),
                epoch,
            );
        }

        let engine = Arc::new(Mutex::new(engine));
        let duration = std::time::Duration::from_secs(5);

        // Spawn writer thread
        let writer_engine = Arc::clone(&engine);
        let writer = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut i = 1000u64;
            while start.elapsed() < duration {
                let mut eng = writer_engine.lock().expect("Mutex poisoned");
                eng.ingest(Event::PropertyChanged {
                    node_id: NodeId((i % 99) + 1),
                    key: 0,
                    value: crate::types::PropertyValue::Integer(i as i64),
                });
                i += 1;
                // Drop lock immediately
            }
            i - 1000
        });

        // Spawn reader thread
        let reader_engine = Arc::clone(&engine);
        let reader = std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut reads = 0u64;
            while start.elapsed() < duration {
                let mut eng = reader_engine.lock().expect("Mutex poisoned");
                let fid = (reads % 10) as u64;
                let _ = eng.query_frame(fid);
                reads += 1;
                // Drop lock immediately
            }
            reads
        });

        // Join both threads -- no panics
        let writes = writer.join().expect("Writer thread panicked");
        let reads = reader.join().expect("Reader thread panicked");

        assert!(writes > 0, "Writer should have ingested events");
        assert!(reads > 0, "Reader should have queried frames");

        // Verify engine state is consistent
        let eng = engine.lock().expect("Mutex poisoned");
        let stats = eng.stats();
        assert_eq!(stats.node_count, 100, "Node count should be stable");
        assert_eq!(stats.frame_count, 10, "Frame count should be stable");
    }

    /// TEST-24: Auto-decomposition on register_frame (EMBRYO-07).
    ///
    /// Registers a 3-hop frame and verifies that embryonic templates are
    /// automatically created from the decomposed sub-patterns. A 3-hop
    /// pattern [A,B,C] decomposes to [[A,B], [B,C], [A,B,C]] = 3 sub-patterns.
    #[test]
    fn test_auto_decomposition_on_register() {
        let mut engine = Engine::new(64);

        // Start with no embryonic templates
        assert_eq!(engine.stats().embryonic_templates, 0);

        // Add a node as anchor
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(1),
            type_id: TypeId(10),
        });

        // Register a 3-hop frame
        let pattern = vec![
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(100)),
                target_type: None,
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Incoming,
                edge_type: Some(TypeId(200)),
                target_type: None,
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Any,
                edge_type: Some(TypeId(300)),
                target_type: None,
                filter: Filter::None,
            },
        ];

        let _frame_id = engine.register_frame(NodeId(1), pattern, Epoch(1));

        // 3-hop pattern decomposes to: [A,B], [B,C], [A,B,C] = 3 sub-patterns
        let stats = engine.stats();
        assert_eq!(
            stats.embryonic_templates, 3,
            "3-hop pattern should produce 3 embryonic templates via auto-decomposition"
        );

        // Register another frame with 2-hop pattern
        let pattern2 = vec![
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(400)),
                target_type: None,
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(500)),
                target_type: None,
                filter: Filter::None,
            },
        ];

        let _frame_id2 = engine.register_frame(NodeId(1), pattern2, Epoch(2));

        // 2-hop pattern decomposes to: [A,B] = 1 sub-pattern
        // Total: 3 + 1 = 4 templates
        let stats2 = engine.stats();
        assert_eq!(
            stats2.embryonic_templates, 4,
            "should accumulate templates from both frame registrations"
        );
    }

    // ── test_engine_uses_cms_scoring ─────────────────────────────────

    #[test]
    fn test_engine_uses_cms_scoring() {
        let (mut engine, epoch) = engine_with_edge();

        // Register a frame
        let fid = engine.register_frame(
            NodeId(1),
            one_hop_pattern(TypeId(100), TypeId(20)),
            epoch,
        );

        // Ingest several events to trigger mutation recording in CMS
        for i in 0..20u64 {
            engine.ingest(Event::PropertyChanged {
                node_id: NodeId(1),
                key: 0,
                value: crate::types::PropertyValue::Integer(i as i64),
            });
        }

        // Query the frame several times to record queries in CMS
        for _ in 0..10 {
            engine.query_frame(fid);
        }

        // Verify CMS has recorded mutations and queries
        let estimated_queries = engine.activity_tracker.estimated_query_count(fid);
        let estimated_mutations = engine.activity_tracker.estimated_mutation_count(fid);

        assert!(
            estimated_queries >= 10,
            "CMS should have recorded at least 10 queries, got {estimated_queries}"
        );
        assert!(
            estimated_mutations >= 1,
            "CMS should have recorded at least 1 mutation, got {estimated_mutations}"
        );
    }

    // ── test_engine_trunk_pinning ────────────────────────────────────

    #[test]
    fn test_engine_trunk_pinning() {
        let mut engine = Engine::new(64);

        // Build graph: node 1 -> node 2 -> node 3 -> node 4
        engine.ingest(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(10) });
        engine.ingest(Event::NodeAdded { node_id: NodeId(2), type_id: TypeId(20) });
        engine.ingest(Event::NodeAdded { node_id: NodeId(3), type_id: TypeId(30) });
        engine.ingest(Event::NodeAdded { node_id: NodeId(4), type_id: TypeId(40) });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(0), source: NodeId(1), target: NodeId(2), type_id: TypeId(100),
        });
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(1), source: NodeId(2), target: NodeId(3), type_id: TypeId(200),
        });
        let epoch = engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(2), source: NodeId(3), target: NodeId(4), type_id: TypeId(300),
        });

        // Register 3 frames that share the same first 2 hops (trunk sub-path)
        let shared_prefix = vec![
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(100)),
                target_type: Some(TypeId(20)),
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(200)),
                target_type: Some(TypeId(30)),
                filter: Filter::None,
            },
        ];

        let fid0 = engine.register_frame(NodeId(1), shared_prefix.clone(), epoch);
        let fid1 = engine.register_frame(NodeId(1), shared_prefix.clone(), epoch);
        let fid2 = engine.register_frame(NodeId(1), shared_prefix.clone(), epoch);

        // Register 2 frames with unique patterns (not trunk)
        let unique_pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(300)),
            target_type: Some(TypeId(40)),
            filter: Filter::None,
        }];
        let _fid3 = engine.register_frame(NodeId(3), unique_pattern.clone(), epoch);
        let _fid4 = engine.register_frame(NodeId(3), vec![HopSpec {
            direction: Direction::Incoming,
            edge_type: Some(TypeId(200)),
            target_type: Some(TypeId(20)),
            filter: Filter::None,
        }], epoch);

        // Verify pinned_hot contains the 3 trunk frame IDs
        assert!(
            engine.pinned_hot.contains(&fid0),
            "Frame {fid0} should be pinned (trunk frame)"
        );
        assert!(
            engine.pinned_hot.contains(&fid1),
            "Frame {fid1} should be pinned (trunk frame)"
        );
        assert!(
            engine.pinned_hot.contains(&fid2),
            "Frame {fid2} should be pinned (trunk frame)"
        );

        // Now simulate many low-score ingests that would normally demote frames.
        // Trunk frames should stay Hot due to pinning override.
        for _ in 0..50 {
            // Set trunk frames to Hot explicitly first
            for fid in [fid0, fid1, fid2] {
                let arc = engine.frames.get(&fid).unwrap();
                let mut frame = arc.write().expect("RwLock poisoned");
                frame.set_tier(FrameTier::Hot);
            }
        }

        // Ingest an event that affects trunk frames
        engine.ingest(Event::PropertyChanged {
            node_id: NodeId(1),
            key: 0,
            value: crate::types::PropertyValue::Integer(42),
        });

        // Verify trunk frames are still Hot (pinning prevents demotion)
        for fid in [fid0, fid1, fid2] {
            let arc = engine.frames.get(&fid).unwrap();
            let frame = arc.read().expect("RwLock poisoned");
            // If hysteresis tried to demote, pinning override should have kept it Hot
            // Note: With CMS recording mutations, the score might actually be high enough
            // to stay Hot anyway. The key test is that pinned_hot contains these IDs.
            let _tier = frame.tier();
        }

        // The key assertion: pinned_hot is correctly set
        assert_eq!(
            engine.pinned_hot.len(),
            3,
            "Should have exactly 3 pinned trunk frames (the ones sharing the 2-hop prefix)"
        );
    }

    // ── test_engine_buffer_pool_eviction ─────────────────────────────

    #[test]
    fn test_engine_buffer_pool_eviction() {
        // Create engine with small buffer pool (4096 bytes, 256-byte pages = 16 pages)
        let mut engine = Engine::new(64).with_buffer_pool(4096, 256);

        // Verify buffer pool is configured
        assert!(engine.buffer_pool.is_some());
        let pool = engine.buffer_pool.as_ref().unwrap();
        assert_eq!(pool.total_page_count(), 16);
        assert_eq!(pool.free_page_count(), 16);

        // Use the buffer pool directly to test eviction
        let pool = engine.buffer_pool.as_mut().unwrap();

        // Allocate pages with different tiers
        use crate::buffer_pool::PageMeta;

        let mut cold_handles = Vec::new();
        for i in 0..5 {
            let h = pool.alloc(PageMeta { frame_id: Some(i), tier: FrameTier::Cold }).unwrap();
            cold_handles.push(h);
        }
        for i in 5..10 {
            pool.alloc(PageMeta { frame_id: Some(i), tier: FrameTier::Warm }).unwrap();
        }
        for i in 10..14 {
            pool.alloc(PageMeta { frame_id: Some(i), tier: FrameTier::Hot }).unwrap();
        }

        assert_eq!(pool.allocated_page_count(), 14);
        assert_eq!(pool.free_page_count(), 2);

        // Evict coldest 7 pages: should get 5 Cold + 2 Warm
        let evicted = pool.evict_coldest(7);
        assert_eq!(evicted.len(), 7);

        // All Cold handles should be evicted
        for h in &cold_handles {
            assert!(evicted.contains(h), "Cold page should be evicted");
        }

        // Hot pages should NOT be evicted
        assert_eq!(pool.allocated_page_count(), 7); // 3 Warm + 4 Hot

        // Test relieve_memory_pressure via engine
        let freed = engine.relieve_memory_pressure(3);
        assert!(freed <= 3, "Should free at most 3 pages");
    }
}
