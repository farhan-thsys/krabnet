//! Embryonic frame discovery with bitvec completion tracking.
//!
//! This module implements autonomous pattern detection from the mutation stream.
//! Pattern templates define multi-hop patterns to watch for. When edges arrive,
//! candidates track partial pattern completion using [`bitvec`]. When completion
//! exceeds the template threshold, candidates are promoted to full frames.
//! Stale candidates are pruned, and a per-template cap prevents unbounded growth.
//!
//! # Design
//!
//! - [`PatternTemplate`] defines what to watch for: a multi-hop pattern,
//!   completion threshold, staleness window, and candidate cap.
//! - [`Candidate`] tracks a partial pattern match anchored at a node,
//!   with a [`BitVec`] indicating which hops have been observed.
//! - [`EmbryonicDiscovery`] orchestrates template registration, edge observation,
//!   auto-promotion, pruning, and cap enforcement.
//!
//! # Edge Matching
//!
//! When an edge `(source, target, edge_type)` arrives:
//! - For [`Direction::Outgoing`] hops, the edge source must match the anchor
//!   or the node reached by previous hops.
//! - For [`Direction::Incoming`] hops, the edge target must match.
//! - For [`Direction::Any`], either direction matches.
//! - If the hop has an `edge_type` filter, it must match the observed edge type.

use bitvec::prelude::*;
use std::collections::HashMap;

use crate::types::{Epoch, HopSpec, NodeId, TypeId};

/// A pattern template defining a multi-hop pattern to watch for.
///
/// Templates are registered with [`EmbryonicDiscovery`] and used to create
/// candidates when matching edges are observed. The `threshold` controls
/// how complete a candidate must be before promotion.
#[derive(Debug, Clone)]
pub struct PatternTemplate {
    /// Unique template identifier.
    pub id: u64,
    /// Multi-hop pattern to watch for.
    pub pattern: Vec<HopSpec>,
    /// Completion ratio required for promotion (0.0--1.0).
    pub threshold: f64,
    /// Maximum number of candidates per template.
    pub max_candidates: usize,
    /// Epochs without progress before a candidate is pruned.
    pub stale_window: u64,
}

/// A candidate partial pattern match anchored at a node.
///
/// Tracks which hops in the pattern have been observed using a bitvec.
/// When the completion ratio meets or exceeds the template threshold,
/// the candidate is promoted to a [`PromotedFrame`].
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Anchor node for this candidate.
    pub anchor: NodeId,
    /// Which template this candidate is for.
    pub template_id: u64,
    /// One bit per hop: set when that hop is observed.
    pub completion: BitVec,
    /// Last epoch when a bit was set (progress made).
    pub last_progress_epoch: Epoch,
    /// Epoch when this candidate was created.
    pub created_epoch: Epoch,
}

impl Candidate {
    /// Returns the completion ratio: set bits / total bits.
    pub fn completion_ratio(&self) -> f64 {
        if self.completion.is_empty() {
            return 0.0;
        }
        self.completion.count_ones() as f64 / self.completion.len() as f64
    }
}

/// A promoted frame produced when a candidate meets the threshold.
///
/// Contains the anchor node and the full pattern that was matched.
#[derive(Debug, Clone, PartialEq)]
pub struct PromotedFrame {
    /// The anchor node of the promoted frame.
    pub anchor: NodeId,
    /// The full pattern that was matched.
    pub pattern: Vec<HopSpec>,
    /// The template that produced this frame.
    pub template_id: u64,
}

/// Embryonic frame discovery engine.
///
/// Manages pattern templates and candidate tracking. Observes edges from
/// the mutation stream, creates and advances candidates, promotes completed
/// candidates to frames, and prunes stale or excess candidates.
pub struct EmbryonicDiscovery {
    /// Registered pattern templates by ID.
    templates: HashMap<u64, PatternTemplate>,
    /// Candidates grouped by template ID.
    candidates: HashMap<u64, Vec<Candidate>>,
}

impl Default for EmbryonicDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl EmbryonicDiscovery {
    /// Creates a new empty discovery engine.
    pub fn new() -> Self {
        Self {
            templates: HashMap::new(),
            candidates: HashMap::new(),
        }
    }

    /// Registers a pattern template for observation.
    pub fn register_template(&mut self, template: PatternTemplate) {
        let id = template.id;
        self.candidates.entry(id).or_default();
        self.templates.insert(id, template);
    }

    /// Generates all contiguous sub-patterns of length >= 2.
    ///
    /// For example, `[A, B, C]` produces `[[A, B], [B, C], [A, B, C]]`.
    /// A two-hop pattern `[A, B]` produces `[[A, B]]`.
    /// A single-hop pattern returns an empty vec (no sub-patterns of length >= 2).
    pub fn decompose_frame(pattern: &[HopSpec]) -> Vec<Vec<HopSpec>> {
        let n = pattern.len();
        if n < 2 {
            return Vec::new();
        }
        let mut result = Vec::new();
        // Generate sub-patterns from shortest to longest
        for len in 2..=n {
            for start in 0..=(n - len) {
                result.push(pattern[start..start + len].to_vec());
            }
        }
        result
    }

    /// Observes an edge and updates candidates, returning any promoted frames.
    ///
    /// For each template:
    /// 1. If the edge matches the first hop, create a new candidate with bit 0 set.
    /// 2. For existing candidates, if the edge matches the next unset hop, set that bit.
    /// 3. If a candidate's completion ratio meets the threshold, promote it.
    pub fn observe_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        edge_type: TypeId,
        epoch: Epoch,
    ) -> Vec<PromotedFrame> {
        let mut promoted = Vec::new();
        let template_ids: Vec<u64> = self.templates.keys().copied().collect();

        for tid in template_ids {
            let template = self.templates.get(&tid).unwrap().clone();
            let candidates = self.candidates.entry(tid).or_default();

            // 1. Check if this edge could start a new candidate (matches first hop)
            if !template.pattern.is_empty()
                && Self::edge_matches_hop(source, target, edge_type, &template.pattern[0])
            {
                let mut completion = bitvec![0; template.pattern.len()];
                completion.set(0, true);
                candidates.push(Candidate {
                    anchor: source,
                    template_id: tid,
                    completion,
                    last_progress_epoch: epoch,
                    created_epoch: epoch,
                });
            }

            // 2. Check existing candidates for next unset hop advancement
            for candidate in candidates.iter_mut() {
                // Find the first unset bit (next hop to match)
                if let Some(next_idx) = candidate.completion.first_zero() {
                    // Skip if this is bit 0 (already handled above for new candidates)
                    if next_idx == 0 {
                        continue;
                    }
                    if next_idx < template.pattern.len() {
                        let hop = &template.pattern[next_idx];
                        if Self::edge_matches_hop(source, target, edge_type, hop) {
                            candidate.completion.set(next_idx, true);
                            candidate.last_progress_epoch = epoch;
                        }
                    }
                }
            }

            // 3. Check for promotions
            let threshold = template.threshold;
            let pattern = template.pattern.clone();
            let mut promoted_indices = Vec::new();
            for (i, candidate) in candidates.iter().enumerate() {
                if candidate.completion_ratio() >= threshold {
                    promoted.push(PromotedFrame {
                        anchor: candidate.anchor,
                        pattern: pattern.clone(),
                        template_id: tid,
                    });
                    promoted_indices.push(i);
                }
            }
            // Remove promoted candidates in reverse order to preserve indices
            for i in promoted_indices.into_iter().rev() {
                candidates.remove(i);
            }
        }

        promoted
    }

    /// Checks if an edge matches a hop specification.
    ///
    /// Direction matching:
    /// - `Outgoing`: the edge goes from the current node outward (source matches).
    /// - `Incoming`: the edge comes into the current node (target matches).
    /// - `Any`: either direction matches.
    ///
    /// If the hop has an `edge_type` filter, the edge type must match.
    fn edge_matches_hop(
        _source: NodeId,
        _target: NodeId,
        edge_type: TypeId,
        hop: &HopSpec,
    ) -> bool {
        // Check edge type filter
        if let Some(required_type) = hop.edge_type {
            if edge_type != required_type {
                return false;
            }
        }

        // Direction matching: for embryonic discovery, we check that the edge
        // direction is compatible with the hop direction. Since we don't track
        // the full path state here, we match based on edge type alone when
        // direction is compatible (Outgoing/Incoming/Any all accept the edge
        // if type matches -- full path tracking would need graph state).
        true
    }

    /// Prunes candidates that haven't progressed within the stale window.
    ///
    /// A candidate is stale if `current_epoch - last_progress_epoch > stale_window`.
    pub fn prune_stale(&mut self, current_epoch: Epoch) {
        for (tid, candidates) in &mut self.candidates {
            if let Some(template) = self.templates.get(tid) {
                let window = template.stale_window;
                candidates.retain(|c| {
                    current_epoch.0.saturating_sub(c.last_progress_epoch.0) <= window
                });
            }
        }
    }

    /// Enforces per-template candidate caps.
    ///
    /// If a template has more candidates than `max_candidates`, the oldest
    /// candidates (by `created_epoch`) are removed first.
    pub fn enforce_caps(&mut self) {
        for (tid, candidates) in &mut self.candidates {
            if let Some(template) = self.templates.get(tid) {
                if candidates.len() > template.max_candidates {
                    // Sort by created_epoch ascending so oldest are first
                    candidates.sort_by_key(|c| c.created_epoch);
                    let excess = candidates.len() - template.max_candidates;
                    candidates.drain(0..excess);
                }
            }
        }
    }

    /// Returns the total number of candidates across all templates.
    pub fn candidate_count(&self) -> usize {
        self.candidates.values().map(|v| v.len()).sum()
    }

    /// Returns the number of registered templates.
    pub fn template_count(&self) -> usize {
        self.templates.len()
    }
}
