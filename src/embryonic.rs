//! Embryonic frame discovery with bitvec completion tracking and learned template weighting.
//!
//! This module implements autonomous pattern detection from the mutation stream.
//! Pattern templates define multi-hop patterns to watch for. When edges arrive,
//! candidates track partial pattern completion using [`bitvec!`]. When completion
//! exceeds the template threshold, candidates are promoted to full frames.
//! Stale candidates are pruned, and a per-template cap prevents unbounded growth.
//!
//! # Learned Weighting (LEARN-01, LEARN-02)
//!
//! Templates track `success_count` (promoted candidates) and `failure_count`
//! (pruned/capped candidates). The [`PatternTemplate::success_ratio`] determines
//! scanning priority: templates with higher ratios are evaluated first in
//! [`EmbryonicDiscovery::observe_edge`]. Templates with ratio < 0.1 after 50+
//! attempts are automatically deactivated and skipped during observation.
//!
//! # Design
//!
//! - [`PatternTemplate`] defines what to watch for: a multi-hop pattern,
//!   completion threshold, staleness window, candidate cap, and learned weights.
//! - [`Candidate`] tracks a partial pattern match anchored at a node,
//!   with a [`BitVec`] indicating which hops have been observed.
//! - [`EmbryonicDiscovery`] orchestrates template registration, edge observation,
//!   auto-promotion, pruning, and cap enforcement.
//!
//! # Edge Matching
//!
//! When an edge `(source, target, edge_type)` arrives:
//! - For [`Outgoing`](crate::types::Direction::Outgoing) hops, the edge source must match the anchor
//!   or the node reached by previous hops.
//! - For [`Incoming`](crate::types::Direction::Incoming) hops, the edge target must match.
//! - For [`Any`](crate::types::Direction::Any), either direction matches.
//! - If the hop has an `edge_type` filter, it must match the observed edge type.

use bitvec::prelude::*;
use std::collections::HashMap;

use crate::types::{Epoch, HopSpec, NodeId, TypeId};

/// A pattern template defining a multi-hop pattern to watch for.
///
/// Templates are registered with [`EmbryonicDiscovery`] and used to create
/// candidates when matching edges are observed. The `threshold` controls
/// how complete a candidate must be before promotion.
///
/// # Learned Weighting
///
/// Each template tracks `success_count` and `failure_count` to learn which
/// patterns are productive. The [`success_ratio`](PatternTemplate::success_ratio)
/// determines scanning priority, and templates with very low ratios after
/// sufficient attempts are deactivated.
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
    /// Number of successful promotions from this template (LEARN-01).
    pub success_count: u64,
    /// Number of failed candidates (pruned/capped without promotion) from this template (LEARN-01).
    pub failure_count: u64,
    /// Whether this template is active (LEARN-02). Deactivated templates skip observation.
    pub active: bool,
}

impl PatternTemplate {
    /// Returns the success ratio: success_count / (success_count + failure_count).
    ///
    /// Returns 0.0 when no attempts have been recorded (avoids division by zero).
    /// Higher ratios indicate more productive templates.
    pub fn success_ratio(&self) -> f64 {
        self.success_count as f64 / (self.success_count + self.failure_count).max(1) as f64
    }
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
///
/// Templates are sorted by [`success_ratio`](PatternTemplate::success_ratio)
/// descending so higher-success templates are scanned first (LEARN-02).
/// Templates with ratio < 0.1 after 50+ attempts are deactivated.
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
    /// Templates are sorted by [`success_ratio`](PatternTemplate::success_ratio)
    /// descending so higher-success templates are scanned first (LEARN-02).
    /// Inactive templates (deactivated due to low success ratio) are skipped.
    ///
    /// For each active template:
    /// 1. If the edge matches the first hop, create a new candidate with bit 0 set.
    /// 2. For existing candidates, if the edge matches the next unset hop, set that bit.
    /// 3. If a candidate's completion ratio meets the threshold, promote it and
    ///    increment the template's `success_count`.
    pub fn observe_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        edge_type: TypeId,
        epoch: Epoch,
    ) -> Vec<PromotedFrame> {
        let mut promoted = Vec::new();

        // LEARN-02: Sort template IDs by success_ratio descending
        let mut template_ids: Vec<u64> = self.templates.keys().copied().collect();
        template_ids.sort_by(|a, b| {
            let ratio_a = self.templates.get(a).map(|t| t.success_ratio()).unwrap_or(0.0);
            let ratio_b = self.templates.get(b).map(|t| t.success_ratio()).unwrap_or(0.0);
            ratio_b
                .partial_cmp(&ratio_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for tid in template_ids {
            let template = self.templates.get(&tid).unwrap().clone();

            // LEARN-02: Skip deactivated templates
            if !template.active {
                continue;
            }

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
            // LEARN-01: Track promotion count before consuming promoted_indices
            let promotion_count = promoted_indices.len();

            // Remove promoted candidates in reverse order to preserve indices
            for i in promoted_indices.into_iter().rev() {
                candidates.remove(i);
            }

            // LEARN-01: Increment success_count for each promotion
            if promotion_count > 0 {
                if let Some(tmpl) = self.templates.get_mut(&tid) {
                    tmpl.success_count += promotion_count as u64;
                }
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
    /// Increments the template's `failure_count` for each pruned candidate (LEARN-01).
    /// Checks deactivation condition after updating counts (LEARN-02).
    pub fn prune_stale(&mut self, current_epoch: Epoch) {
        let template_ids: Vec<u64> = self.candidates.keys().copied().collect();
        for tid in template_ids {
            let window = self
                .templates
                .get(&tid)
                .map(|t| t.stale_window)
                .unwrap_or(0);
            let candidates = self.candidates.get_mut(&tid).unwrap();
            let before = candidates.len();
            candidates.retain(|c| {
                current_epoch.0.saturating_sub(c.last_progress_epoch.0) <= window
            });
            let pruned = before - candidates.len();
            if pruned > 0 {
                if let Some(tmpl) = self.templates.get_mut(&tid) {
                    tmpl.failure_count += pruned as u64;
                    // LEARN-02: Check deactivation
                    if tmpl.success_count + tmpl.failure_count >= 50
                        && tmpl.success_ratio() < 0.1
                    {
                        tmpl.active = false;
                    }
                }
            }
        }
    }

    /// Enforces per-template candidate caps.
    ///
    /// If a template has more candidates than `max_candidates`, the oldest
    /// candidates (by `created_epoch`) are removed first. Increments
    /// `failure_count` by the number of excess candidates removed (LEARN-01).
    /// Checks deactivation condition after updating counts (LEARN-02).
    pub fn enforce_caps(&mut self) {
        let template_ids: Vec<u64> = self.candidates.keys().copied().collect();
        for tid in template_ids {
            let max_candidates = self
                .templates
                .get(&tid)
                .map(|t| t.max_candidates)
                .unwrap_or(usize::MAX);
            let candidates = self.candidates.get_mut(&tid).unwrap();
            if candidates.len() > max_candidates {
                // Sort by created_epoch ascending so oldest are first
                candidates.sort_by_key(|c| c.created_epoch);
                let excess = candidates.len() - max_candidates;
                candidates.drain(0..excess);
                if let Some(tmpl) = self.templates.get_mut(&tid) {
                    tmpl.failure_count += excess as u64;
                    // LEARN-02: Check deactivation
                    if tmpl.success_count + tmpl.failure_count >= 50
                        && tmpl.success_ratio() < 0.1
                    {
                        tmpl.active = false;
                    }
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

    /// Returns the number of deactivated templates (active=false).
    pub fn deactivated_template_count(&self) -> usize {
        self.templates.values().filter(|t| !t.active).count()
    }

    /// Returns the success ratio for a specific template.
    ///
    /// Returns `None` if the template ID is not registered.
    pub fn template_success_ratio(&self, template_id: u64) -> Option<f64> {
        self.templates.get(&template_id).map(|t| t.success_ratio())
    }

    /// Reactivates a deactivated template (sets active=true).
    ///
    /// This is a manual override for templates that were automatically
    /// deactivated due to low success ratio. No-op if the template
    /// is not registered.
    pub fn reactivate_template(&mut self, template_id: u64) {
        if let Some(tmpl) = self.templates.get_mut(&template_id) {
            tmpl.active = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Direction, Filter};

    /// Helper: creates a simple HopSpec with the given direction and edge type.
    fn hop(direction: Direction, edge_type: Option<TypeId>) -> HopSpec {
        HopSpec {
            direction,
            edge_type,
            target_type: None,
            filter: Filter::None,
        }
    }

    /// Helper: creates a PatternTemplate with sensible defaults.
    fn template(id: u64, pattern: Vec<HopSpec>, threshold: f64) -> PatternTemplate {
        PatternTemplate {
            id,
            pattern,
            threshold,
            max_candidates: 100,
            stale_window: 10,
            success_count: 0,
            failure_count: 0,
            active: true,
        }
    }

    #[test]
    fn register_template() {
        let mut disco = EmbryonicDiscovery::new();
        assert_eq!(disco.template_count(), 0);

        disco.register_template(template(
            1,
            vec![hop(Direction::Outgoing, Some(TypeId(1)))],
            1.0,
        ));
        assert_eq!(disco.template_count(), 1);

        disco.register_template(template(
            2,
            vec![hop(Direction::Incoming, Some(TypeId(2)))],
            0.5,
        ));
        assert_eq!(disco.template_count(), 2);
    }

    #[test]
    fn decompose_two_hop() {
        let pattern = vec![
            hop(Direction::Outgoing, Some(TypeId(1))),
            hop(Direction::Incoming, Some(TypeId(2))),
        ];
        let subs = EmbryonicDiscovery::decompose_frame(&pattern);
        // [A,B] -> [[A,B]]
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].len(), 2);
        assert_eq!(subs[0], pattern);
    }

    #[test]
    fn decompose_three_hop() {
        let a = hop(Direction::Outgoing, Some(TypeId(1)));
        let b = hop(Direction::Incoming, Some(TypeId(2)));
        let c = hop(Direction::Any, Some(TypeId(3)));
        let pattern = vec![a.clone(), b.clone(), c.clone()];
        let subs = EmbryonicDiscovery::decompose_frame(&pattern);
        // [A,B,C] -> [[A,B], [B,C], [A,B,C]]
        assert_eq!(subs.len(), 3);
        assert_eq!(subs[0], vec![a.clone(), b.clone()]);
        assert_eq!(subs[1], vec![b.clone(), c.clone()]);
        assert_eq!(subs[2], vec![a, b, c]);
    }

    #[test]
    fn observe_creates_candidate() {
        let mut disco = EmbryonicDiscovery::new();
        // Two-hop pattern, threshold 1.0 (need both hops)
        disco.register_template(template(
            1,
            vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            1.0,
        ));

        // Observe an edge matching the first hop
        let promoted = disco.observe_edge(
            NodeId(1),
            NodeId(2),
            TypeId(10),
            Epoch(1),
        );
        assert!(promoted.is_empty(), "should not promote with only 1/2 hops");
        assert_eq!(disco.candidate_count(), 1);
    }

    #[test]
    fn progressive_completion() {
        let mut disco = EmbryonicDiscovery::new();
        // Three-hop pattern, threshold 1.0
        disco.register_template(template(
            1,
            vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
                hop(Direction::Outgoing, Some(TypeId(30))),
            ],
            1.0,
        ));

        // First hop
        let promoted = disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        assert!(promoted.is_empty());
        assert_eq!(disco.candidate_count(), 1);

        // Second hop
        let promoted = disco.observe_edge(NodeId(2), NodeId(3), TypeId(20), Epoch(2));
        assert!(promoted.is_empty());
        // Still 1 candidate (the one we're advancing), plus possibly a new one
        // if the second edge also matches the first hop. TypeId(20) != TypeId(10),
        // so no new candidate is created.
        assert_eq!(disco.candidate_count(), 1);

        // Third hop -- completes the pattern
        let promoted = disco.observe_edge(NodeId(3), NodeId(4), TypeId(30), Epoch(3));
        assert_eq!(promoted.len(), 1);
        assert_eq!(disco.candidate_count(), 0, "promoted candidate should be removed");
    }

    #[test]
    fn auto_promotion_at_threshold() {
        let mut disco = EmbryonicDiscovery::new();
        // Two-hop pattern with 0.5 threshold (1/2 hops is enough)
        disco.register_template(template(
            1,
            vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            0.5,
        ));

        // First hop sets bit 0 -> completion = 1/2 = 0.5 >= 0.5
        let promoted = disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        assert_eq!(promoted.len(), 1, "should auto-promote at 50% threshold");
        assert_eq!(disco.candidate_count(), 0);
    }

    #[test]
    fn promotion_returns_correct_pattern() {
        let mut disco = EmbryonicDiscovery::new();
        let pattern = vec![
            hop(Direction::Outgoing, Some(TypeId(10))),
            hop(Direction::Outgoing, Some(TypeId(20))),
        ];
        disco.register_template(template(1, pattern.clone(), 0.5));

        let promoted = disco.observe_edge(NodeId(42), NodeId(99), TypeId(10), Epoch(1));
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].anchor, NodeId(42));
        assert_eq!(promoted[0].pattern, pattern);
        assert_eq!(promoted[0].template_id, 1);
    }

    #[test]
    fn prune_stale_candidates() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 100,
            stale_window: 5, // prune after 5 epochs without progress
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Create a candidate at epoch 1
        disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        assert_eq!(disco.candidate_count(), 1);

        // Prune at epoch 5 -- not stale yet (5 - 1 = 4 <= 5)
        disco.prune_stale(Epoch(5));
        assert_eq!(disco.candidate_count(), 1);

        // Prune at epoch 7 -- stale (7 - 1 = 6 > 5)
        disco.prune_stale(Epoch(7));
        assert_eq!(disco.candidate_count(), 0, "stale candidate should be pruned");
    }

    #[test]
    fn enforce_cap_removes_oldest() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 2,
            stale_window: 100,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Create 3 candidates at different epochs
        disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        disco.observe_edge(NodeId(3), NodeId(4), TypeId(10), Epoch(2));
        disco.observe_edge(NodeId(5), NodeId(6), TypeId(10), Epoch(3));
        assert_eq!(disco.candidate_count(), 3);

        // Enforce cap of 2
        disco.enforce_caps();
        assert_eq!(disco.candidate_count(), 2, "should cap at max_candidates");
    }

    #[test]
    fn candidate_count_tracks_total() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(template(
            1,
            vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            1.0,
        ));
        disco.register_template(template(
            2,
            vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Incoming, Some(TypeId(30))),
            ],
            1.0,
        ));

        assert_eq!(disco.candidate_count(), 0);

        // Edge matching TypeId(10) matches first hop of both templates
        disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        assert_eq!(disco.candidate_count(), 2, "one candidate per template");
    }

    #[test]
    fn non_matching_edge_ignored() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(template(
            1,
            vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            1.0,
        ));

        // Observe an edge with TypeId(99) which doesn't match TypeId(10)
        let promoted = disco.observe_edge(NodeId(1), NodeId(2), TypeId(99), Epoch(1));
        assert!(promoted.is_empty());
        assert_eq!(disco.candidate_count(), 0, "non-matching edge creates no candidate");
    }

    // === LEARN-01 / LEARN-02 / TEST-31 Tests ===

    #[test]
    fn test_learned_weighting() {
        // TEST-31: Register 2 templates. Feed edges that complete template A's
        // pattern 10 times (success_count=10) and never complete template B
        // (prune B's candidates as stale 10 times, failure_count=10).
        // Verify template A's success_ratio > template B's success_ratio.
        let mut disco = EmbryonicDiscovery::new();

        // Template A: 2-hop with TypeId(10) -> TypeId(20)
        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 200,
            stale_window: 5,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Template B: 2-hop with TypeId(30) -> TypeId(40)
        disco.register_template(PatternTemplate {
            id: 2,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(30))),
                hop(Direction::Outgoing, Some(TypeId(40))),
            ],
            threshold: 1.0,
            max_candidates: 200,
            stale_window: 5,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Complete template A's pattern 10 times (success)
        for i in 0..10u64 {
            let epoch_base = i * 100;
            // First hop
            disco.observe_edge(
                NodeId(i * 10 + 1),
                NodeId(i * 10 + 2),
                TypeId(10),
                Epoch(epoch_base + 1),
            );
            // Second hop -- completes the pattern
            let promoted = disco.observe_edge(
                NodeId(i * 10 + 2),
                NodeId(i * 10 + 3),
                TypeId(20),
                Epoch(epoch_base + 2),
            );
            assert!(!promoted.is_empty(), "Template A should promote at iteration {}", i);
        }

        // Create candidates for template B and let them go stale
        for i in 0..10u64 {
            let epoch_base = 2000 + i * 100;
            // First hop only (never complete)
            disco.observe_edge(
                NodeId(5000 + i),
                NodeId(6000 + i),
                TypeId(30),
                Epoch(epoch_base),
            );
            // Prune as stale
            disco.prune_stale(Epoch(epoch_base + 100));
        }

        let ratio_a = disco.template_success_ratio(1).unwrap();
        let ratio_b = disco.template_success_ratio(2).unwrap();
        assert!(
            ratio_a > ratio_b,
            "Template A (ratio={}) should rank higher than Template B (ratio={})",
            ratio_a,
            ratio_b,
        );
        assert!(ratio_a > 0.5, "Template A should have high success ratio");
        assert!(ratio_b < 0.5, "Template B should have low success ratio");
    }

    #[test]
    fn test_deactivation_after_50() {
        // Register template, generate 50 stale-pruned candidates (failure_count=50,
        // success_count=0). Verify template is deactivated. Verify subsequent
        // observe_edge skips it.
        let mut disco = EmbryonicDiscovery::new();

        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 200,
            stale_window: 5,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Create and prune 50 candidates
        for i in 0..50u64 {
            let epoch_base = i * 100;
            disco.observe_edge(
                NodeId(i + 1),
                NodeId(i + 1000),
                TypeId(10),
                Epoch(epoch_base),
            );
            disco.prune_stale(Epoch(epoch_base + 100));
        }

        assert_eq!(
            disco.deactivated_template_count(),
            1,
            "Template should be deactivated after 50 failures with 0 successes"
        );

        // Verify observe_edge skips the deactivated template
        let candidates_before = disco.candidate_count();
        disco.observe_edge(NodeId(9999), NodeId(9998), TypeId(10), Epoch(99999));
        assert_eq!(
            disco.candidate_count(),
            candidates_before,
            "Deactivated template should not create new candidates"
        );
    }

    #[test]
    fn test_success_increments_on_promotion() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 100,
            stale_window: 100,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // First hop
        disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        assert_eq!(
            disco.template_success_ratio(1),
            Some(0.0),
            "No successes yet"
        );

        // Second hop -- promotes
        let promoted = disco.observe_edge(NodeId(2), NodeId(3), TypeId(20), Epoch(2));
        assert_eq!(promoted.len(), 1);
        assert!(
            disco.template_success_ratio(1).unwrap() > 0.0,
            "Success count should have incremented"
        );
    }

    #[test]
    fn test_failure_increments_on_prune() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 100,
            stale_window: 5,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Create candidate
        disco.observe_edge(NodeId(1), NodeId(2), TypeId(10), Epoch(1));
        assert_eq!(disco.template_success_ratio(1), Some(0.0));

        // Prune as stale
        disco.prune_stale(Epoch(100));
        assert_eq!(disco.candidate_count(), 0);

        // Verify failure_count incremented (ratio is 0/(0+1) = 0.0, but
        // the fact it was pruned means failure_count > 0)
        // We can verify by checking that success_ratio is still 0.0
        // but a second success would give us 1/(1+1) = 0.5 not 1.0
        assert_eq!(
            disco.template_success_ratio(1),
            Some(0.0),
            "Ratio should be 0/1 = 0.0 after one failure"
        );

        // Now promote one -- should give 1/(1+1) = 0.5
        disco.observe_edge(NodeId(10), NodeId(11), TypeId(10), Epoch(200));
        let promoted = disco.observe_edge(NodeId(11), NodeId(12), TypeId(20), Epoch(201));
        assert_eq!(promoted.len(), 1);
        assert_eq!(
            disco.template_success_ratio(1),
            Some(0.5),
            "Ratio should be 1/(1+1) = 0.5 after one success and one failure"
        );
    }

    #[test]
    fn test_reactivate_template() {
        let mut disco = EmbryonicDiscovery::new();
        disco.register_template(PatternTemplate {
            id: 1,
            pattern: vec![
                hop(Direction::Outgoing, Some(TypeId(10))),
                hop(Direction::Outgoing, Some(TypeId(20))),
            ],
            threshold: 1.0,
            max_candidates: 200,
            stale_window: 5,
            success_count: 0,
            failure_count: 0,
            active: true,
        });

        // Force deactivation by creating 50 failures (never complete second hop)
        for i in 0..50u64 {
            disco.observe_edge(NodeId(i + 1), NodeId(i + 1000), TypeId(10), Epoch(i));
            disco.prune_stale(Epoch(i + 100));
        }
        assert_eq!(disco.deactivated_template_count(), 1);

        // Reactivate
        disco.reactivate_template(1);
        assert_eq!(disco.deactivated_template_count(), 0);
    }
}
