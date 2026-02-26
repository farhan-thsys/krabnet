//! Adaptive frame tiering with priority scoring.
//!
//! Scores frames by their access pattern (query frequency, mutation rate,
//! and recency) to classify them into temperature tiers: [`FrameTier::Hot`],
//! [`FrameTier::Warm`], or [`FrameTier::Cold`]. The tier determines
//! materialization and interpretation priority.
//!
//! # Scoring Formula
//!
//! The priority score is a weighted combination of three components:
//!
//! - **Query component:** `ln(1 + query_count) / 10`, capped at 1.0.
//!   Captures how frequently the frame is read.
//! - **Mutation component:** `ln(1 + mutation_count) / 10`, capped at 1.0.
//!   Captures how frequently the frame is modified.
//! - **Recency component:** `exp(-epochs_since_last / half_life)`.
//!   Exponential decay ensuring recently-active frames score higher.
//!
//! The raw weighted sum is clamped to [0.0, 1.0].
//!
//! # Tier Thresholds
//!
//! - **Hot:** score > 0.7
//! - **Warm:** 0.2 <= score <= 0.7
//! - **Cold:** score < 0.2
//!
//! # Usage
//!
//! ```
//! use krabnet::tiering::{priority_score, recommend_tier, TierConfig};
//! use krabnet::FrameTier;
//!
//! let config = TierConfig::default();
//! let score = priority_score(1000, 500, 5, &config);
//! let tier = recommend_tier(score);
//! assert_eq!(tier, FrameTier::Hot);
//! ```

use crate::count_min_sketch::CountMinSketch;
use crate::types::FrameTier;

/// Configuration for the priority scoring formula.
///
/// Controls the relative importance of query frequency, mutation rate,
/// and recency in the overall priority score. The three weights should
/// ideally sum to 1.0 for a normalized output, though this is not enforced.
///
/// The `recency_half_life` controls the exponential decay rate: after
/// `half_life` epochs of inactivity, the recency component drops to ~0.37.
#[derive(Debug, Clone, PartialEq)]
pub struct TierConfig {
    /// Weight for the query frequency component (default: 0.4).
    pub query_weight: f64,
    /// Weight for the mutation rate component (default: 0.3).
    pub mutation_weight: f64,
    /// Weight for the recency decay component (default: 0.3).
    pub recency_weight: f64,
    /// Half-life in epochs for the recency exponential decay (default: 100.0).
    pub recency_half_life: f64,
}

impl Default for TierConfig {
    /// Returns a `TierConfig` with default weights:
    /// - query_weight: 0.4
    /// - mutation_weight: 0.3
    /// - recency_weight: 0.3
    /// - recency_half_life: 100.0
    fn default() -> Self {
        Self {
            query_weight: 0.4,
            mutation_weight: 0.3,
            recency_weight: 0.3,
            recency_half_life: 100.0,
        }
    }
}

/// Computes a normalized priority score for a frame based on access patterns.
///
/// The score combines query frequency, mutation rate, and recency into a
/// single `f64` value in the range [0.0, 1.0]. Higher scores indicate
/// higher priority frames that should be kept materialized and interpreted
/// more frequently.
///
/// # Arguments
///
/// * `query_count` - Number of times the frame has been queried.
/// * `mutation_count` - Number of deltas applied to the frame.
/// * `epochs_since_last` - Number of epochs since the frame was last modified.
/// * `config` - Weights and decay parameters for the scoring formula.
///
/// # Formula
///
/// ```text
/// query_component    = min(ln(1 + query_count) / 10, 1.0)
/// mutation_component = min(ln(1 + mutation_count) / 10, 1.0)
/// recency_component  = exp(-epochs_since_last / half_life)
/// raw = query_component * query_weight
///     + mutation_component * mutation_weight
///     + recency_component * recency_weight
/// score = clamp(raw, 0.0, 1.0)
/// ```
///
/// # Examples
///
/// ```
/// use krabnet::tiering::{priority_score, TierConfig};
///
/// let config = TierConfig::default();
///
/// // High activity, recent → high score
/// let hot = priority_score(10000, 5000, 1, &config);
/// assert!(hot > 0.7);
///
/// // No activity, old → low score
/// let cold = priority_score(0, 0, 10000, &config);
/// assert!(cold < 0.2);
/// ```
pub fn priority_score(
    query_count: u64,
    mutation_count: u64,
    epochs_since_last: u64,
    config: &TierConfig,
) -> f64 {
    let query_component = ((query_count as f64).ln_1p() / 10.0).min(1.0);
    let mutation_component = ((mutation_count as f64).ln_1p() / 10.0).min(1.0);
    let recency_component = (-(epochs_since_last as f64) / config.recency_half_life).exp();

    let raw = query_component * config.query_weight
        + mutation_component * config.mutation_weight
        + recency_component * config.recency_weight;

    raw.clamp(0.0, 1.0)
}

/// Recommends a [`FrameTier`] based on the priority score.
///
/// - **Hot:** score > 0.7
/// - **Cold:** score < 0.2
/// - **Warm:** everything else (0.2 <= score <= 0.7)
///
/// # Examples
///
/// ```
/// use krabnet::tiering::recommend_tier;
/// use krabnet::FrameTier;
///
/// assert_eq!(recommend_tier(0.9), FrameTier::Hot);
/// assert_eq!(recommend_tier(0.5), FrameTier::Warm);
/// assert_eq!(recommend_tier(0.1), FrameTier::Cold);
/// ```
pub fn recommend_tier(score: f64) -> FrameTier {
    if score > 0.7 {
        FrameTier::Hot
    } else if score < 0.2 {
        FrameTier::Cold
    } else {
        FrameTier::Warm
    }
}

/// Hysteresis state for preventing tier thrashing (HYST-01).
///
/// Tracks consecutive windows where a frame's score is above the Hot
/// threshold or below the Cold threshold. A tier change is only allowed
/// after `required_consecutive` consecutive windows in the new direction.
/// Oscillating scores reset both counters, keeping the frame in Warm.
///
/// # Thresholds
///
/// - **Hot threshold:** score > 0.7
/// - **Cold threshold:** score < 0.2
/// - **Neutral zone:** 0.2 <= score <= 0.7 (resets both counters)
///
/// # Usage
///
/// ```
/// use krabnet::tiering::HysteresisState;
/// use krabnet::FrameTier;
///
/// let mut hyst = HysteresisState::new(5);
///
/// // Score oscillates -- tier stays Warm
/// let tier1 = hyst.update(0.8, FrameTier::Warm);
/// let tier2 = hyst.update(0.1, FrameTier::Warm);
/// assert_eq!(tier2, FrameTier::Warm); // not enough consecutive
/// ```
#[derive(Debug, Clone)]
pub struct HysteresisState {
    /// How many consecutive windows the score was below the Cold threshold (< 0.2).
    consecutive_below_cold: u32,
    /// How many consecutive windows the score was above the Hot threshold (> 0.7).
    consecutive_above_hot: u32,
    /// Number of consecutive windows required before allowing a tier change (HYST-02/HYST-03).
    required_consecutive: u32,
}

impl HysteresisState {
    /// Creates a new hysteresis state with the given consecutive window requirement.
    ///
    /// # Arguments
    ///
    /// * `required_consecutive` - Number of consecutive windows a score must
    ///   remain above/below threshold before allowing tier change. Default: 5.
    pub fn new(required_consecutive: u32) -> Self {
        Self {
            consecutive_below_cold: 0,
            consecutive_above_hot: 0,
            required_consecutive,
        }
    }

    /// Updates the hysteresis state with a new score and returns the recommended tier.
    ///
    /// - **score > 0.7:** Increments `consecutive_above_hot`, resets `consecutive_below_cold`.
    ///   If `consecutive_above_hot >= required_consecutive` AND `current_tier != Hot`,
    ///   returns `Hot`. Otherwise returns `current_tier`.
    /// - **score < 0.2:** Increments `consecutive_below_cold`, resets `consecutive_above_hot`.
    ///   If `consecutive_below_cold >= required_consecutive` AND `current_tier != Cold`,
    ///   returns `Cold`. Otherwise returns `current_tier`.
    /// - **0.2 <= score <= 0.7:** Resets BOTH counters. Returns `Warm` (the safe
    ///   middle state -- oscillating scores stay Warm per HYST-01).
    pub fn update(&mut self, score: f64, current_tier: FrameTier) -> FrameTier {
        if score > 0.7 {
            self.consecutive_above_hot += 1;
            self.consecutive_below_cold = 0;

            if self.consecutive_above_hot >= self.required_consecutive
                && current_tier != FrameTier::Hot
            {
                return FrameTier::Hot;
            }
            current_tier
        } else if score < 0.2 {
            self.consecutive_below_cold += 1;
            self.consecutive_above_hot = 0;

            if self.consecutive_below_cold >= self.required_consecutive
                && current_tier != FrameTier::Cold
            {
                return FrameTier::Cold;
            }
            current_tier
        } else {
            // Neutral zone: reset both counters, return Warm.
            self.consecutive_above_hot = 0;
            self.consecutive_below_cold = 0;
            FrameTier::Warm
        }
    }
}

/// Tracks frame activity using Count-Min Sketches for probabilistic frequency counting.
///
/// Replaces per-frame query/mutation counters (CMS-02) with two space-efficient
/// Count-Min Sketches: one for query frequency and one for mutation frequency.
/// This is the PRIMARY scoring interface for the frame prioritizer.
///
/// The `priority_score` method delegates to the free function [`priority_score()`],
/// using CMS-estimated counts instead of per-frame counters.
///
/// # Example
///
/// ```
/// use krabnet::tiering::{FrameActivityTracker, TierConfig};
///
/// let mut tracker = FrameActivityTracker::new();
/// for _ in 0..100 {
///     tracker.record_query(42);
///     tracker.record_mutation(42);
/// }
/// let score = tracker.priority_score(42, 1, &TierConfig::default());
/// assert!(score > 0.5);
/// ```
#[derive(Debug, Clone)]
pub struct FrameActivityTracker {
    /// Count-Min Sketch for query frequency estimation.
    query_sketch: CountMinSketch,
    /// Count-Min Sketch for mutation frequency estimation.
    mutation_sketch: CountMinSketch,
}

impl FrameActivityTracker {
    /// Creates a new tracker with default 1024x4 sketches.
    pub fn new() -> Self {
        Self {
            query_sketch: CountMinSketch::default(),
            mutation_sketch: CountMinSketch::default(),
        }
    }

    /// Records a query event for the given frame.
    pub fn record_query(&mut self, frame_id: u64) {
        self.query_sketch.increment(frame_id);
    }

    /// Records a mutation event for the given frame.
    pub fn record_mutation(&mut self, frame_id: u64) {
        self.mutation_sketch.increment(frame_id);
    }

    /// Returns the estimated query count for a frame.
    pub fn estimated_query_count(&self, frame_id: u64) -> u64 {
        self.query_sketch.estimate(frame_id)
    }

    /// Returns the estimated mutation count for a frame.
    pub fn estimated_mutation_count(&self, frame_id: u64) -> u64 {
        self.mutation_sketch.estimate(frame_id)
    }

    /// Computes the priority score for a frame using CMS-estimated counts.
    ///
    /// Delegates to the free function [`priority_score()`] with estimated
    /// query and mutation counts from the Count-Min Sketches.
    pub fn priority_score(&self, frame_id: u64, epochs_since_last: u64, config: &TierConfig) -> f64 {
        let qc = self.query_sketch.estimate(frame_id);
        let mc = self.mutation_sketch.estimate(frame_id);
        priority_score(qc, mc, epochs_since_last, config)
    }

    /// Resets both sketches (for epoch window rotation).
    pub fn reset(&mut self) {
        self.query_sketch.reset();
        self.mutation_sketch.reset();
    }
}

impl Default for FrameActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_frame_high_activity() {
        let config = TierConfig::default();
        let score = priority_score(10000, 5000, 1, &config);
        assert!(score > 0.7, "Expected Hot score > 0.7, got {score}");
        assert_eq!(recommend_tier(score), FrameTier::Hot);
    }

    #[test]
    fn cold_frame_no_activity() {
        let config = TierConfig::default();
        let score = priority_score(0, 0, 10000, &config);
        assert!(score < 0.2, "Expected Cold score < 0.2, got {score}");
        assert_eq!(recommend_tier(score), FrameTier::Cold);
    }

    #[test]
    fn warm_frame_moderate() {
        let config = TierConfig::default();
        let score = priority_score(100, 50, 50, &config);
        assert!(
            (0.2..=0.7).contains(&score),
            "Expected Warm score in [0.2, 0.7], got {score}"
        );
        assert_eq!(recommend_tier(score), FrameTier::Warm);
    }

    #[test]
    fn score_is_clamped() {
        let config = TierConfig::default();

        // Very high activity should not exceed 1.0
        let score = priority_score(u64::MAX / 2, u64::MAX / 2, 0, &config);
        assert!(score <= 1.0, "Score should be <= 1.0, got {score}");
        assert!(score >= 0.0, "Score should be >= 0.0, got {score}");

        // Zero everything: recency = exp(0) = 1.0, query/mutation = 0
        // raw = 0 * 0.4 + 0 * 0.3 + 1.0 * 0.3 = 0.3
        let score_zero = priority_score(0, 0, 0, &config);
        assert!(
            (0.0..=1.0).contains(&score_zero),
            "Score should be in [0.0, 1.0], got {score_zero}"
        );
    }

    #[test]
    fn custom_config_weights() {
        // All weight on queries -- mutation and recency don't matter
        let query_only = TierConfig {
            query_weight: 1.0,
            mutation_weight: 0.0,
            recency_weight: 0.0,
            recency_half_life: 100.0,
        };

        let default_config = TierConfig::default();

        let score_custom = priority_score(1000, 0, 1000, &query_only);
        let score_default = priority_score(1000, 0, 1000, &default_config);

        // With all weight on queries, score should be higher than default
        // (which penalizes for zero mutations and old age)
        assert!(
            score_custom > score_default,
            "Custom ({score_custom}) should be > default ({score_default})"
        );
    }

    // ── HysteresisState tests ─────────────────────────────────────────

    /// Oscillate score between 0.1 and 0.8 each window for 20 windows.
    /// With required_consecutive=5, tier should stay Warm (never reaches 5
    /// consecutive below or above). This directly validates HYST-01.
    #[test]
    fn test_hysteresis_prevents_thrashing() {
        let mut hyst = HysteresisState::new(5);
        let mut tier = FrameTier::Warm;

        for i in 0..20 {
            let score = if i % 2 == 0 { 0.1 } else { 0.8 };
            tier = hyst.update(score, tier);
        }

        assert_eq!(
            tier,
            FrameTier::Warm,
            "Oscillating scores should keep frame in Warm due to hysteresis"
        );
    }

    /// Score above 0.8 for 5 consecutive windows: verify promotion to Hot.
    #[test]
    fn test_hysteresis_promotes_after_consecutive() {
        let mut hyst = HysteresisState::new(5);
        let mut tier = FrameTier::Warm;

        for _ in 0..4 {
            tier = hyst.update(0.8, tier);
            assert_eq!(
                tier,
                FrameTier::Warm,
                "Should not promote before reaching required_consecutive"
            );
        }

        // 5th consecutive window above threshold
        tier = hyst.update(0.8, tier);
        assert_eq!(
            tier,
            FrameTier::Hot,
            "Should promote to Hot after 5 consecutive windows above threshold"
        );
    }

    /// Score below 0.1 for 5 consecutive windows: verify demotion to Cold.
    #[test]
    fn test_hysteresis_demotes_after_consecutive() {
        let mut hyst = HysteresisState::new(5);
        let mut tier = FrameTier::Warm;

        for _ in 0..4 {
            tier = hyst.update(0.1, tier);
            assert_eq!(
                tier,
                FrameTier::Warm,
                "Should not demote before reaching required_consecutive"
            );
        }

        // 5th consecutive window below threshold
        tier = hyst.update(0.1, tier);
        assert_eq!(
            tier,
            FrameTier::Cold,
            "Should demote to Cold after 5 consecutive windows below threshold"
        );
    }

    // ── FrameActivityTracker tests ────────────────────────────────────

    #[test]
    fn test_frame_activity_tracker_basic() {
        let mut tracker = FrameActivityTracker::new();

        for _ in 0..100 {
            tracker.record_query(42);
        }
        for _ in 0..50 {
            tracker.record_mutation(42);
        }

        assert!(tracker.estimated_query_count(42) >= 100);
        assert!(tracker.estimated_mutation_count(42) >= 50);

        let config = TierConfig::default();
        let score = tracker.priority_score(42, 1, &config);
        assert!(score > 0.0, "Active frame should have positive score");
    }

    #[test]
    fn test_frame_activity_tracker_reset() {
        let mut tracker = FrameActivityTracker::new();
        tracker.record_query(1);
        tracker.record_mutation(1);

        tracker.reset();

        assert_eq!(tracker.estimated_query_count(1), 0);
        assert_eq!(tracker.estimated_mutation_count(1), 0);
    }

    /// TEST-27: Increment 10K different keys with known frequencies, verify
    /// all estimates >= true count (no underestimate) and that heavy hitters
    /// (top 1%) have estimates within 10% of true count.
    #[test]
    fn test_count_min_sketch_accuracy() {
        use crate::count_min_sketch::CountMinSketch;

        // Use a larger sketch (width=16384, depth=8) for 10K keys to keep
        // collision-induced overestimates within bounds.
        let mut cms = CountMinSketch::new(16384, 8);

        // Create known frequency distribution: key i gets frequency (i + 1)
        // Keys 0..10000 with frequencies 1..10001
        let n = 10_000u64;
        let mut true_counts = std::collections::HashMap::new();

        for key in 0..n {
            let freq = key + 1;
            for _ in 0..freq {
                cms.increment(key);
            }
            true_counts.insert(key, freq);
        }

        // Verify no underestimate for ALL keys
        let mut underestimate_count = 0u64;
        for (&key, &true_count) in &true_counts {
            let est = cms.estimate(key);
            if est < true_count {
                underestimate_count += 1;
            }
        }
        assert_eq!(
            underestimate_count, 0,
            "No underestimate guarantee violated: {underestimate_count} keys underestimated"
        );

        // Verify heavy hitters (top 1% = keys with highest frequencies)
        // have estimates within 10% of true count
        let threshold = n - (n / 100); // top 1% = keys >= 9900
        let mut heavy_hitter_errors = 0u64;
        for key in threshold..n {
            let true_count = true_counts[&key];
            let est = cms.estimate(key);
            let error_ratio = (est as f64 - true_count as f64) / true_count as f64;
            if error_ratio > 0.10 {
                heavy_hitter_errors += 1;
            }
        }
        // Allow some tolerance: most heavy hitters should be within 10%
        let heavy_hitter_count = n / 100;
        assert!(
            heavy_hitter_errors <= heavy_hitter_count / 5,
            "Too many heavy hitters with >10% error: {heavy_hitter_errors}/{heavy_hitter_count}"
        );
    }
}
