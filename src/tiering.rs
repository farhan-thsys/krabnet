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
            score >= 0.2 && score <= 0.7,
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
            score_zero >= 0.0 && score_zero <= 1.0,
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
}
