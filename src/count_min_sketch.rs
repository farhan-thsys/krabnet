//! Count-Min Sketch probabilistic frequency counter.
//!
//! A fixed-size data structure that estimates the frequency of elements in a
//! stream using sub-linear space. Provides a **no underestimate guarantee**:
//! the estimated count is always >= the true count. Overestimates are bounded
//! by hash collisions and decrease with larger width/depth parameters.
//!
//! # Design
//!
//! The sketch maintains a `depth x width` matrix of counters with one
//! independent hash function per row. Each `increment` adds 1 to one cell
//! per row, and `estimate` returns the minimum across all rows for a given key.
//!
//! # Default Parameters
//!
//! - Width: 1024 (columns per row)
//! - Depth: 4 (number of independent hash rows)
//!
//! # Example
//!
//! ```
//! use krabnet::count_min_sketch::CountMinSketch;
//!
//! let mut cms = CountMinSketch::new(1024, 4);
//! for _ in 0..10 {
//!     cms.increment(42);
//! }
//! assert!(cms.estimate(42) >= 10);
//! ```

/// A Count-Min Sketch for probabilistic frequency estimation.
///
/// Stores a `depth x width` matrix of `u64` counters. Each row uses an
/// independent hash function derived from well-known seed constants.
/// The `estimate` method returns the minimum counter value across all rows,
/// which provides the no-underestimate guarantee.
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    /// Number of columns per row.
    width: usize,
    /// Number of independent hash rows.
    depth: usize,
    /// Counter matrix: `depth` rows of `width` columns.
    matrix: Vec<Vec<u64>>,
    /// Hash seeds, one per row.
    seeds: Vec<u64>,
}

/// Well-known hash seed constants for reproducible hashing.
const SEED_CONSTANTS: [u64; 8] = [
    0x9e3779b97f4a7c15,
    0x517cc1b727220a95,
    0x6c62272e07bb0142,
    0x62b821756295c58d,
    0xbf58476d1ce4e5b9,
    0x94d049bb133111eb,
    0xd6e8feb86659fd93,
    0xa0761d6478bd642f,
];

impl CountMinSketch {
    /// Creates a new Count-Min Sketch with the given dimensions.
    ///
    /// # Arguments
    ///
    /// * `width` - Number of columns per row (higher = less collision).
    /// * `depth` - Number of independent hash rows (higher = tighter estimate).
    ///
    /// Seeds are deterministic: the first `depth` entries from `SEED_CONSTANTS`
    /// are used, or generated via `row_index * prime` for depths > 8.
    pub fn new(width: usize, depth: usize) -> Self {
        let matrix = vec![vec![0u64; width]; depth];
        let seeds: Vec<u64> = (0..depth)
            .map(|i| {
                if i < SEED_CONSTANTS.len() {
                    SEED_CONSTANTS[i]
                } else {
                    // Fallback: generate seed from row index * large prime
                    (i as u64).wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(0x517cc1b727220a95)
                }
            })
            .collect();

        Self {
            width,
            depth,
            matrix,
            seeds,
        }
    }

    /// Increments the count for a key by 1.
    ///
    /// For each row, computes a hash of the key using that row's seed,
    /// then increments the corresponding column. This ensures the minimum
    /// across all rows is always >= the true count.
    pub fn increment(&mut self, key: u64) {
        for row in 0..self.depth {
            let col = self.hash(key, row);
            self.matrix[row][col] = self.matrix[row][col].saturating_add(1);
        }
    }

    /// Returns the estimated count for a key.
    ///
    /// Computes the hash for each row and returns the minimum counter value.
    /// This is the tightest possible estimate: the row with the least
    /// collision gives the closest approximation to the true count.
    ///
    /// # No Underestimate Guarantee
    ///
    /// The returned value is always >= the true count of `increment` calls
    /// for this key.
    pub fn estimate(&self, key: u64) -> u64 {
        (0..self.depth)
            .map(|row| {
                let col = self.hash(key, row);
                self.matrix[row][col]
            })
            .min()
            .unwrap_or(0)
    }

    /// Resets all counters to zero.
    ///
    /// Useful for epoch window rotation where frequency counts are
    /// periodically cleared.
    pub fn reset(&mut self) {
        for row in &mut self.matrix {
            for cell in row.iter_mut() {
                *cell = 0;
            }
        }
    }

    /// Computes the column index for a key in the given row.
    ///
    /// Hash function: `(key * seed + (seed >> 16)) % width`
    #[inline]
    fn hash(&self, key: u64, row: usize) -> usize {
        let seed = self.seeds[row];
        (key.wrapping_mul(seed).wrapping_add(seed >> 16) % self.width as u64) as usize
    }
}

impl Default for CountMinSketch {
    /// Returns a CountMinSketch with default parameters: width=1024, depth=4.
    fn default() -> Self {
        Self::new(1024, 4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_increment_estimate() {
        let mut cms = CountMinSketch::new(1024, 4);
        for _ in 0..5 {
            cms.increment(42);
        }
        let est = cms.estimate(42);
        assert!(est >= 5, "Expected estimate >= 5, got {est}");
    }

    #[test]
    fn test_no_underestimate() {
        let mut cms = CountMinSketch::new(1024, 4);
        let n = 1000u64;
        for _ in 0..n {
            cms.increment(99);
        }
        let est = cms.estimate(99);
        assert!(
            est >= n,
            "No underestimate violated: expected >= {n}, got {est}"
        );
    }

    #[test]
    fn test_reset_clears() {
        let mut cms = CountMinSketch::new(1024, 4);
        cms.increment(1);
        cms.increment(2);
        cms.increment(3);

        cms.reset();

        assert_eq!(cms.estimate(1), 0);
        assert_eq!(cms.estimate(2), 0);
        assert_eq!(cms.estimate(3), 0);
    }

    #[test]
    fn test_different_keys_independent() {
        let mut cms = CountMinSketch::new(1024, 4);

        // Key A incremented 100 times
        for _ in 0..100 {
            cms.increment(1);
        }
        // Key B incremented 10 times
        for _ in 0..10 {
            cms.increment(2);
        }

        let est_a = cms.estimate(1);
        let est_b = cms.estimate(2);

        assert!(est_a >= 100, "Key A: expected >= 100, got {est_a}");
        assert!(est_b >= 10, "Key B: expected >= 10, got {est_b}");
        // A should be estimated higher than B (with high probability at width=1024)
        assert!(
            est_a > est_b,
            "Key A ({est_a}) should be > Key B ({est_b})"
        );
    }
}
