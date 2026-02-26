---
phase: 13-scale-and-optimize
plan: 01
subsystem: data-structures
tags: [set-trie, count-min-sketch, probabilistic, inverted-index, routing, tiering]

# Dependency graph
requires:
  - phase: 06-signal-routing
    provides: InvertedIndex with HashMap posting lists
  - phase: 07-interpretation-and-adaptive-tiering
    provides: priority_score and TierConfig
provides:
  - SetTrie data structure for O(|pattern|) set containment/intersection queries
  - CountMinSketch probabilistic frequency counter with no-underestimate guarantee
  - InvertedIndex backed by SetTrie for node posting lists
  - FrameActivityTracker wrapping dual CountMinSketches for priority scoring
affects: [13-02-PLAN, engine, routing, tiering]

# Tech tracking
tech-stack:
  added: []
  patterns: [probabilistic-data-structures, trie-based-indexing]

key-files:
  created:
    - src/set_trie.rs
    - src/count_min_sketch.rs
  modified:
    - src/routing.rs
    - src/tiering.rs
    - src/lib.rs
    - benches/krabnet_bench.rs

key-decisions:
  - "SetTrie stores per-frame node sets and uses query_intersecting for single-node lookups"
  - "InvertedIndex keeps frame_nodes HashMap for unregister path reconstruction"
  - "FrameActivityTracker delegates to free-function priority_score with CMS estimates"
  - "TEST-27 uses larger CMS (16384x8) for 10K-key accuracy validation"

patterns-established:
  - "Probabilistic data structures sized proportionally to key cardinality"
  - "Acceleration structures (SetTrie) added alongside source-of-truth stores"

requirements-completed: [SETTRIE-01, SETTRIE-02, CMS-01, CMS-02, TEST-25, TEST-26, TEST-27, BENCH-03]

# Metrics
duration: 9min
completed: 2026-02-25
---

# Phase 13 Plan 01: Set-Trie and Count-Min Sketch Summary

**Set-Trie data structure for O(|pattern|) set queries and Count-Min Sketch for probabilistic frequency counting, wired into InvertedIndex and frame prioritizer**

## Performance

- **Duration:** 9 min
- **Started:** 2026-02-24T22:34:39Z
- **Completed:** 2026-02-24T22:43:36Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- SetTrie with insert/remove/query_containing/query_intersecting supporting arbitrary sorted-set keys
- CountMinSketch with no-underestimate guarantee and bounded overestimate for heavy hitters
- InvertedIndex internally backed by SetTrie for node posting lists (identical public API preserved)
- FrameActivityTracker as PRIMARY scoring interface using dual CMS instead of per-frame counters
- Comprehensive test coverage: TEST-25 (1000-set correctness), TEST-26 (10K-frame scale), TEST-27 (CMS accuracy)
- BENCH-03 benchmarks comparing Set-Trie vs HashMap lookup latency

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement Set-Trie and Count-Min Sketch data structures** - `ce09981` (feat)
2. **Task 2: Wire Set-Trie into InvertedIndex and CMS into prioritizer** - `7f748f0` (feat)

## Files Created/Modified
- `src/set_trie.rs` - Set-Trie with insert/remove/query_containing/query_intersecting and 5 unit tests
- `src/count_min_sketch.rs` - Count-Min Sketch with increment/estimate/reset and 4 unit tests
- `src/routing.rs` - InvertedIndex refactored to use SetTrie for node posting lists, added TEST-25 and TEST-26
- `src/tiering.rs` - Added FrameActivityTracker wrapping dual CMS, TEST-27 accuracy validation
- `src/lib.rs` - Module declarations and re-exports for set_trie and count_min_sketch
- `benches/krabnet_bench.rs` - Added bench_set_trie_lookup and bench_hashmap_lookup (BENCH-03)

## Decisions Made
- SetTrie stores per-frame node sets as sorted u64 arrays; query_intersecting with single element replaces HashMap lookup semantics
- InvertedIndex keeps frame_nodes: HashMap<u64, Vec<u64>> for unregister path reconstruction since SetTrie removal requires the original element set
- FrameActivityTracker delegates to existing free-function priority_score() with CMS-estimated counts, preserving function signature compatibility
- TEST-27 uses larger CMS dimensions (16384x8) for 10K-key accuracy test since default 1024x4 has too many collisions at that cardinality

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed format string brace escaping in test assertions**
- **Found during:** Task 1
- **Issue:** Test assertion messages contained `{1,3}` which Rust interpreted as format specifiers
- **Fix:** Escaped to `{{1,3}}` for literal brace display
- **Files modified:** src/set_trie.rs
- **Verification:** Compilation succeeded
- **Committed in:** ce09981 (part of task commit)

**2. [Rule 1 - Bug] Adjusted CMS accuracy test dimensions for 10K key cardinality**
- **Found during:** Task 2
- **Issue:** Default 1024x4 CMS had >10% overestimate for heavy hitters with 10K keys due to collision rate
- **Fix:** TEST-27 uses CountMinSketch::new(16384, 8) directly instead of through FrameActivityTracker default
- **Files modified:** src/tiering.rs
- **Verification:** Test passes with all heavy hitters within 10% error tolerance
- **Committed in:** 7f748f0 (part of task commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both auto-fixes necessary for correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- SetTrie and CountMinSketch modules ready for use by 13-02 (engine integration)
- FrameActivityTracker ready to be wired into engine.rs ingest pipeline (13-02 Task 3)
- All 161 lib tests, 46 doc tests pass; zero clippy warnings

## Self-Check: PASSED

All 7 files verified present. Both task commits (ce09981, 7f748f0) verified in git log.

---
*Phase: 13-scale-and-optimize*
*Completed: 2026-02-25*
