---
phase: 11-harden-engine
plan: 01
subsystem: engine
tags: [compaction, concurrency, threading, rwlock, crossbeam, double-buffer]

# Dependency graph
requires:
  - phase: 09-engine-orchestration
    provides: Engine with Frame map, ingest pipeline, inverted index routing
  - phase: 04-differential-mvcc-engine
    provides: DiffCollection with compact() method
provides:
  - CompactionWorker with background thread and double-buffer compaction
  - Engine with Arc<RwLock<Frame>> wrapping for concurrent frame access
  - Parallel frame evaluation via std::thread::scope
  - Automatic compaction triggered by tuple count threshold
  - CompactionStats for monitoring compaction activity
affects: [11-02-coalescer-fanout, 11-03-quality-gates, benchmarks]

# Tech tracking
tech-stack:
  added: [std::sync::RwLock, std::sync::Mutex, std::thread::scope]
  patterns: [double-buffer-compaction, scoped-thread-fanout, arc-rwlock-wrapping]

key-files:
  created: [src/compaction.rs]
  modified: [src/engine.rs, src/frame.rs, src/lib.rs]

key-decisions:
  - "Used std::sync::{RwLock, Mutex} instead of parking_lot -- GNU toolchain dlltool cannot compile parking_lot_core on Windows"
  - "Scoped threads via std::thread::scope for frame evaluation -- zero-overhead, no thread pool, automatic lifetime management"
  - "Delta updates collected in scoped threads and merged on main thread after scope returns -- avoids shared mutable state"
  - "Engine::new() backward compatible (no compaction worker); Engine::with_compaction() for opt-in compaction"

patterns-established:
  - "Double-buffer compaction: read lock to clone, compact off-lock, write lock only to swap"
  - "Arc<RwLock<Frame>> wrapping for all frame access in Engine"
  - "std::thread::scope fan-out for parallel frame evaluation after single-threaded index lookup"

requirements-completed: [COMPACT-01, COMPACT-02, COMPACT-03, COMPACT-04, EVAL-01, EVAL-02, EVAL-03]

# Metrics
duration: 8min
completed: 2026-02-25
---

# Phase 11 Plan 01: Compaction Worker & Parallel Eval Summary

**Background compaction worker with double-buffer strategy on dedicated std::thread, and multi-threaded frame evaluation via std::thread::scope with Arc<RwLock<Frame>> wrapping**

## Performance

- **Duration:** 8 min
- **Started:** 2026-02-24T20:44:12Z
- **Completed:** 2026-02-24T20:52:13Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- CompactionWorker running on dedicated background thread with crossbeam channel communication
- Double-buffer compaction: clone DiffCollection under read lock, compact off-lock, swap back under write lock (minimizes lock contention)
- Configurable tuple_count threshold (default 10,000) for automatic compaction
- CompactionStats tracking compactions_completed, tuples_before, tuples_after, total_compaction_time_us
- Frame evaluation parallelized via std::thread::scope after single-threaded inverted index lookup
- All frame state wrapped in Arc<RwLock<Frame>> for concurrent read/write access
- 127 lib tests pass, 39 doc tests pass, zero clippy warnings

## Task Commits

Each task was committed atomically:

1. **Task 1: Create CompactionWorker with background thread and double-buffer compaction** - `702b5c6` (feat)
2. **Task 2: Integrate compaction worker and multi-threaded frame evaluation into Engine** - `f461fa9` (feat)

## Files Created/Modified
- `src/compaction.rs` - CompactionWorker, CompactionRequest, CompactionStats with background thread and double-buffer pattern
- `src/engine.rs` - Engine with Arc<RwLock<Frame>>, thread::scope fan-out, compaction integration, with_compaction() constructor
- `src/frame.rs` - Added clone_diff_collection() and swap_diff_collection() for double-buffer support
- `src/lib.rs` - Added compaction module declaration and CompactionWorker re-export

## Decisions Made
- Used std::sync::{RwLock, Mutex} instead of parking_lot -- the GNU toolchain's dlltool cannot compile parking_lot_core on Windows (same dlltool issue encountered in Phase 10 with criterion html_reports). The std primitives provide identical API semantics with slightly higher overhead that is negligible for this use case.
- Used std::thread::scope instead of a persistent thread pool for frame evaluation -- zero overhead when no frames are affected, automatic lifetime management, and no unsafe code needed.
- Delta updates from scoped threads are collected as (frame_id, delta) pairs and merged on main thread after scope returns, avoiding shared mutable state for previous_deltas.
- Engine::new() remains backward compatible with no compaction worker; Engine::with_compaction() is the opt-in constructor.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Used std::sync instead of parking_lot due to toolchain limitation**
- **Found during:** Task 1 (CompactionWorker creation)
- **Issue:** parking_lot 0.12 depends on parking_lot_core which calls dlltool.exe during compilation; the GNU toolchain's bundled dlltool cannot create import libraries on this Windows system
- **Fix:** Replaced all parking_lot::{RwLock, Mutex} with std::sync::{RwLock, Mutex}. Added .expect("RwLock poisoned") for lock unwrapping (std locks return Result unlike parking_lot). API semantics are identical.
- **Files modified:** src/compaction.rs, src/engine.rs
- **Verification:** cargo test --lib (127 pass), cargo clippy -- -D warnings (0 warnings)
- **Committed in:** 702b5c6 (Task 1), f461fa9 (Task 2)

---

**Total deviations:** 1 auto-fixed (1 blocking -- toolchain limitation)
**Impact on plan:** Minimal. std::sync primitives provide identical correctness guarantees. The only difference is slightly higher overhead due to OS-level mutex vs parking_lot's spinlock-first approach, which is negligible for this workload.

## Issues Encountered
- Cargo.toml modifications were repeatedly reverted by a parallel agent/watcher during execution. Resolved by ensuring compaction module was re-added to lib.rs as needed and not depending on Cargo.toml changes (since parking_lot was dropped in favor of std::sync).

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- CompactionWorker and parallel frame evaluation are fully functional
- Engine provides both legacy (Engine::new) and concurrent (Engine::with_compaction) constructors
- Ready for integration with coalescer (11-02) and quality gates (11-03)
- All existing tests remain green -- no regressions

---
## Self-Check: PASSED

- FOUND: src/compaction.rs
- FOUND: src/engine.rs
- FOUND: src/frame.rs
- FOUND: .planning/phases/11-harden-engine/11-01-SUMMARY.md
- FOUND: commit 702b5c6
- FOUND: commit f461fa9

---
*Phase: 11-harden-engine*
*Completed: 2026-02-25*
