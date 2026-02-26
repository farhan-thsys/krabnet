---
phase: 02-epoch-sequencer-and-ring-buffer
plan: 01
subsystem: infra
tags: [atomicu64, ring-buffer, sequencer, lock-free, zero-allocation]

# Dependency graph
requires:
  - phase: 01-core-types-and-string-interning
    provides: "Epoch, Event, NodeId, TypeId newtypes and enums"
provides:
  - "EpochSequencer: global monotonic epoch counter via AtomicU64 with SeqCst ordering"
  - "RingBuffer: pre-allocated power-of-2 circular buffer with epoch-verified reads"
  - "Send + Sync implementations for both types (automatic, no unsafe)"
affects: [engine-orchestration, signal-routing, frame-materialization]

# Tech tracking
tech-stack:
  added: []
  patterns: ["AtomicU64 with SeqCst for monotonic sequencing", "power-of-2 bitmask slot addressing", "epoch-in-slot overwrite detection"]

key-files:
  created: [src/sequencer.rs, src/ring_buffer.rs]
  modified: [src/lib.rs]

key-decisions:
  - "RingBuffer uses &mut self for push (single-writer) -- concurrent multi-producer deferred to v2 PERF-02"
  - "Epoch-in-slot pattern for overwrite detection: each slot stores (Epoch, Event), read verifies epoch match"
  - "Send+Sync derived automatically from AtomicU64 and Vec<Option<(Epoch, Event)>> -- no unsafe impl needed"

patterns-established:
  - "Bitmask slot addressing: slot = epoch.0 as usize & mask (replaces modulo with single AND)"
  - "Pre-allocation pattern: Vec::resize_with(capacity, || None) at construction, zero alloc on hot path"
  - "Module layout: doc comment, imports, struct, impl with pub methods, Default impl, #[cfg(test)] tests"

requirements-completed: [INFRA-03, INFRA-04, INFRA-05, INFRA-06, TEST-02]

# Metrics
duration: 3min
completed: 2026-02-24
---

# Phase 2 Plan 1: Epoch Sequencer and Ring Buffer Summary

**AtomicU64-based monotonic epoch sequencer and pre-allocated power-of-2 ring buffer with zero hot-path allocation and epoch-verified reads**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-24T18:54:10Z
- **Completed:** 2026-02-24T18:56:58Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- EpochSequencer producing strictly increasing u64 epochs via AtomicU64 fetch_add with SeqCst ordering
- RingBuffer with pre-allocated power-of-2 slots, bitmask addressing, and epoch-in-slot overwrite detection
- 34 unit tests and 16 doc-tests all passing with zero clippy warnings
- Both types automatically implement Send + Sync with no unsafe code

## Task Commits

Each task was committed atomically:

1. **Task 1: Create epoch sequencer and ring buffer modules** - `7d92696` (feat)
2. **Task 2: Add comprehensive tests for sequencer and ring buffer** - `83bab0e` (feat)

## Files Created/Modified
- `src/sequencer.rs` - Global monotonic epoch sequencer using AtomicU64 with SeqCst ordering
- `src/ring_buffer.rs` - Pre-allocated lock-free ring buffer with power-of-2 masking and epoch verification
- `src/lib.rs` - Added pub mod and pub use for sequencer and ring_buffer modules

## Decisions Made
- RingBuffer uses `&mut self` for push, deferring concurrent multi-producer to v2 (PERF-02). Single-writer is sufficient for the current sequential pipeline design.
- Epoch-in-slot overwrite detection: each slot stores `(Epoch, Event)`, and reads verify the stored epoch matches the requested epoch. This cleanly handles wraparound without extra bookkeeping.
- Send+Sync derived automatically -- no unsafe impl needed because `AtomicU64` and `Vec<Option<(Epoch, Event)>>` are both Send+Sync, and mutation requires exclusive `&mut self`.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Sequencer and ring buffer are ready for use by the Engine in Phase 9
- Phase 3 (Property Graph Storage) and Phase 4 (Differential MVCC) can proceed independently as they depend only on Phase 1
- The ring buffer's `push()` returns `Epoch` which downstream phases (frames, signal routing) will use for temporal ordering

## Self-Check: PASSED

- [x] src/sequencer.rs exists (181 lines, min 30)
- [x] src/ring_buffer.rs exists (389 lines, min 80)
- [x] src/lib.rs contains `pub mod sequencer` and `pub mod ring_buffer`
- [x] Commit 7d92696 exists (Task 1)
- [x] Commit 83bab0e exists (Task 2)
- [x] 34 unit tests + 16 doc-tests pass
- [x] Zero clippy warnings

---
*Phase: 02-epoch-sequencer-and-ring-buffer*
*Completed: 2026-02-24*
