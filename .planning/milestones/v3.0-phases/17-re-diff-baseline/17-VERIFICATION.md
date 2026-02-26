---
phase: 17-re-diff-baseline
verified: 2026-02-26T17:00:00Z
status: passed
score: 4/4 must-haves verified
re_verification: false
gaps: []
human_verification: []
---

# Phase 17: Re-Diff Baseline Verification Report

**Phase Goal:** Frames stay in sync with the graph as it mutates, using full re-traverse + diff as the correctness baseline for all subsequent incremental phases
**Verified:** 2026-02-26T17:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | After any graph mutation routed to a frame, the frame's differential state matches what a fresh full DFS materialization would produce | VERIFIED | `maintain_and_evaluate_frames` acquires write lock, calls `frame.rematerialize(graph, epoch)` which does `evict() + materialize()` (full DFS). `oracle_check` asserts maintained == fresh on every mutation. All 6 oracle tests pass. |
| 2 | Frame maintenance runs on every ingest event that routes to at least one frame (not just at registration time) | VERIFIED | `Engine::ingest()` Step 4 calls `Self::maintain_and_evaluate_frames(&affected_frames, graph_ref, epoch, prev_deltas)` after every graph mutation in Step 2, for every affected frame found via inverted index in Step 3. |
| 3 | A correctness oracle test compares maintained frame state against fresh re-traverse and asserts exact match | VERIFIED | `oracle_check()` at engine.rs:2319 builds a fresh `Frame::new(u64::MAX, anchor, pattern)` + `materialize()`, compares as `HashSet<Vec<NodeId>>` against maintained frame's `query_frame()` output, asserts equality with detailed diagnostic message. |
| 4 | `flush_coalescer` also maintains frames via rematerialize, not just read-only tier1_check | VERIFIED | `Engine::flush_coalescer()` at engine.rs:661 also calls `Self::maintain_and_evaluate_frames(&affected_frames, graph_ref, flush_epoch, prev_deltas)` with epoch derived from `max(epoch_end)` across coalesced entries. |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/engine.rs` | Write-lock rematerialize in ingest Step 4 and flush_coalescer, graph accessor for tests, oracle test harness | VERIFIED | 2678 lines. Contains `maintain_and_evaluate_frames` helper (lines 763-791), write-lock rematerialize in ingest Step 4 (lines 363-369), write-lock rematerialize in flush_coalescer (lines 661-666), `#[cfg(test)] pub(crate) fn graph()` accessor (lines 798-802), 6 oracle tests (lines 2358-2677). |
| `src/frame.rs` | `Frame::rematerialize()` doing evict + DFS materialize | VERIFIED | `rematerialize()` at line 249 calls `self.evict()` then `self.materialize(graph, epoch)` — full re-traverse, not a stub. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `Engine::ingest()` Step 4 | `Frame::rematerialize()` | Write lock on `Arc<RwLock<Frame>>` inside `thread::scope` | WIRED | `maintain_and_evaluate_frames` at engine.rs:776 does `arc.write().expect("RwLock poisoned")` then `frame.rematerialize(graph, epoch)`. Called from ingest at line 364. |
| `Engine::flush_coalescer()` | `Frame::rematerialize()` | Write lock on `Arc<RwLock<Frame>>` inside `thread::scope` | WIRED | Same `maintain_and_evaluate_frames` helper called from flush_coalescer at line 661, confirming identical maintenance path. |
| `oracle_check()` | `Frame::materialize()` | Fresh `Frame` construction + `HashSet` comparison | WIRED | `oracle_check` at engine.rs:2336-2342 constructs `Frame::new(u64::MAX, anchor, pattern)`, calls `reference.materialize(engine.graph(), epoch)`, collects `HashSet<Vec<NodeId>>`, then `assert_eq!` at line 2344 against maintained paths. |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| RDIF-01 | 17-01-PLAN.md | Engine ingest pipeline wires frame state maintenance after initial materialization (frames update on every routed event, not just at registration) | SATISFIED | `ingest()` Step 4 calls `maintain_and_evaluate_frames` for every affected frame on every event. 6 oracle tests confirm frame state updates after EdgeAdded, EdgeRemoved, NodeRemoved, PropertyChanged mutations. `test_oracle_edge_added_after_registration` directly proves frames update post-registration. |
| RDIF-02 | 17-01-PLAN.md | Frame maintenance produces correct differential state matching what full DFS re-materialization would produce at every epoch | SATISFIED | `frame.rematerialize()` performs evict + full DFS. `oracle_check` compares maintained vs fresh DFS as unordered `HashSet<Vec<NodeId>>`. 6 oracle tests all pass, covering 1-hop, 2-hop diamond, property filter, and multi-frame isolation scenarios. |
| RDIF-03 | 17-01-PLAN.md | Correctness oracle test harness compares incremental result against full re-traverse for every frame update and asserts exact match | SATISFIED | `oracle_check()` function exists at engine.rs:2319. Called after every mutation in all 6 oracle tests. `assert_eq!(maintained_paths, expected_paths, ...)` with full diagnostic output on failure. All 6 tests pass: `cargo test --lib oracle` reports 6 passed, 0 failed. |

No orphaned requirements — REQUIREMENTS.md maps RDIF-01, RDIF-02, RDIF-03 exclusively to Phase 17, all covered by plan 17-01.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | None found | — | — |

No TODO/FIXME/placeholder comments in `src/engine.rs` or `src/tiering.rs`. No empty implementations or stub returns. All handlers route to real logic.

### Human Verification Required

None. All success criteria are verifiable programmatically:

- Frame sync correctness: proven by oracle tests asserting `HashSet` equality
- Maintenance on every event: verified by code path analysis showing `maintain_and_evaluate_frames` called unconditionally in ingest Step 4
- Flush coalescer path: verified by reading flush_coalescer implementation
- Test passage: confirmed by running `cargo test --lib oracle` (6 passed) and `cargo test --lib` (191 passed, 0 failed)

### Test Execution Results

```
cargo test --lib oracle
running 6 tests
test engine::tests::test_oracle_edge_added_after_registration ... ok
test engine::tests::test_oracle_property_changed ... ok
test engine::tests::test_oracle_node_removed ... ok
test engine::tests::test_oracle_edge_removed ... ok
test engine::tests::test_oracle_unaffected_frame_unchanged ... ok
test engine::tests::test_oracle_multi_hop_diamond ... ok
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 187 filtered out

cargo test --lib
test result: ok. 191 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out

cargo clippy --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.61s
(clean — no warnings)
```

### Gaps Summary

No gaps. All 4 must-have truths are verified, all 3 requirement IDs satisfied, all 3 key links wired, no anti-patterns, no regressions (191 tests pass), clippy clean.

The phase goal is fully achieved: frames stay in sync with the graph as it mutates. The full re-traverse + diff baseline is operational and proven correct by a 6-scenario oracle test harness that asserts exact match between maintained frame state and fresh DFS materialization after every mutation type.

---

_Verified: 2026-02-26T17:00:00Z_
_Verifier: Claude (gsd-verifier)_
