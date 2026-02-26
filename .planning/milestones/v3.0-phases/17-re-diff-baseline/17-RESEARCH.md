# Phase 17: Re-Diff Baseline - Research

**Researched:** 2026-02-26
**Domain:** Frame maintenance pipeline wiring, full re-traverse + diff correctness oracle
**Confidence:** HIGH

## Summary

Phase 17 bridges the gap between the existing engine ingest pipeline and frame state maintenance. Currently, after `register_frame()` materializes a frame via DFS, the `ingest()` pipeline routes events to affected frames but only performs a Tier 1 net_delta check (read-only). **No frame state update occurs on mutation events.** The frame's DiffCollection remains frozen at the registration-time snapshot.

This phase wires a `rematerialize()` call into the ingest pipeline for every affected frame on every routed event, producing a fresh DFS + diff against the frame's prior state. This is intentionally the "naive but provably correct" baseline: full re-traverse on every mutation. Subsequent phases (18-20) replace this with incremental O(affected) path extension, but the correctness oracle built in this phase persists as the verification backbone for all future incremental work.

**Primary recommendation:** Add a `maintain_frame()` method to Engine that calls `frame.rematerialize(&graph, epoch)` for each affected frame during ingest Step 4, replacing the current read-only tier1_check with a write-lock re-traverse + tier1 check sequence. Build a correctness oracle test harness that compares incremental frame state against full re-traverse after every update.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| RDIF-01 | Engine ingest pipeline wires frame state maintenance after initial materialization (frames update on every routed event, not just at registration) | The ingest() method at line 255 of engine.rs currently only reads net_delta via tier1_check in Step 4. Must be modified to acquire write lock and call rematerialize() or equivalent on each affected frame before the tier1_check. The parallel thread::scope fan-out already collects affected frame Arcs -- change from read lock to write lock and add rematerialize call. |
| RDIF-02 | Frame maintenance produces correct differential state matching what full DFS re-materialization would produce at every epoch | Frame::rematerialize() already exists (line 249 of frame.rs) -- it calls evict() then materialize(). This is the full-DFS baseline. The key is that after rematerialize, the DiffCollection state must exactly match a fresh Frame constructed from scratch with the same pattern on the same graph. |
| RDIF-03 | Correctness oracle test harness compares incremental result against full re-traverse for every frame update and asserts exact match | Build a test function oracle_check(engine, frame_id, graph, epoch) that constructs a fresh Frame with the same anchor/pattern, materializes it, and asserts current_state() sets match exactly. Wire this into multi-step mutation test scenarios covering EdgeAdded, EdgeRemoved, NodeRemoved, PropertyChanged. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| (no new deps) | - | All work is within existing Rust crate | Purely algorithmic; uses existing Frame, DiffCollection, Graph, Engine APIs |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| std::collections::HashSet | stable | Set comparison for oracle path matching | Comparing unordered path sets between incremental and fresh frames |
| std::thread::scope | stable | Parallel frame evaluation (existing) | Already used in engine ingest pipeline |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| rematerialize() (evict+full DFS) | Diff-based apply_delta | Phase 18-20 work; this phase intentionally uses full re-traverse as correctness baseline |
| Per-event re-traverse | Batch re-traverse per epoch window | Would reduce overhead but complicates correctness oracle; defer to Phase 21 optimization |

**Installation:**
```bash
# No new dependencies -- purely algorithmic changes within existing crate
```

## Architecture Patterns

### Recommended Project Structure
```
src/
  engine.rs     # Modified: wire maintain_frame into ingest pipeline Step 4
  frame.rs      # Existing: rematerialize() already correct, may add oracle helper
  diff.rs       # Existing: DiffCollection unchanged
  routing.rs    # Existing: InvertedIndex unchanged
  (tests inline) # Oracle test harness as #[cfg(test)] in engine.rs
```

### Pattern 1: Write-Lock Frame Maintenance in Parallel Fan-Out
**What:** Replace the read-lock tier1_check-only fan-out with a write-lock rematerialize + tier1_check sequence.
**When to use:** Every ingest event that routes to at least one frame.
**Example:**
```rust
// Current (read-only, no maintenance):
s.spawn(move || {
    let frame = arc.read().expect("RwLock poisoned");
    let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
    let current = frame.net_delta();
    let _changed = tier1_check(previous, current);
    (fid, current)
});

// Phase 17 (write-lock, full re-traverse):
s.spawn(move || {
    let mut frame = arc.write().expect("RwLock poisoned");
    frame.rematerialize(graph_ref, epoch);
    let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
    let current = frame.net_delta();
    let _changed = tier1_check(previous, current);
    (fid, current)
});
```

### Pattern 2: Graph Reference Sharing for Parallel Frame Maintenance
**What:** The `rematerialize()` call requires `&Graph`. The current fan-out uses `std::thread::scope` which allows borrowing from the parent stack. The `&self.graph` reference can be captured by the scope closure and shared across all spawn calls as an immutable reference since graph mutation (Step 2) has already completed before Step 4 begins.
**When to use:** When passing graph reference to parallel frame re-traverse threads.
**Example:**
```rust
// Graph mutation already done in Step 2.
// Step 4: scope borrows &self.graph immutably
let graph_ref = &self.graph;
std::thread::scope(|s| {
    for (fid, frame_arc) in &affected_frames {
        let arc = Arc::clone(frame_arc);
        s.spawn(move || {
            let mut frame = arc.write().expect("RwLock poisoned");
            frame.rematerialize(graph_ref, epoch);
            // ...
        });
    }
});
```

### Pattern 3: Correctness Oracle as Comparison Function
**What:** A test-only function that creates a fresh Frame from scratch, materializes against current graph, and compares its current_state() against the maintained frame's current_state() as unordered sets.
**When to use:** In every test that mutates the graph after frame registration.
**Example:**
```rust
#[cfg(test)]
fn oracle_check(
    frame: &mut Frame,
    graph: &Graph,
    epoch: Epoch,
) {
    // Build fresh reference frame
    let mut reference = Frame::new(
        u64::MAX, // dummy ID
        frame.anchor(),
        frame.pattern().to_vec(),
    );
    reference.materialize(graph, epoch);

    // Compare as unordered sets
    let maintained: HashSet<Vec<NodeId>> = frame.query().into_iter().cloned().collect();
    let expected: HashSet<Vec<NodeId>> = reference.query().into_iter().cloned().collect();
    assert_eq!(maintained, expected,
        "Oracle mismatch: maintained {} paths vs expected {} paths",
        maintained.len(), expected.len());
}
```

### Anti-Patterns to Avoid
- **Reading frame state without maintenance on mutation:** The current code reads net_delta without updating frame state. After Phase 17, every routed event MUST trigger rematerialize before reading.
- **Mutating graph after frame maintenance in same epoch:** Graph mutation (Step 2) must complete BEFORE frame re-traverse (Step 4). The current pipeline already enforces this order -- do not reorder.
- **Comparing paths in order-dependent way:** DFS may produce paths in different order depending on HashMap iteration order. Always compare as HashSet<Vec<NodeId>>, never as Vec<Vec<NodeId>>.
- **Calling rematerialize on frames NOT affected by the event:** Only frames returned by InvertedIndex::affected_frames should be re-traversed. Re-traversing unaffected frames wastes CPU and does not change state.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Full DFS from anchor | Custom traversal | Frame::rematerialize() | Already exists, tested with 12 unit tests |
| Diff collection management | Manual path tracking | DiffCollection<Vec<NodeId>> | Proven exact +1/-1 math with compaction |
| Event-to-frame routing | Linear scan of all frames | InvertedIndex::affected_frames() | O(affected) via SetTrie, tested at 10K scale |
| Set comparison for oracle | Manual loop comparison | HashSet equality | Standard library, handles duplicates and order |

**Key insight:** All the building blocks exist. Phase 17 is wiring work, not new data structure work. Frame::rematerialize, DiffCollection, InvertedIndex are all battle-tested.

## Common Pitfalls

### Pitfall 1: Borrow Checker Conflict -- &mut self.graph vs &self.frames
**What goes wrong:** Engine::ingest takes `&mut self`. You need `&self.graph` (immutable) for rematerialize while also holding `Arc<RwLock<Frame>>` clones from `self.frames`. The borrow checker may reject simultaneous access.
**Why it happens:** Rust's borrow rules prevent taking `&self.graph` and `&mut self` simultaneously.
**How to avoid:** The existing code already solves this: `affected_frames` is a `Vec<(u64, Arc<RwLock<Frame>>)>` cloned from the HashMap. Inside `thread::scope`, borrow `&self.graph` as a local ref (`let graph_ref = &self.graph;`) BEFORE the scope closure. Both `graph_ref` and the Arc clones can be moved into spawned threads without conflict.
**Warning signs:** Compilation error mentioning "cannot borrow `self` as immutable because it is also borrowed as mutable."

### Pitfall 2: Write Lock Contention During Parallel Re-Traverse
**What goes wrong:** Switching from read locks to write locks means each spawned thread holds an exclusive lock on its frame during DFS. If two events route to the same frame in rapid succession, the second event's thread blocks.
**Why it happens:** Each frame Arc<RwLock<Frame>> allows only one writer at a time.
**How to avoid:** This is acceptable for Phase 17 (correctness baseline). Each event processes sequentially through ingest(), so only one epoch's fan-out runs at a time. Within a single fan-out, each frame appears at most once in the affected set (HashSet deduplication in InvertedIndex). So no two threads in the same scope will contend on the same frame.
**Warning signs:** Deadlock or excessive latency in stress tests.

### Pitfall 3: Epoch Ordering in Rematerialize
**What goes wrong:** `rematerialize(graph, epoch)` calls `evict()` then `materialize(graph, epoch)`. The evict clears ALL state including historical tuples. After re-materialize, the frame only contains tuples asserted at the new epoch. Historical snapshots are lost.
**Why it happens:** `evict()` replaces the DiffCollection with a fresh one. This is by design for the re-diff baseline.
**How to avoid:** Accept that the re-diff baseline loses temporal history on each re-traverse. The oracle test should compare current_state() only, not historical snapshots. Temporal snapshot preservation is a future optimization concern.
**Warning signs:** Tests asserting snapshot(old_epoch) fail after re-traverse.

### Pitfall 4: Coalescer/FanOut Interaction with Frame Maintenance
**What goes wrong:** When coalescer is active, events are batched before frame evaluation. The frame maintenance must still happen for every batch that reaches evaluation, not just every raw event.
**Why it happens:** The coalescer deduplicates same-node mutations within an epoch window, then flushes a batch. The existing code already handles this in the `should_evaluate` branch.
**How to avoid:** Wire frame maintenance in the same code path that currently does tier1_check -- both the normal (no coalescer) path and the coalescer flush path. Also update `flush_coalescer()` method similarly.
**Warning signs:** Frame state diverges from oracle when coalescer is enabled.

### Pitfall 5: Newly Promoted Embryonic Frames Not Maintained
**What goes wrong:** Step 5 of ingest auto-promotes embryonic candidates to new frames and calls materialize(). These frames are correctly materialized at creation but won't be maintained by subsequent events unless they're in the inverted index.
**Why it happens:** Promoted frames are registered in the index after creation, so they WILL be routed to by future events. No additional work needed for Phase 17.
**How to avoid:** Verify that auto-promoted frames appear in InvertedIndex routing for subsequent events via a test.
**Warning signs:** Oracle check fails on auto-promoted frames after subsequent mutations.

### Pitfall 6: DiffCollection Tuple Growth Under Re-Traverse Baseline
**What goes wrong:** Each `rematerialize()` calls `evict()` (clears) then `materialize()` (re-asserts all paths). This means the DiffCollection is rebuilt from scratch each time. Tuple count stays bounded by the number of current paths, not growing with the number of events.
**Why it happens:** `evict()` resets the DiffCollection to empty before `materialize()` adds fresh tuples.
**How to avoid:** This is actually a benefit of the re-diff baseline -- no unbounded growth. However, compaction becomes irrelevant (no historical tuples to compact). Tests should not assert on compaction behavior during re-diff baseline operation.
**Warning signs:** If tuple count grows unboundedly, the evict() call is being skipped.

## Code Examples

### Full Ingest Pipeline with Frame Maintenance (Phase 17 Target)
```rust
// In Engine::ingest(), Step 4 replacement:
// Currently at engine.rs lines 362-382

let graph_ref = &self.graph;
let delta_updates: Vec<(u64, i64)> = std::thread::scope(|s| {
    let handles: Vec<_> = affected_frames
        .iter()
        .map(|(frame_id, frame_arc)| {
            let fid = *frame_id;
            let arc = Arc::clone(frame_arc);
            s.spawn(move || {
                // PHASE 17: Write lock + rematerialize instead of read-only tier1
                let mut frame = arc.write().expect("RwLock poisoned");
                frame.rematerialize(graph_ref, epoch);
                let previous = prev_deltas.get(&fid).copied().unwrap_or(0);
                let current = frame.net_delta();
                let _changed = tier1_check(previous, current);
                (fid, current)
            })
        })
        .collect();

    handles.into_iter()
        .map(|h| h.join().expect("Scoped thread panicked"))
        .collect()
});
```

### Oracle Test Harness
```rust
#[cfg(test)]
fn oracle_check(
    engine_frame: &Arc<RwLock<Frame>>,
    graph: &Graph,
    epoch: Epoch,
) {
    let mut frame = engine_frame.write().expect("RwLock poisoned");

    // Build a fresh reference frame from scratch
    let mut reference = Frame::new(
        u64::MAX,
        frame.anchor(),
        frame.pattern().to_vec(),
    );
    reference.materialize(graph, epoch);

    // Compare current states as unordered sets
    let maintained: HashSet<Vec<NodeId>> =
        frame.query().into_iter().cloned().collect();
    let expected: HashSet<Vec<NodeId>> =
        reference.query().into_iter().cloned().collect();

    assert_eq!(
        maintained, expected,
        "Oracle mismatch at epoch {:?}: maintained={:?}, expected={:?}",
        epoch, maintained, expected,
    );
}
```

### Multi-Mutation Oracle Test Scenario
```rust
#[test]
fn test_rediff_oracle_edge_added_then_removed() {
    let mut engine = Engine::new(64);

    // Build initial graph
    engine.ingest(Event::NodeAdded { node_id: NodeId(1), type_id: TypeId(10) });
    engine.ingest(Event::NodeAdded { node_id: NodeId(2), type_id: TypeId(20) });
    engine.ingest(Event::NodeAdded { node_id: NodeId(3), type_id: TypeId(30) });

    let e1 = engine.ingest(Event::EdgeAdded {
        edge_id: EdgeId(0), source: NodeId(1), target: NodeId(2), type_id: TypeId(100),
    });

    // Register frame: anchor=1, pattern=1-hop outgoing type 100
    let pattern = vec![HopSpec {
        direction: Direction::Outgoing,
        edge_type: Some(TypeId(100)),
        target_type: Some(TypeId(20)),
        filter: Filter::None,
    }];
    let fid = engine.register_frame(NodeId(1), pattern, e1);

    // Oracle check after registration
    // (need graph access -- either expose Engine::graph() or check via query_frame)

    // Add another edge that matches the pattern
    let e2 = engine.ingest(Event::EdgeAdded {
        edge_id: EdgeId(1), source: NodeId(1), target: NodeId(3), type_id: TypeId(100),
    });
    // Frame should now have 2 paths (if NodeId(3) matches TypeId(20))
    // Oracle check here

    // Remove the first edge
    let e3 = engine.ingest(Event::EdgeRemoved {
        edge_id: EdgeId(0), source: NodeId(1), target: NodeId(2),
    });
    // Frame should now have 1 path (or 0 depending on types)
    // Oracle check here
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Full DFS on every query | Pre-materialized frames with DFS at registration | v1.0 | Zero query-time traversal |
| No post-registration maintenance | **Phase 17: Full re-traverse on every mutation** | v3.0 (this phase) | Frames stay in sync with graph; enables incremental phases |
| Full re-traverse baseline | Incremental path extension | v3.0 Phase 18-20 (future) | O(affected) instead of O(full_DFS) |

**Deprecated/outdated:**
- Read-only tier1_check without frame state update: After Phase 17, this pattern is replaced by write-lock rematerialize + tier1_check.

## Open Questions

1. **Graph accessor for oracle tests**
   - What we know: Engine does not expose `&self.graph` publicly. Oracle tests need graph access to build reference frames.
   - What's unclear: Should we add `pub fn graph(&self) -> &Graph` to Engine, or should oracle tests use a different approach (e.g., building graph + frames independently of Engine)?
   - Recommendation: Add a `#[cfg(test)] pub fn graph(&self) -> &Graph` accessor or a `pub(crate)` accessor. This is minimal API surface for testing correctness. Alternatively, use `query_frame()` output compared against independently-constructed reference frames.

2. **Should flush_coalescer also maintain frames?**
   - What we know: `flush_coalescer()` (engine.rs line 634) duplicates the fan-out logic but also only does tier1_check. Phase 17 needs to update this path too.
   - What's unclear: Whether the flush path is exercised in production or only in tests.
   - Recommendation: Update flush_coalescer to also rematerialize affected frames for consistency. Extract common maintenance logic into a shared helper to avoid duplication.

3. **Performance impact of full re-traverse on every event**
   - What we know: This is O(full_DFS) per affected frame per event. For complex patterns (3+ hops) on dense graphs, this is expensive.
   - What's unclear: Whether existing stress tests (50K events/sec) will still pass with re-traverse enabled.
   - Recommendation: Accept the performance regression in Phase 17 as the correctness baseline. Phase 21 benchmarks will quantify the cost and validate that Phases 18-20 incremental improvements restore performance. Existing stress tests may need adjusted throughput expectations or may need to be marked as Phase 21 concerns.

4. **Edge ID management for oracle tests**
   - What we know: Engine does not auto-assign EdgeIds -- they come from the Event. Graph::add_edge auto-assigns via next_edge_id. The Event::EdgeAdded carries an edge_id but Engine::ingest calls graph.add_edge(source, target, type_id) which uses the graph's auto-incrementing counter, ignoring the Event's edge_id.
   - What's unclear: Whether EdgeRemoved using the Event's edge_id matches the graph's auto-assigned edge_id.
   - Recommendation: Verify edge_id consistency in oracle tests. The graph's add_edge returns an EdgeId, but ingest() ignores the Event's edge_id for graph mutation. EdgeRemoved uses `graph.remove_edge(*edge_id)` from the Event -- this could be a bug if IDs don't match. Investigate during implementation.

## Sources

### Primary (HIGH confidence)
- `src/engine.rs` lines 255-475 -- Full ingest pipeline, showing read-only tier1_check pattern
- `src/frame.rs` lines 128-252 -- Frame::materialize, rematerialize, apply_delta, evict implementations
- `src/diff.rs` lines 63-213 -- DiffCollection assert/retract/compact/snapshot/current_state
- `src/routing.rs` lines 56-241 -- InvertedIndex affected_frames routing
- `src/types.rs` -- Event enum variants, HopSpec, Filter, all core types

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` -- RDIF-01, RDIF-02, RDIF-03 requirement definitions
- `.planning/STATE.md` -- Project decisions: "No new Cargo dependencies", "PathExtender is stateless module"
- `.planning/PROJECT.md` -- Architecture overview, constraint documentation

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - No new dependencies, all existing APIs verified in source code
- Architecture: HIGH - Pipeline modification point clearly identified at engine.rs lines 362-382, pattern is well-understood wiring change
- Pitfalls: HIGH - Borrow checker, write lock contention, and epoch ordering identified from direct code analysis
- Oracle design: HIGH - Frame::rematerialize and DiffCollection::current_state provide exact comparison primitives

**Research date:** 2026-02-26
**Valid until:** 2026-03-26 (stable -- no external dependencies, all internal code)
