# Phase 21: Performance and Benchmarks - Research

**Researched:** 2026-02-27
**Domain:** Performance verification, Criterion benchmarking, stress testing for incremental path extension
**Confidence:** HIGH

## Summary

Phase 21 is the capstone of the v3.0 milestone. It verifies that the incremental path extension work (Phases 17-20) delivers on the O(affected_paths) scaling promise, establishes Criterion benchmarks comparing incremental vs full re-traverse for EdgeAdded and EdgeRemoved events, validates correctness under sustained high-throughput stress with concurrent compaction, and confirms zero regressions across all 243 lib tests and 54+ doc-tests.

The codebase already has substantial benchmark infrastructure: `benches/krabnet_bench.rs` contains 13 Criterion benchmarks from Phases 10 and 13, including `setup_engine()` (100 nodes, 200 edges, 5 frames) and `setup_scale_engine()` (100K nodes, 1M edges, 50 frames). The existing `test_sustained_throughput` stress test (engine.rs line 1960, `#[ignore]`) ingests 500K events and asserts >50K events/sec with compaction. Phase 21 needs to ADD new benchmarks and tests focused specifically on incremental vs full-DFS comparison, not replace existing ones.

The key technical challenge is designing benchmarks that isolate incremental path extension cost from the rest of the ingest pipeline (ring buffer push, graph mutation, routing lookup, embryonic observation). The approach is to benchmark `extend_edge_added` and `retract_edge_removed` directly as standalone functions, and then also benchmark the full `ingest()` pipeline to show end-to-end benefit. For O(affected) scaling proof, a parameterized benchmark at multiple graph scales (small, medium, large) with a localized mutation must show that incremental latency stays approximately constant while full re-traverse grows linearly with graph size.

**Primary recommendation:** Add 4-5 new Criterion benchmarks to `benches/krabnet_bench.rs` (incremental EdgeAdded vs rematerialize, incremental EdgeRemoved vs rematerialize, parameterized scaling proof) and a new stress test to engine.rs that validates incremental correctness under sustained load with concurrent compaction. Run `cargo test --lib` and `cargo test --doc` to confirm zero regressions.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| PERF-01 | Incremental extension cost is O(affected_paths) not O(full_DFS) for localized mutations | A parameterized Criterion benchmark at 3+ graph scales (100 nodes, 10K nodes, 100K nodes) with a localized edge addition (touching 1-2 frames with 1-hop patterns). Incremental latency should remain roughly constant (~microseconds) while full re-traverse grows with graph density. Use `criterion::BenchmarkGroup` with `BenchmarkId::new()` for parameterized runs. |
| PERF-02 | Criterion benchmark comparing incremental vs full re-traverse latency for EdgeAdded on multi-hop frames | Two benchmarks: `bench_incremental_edge_added` calls `extend_edge_added()` directly with a multi-hop pattern (2-3 hops), and `bench_rematerialize_edge_added` calls `frame.rematerialize()` on the same frame/graph. Both use `iter_batched` to isolate setup from measurement. The benchmark demonstrates incremental is faster than full re-traverse. |
| PERF-03 | Criterion benchmark for incremental EdgeRemoved latency | Similar paired benchmark: `bench_incremental_edge_removed` calls `retract_edge_removed()` directly, `bench_rematerialize_edge_removed` calls `frame.rematerialize()`. Tests on a multi-hop frame with an edge removal that breaks 1-2 paths. |
| PERF-04 | Stress test validating incremental correctness under sustained 50K+ events/sec | Extend or adapt the existing `test_sustained_throughput` pattern: build a graph with 1K+ nodes, 2K+ edges, 20+ multi-hop frames; ingest 500K+ mixed events (EdgeAdded, EdgeRemoved, PropertyChanged) in a tight loop with compaction enabled; assert >50K events/sec throughput; AND after every N events (e.g., every 10K), run oracle_check on a sample frame to verify incremental correctness hasn't drifted. |
| PERF-05 | All existing 180+ lib tests and 54+ doc-tests continue to pass (no regressions) | Run `cargo test --lib` (expects 243 tests: 241 pass, 2 ignored) and `cargo test --doc` (expects 54+ pass). Phase 21 adds NEW tests/benchmarks but must not break existing ones. Also run `cargo clippy --lib -- -D warnings` for zero lint warnings. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| criterion | 0.5 | Criterion benchmarking framework (already in dev-dependencies) | Industry-standard Rust benchmarking; already used for 13 existing benchmarks |
| std::time::Instant | stable | Wall-clock timing for stress test throughput measurement | Already used in test_sustained_throughput |
| std::collections::HashSet | stable | Oracle check path comparison (already used) | Standard library |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| (no new deps) | - | All work uses existing crate dependencies | No new Cargo dependencies needed |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Criterion BenchmarkGroup for parameterized | Multiple separate bench_functions | BenchmarkGroup produces comparison charts in HTML report; more informative for scaling proof |
| Manual wall-clock in stress test | Criterion for stress test | Stress test needs correctness assertions (oracle checks), not just latency measurement; #[test] is more appropriate |
| iai-callgrind for instruction counts | Criterion for wall-clock latency | iai gives deterministic instruction counts but requires valgrind (not available on Windows); Criterion is already in the project |

**Installation:**
```bash
# No new dependencies -- criterion 0.5 already in Cargo.toml dev-dependencies
```

## Architecture Patterns

### Recommended Project Structure
```
benches/
  krabnet_bench.rs   # MODIFIED: Add 4-5 new benchmarks for incremental vs full re-traverse
src/
  engine.rs          # MODIFIED: Add stress test with oracle verification under sustained load
  path_extender.rs   # EXISTING: Used directly in benchmarks (extend_edge_added, retract_edge_removed)
  frame.rs           # EXISTING: Used for rematerialize baseline comparison
  graph.rs           # EXISTING: Used for graph setup in benchmarks
```

### Pattern 1: Paired Incremental-vs-Rematerialize Benchmark
**What:** For each event type (EdgeAdded, EdgeRemoved), create a paired benchmark that measures the same logical mutation using both the incremental path and full rematerialize. Both benchmarks use identical graph/frame setup so the comparison is fair.
**When to use:** PERF-02 and PERF-03.

**Example:**
```rust
/// Benchmark: incremental EdgeAdded via extend_edge_added (PERF-02).
fn bench_incremental_edge_added(c: &mut Criterion) {
    c.bench_function("incremental_edge_added", |b| {
        b.iter_batched(
            || {
                // Setup: Build graph with multi-hop frame
                let mut graph = Graph::new();
                // ... add nodes and edges ...
                let pattern = vec![/* 2-hop pattern */];
                let mut frame = Frame::new(0, NodeId(1), pattern.clone());
                frame.materialize(&graph, Epoch(1));
                // Add the edge to graph (pre-mutation)
                graph.add_edge(NodeId(50), NodeId(60), TypeId(100));
                (graph, frame, pattern)
            },
            |(graph, frame, pattern)| {
                // Measured: just the incremental path extension
                let _deltas = black_box(extend_edge_added(
                    frame.anchor(), &pattern, &graph,
                    NodeId(50), NodeId(60), TypeId(100),
                ));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: full rematerialize for same EdgeAdded scenario (PERF-02 baseline).
fn bench_rematerialize_edge_added(c: &mut Criterion) {
    c.bench_function("rematerialize_edge_added", |b| {
        b.iter_batched(
            || {
                // Identical setup to incremental benchmark
                let mut graph = Graph::new();
                // ... same nodes and edges ...
                let pattern = vec![/* same 2-hop pattern */];
                let mut frame = Frame::new(0, NodeId(1), pattern);
                frame.materialize(&graph, Epoch(1));
                graph.add_edge(NodeId(50), NodeId(60), TypeId(100));
                (graph, frame)
            },
            |(graph, mut frame)| {
                // Measured: full evict + DFS rematerialize
                black_box(frame.rematerialize(&graph, Epoch(2)));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}
```

### Pattern 2: Parameterized Scaling Benchmark with BenchmarkGroup
**What:** A single benchmark function that takes a graph-size parameter and measures incremental EdgeAdded latency at multiple scales. The graph grows (100, 1K, 10K, 100K nodes) but the mutation is always localized (one edge touching one frame). Criterion's BenchmarkGroup with BenchmarkId produces a comparison chart showing that incremental latency stays flat.
**When to use:** PERF-01 (O(affected) scaling proof).

**Example:**
```rust
fn bench_incremental_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_scaling");

    for &node_count in &[100u64, 1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("incremental_edge_added", node_count),
            &node_count,
            |b, &n| {
                b.iter_batched(
                    || {
                        // Build graph with n nodes, ~2n edges, 1 frame
                        let mut graph = Graph::new();
                        for i in 1..=n {
                            graph.add_node(NodeId(i), TypeId(10));
                        }
                        for i in 1..n {
                            graph.add_edge(NodeId(i), NodeId(i + 1), TypeId(100));
                        }
                        // Register 1-hop frame at anchor=1
                        let pattern = vec![HopSpec {
                            direction: Direction::Outgoing,
                            edge_type: Some(TypeId(100)),
                            target_type: None,
                            filter: Filter::None,
                        }];
                        let mut frame = Frame::new(0, NodeId(1), pattern.clone());
                        frame.materialize(&graph, Epoch(1));
                        // Add new edge at anchor
                        let new_target = NodeId(n + 1);
                        graph.add_node(new_target, TypeId(10));
                        graph.add_edge(NodeId(1), new_target, TypeId(100));
                        (graph, frame, pattern)
                    },
                    |(graph, frame, pattern)| {
                        black_box(extend_edge_added(
                            frame.anchor(), &pattern, &graph,
                            NodeId(1), NodeId(graph.node_count() as u64), TypeId(100),
                        ));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Also benchmark full rematerialize at same scale for comparison
        group.bench_with_input(
            BenchmarkId::new("full_rematerialize", node_count),
            &node_count,
            |b, &n| {
                b.iter_batched(
                    || {
                        // Identical setup
                        // ...
                        (graph, frame)
                    },
                    |(graph, mut frame)| {
                        black_box(frame.rematerialize(&graph, Epoch(2)));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}
```

### Pattern 3: Stress Test with Oracle Verification
**What:** A long-running test (marked `#[ignore]`) that ingests 500K+ mixed events through the full engine pipeline with compaction enabled, checks throughput exceeds 50K events/sec, and periodically runs `oracle_check` to verify incremental correctness hasn't drifted. This is the PERF-04 requirement.
**When to use:** As a comprehensive correctness+performance gate.

**Example:**
```rust
#[test]
#[ignore]
fn test_incremental_stress_with_oracle() {
    let mut engine = Engine::with_config(1024, Some(5000), Some(16), Some(1000));

    // Build initial graph: 1K nodes, 2K edges, 20 multi-hop frames
    // ... setup code ...

    let start = std::time::Instant::now();
    let event_count = 500_000u64;

    for i in 0..event_count {
        // Mix of EdgeAdded (60%), EdgeRemoved (20%), PropertyChanged (20%)
        match i % 5 {
            0 | 1 | 2 => { /* EdgeAdded */ }
            3 => { /* EdgeRemoved */ }
            4 => { /* PropertyChanged */ }
            _ => unreachable!()
        }

        // Oracle check every 10K events on first frame
        if i % 10_000 == 0 && i > 0 {
            oracle_check(&mut engine, first_frame_id);
        }
    }

    let elapsed_secs = start.elapsed().as_secs_f64();
    let events_per_sec = event_count as f64 / elapsed_secs;
    assert!(events_per_sec > 50_000.0, ...);

    // Final oracle check on all frames
    for fid in frame_ids {
        oracle_check(&mut engine, fid);
    }
}
```

### Pattern 4: Regression Gate via cargo test
**What:** Simply running `cargo test --lib` and `cargo test --doc` and `cargo clippy --lib -- -D warnings` after all changes are complete. This is PERF-05 -- purely a verification step, no new code required.
**When to use:** At the end of the phase, as the final gate.

### Anti-Patterns to Avoid
- **Benchmarking setup cost:** Use `iter_batched` with `SmallInput` or `LargeInput` to separate graph construction from measurement. Never include graph building in the measured section.
- **Non-deterministic benchmarks:** Use deterministic node IDs and edge patterns (no randomness). Criterion needs stable measurements across runs.
- **Benchmarking the full pipeline when you want just path extension:** For PERF-01/02/03, benchmark `extend_edge_added()` and `retract_edge_removed()` as standalone functions, not `engine.ingest()`. The ingest pipeline includes ring buffer, graph mutation, routing, embryonic, compaction checks -- none of which are relevant to incremental path extension cost.
- **Stress test without oracle checks:** A throughput-only stress test (like existing `test_sustained_throughput`) proves speed but not correctness. PERF-04 explicitly requires "validating incremental correctness under sustained load." The oracle check is essential.
- **Modifying existing benchmarks/tests:** Phase 21 ADDS new benchmarks. It must NOT modify existing ones (that would risk regressions).
- **Using LargeInput for small benchmarks:** Only use `LargeInput` when setup is genuinely expensive (100K+ nodes). For the paired incremental/rematerialize benchmarks with ~100 nodes, use `SmallInput`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Benchmark timing | Manual Instant::now() wrapping | Criterion's `bench_function` / `iter_batched` | Statistical analysis, outlier detection, HTML reports |
| Parameterized benchmarks | Multiple copy-pasted bench functions | `BenchmarkGroup` + `BenchmarkId::new()` | Generates comparison charts, reduces code duplication |
| Correctness verification | Manual path comparison in stress test | Existing `oracle_check()` (engine.rs:2483) | Battle-tested across 21 oracle test scenarios |
| Graph/frame setup | New setup functions | Extend existing `setup_engine()` / `setup_scale_engine()` patterns | Consistent with existing benchmarks |
| Full-DFS baseline | Custom DFS traversal | `frame.rematerialize(&graph, epoch)` | Exact same code path the engine uses for non-incremental maintenance |

**Key insight:** Phase 21 is a MEASUREMENT phase, not an implementation phase. All the incremental algorithms already exist and are oracle-verified. The only new code is benchmark harnesses and a stress test. The risk is in benchmark design (measuring the right thing), not in algorithmic correctness.

## Common Pitfalls

### Pitfall 1: Measuring Setup Instead of Operation
**What goes wrong:** The benchmark includes graph construction, frame registration, and edge addition in the measured section, making the incremental path extension cost invisible relative to setup overhead.
**Why it happens:** Using `bench_function` without `iter_batched`, or including `graph.add_edge()` inside the measured lambda.
**How to avoid:** Use `iter_batched` with setup closure that builds the graph AND pre-adds the edge. The measured closure only calls `extend_edge_added()` or `frame.rematerialize()`. The graph mutation is part of setup because by Phase 17+, the engine applies graph mutation in Step 2 before calling path extension in Step 4.
**Warning signs:** Incremental and rematerialize show similar latency even though they should differ by 10-100x.

### Pitfall 2: Scaling Benchmark with Non-Localized Mutation
**What goes wrong:** The "localized mutation" at large scale actually touches many frames because the InvertedIndex routes the event to hundreds of frames, making incremental cost appear to scale with graph size.
**Why it happens:** Using `engine.ingest()` for the scaling proof, which includes routing and multi-frame evaluation.
**How to avoid:** For PERF-01, benchmark `extend_edge_added()` directly as a standalone function with a single frame. This isolates the path extension cost from routing fan-out. The frame's pattern and the mutation should be designed so only 1-2 new paths are generated regardless of graph size.
**Warning signs:** Incremental latency grows linearly with graph size.

### Pitfall 3: Stress Test Edge Removal on Non-Existent Edges
**What goes wrong:** The stress test randomly generates EdgeRemoved events with edge IDs that don't exist in the graph, causing graph.remove_edge() to silently no-op and the InvertedIndex to return empty affected sets. The test appears fast but isn't exercising incremental path extension at all.
**Why it happens:** Using random or sequential edge IDs without tracking which edges actually exist.
**How to avoid:** Track edge IDs that have been successfully added and only remove edges that exist. Use a ring of recent edge IDs: when generating EdgeRemoved, pop from the ring of recently-added edges.
**Warning signs:** Zero affected frames for EdgeRemoved events; oracle_check shows no delta changes.

### Pitfall 4: Multi-Hop Frame Benchmark with Empty Results
**What goes wrong:** The multi-hop frame's pattern doesn't match any paths in the benchmark graph, so both incremental and rematerialize produce zero paths. The benchmark measures overhead only, not actual path computation cost.
**Why it happens:** Pattern requires specific node types or edge types that the benchmark graph doesn't have, or the graph topology doesn't support multi-hop paths from the anchor.
**How to avoid:** Verify the benchmark frame actually has materialized paths after setup. Add an assertion in the setup closure: `assert!(frame.query().len() > 0, "Benchmark frame must have paths")`.
**Warning signs:** Both benchmarks complete in nanoseconds (instead of microseconds).

### Pitfall 5: Compaction Race in Stress Test
**What goes wrong:** The background compaction worker compacts a frame while the stress test is running oracle_check, causing a race condition where the oracle sees different state than expected.
**Why it happens:** Compaction evicts and reinserts tuples, temporarily changing the DiffCollection state. The oracle_check takes a snapshot at Epoch::MAX which should be stable, but if compaction is actively modifying the frame, the read lock might block.
**How to avoid:** The existing RwLock on Frame handles this correctly -- compaction takes a write lock, oracle_check takes a read lock (via query_frame). The oracle will block until compaction finishes, then see consistent state. This is a non-issue in practice, but be aware that oracle_check latency may vary during active compaction. Use `Engine::with_config()` with a high compaction threshold (e.g., 10000) to reduce compaction frequency during the stress test.
**Warning signs:** Intermittent oracle failures during stress test (should not happen with correct locking).

### Pitfall 6: Criterion BenchmarkGroup with Missing `group.finish()`
**What goes wrong:** The BenchmarkGroup is created but `group.finish()` is never called, causing Criterion to panic or not produce the comparison chart.
**Why it happens:** Early return or forgetting the `group.finish()` call.
**How to avoid:** Always call `group.finish()` at the end of the benchmark function. This is required by Criterion's API.
**Warning signs:** Panic in benchmark runner.

### Pitfall 7: stress test using only EdgeAdded events
**What goes wrong:** The stress test only ingests EdgeAdded events, never exercising EdgeRemoved or PropertyChanged incremental paths. PERF-04 requires validating "incremental correctness under sustained load" -- all event types must be exercised.
**Why it happens:** Simplifying the event mix to avoid edge tracking complexity.
**How to avoid:** Mix event types: ~60% EdgeAdded, ~20% EdgeRemoved (from recently-added edges), ~20% PropertyChanged. This exercises all four incremental dispatch paths (EdgeAdded, EdgeRemoved, NodeRemoved is harder to test in stress because it destroys graph structure -- skip it in the stress test or use it sparingly).
**Warning signs:** Oracle check passes but only because no retraction paths were ever exercised.

## Code Examples

### Criterion BenchmarkGroup for Scaling Proof (PERF-01)

```rust
use criterion::{BenchmarkGroup, BenchmarkId, measurement::WallTime};

fn bench_incremental_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_scaling");

    for &node_count in &[100u64, 1_000, 10_000] {
        // Incremental path extension at this scale
        group.bench_with_input(
            BenchmarkId::new("extend_edge_added", node_count),
            &node_count,
            |b, &n| {
                b.iter_batched(
                    || setup_scaling_graph(n),
                    |(graph, frame, pattern, new_target)| {
                        black_box(extend_edge_added(
                            frame.anchor(), &pattern, &graph,
                            NodeId(1), new_target, TypeId(100),
                        ));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Full rematerialize at this scale (for comparison)
        group.bench_with_input(
            BenchmarkId::new("full_rematerialize", node_count),
            &node_count,
            |b, &n| {
                b.iter_batched(
                    || setup_scaling_graph(n),
                    |(graph, mut frame, _, _)| {
                        black_box(frame.rematerialize(&graph, Epoch(2)));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}
```

### Paired EdgeRemoved Benchmark (PERF-03)

```rust
fn bench_incremental_edge_removed(c: &mut Criterion) {
    c.bench_function("incremental_edge_removed", |b| {
        b.iter_batched(
            || {
                // Build graph with paths, then remove one edge
                let mut graph = Graph::new();
                // ... setup nodes, edges ...
                let pattern = vec![/* 2-hop pattern */];
                let mut frame = Frame::new(0, NodeId(1), pattern.clone());
                frame.materialize(&graph, Epoch(1));
                // Remove the edge from graph
                graph.remove_edge(EdgeId(target_edge));
                let current = frame.snapshot(Epoch(u64::MAX));
                (graph, frame, pattern, current)
            },
            |(graph, frame, pattern, current)| {
                let current_refs: Vec<&Vec<NodeId>> = current.iter().collect();
                black_box(retract_edge_removed(
                    &pattern, &graph, &current_refs,
                    source, target,
                ));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}
```

### Stress Test with Oracle (PERF-04)

```rust
#[test]
#[ignore]
fn test_incremental_stress_with_oracle() {
    let mut engine = Engine::with_config(1024, Some(10_000), Some(16), Some(1000));

    // Build initial graph
    for i in 1..=1000u64 {
        engine.ingest(Event::NodeAdded {
            node_id: NodeId(i),
            type_id: TypeId(10 + (i % 3) as u32),
        });
    }

    let mut edge_id = 0u64;
    let mut active_edges: Vec<(u64, u64, u64)> = Vec::new(); // (edge_id, source, target)

    // Chain + cross-links
    for i in 1..1000u64 {
        engine.ingest(Event::EdgeAdded {
            edge_id: EdgeId(edge_id), source: NodeId(i), target: NodeId(i+1), type_id: TypeId(100),
        });
        active_edges.push((edge_id, i, i+1));
        edge_id += 1;
    }

    // Register 20 multi-hop frames
    let epoch = Epoch(5000);
    let mut frame_ids = Vec::new();
    for anchor in (1..=200u64).step_by(10) {
        let fid = engine.register_frame(NodeId(anchor), two_hop_pattern(), epoch);
        frame_ids.push(fid);
    }

    let start = std::time::Instant::now();
    let event_count = 500_000u64;
    let mut remove_idx = 0usize;

    for i in 0..event_count {
        match i % 5 {
            0 | 1 | 2 => {
                // EdgeAdded
                engine.ingest(Event::EdgeAdded {
                    edge_id: EdgeId(edge_id),
                    source: NodeId((i % 999) + 1),
                    target: NodeId((i % 999) + 2),
                    type_id: TypeId(100),
                });
                active_edges.push((edge_id, (i % 999) + 1, (i % 999) + 2));
                edge_id += 1;
            }
            3 => {
                // EdgeRemoved (from active edges)
                if remove_idx < active_edges.len() {
                    let (eid, src, tgt) = active_edges[remove_idx];
                    engine.ingest(Event::EdgeRemoved {
                        edge_id: EdgeId(eid), source: NodeId(src), target: NodeId(tgt),
                        type_id: TypeId(100),
                    });
                    remove_idx += 1;
                }
            }
            4 => {
                // PropertyChanged
                engine.ingest(Event::PropertyChanged {
                    node_id: NodeId((i % 999) + 1),
                    key: 0,
                    value: PropertyValue::Integer(i as i64),
                });
            }
            _ => unreachable!()
        }

        // Oracle check every 10K events
        if i % 10_000 == 0 && i > 0 {
            oracle_check(&mut engine, frame_ids[0]);
        }
    }

    let elapsed_secs = start.elapsed().as_secs_f64();
    let events_per_sec = event_count as f64 / elapsed_secs;
    assert!(events_per_sec > 50_000.0, "Expected >50K, got {events_per_sec:.0}");

    // Final oracle check on all frames
    for &fid in &frame_ids {
        oracle_check(&mut engine, fid);
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Full re-traverse on every event (Phase 17 baseline) | Incremental dispatch for EdgeAdded/EdgeRemoved/NodeRemoved/PropertyChanged (Phases 18-20) | v3.0 Phases 18-20 (2026-02-26) | O(affected_paths) per event instead of O(full_DFS); Phase 21 proves this empirically |
| No incremental benchmarks | Phase 10/13 benchmarks exist for ingest, query, routing, compaction | v1.0/v2.0 (2026-02-24) | Good baseline infrastructure; Phase 21 adds incremental-specific benchmarks |
| test_sustained_throughput (throughput only) | Phase 21 stress test adds oracle correctness checks under load | v3.0 Phase 21 (this phase) | Throughput + correctness guarantee |

**Deprecated/outdated:**
- After Phase 20, `frame.rematerialize()` is only called for `NodeAdded` events and when `force_rematerialize=true` (coalescer flush). It is no longer the primary maintenance path. Phase 21 benchmarks prove the incremental path is faster.

## Open Questions

1. **100K node scaling benchmark setup time**
   - What we know: `setup_scale_engine()` builds 100K nodes + 1M edges and takes significant time. Using this in a parameterized benchmark with `SmallInput` batch size may be too slow.
   - What's unclear: Whether Criterion's `LargeInput` batch size is sufficient to amortize the setup cost at 100K+ scale.
   - Recommendation: Use `LargeInput` for the 100K data point in the scaling benchmark. If setup is still too slow, cap the scaling proof at 10K nodes (still demonstrates 100x scale difference from 100 nodes, which is sufficient to show O(affected) vs O(full_DFS) divergence).

2. **Multi-hop frame design for benchmarks**
   - What we know: The scaling proof works best with a simple 1-hop frame (localized mutation affects exactly 1 path). But PERF-02/03 require "multi-hop frames."
   - What's unclear: How deep the multi-hop pattern should be (2 hops? 3 hops?) and how many paths the benchmark frame should have.
   - Recommendation: Use 2-hop patterns for PERF-02/03 paired benchmarks. The frame should have 5-20 materialized paths (enough to be meaningful but not so many that rematerialize is trivially fast). Design the graph topology so the edge addition creates 1-3 new 2-hop paths.

3. **Stress test event mix ratio**
   - What we know: The existing `test_sustained_throughput` uses 33% PropertyChanged + 67% EdgeAdded. PERF-04 needs to also exercise EdgeRemoved for full incremental coverage.
   - What's unclear: Whether including EdgeRemoved will significantly slow throughput (since it requires edge tracking).
   - Recommendation: Use 60% EdgeAdded, 20% EdgeRemoved (from a ring buffer of recently-added edges), 20% PropertyChanged. Skip NodeRemoved in the stress test since it destroys graph topology and makes oracle checks unreliable over 500K events.

4. **Oracle check frequency in stress test**
   - What we know: oracle_check builds a fresh Frame and does full DFS, which is expensive at scale. Checking every event would make the test take hours.
   - What's unclear: What interval balances coverage with performance.
   - Recommendation: Oracle check every 10,000 events (50 checks over 500K events). This catches drift without dominating runtime. Also do a final oracle check on ALL frames at the end.

## Sources

### Primary (HIGH confidence)
- `benches/krabnet_bench.rs` -- Existing 13 Criterion benchmarks, setup_engine(), setup_scale_engine() patterns
- `src/engine.rs` lines 807-901 -- maintain_and_evaluate_frames with full incremental dispatch (EdgeAdded, EdgeRemoved, NodeRemoved, PropertyChanged)
- `src/engine.rs` lines 1956-2048 -- Existing test_sustained_throughput stress test pattern
- `src/engine.rs` lines 2475-2513 -- oracle_check() function used across 21 oracle tests
- `src/path_extender.rs` lines 84-179 -- extend_edge_added public API
- `src/path_extender.rs` lines 193-286 -- retract_edge_removed public API
- `src/path_extender.rs` lines 289-302 -- retract_node_removed public API
- `src/path_extender.rs` lines 334-407 -- reevaluate_property_changed public API
- `src/frame.rs` lines 249-252 -- frame.rematerialize() (evict + materialize)
- `Cargo.toml` line 25 -- criterion 0.5 already in dev-dependencies
- Phase 20 verification report -- 241 lib tests pass, 2 ignored, 0 clippy warnings

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` -- PERF-01 through PERF-05 requirement definitions
- `.planning/STATE.md` -- "Backward prefix resolution is O(B^K) per mutation" and "Double-buffer compaction race with incremental writes" concerns
- `.planning/ROADMAP.md` -- Phase 21 success criteria definition
- Previous phase research (18, 19, 20) -- PathExtender API signatures and integration patterns

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - No new dependencies; criterion 0.5 already present with 13 existing benchmarks as proven pattern
- Architecture: HIGH - Benchmark patterns follow existing conventions in krabnet_bench.rs; stress test follows existing test_sustained_throughput pattern; oracle_check is battle-tested across 21 tests
- Pitfalls: HIGH - All pitfalls identified from direct analysis of existing benchmark code, engine.rs ingest pipeline, and path_extender.rs API signatures
- Requirements mapping: HIGH - All 5 PERF requirements have clear implementation strategies with existing infrastructure support

**Research date:** 2026-02-27
**Valid until:** 2026-03-27 (stable -- no external dependencies, all internal code)
