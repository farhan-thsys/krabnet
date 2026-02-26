//! Criterion benchmarks for all critical Krabnet operations.
//!
//! Benchmarks:
//! 1. `bench_ingest_event` -- event ingestion through the full engine pipeline
//! 2. `bench_frame_query` -- querying a materialized frame
//! 3. `bench_inverted_index_lookup` -- O(affected) event-to-frame routing
//! 4. `bench_tier1_check` -- fast binary delta comparison
//! 5. `bench_embryonic_observe` -- embryonic template observation with candidates
//! 6. `bench_compaction` -- compaction of frames with mixed assert/retract tuples
//! 7. `bench_concurrent_ingest` -- concurrent ingest with hardened engine
//! 8. `bench_set_trie_lookup` -- Set-Trie lookup at 1000-set scale (BENCH-03)
//! 9. `bench_hashmap_lookup` -- HashMap lookup baseline for BENCH-03 comparison
//! 10. `bench_scale_ingest` -- enterprise-scale 100K node / 1M edge ingest (BENCH-04)
//! 11. `bench_scale_frame_query` -- enterprise-scale frame query latency (BENCH-05)
//! 12. `bench_scale_set_trie_routing` -- Set-Trie routing at enterprise scale (BENCH-06)
//! 13. `bench_scale_embryonic` -- embryonic observation with 100 templates (BENCH-07)

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use krabnet::*;

use krabnet::embryonic::PatternTemplate;
use krabnet::interpret::tier1_check;
use krabnet::routing::InvertedIndex;

/// Creates a pre-populated engine with ~100 nodes, ~200 edges, 5 registered
/// frames, and 2 embryonic templates. Returns the engine and the last epoch.
fn setup_engine() -> (engine::Engine, Epoch) {
    let mut eng = Engine::new(1024);

    // Add 100 nodes with alternating types (10, 20, 30).
    for i in 1..=100u64 {
        let type_id = match i % 3 {
            0 => TypeId(30),
            1 => TypeId(10),
            _ => TypeId(20),
        };
        eng.ingest(Event::NodeAdded {
            node_id: NodeId(i),
            type_id,
        });
    }

    // Add ~200 edges: chain edges and cross-links.
    let mut edge_counter = 0u64;
    // Chain: 1->2->3->...->100
    for i in 1..100u64 {
        eng.ingest(Event::EdgeAdded {
            edge_id: EdgeId(edge_counter),
            source: NodeId(i),
            target: NodeId(i + 1),
            type_id: TypeId(100),
        });
        edge_counter += 1;
    }
    // Cross-links: every 5th node connects to node (i + 10) mod 100 + 1
    for i in (1..=100u64).step_by(5) {
        let target = (i + 10 - 1) % 100 + 1;
        if target != i {
            eng.ingest(Event::EdgeAdded {
                edge_id: EdgeId(edge_counter),
                source: NodeId(i),
                target: NodeId(target),
                type_id: TypeId(200),
            });
            edge_counter += 1;
        }
    }

    // Register 5 frames at various anchors.
    let anchors = [NodeId(1), NodeId(10), NodeId(25), NodeId(50), NodeId(75)];
    let epoch = Epoch(edge_counter + 100); // safe epoch past all ingested events
    for anchor in &anchors {
        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: None,
            filter: Filter::None,
        }];
        eng.register_frame(*anchor, pattern, epoch);
    }

    // Register 2 embryonic templates.
    eng.register_template(PatternTemplate {
        id: 1,
        pattern: vec![
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(100)),
                target_type: None,
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(200)),
                target_type: None,
                filter: Filter::None,
            },
        ],
        threshold: 1.0,
        max_candidates: 200,
        stale_window: 50,
        success_count: 0,
        failure_count: 0,
        active: true,
    });
    eng.register_template(PatternTemplate {
        id: 2,
        pattern: vec![
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(200)),
                target_type: None,
                filter: Filter::None,
            },
            HopSpec {
                direction: Direction::Outgoing,
                edge_type: Some(TypeId(100)),
                target_type: None,
                filter: Filter::None,
            },
        ],
        threshold: 1.0,
        max_candidates: 200,
        stale_window: 50,
        success_count: 0,
        failure_count: 0,
        active: true,
    });

    let last_epoch = eng.ingest(Event::NodeAdded {
        node_id: NodeId(200),
        type_id: TypeId(10),
    });

    (eng, last_epoch)
}

/// Creates an enterprise-scale engine with 100K nodes, 1M edges, and registered frames.
///
/// Hardened engine matching production config for realistic benchmarks.
/// Used by BENCH-04 and BENCH-05. Setup is expensive so benchmarks using this
/// should use `LargeInput` batch size.
fn setup_scale_engine() -> (engine::Engine, u64) {
    let mut eng = Engine::with_config(
        2048,         // ring buffer capacity (larger for scale)
        Some(10_000), // compaction threshold
        Some(16),     // coalescing window
        Some(1000),   // max fanout
    );

    // Add 100K nodes with alternating types
    for i in 1..=100_000u64 {
        let type_id = match i % 3 {
            0 => TypeId(30),
            1 => TypeId(10),
            _ => TypeId(20),
        };
        eng.ingest(Event::NodeAdded {
            node_id: NodeId(i),
            type_id,
        });
    }

    // Add 1M edges: 100K chain edges + 900K random cross-links
    let mut edge_counter = 0u64;

    // Chain: 1->2->3->...->100000
    for i in 1..100_000u64 {
        eng.ingest(Event::EdgeAdded {
            edge_id: EdgeId(edge_counter),
            source: NodeId(i),
            target: NodeId(i + 1),
            type_id: TypeId(100),
        });
        edge_counter += 1;
    }

    // Cross-links: ~900K additional edges using modular arithmetic for determinism
    for i in 0..900_000u64 {
        let source = (i % 99_999) + 1;
        let target = ((i * 7 + 13) % 99_999) + 1;
        if source != target {
            eng.ingest(Event::EdgeAdded {
                edge_id: EdgeId(edge_counter),
                source: NodeId(source),
                target: NodeId(target),
                type_id: if i % 3 == 0 {
                    TypeId(200)
                } else {
                    TypeId(100)
                },
            });
            edge_counter += 1;
        }
    }

    // Register 50 frames at various anchors
    let epoch = Epoch(edge_counter + 200);
    for i in 0..50u64 {
        let anchor = NodeId(i * 2000 + 1);
        let pattern = vec![HopSpec {
            direction: Direction::Outgoing,
            edge_type: Some(TypeId(100)),
            target_type: None,
            filter: Filter::None,
        }];
        eng.register_frame(anchor, pattern, epoch);
    }

    (eng, edge_counter)
}

/// Benchmark: ingest an EdgeAdded event through the full engine pipeline.
fn bench_ingest_event(c: &mut Criterion) {
    c.bench_function("ingest_event", |b| {
        b.iter_batched(
            setup_engine,
            |(mut eng, _epoch)| {
                eng.ingest(black_box(Event::EdgeAdded {
                    edge_id: EdgeId(9999),
                    source: NodeId(10),
                    target: NodeId(50),
                    type_id: TypeId(100),
                }));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: query a materialized frame by ID.
fn bench_frame_query(c: &mut Criterion) {
    c.bench_function("frame_query", |b| {
        b.iter_batched(
            setup_engine,
            |(mut eng, _epoch)| {
                // Frame 0 was the first registered frame.
                let _ = black_box(eng.query_frame(0));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: inverted index lookup for affected frames.
fn bench_inverted_index_lookup(c: &mut Criterion) {
    // Build an index with multiple registered frames.
    let mut index = InvertedIndex::new();
    for fid in 0..50u64 {
        let nodes: Vec<NodeId> = (fid * 10..fid * 10 + 10).map(NodeId).collect();
        index.register_frame(fid, &nodes, &[]);
    }

    let event = Event::EdgeAdded {
        edge_id: EdgeId(0),
        source: NodeId(5),
        target: NodeId(15),
        type_id: TypeId(100),
    };

    c.bench_function("inverted_index_lookup", |b| {
        b.iter(|| {
            black_box(index.affected_frames(black_box(&event)));
        });
    });
}

/// Benchmark: tier1_check with varying deltas.
fn bench_tier1_check(c: &mut Criterion) {
    let deltas: Vec<(i64, i64)> = (0..100).map(|i| (i, i + 1)).collect();

    c.bench_function("tier1_check", |b| {
        b.iter(|| {
            for &(prev, curr) in black_box(&deltas) {
                black_box(tier1_check(prev, curr));
            }
        });
    });
}

/// Benchmark: embryonic observe_edge with registered templates and existing
/// candidates.
fn bench_embryonic_observe(c: &mut Criterion) {
    c.bench_function("embryonic_observe", |b| {
        b.iter_batched(
            || {
                let mut disco = EmbryonicDiscovery::new();
                disco.register_template(PatternTemplate {
                    id: 1,
                    pattern: vec![
                        HopSpec {
                            direction: Direction::Outgoing,
                            edge_type: Some(TypeId(100)),
                            target_type: None,
                            filter: Filter::None,
                        },
                        HopSpec {
                            direction: Direction::Outgoing,
                            edge_type: Some(TypeId(200)),
                            target_type: None,
                            filter: Filter::None,
                        },
                    ],
                    threshold: 1.0,
                    max_candidates: 500,
                    stale_window: 100,
                    success_count: 0,
                    failure_count: 0,
                    active: true,
                });
                // Pre-populate some candidates by observing edges.
                for i in 0..50u64 {
                    disco.observe_edge(NodeId(i), NodeId(i + 100), TypeId(100), Epoch(i));
                }
                disco
            },
            |mut disco| {
                black_box(disco.observe_edge(
                    black_box(NodeId(999)),
                    black_box(NodeId(1000)),
                    black_box(TypeId(100)),
                    black_box(Epoch(999)),
                ));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: compaction of frames with mixed assert/retract tuples.
fn bench_compaction(c: &mut Criterion) {
    c.bench_function("compaction", |b| {
        b.iter_batched(
            || {
                let mut eng = Engine::new(1024);
                // Build a small graph.
                for i in 1..=20u64 {
                    eng.ingest(Event::NodeAdded {
                        node_id: NodeId(i),
                        type_id: TypeId(10),
                    });
                }
                for i in 1..20u64 {
                    eng.ingest(Event::EdgeAdded {
                        edge_id: EdgeId(i),
                        source: NodeId(i),
                        target: NodeId(i + 1),
                        type_id: TypeId(100),
                    });
                }
                // Register frames.
                for anchor in 1..=10u64 {
                    let pattern = vec![HopSpec {
                        direction: Direction::Outgoing,
                        edge_type: Some(TypeId(100)),
                        target_type: None,
                        filter: Filter::None,
                    }];
                    eng.register_frame(NodeId(anchor), pattern, Epoch(50));
                }
                eng
            },
            |mut eng| {
                eng.compact_all(black_box(Epoch(50)));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: concurrent ingest with the full hardened engine (compaction enabled).
///
/// Measures throughput of 100 event ingestions through the full pipeline
/// with compaction worker active. Uses `iter_batched` with `SmallInput`
/// to isolate setup cost from measurement.
fn bench_concurrent_ingest(c: &mut Criterion) {
    c.bench_function("concurrent_ingest", |b| {
        b.iter_batched(
            || {
                // Setup: Create hardened engine with compaction at 5000 tuples
                let mut eng =
                    krabnet::engine::Engine::with_config(1024, Some(5000), None, None);

                // Add 100 nodes
                for i in 1..=100u64 {
                    eng.ingest(Event::NodeAdded {
                        node_id: NodeId(i),
                        type_id: match i % 3 {
                            0 => TypeId(30),
                            1 => TypeId(10),
                            _ => TypeId(20),
                        },
                    });
                }

                // Add 200 edges (chain + cross-links)
                let mut edge_counter = 0u64;
                for i in 1..100u64 {
                    eng.ingest(Event::EdgeAdded {
                        edge_id: EdgeId(edge_counter),
                        source: NodeId(i),
                        target: NodeId(i + 1),
                        type_id: TypeId(100),
                    });
                    edge_counter += 1;
                }
                for i in (1..=100u64).step_by(5) {
                    let target = (i + 10 - 1) % 100 + 1;
                    if target != i {
                        eng.ingest(Event::EdgeAdded {
                            edge_id: EdgeId(edge_counter),
                            source: NodeId(i),
                            target: NodeId(target),
                            type_id: TypeId(200),
                        });
                        edge_counter += 1;
                    }
                }

                // Register 10 frames
                let epoch = Epoch(edge_counter + 100);
                for anchor_val in [1u64, 10, 20, 30, 40, 50, 60, 70, 80, 90] {
                    let pattern = vec![HopSpec {
                        direction: Direction::Outgoing,
                        edge_type: Some(TypeId(100)),
                        target_type: None,
                        filter: Filter::None,
                    }];
                    eng.register_frame(NodeId(anchor_val), pattern, epoch);
                }

                (eng, edge_counter)
            },
            |(mut eng, edge_counter)| {
                // Measured: Ingest 100 events through the full pipeline
                for i in 0..100u64 {
                    eng.ingest(black_box(Event::EdgeAdded {
                        edge_id: EdgeId(edge_counter + i + 1000),
                        source: NodeId((i % 99) + 1),
                        target: NodeId((i % 99) + 2),
                        type_id: TypeId(100),
                    }));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark: Set-Trie lookup with 1000 sets of 10 elements each (BENCH-03).
fn bench_set_trie_lookup(c: &mut Criterion) {
    use krabnet::set_trie::SetTrie;

    let mut trie = SetTrie::new();
    for fid in 0..1000u64 {
        let elements: Vec<u64> = (fid * 10..fid * 10 + 10).collect();
        trie.insert(&elements, fid);
    }

    c.bench_function("set_trie_lookup", |b| {
        b.iter(|| {
            black_box(trie.query_intersecting(black_box(&[5])));
        });
    });
}

/// Benchmark: HashMap posting list lookup equivalent to Set-Trie (BENCH-03).
fn bench_hashmap_lookup(c: &mut Criterion) {
    use std::collections::{HashMap, HashSet};

    let mut map: HashMap<u64, HashSet<u64>> = HashMap::new();
    for fid in 0..1000u64 {
        for elem in fid * 10..fid * 10 + 10 {
            map.entry(elem).or_default().insert(fid);
        }
    }

    c.bench_function("hashmap_lookup", |b| {
        b.iter(|| {
            let result: HashSet<u64> = map
                .get(black_box(&5))
                .cloned()
                .unwrap_or_default();
            black_box(result);
        });
    });
}

// === Enterprise-Scale Benchmarks (BENCH-04 through BENCH-07) ===

/// Benchmark: enterprise-scale ingest throughput (BENCH-04).
///
/// 100K nodes, 1M edges, 50 registered frames. Measures throughput of
/// 1000 new EdgeAdded events through the full pipeline.
fn bench_scale_ingest(c: &mut Criterion) {
    c.bench_function("scale_ingest", |b| {
        b.iter_batched(
            setup_scale_engine,
            |(mut eng, edge_counter)| {
                // Measured: Ingest 1000 new EdgeAdded events
                for i in 0..1000u64 {
                    eng.ingest(black_box(Event::EdgeAdded {
                        edge_id: EdgeId(edge_counter + i + 10_000_000),
                        source: NodeId((i % 99_999) + 1),
                        target: NodeId(((i * 3 + 7) % 99_999) + 1),
                        type_id: TypeId(100),
                    }));
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

/// Benchmark: enterprise-scale frame query latency (BENCH-05).
///
/// 100K nodes, 1M edges, 100 registered frames. Measures latency of
/// querying each of 100 frames.
fn bench_scale_frame_query(c: &mut Criterion) {
    c.bench_function("scale_frame_query", |b| {
        b.iter_batched(
            || {
                let (mut eng, edge_counter) = setup_scale_engine();
                // Register 50 more frames (total 100)
                let epoch = Epoch(edge_counter + 500);
                for i in 50..100u64 {
                    let anchor = NodeId(i * 1000 + 1);
                    let pattern = vec![HopSpec {
                        direction: Direction::Outgoing,
                        edge_type: Some(TypeId(100)),
                        target_type: None,
                        filter: Filter::None,
                    }];
                    eng.register_frame(anchor, pattern, epoch);
                }
                eng
            },
            |mut eng| {
                // Measured: Query each of the 100 frames
                for fid in 0..100u64 {
                    black_box(eng.query_frame(fid));
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

/// Benchmark: Set-Trie routing at enterprise scale (BENCH-06).
///
/// 500 frames with 20 nodes each (10K unique nodes registered).
/// Measures affected_frames for an EdgeAdded event touching a high-fan-out node.
fn bench_scale_set_trie_routing(c: &mut Criterion) {
    // Setup: Build inverted index with 500 frames, 20 nodes each
    let mut index = InvertedIndex::new();
    for fid in 0..500u64 {
        // Each frame covers 20 nodes, with overlap to create high-fan-out nodes
        let nodes: Vec<NodeId> = (0..20u64).map(|j| NodeId(fid % 200 + j * 50)).collect();
        index.register_frame(fid, &nodes, &[]);
    }

    // Event touching a high-fan-out node (NodeId(0) appears in many frames)
    let event = Event::EdgeAdded {
        edge_id: EdgeId(0),
        source: NodeId(0),
        target: NodeId(50),
        type_id: TypeId(100),
    };

    c.bench_function("scale_set_trie_routing", |b| {
        b.iter(|| {
            black_box(index.affected_frames(black_box(&event)));
        });
    });
}

/// Benchmark: embryonic observation at enterprise scale (BENCH-07).
///
/// 100 templates with 2-hop patterns (distinct edge types per template).
/// ~50 candidates per template pre-populated (~5000 total candidates).
/// Measures observe_edge with a new matching edge.
fn bench_scale_embryonic(c: &mut Criterion) {
    c.bench_function("scale_embryonic", |b| {
        b.iter_batched(
            || {
                let mut disco = EmbryonicDiscovery::new();

                // Register 100 templates with distinct 2-hop patterns
                for tid in 0..100u64 {
                    let edge_type_1 = TypeId(1000 + tid as u32);
                    let edge_type_2 = TypeId(2000 + tid as u32);
                    disco.register_template(PatternTemplate {
                        id: tid,
                        pattern: vec![
                            HopSpec {
                                direction: Direction::Outgoing,
                                edge_type: Some(edge_type_1),
                                target_type: None,
                                filter: Filter::None,
                            },
                            HopSpec {
                                direction: Direction::Outgoing,
                                edge_type: Some(edge_type_2),
                                target_type: None,
                                filter: Filter::None,
                            },
                        ],
                        threshold: 1.0,
                        max_candidates: 200,
                        stale_window: 10000,
                        success_count: 0,
                        failure_count: 0,
                        active: true,
                    });
                }

                // Pre-populate ~50 candidates per template by observing edges
                // Each template's first-hop edge type is TypeId(1000 + tid)
                for tid in 0..100u64 {
                    for j in 0..50u64 {
                        disco.observe_edge(
                            NodeId(tid * 1000 + j),
                            NodeId(tid * 1000 + j + 500),
                            TypeId(1000 + tid as u32),
                            Epoch(tid * 100 + j),
                        );
                    }
                }

                disco
            },
            |mut disco| {
                // Measured: observe_edge with an edge matching template 0's first hop
                black_box(disco.observe_edge(
                    black_box(NodeId(999_999)),
                    black_box(NodeId(999_998)),
                    black_box(TypeId(1000)), // matches template 0
                    black_box(Epoch(99_999)),
                ));
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_ingest_event,
    bench_frame_query,
    bench_inverted_index_lookup,
    bench_tier1_check,
    bench_embryonic_observe,
    bench_compaction,
    bench_concurrent_ingest,
    bench_set_trie_lookup,
    bench_hashmap_lookup,
    bench_scale_ingest,
    bench_scale_frame_query,
    bench_scale_set_trie_routing,
    bench_scale_embryonic,
);
criterion_main!(benches);
