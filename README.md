# Krabnet

Streaming graph runtime with differential MVCC and pre-materialized traversals.

## Overview

Krabnet is a real-time graph processing engine designed for AI agent context systems. When a signal arrives -- a new edge, a property change, a node removal -- decision-relevant context is already materialized. There is zero query-time graph traversal because frame evaluation happens incrementally at ingest time, not at read time.

The core insight is differential math: every mutation generates +1/-1 deltas that propagate through pre-computed traversal frames. These deltas are mathematically exact, meaning incremental maintenance produces identical results to full recomputation. Frames act as parked traversers that stay current as the graph evolves, providing O(1) reads and O(affected) writes.

## Architecture

### Module DAG

Modules are listed in compilation order. Each module depends only on modules listed above it.

| Module | Description |
|--------|-------------|
| `types` | Shared newtypes and enums (NodeId, EdgeId, Epoch, Event, HopSpec, etc.) |
| `interner` | Bidirectional string-to-u32 interning for zero-allocation hot path |
| `sequencer` | Global monotonic epoch sequencer using AtomicU64 |
| `ring_buffer` | Lock-free pre-allocated ring buffer for event ingestion |
| `graph` | In-memory property graph with adjacency-on-node storage |
| `diff` | Differential MVCC collection with +1/-1 multiset math |
| `frame` | Parked traversers with multi-hop DFS materialization |
| `routing` | Inverted index (Set-Trie backed) for O(affected) event-to-frame routing |
| `interpret` | Two-tier signal interpretation (binary delta + structural analysis) |
| `tiering` | Adaptive frame priority scoring and tier recommendation |
| `embryonic` | Embryonic frame discovery with bitvec completion tracking and learned weighting |
| `coalescer` | Mutation coalescing with epoch-window deduplication |
| `fanout` | Fan-out limiting with priority-based deferred evaluation |
| `set_trie` | Set-Trie for O(\|pattern\|) set containment/intersection queries |
| `count_min_sketch` | Count-Min Sketch for probabilistic frequency counting |
| `trunk` | Trunk/leaf detection for identifying structural spines across frames |
| `buffer_pool` | Custom buffer pool with graph-aware eviction ordering |
| `compaction` | Background compaction with double-buffering |
| `wal` | Write-ahead log for crash recovery with binary event persistence |
| `engine` | Top-level orchestrator wiring all components into a single pipeline |
| `grpc` | gRPC server with 8 RPC methods wrapping the engine |
| `mcp` | MCP JSON-RPC 2.0 server with 5 tools over stdio |
| `tier3` | Tier 3 LLM integration with bounded channel and mock client |

### Core Pipeline

1. **Event ingestion** via ring buffer with monotonic epoch assignment
2. **Graph mutation** applied to in-memory property graph
3. **Inverted index** (Set-Trie backed) routes to affected frames in O(affected)
4. **Parallel frame evaluation** via scoped threads
5. **Two-tier interpretation:** binary delta check followed by structural analysis
6. **Adaptive tiering:** priority scoring with hysteresis for tier recommendation

### v2.0 Features

- Background compaction with double-buffering
- Mutation coalescing with epoch-window deduplication
- Fan-out limiting with priority-based deferred evaluation
- gRPC server (8 RPCs) + MCP server (5 tools)
- Tier 3 LLM integration with bounded channel
- Write-ahead log for crash recovery
- Set-Trie inverted index for efficient set containment queries
- Count-Min Sketch for probabilistic frequency counting
- Trunk/leaf detection with Hot pinning
- Custom buffer pool with graph-aware eviction
- Learned template weighting for embryonic discovery

## Building

```
cargo build --release
```

## Running

### gRPC Server

```
cargo run --release --bin krabnet-server
```

### MCP Server (AI Agent Interface)

```
cargo run --release --bin krabnet-mcp
```

## Testing

```
cargo test
```

## Benchmarks

```
cargo bench
```

Enterprise-scale benchmarks exercise 100K nodes, 1M edges, and 500 frames to validate production-level performance.

## License

[TBD]
