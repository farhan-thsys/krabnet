//! Krabnet: a streaming graph runtime with differential MVCC.
//!
//! Pre-materializes graph traversal results for AI agent context systems.
//! When a signal arrives, decision-relevant context is already materialized --
//! zero query-time graph traversal. The differential math (+1/-1 deltas)
//! ensures mathematically exact incremental maintenance of pre-computed
//! traversal results.
//!
//! # Architecture
//!
//! The crate is organized into modules following a strict compilation DAG:
//! - [`types`] -- Shared newtypes and enums used by every module
//! - [`interner`] -- Bidirectional string-to-u32 interning for zero-allocation hot path
//! - [`sequencer`] -- Global monotonic epoch sequencer using AtomicU64
//! - [`ring_buffer`] -- Lock-free pre-allocated ring buffer for event ingestion
//! - [`graph`] -- In-memory property graph with adjacency-on-node storage
//! - [`diff`] -- Differential MVCC collection with +1/-1 multiset math
//! - [`frame`] -- Parked traversers with multi-hop DFS materialization
//! - [`routing`] -- Inverted index for O(affected) event-to-frame routing
//! - [`interpret`] -- Two-tier signal interpretation (binary + structural)
//! - [`tiering`] -- Adaptive frame priority scoring and tier recommendation
//! - [`embryonic`] -- Embryonic frame discovery with bitvec completion tracking
//! - [`coalescer`] -- Mutation coalescing with epoch-window deduplication
//! - [`fanout`] -- Fan-out limiting with priority-based deferred evaluation
//! - [`engine`] -- Top-level orchestrator wiring all components into a single pipeline
//! - [`grpc`] -- gRPC server with 8 RPC methods wrapping the engine
//! - [`mcp`] -- MCP JSON-RPC 2.0 server with 5 tools over stdio
//! - [`tier3`] -- Tier 3 LLM integration with bounded channel and mock client
//! - [`wal`] -- Write-ahead log for crash recovery with binary event persistence

pub mod coalescer;
pub mod compaction;
pub mod diff;
pub mod embryonic;
pub mod engine;
pub mod fanout;
pub mod grpc;
pub mod mcp;
pub mod frame;
pub mod graph;
pub mod interpret;
pub mod interner;
pub mod ring_buffer;
pub mod routing;
pub mod sequencer;
pub mod tier3;
pub mod tiering;
pub mod types;
pub mod wal;

// Re-export core types for ergonomic use: `use krabnet::*`
pub use coalescer::MutationCoalescer;
pub use compaction::CompactionWorker;
pub use diff::DiffCollection;
pub use fanout::FanOutLimiter;
pub use embryonic::EmbryonicDiscovery;
pub use engine::Engine;
pub use grpc::KrabnetServer;
pub use mcp::McpServer;
pub use frame::Frame;
pub use graph::Graph;
pub use interner::Interner;
pub use ring_buffer::RingBuffer;
pub use routing::InvertedIndex;
pub use sequencer::EpochSequencer;
pub use tier3::{LlmClient, MockLlmClient, Tier3Worker};
pub use types::*;
pub use wal::{WalReader, WalWriter};
