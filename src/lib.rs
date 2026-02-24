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

pub mod interner;
pub mod types;

// Re-export core types for ergonomic use: `use krabnet::*`
pub use interner::Interner;
pub use types::*;
