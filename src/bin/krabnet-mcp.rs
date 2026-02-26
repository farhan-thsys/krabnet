//! krabnet-mcp: MCP stdio server for the Krabnet engine.
//!
//! Starts an MCP JSON-RPC 2.0 server reading from stdin and writing to stdout.
//! AI agents connect to this binary via stdio transport.
//!
//! # WAL Persistence
//!
//! Set `KRABNET_MCP_WAL` to a file path to enable write-ahead log persistence.
//! Defaults to `krabnet-mcp.wal` if the variable is not set.
//! On startup, replays any existing WAL file to recover state.
//!
//! # Usage
//!
//! ```sh
//! echo '{"jsonrpc":"2.0","id":1,"method":"initialize"}' | krabnet-mcp
//! ```

use krabnet::engine::Engine;
use krabnet::mcp::McpServer;
use krabnet::wal::{WalReader, WalWriter};

fn main() {
    let wal_path_str = std::env::var("KRABNET_MCP_WAL")
        .unwrap_or_else(|_| "krabnet-mcp.wal".to_string());
    let wal_path = std::path::Path::new(&wal_path_str);

    // Hardened engine with background compaction, mutation coalescing, and fan-out limits.
    // Matches krabnet-server configuration for consistent behavior.
    let mut engine = Engine::with_config(
        1024,         // ring buffer capacity
        Some(10_000), // compaction threshold (COMPACT-01, COMPACT-03)
        Some(16),     // coalescing window (COALESCE-01)
        Some(1000),   // max fanout (FANOUT-01)
    );

    // Replay WAL if it exists (crash recovery)
    if wal_path.exists() {
        eprintln!("Replaying WAL from {}...", wal_path.display());
        match WalReader::replay(wal_path) {
            Ok(entries) => {
                eprintln!("Replaying {} events...", entries.len());
                for (_epoch, event) in entries {
                    engine.ingest(event);
                }
                eprintln!("WAL replay complete.");
            }
            Err(e) => {
                eprintln!("WAL replay error (starting fresh): {}", e);
            }
        }
    }

    // Create WAL writer for live event persistence (fsync every 1000 events)
    let mut server = match WalWriter::new(wal_path, 1000) {
        Ok(wal_writer) => McpServer::with_wal(engine, wal_writer),
        Err(e) => {
            eprintln!("WARNING: Could not create WAL writer ({}), running without WAL", e);
            McpServer::new(engine)
        }
    };

    if let Err(e) = server.run() {
        eprintln!("krabnet-mcp error: {}", e);
        std::process::exit(1);
    }
}
