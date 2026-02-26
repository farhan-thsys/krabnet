//! krabnet-mcp: MCP stdio server for the Krabnet engine.
//!
//! Starts an MCP JSON-RPC 2.0 server reading from stdin and writing to stdout.
//! AI agents connect to this binary via stdio transport.
//!
//! # Usage
//!
//! ```sh
//! echo '{"jsonrpc":"2.0","id":1,"method":"initialize"}' | krabnet-mcp
//! ```

use krabnet::engine::Engine;
use krabnet::mcp::McpServer;

fn main() {
    // Hardened engine with background compaction, mutation coalescing, and fan-out limits.
    // Matches krabnet-server configuration for consistent behavior.
    let engine = Engine::with_config(
        1024,         // ring buffer capacity
        Some(10_000), // compaction threshold (COMPACT-01, COMPACT-03)
        Some(16),     // coalescing window (COALESCE-01)
        Some(1000),   // max fanout (FANOUT-01)
    );

    let mut server = McpServer::new(engine);

    if let Err(e) = server.run() {
        eprintln!("krabnet-mcp error: {}", e);
        std::process::exit(1);
    }
}
