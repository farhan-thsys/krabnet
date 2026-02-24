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
    // Create engine with 1024-slot ring buffer.
    // Production configuration would be larger, but for MCP single-user usage this is fine.
    let engine = Engine::new(1024);

    let mut server = McpServer::new(engine);

    if let Err(e) = server.run() {
        eprintln!("krabnet-mcp error: {}", e);
        std::process::exit(1);
    }
}
