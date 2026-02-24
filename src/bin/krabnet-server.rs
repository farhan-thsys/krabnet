//! krabnet-server: production Krabnet server.
//!
//! Starts gRPC server on `[::1]:50051` with:
//! - Background compaction worker (threshold: 10,000 tuples)
//! - Tier 3 LLM worker (mock client in this version)
//! - WAL persistence (`krabnet-wal.bin` in current directory)
//! - Graceful shutdown on Ctrl+C
//!
//! On startup, replays any existing WAL file to recover state from
//! the last run. Every `IngestEvent` RPC appends to the WAL before
//! responding, ensuring durability for crash recovery.

use std::sync::{Arc, Mutex, RwLock};

use krabnet::engine::Engine;
use krabnet::grpc::KrabnetServer;
use krabnet::tier3::{MockLlmClient, Tier3Worker};
use krabnet::wal::{WalReader, WalWriter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal_path = std::path::Path::new("krabnet-wal.bin");

    // Create engine with hardening features
    let mut engine = Engine::with_config(
        4096,         // ring buffer capacity
        Some(10_000), // compaction threshold
        Some(16),     // coalescing window
        Some(1000),   // max fanout
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
    let wal_writer = WalWriter::new(wal_path, 1000)?;
    let wal_writer = Arc::new(Mutex::new(wal_writer));

    // Set up Tier 3 worker (mock LLM client; swap for production client as needed)
    let mock_client = MockLlmClient::new(vec![]);
    let (tier3_worker, _tier3_sender) = Tier3Worker::new(Box::new(mock_client));

    // Spawn Tier 3 worker in background thread
    let tier3_handle = std::thread::spawn(move || {
        tier3_worker.run();
    });

    // Wrap engine for gRPC server, passing WAL writer for live persistence
    let engine = Arc::new(RwLock::new(engine));
    let grpc_server = KrabnetServer::with_wal(Arc::clone(&engine), Arc::clone(&wal_writer));

    let addr = "[::1]:50051".parse()?;
    eprintln!("krabnet-server listening on {}", addr);

    // Start gRPC server with graceful shutdown on Ctrl+C
    tonic::transport::Server::builder()
        .add_service(grpc_server.into_service())
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c().await.ok();
            eprintln!("\nShutting down...");
        })
        .await?;

    // Flush WAL on shutdown to ensure all buffered events are persisted
    if let Ok(mut wal) = wal_writer.lock() {
        wal.flush().ok();
    }

    // Clean up Tier 3 worker thread
    drop(tier3_handle);
    eprintln!("krabnet-server stopped.");
    Ok(())
}
