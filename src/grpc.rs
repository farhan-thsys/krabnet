//! gRPC server for the Krabnet engine.
//!
//! Implements [`KrabnetService`] with the engine held via `Arc<RwLock<Engine>>`.
//! SubscribeFrame uses `tokio::sync::broadcast` for real-time streaming.
//!
//! # Architecture
//!
//! The [`KrabnetServer`] struct wraps the engine in `Arc<RwLock<Engine>>` and
//! implements the tonic-generated `KrabnetService` trait. All RPC methods
//! acquire read or write locks as needed:
//!
//! - **Read lock**: `QueryFrame` (increments count, needs write), `ListFrames`,
//!   `GetStats`
//! - **Write lock**: `IngestEvent`, `RegisterFrame`, `EvictFrame`,
//!   `RegisterEmbryonicTemplate`
//!
//! Note: `QueryFrame` actually needs a write lock because `frame.query()`
//! increments the query counter. The engine's `query_frame()` takes `&mut self`.

use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

use crate::tier3::{Tier2Result, Tier3Sender};
use crate::wal::WalWriter;

/// Include generated protobuf types.
pub mod proto {
    tonic::include_proto!("krabnet");
}

use proto::krabnet_service_server::{KrabnetService, KrabnetServiceServer};
use proto::*;

use crate::embryonic::PatternTemplate;
use crate::engine::Engine;
use crate::types::{
    Direction as KDirection, EdgeId, Epoch, Filter as KFilter, FrameTier, HopSpec as KHopSpec,
    NodeId, PropertyValue, TypeId,
};

/// gRPC server wrapping the Krabnet engine.
///
/// Holds the engine via `Arc<RwLock<Engine>>` for shared access across
/// tonic's async handlers. A `broadcast::Sender` is used for
/// `SubscribeFrame` streaming. An optional `WalWriter` is wrapped in
/// `Arc<Mutex<>>` for durable event persistence.
pub struct KrabnetServer {
    engine: Arc<RwLock<Engine>>,
    frame_tx: broadcast::Sender<proto::FrameUpdate>,
    /// Optional WAL writer for durable event persistence.
    /// When set, every `IngestEvent` RPC appends to the WAL before responding.
    wal_writer: Option<Arc<Mutex<WalWriter>>>,
    /// Optional Tier 3 sender for dispatching Tier 2 results to the LLM worker.
    /// When set, every `IngestEvent` RPC dispatches results via non-blocking `try_send`.
    tier3_sender: Option<Tier3Sender>,
}

impl KrabnetServer {
    /// Create a new KrabnetServer without WAL persistence.
    pub fn new(engine: Arc<RwLock<Engine>>) -> Self {
        let (frame_tx, _) = broadcast::channel(1024);
        Self {
            engine,
            frame_tx,
            wal_writer: None,
            tier3_sender: None,
        }
    }

    /// Create a new KrabnetServer with WAL persistence.
    ///
    /// Every `IngestEvent` RPC will append the event to the WAL before
    /// responding, ensuring durability for crash recovery. The WAL writer
    /// is shared via `Arc<Mutex<>>` so the server binary can also flush
    /// on graceful shutdown.
    pub fn with_wal(engine: Arc<RwLock<Engine>>, wal_writer: Arc<Mutex<WalWriter>>) -> Self {
        let (frame_tx, _) = broadcast::channel(1024);
        Self {
            engine,
            frame_tx,
            wal_writer: Some(wal_writer),
            tier3_sender: None,
        }
    }

    /// Create a new KrabnetServer with WAL persistence and Tier 3 dispatch.
    ///
    /// Combines WAL durability with Tier 3 LLM worker integration. Every
    /// `IngestEvent` RPC appends to the WAL and dispatches `Tier2Result`s
    /// to the Tier 3 worker via non-blocking `try_send`.
    pub fn with_wal_and_tier3(
        engine: Arc<RwLock<Engine>>,
        wal_writer: Arc<Mutex<WalWriter>>,
        tier3_sender: Tier3Sender,
    ) -> Self {
        let (frame_tx, _) = broadcast::channel(1024);
        Self {
            engine,
            frame_tx,
            wal_writer: Some(wal_writer),
            tier3_sender: Some(tier3_sender),
        }
    }

    /// Create a new KrabnetServer with Tier 3 dispatch but no WAL.
    ///
    /// Used primarily in tests where WAL persistence is not needed but
    /// Tier 3 integration must be verified.
    pub fn with_tier3(engine: Arc<RwLock<Engine>>, tier3_sender: Tier3Sender) -> Self {
        let (frame_tx, _) = broadcast::channel(1024);
        Self {
            engine,
            frame_tx,
            wal_writer: None,
            tier3_sender: Some(tier3_sender),
        }
    }

    /// Convert this server into a tonic gRPC service.
    pub fn into_service(self) -> KrabnetServiceServer<Self> {
        KrabnetServiceServer::new(self)
    }
}

// ── Conversion functions ─────────────────────────────────────────────

/// Convert a proto IngestEventRequest into a krabnet Event.
#[allow(clippy::result_large_err)]
fn proto_event_to_event(
    req: &IngestEventRequest,
) -> Result<crate::types::Event, Status> {
    let event = req
        .event
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing event field"))?;

    match event {
        ingest_event_request::Event::NodeAdded(e) => {
            Ok(crate::types::Event::NodeAdded {
                node_id: NodeId(e.node_id),
                type_id: TypeId(e.type_id),
            })
        }
        ingest_event_request::Event::NodeRemoved(e) => {
            Ok(crate::types::Event::NodeRemoved {
                node_id: NodeId(e.node_id),
            })
        }
        ingest_event_request::Event::EdgeAdded(e) => {
            Ok(crate::types::Event::EdgeAdded {
                edge_id: EdgeId(e.edge_id),
                source: NodeId(e.source),
                target: NodeId(e.target),
                type_id: TypeId(e.type_id),
            })
        }
        ingest_event_request::Event::EdgeRemoved(e) => {
            Ok(crate::types::Event::EdgeRemoved {
                edge_id: EdgeId(e.edge_id),
                source: NodeId(e.source),
                target: NodeId(e.target),
            })
        }
        ingest_event_request::Event::PropertyChanged(e) => {
            let value = match e.value {
                Some(property_changed_event::Value::IntegerValue(v)) => {
                    PropertyValue::Integer(v)
                }
                Some(property_changed_event::Value::FloatValue(v)) => {
                    PropertyValue::Float(v)
                }
                Some(property_changed_event::Value::TextValue(v)) => {
                    PropertyValue::Text(v)
                }
                Some(property_changed_event::Value::BooleanValue(v)) => {
                    PropertyValue::Boolean(v)
                }
                None => {
                    return Err(Status::invalid_argument(
                        "PropertyChanged event missing value",
                    ))
                }
            };
            Ok(crate::types::Event::PropertyChanged {
                node_id: NodeId(e.node_id),
                key: e.key,
                value,
            })
        }
    }
}

/// Convert proto HopSpec messages into krabnet HopSpec values.
#[allow(clippy::result_large_err)]
fn hopspecs_from_proto(hops: &[proto::HopSpec]) -> Result<Vec<KHopSpec>, Status> {
    hops.iter()
        .map(|h| {
            let direction = match h.direction {
                0 => KDirection::Outgoing,
                1 => KDirection::Incoming,
                2 => KDirection::Any,
                other => {
                    return Err(Status::invalid_argument(format!(
                        "invalid direction: {other}"
                    )))
                }
            };
            let edge_type = h.edge_type.map(TypeId);
            let target_type = h.target_type.map(TypeId);
            let filter = match h.filter.as_ref().and_then(|f| f.filter_type.as_ref()) {
                None | Some(filter::FilterType::None(_)) => KFilter::None,
                Some(filter::FilterType::PropertyEquals(pf)) => {
                    let value = match pf.value {
                        Some(property_filter::Value::IntegerValue(v)) => {
                            PropertyValue::Integer(v)
                        }
                        Some(property_filter::Value::FloatValue(v)) => {
                            PropertyValue::Float(v)
                        }
                        Some(property_filter::Value::TextValue(v)) => {
                            PropertyValue::Text(v)
                        }
                        Some(property_filter::Value::BooleanValue(v)) => {
                            PropertyValue::Boolean(v)
                        }
                        None => {
                            return Err(Status::invalid_argument(
                                "PropertyEquals filter missing value",
                            ))
                        }
                    };
                    KFilter::PropertyEquals {
                        key: pf.key,
                        value,
                    }
                }
                Some(filter::FilterType::HasPropertyKey(key)) => {
                    KFilter::HasProperty { key: *key }
                }
            };
            Ok(KHopSpec {
                direction,
                edge_type,
                target_type,
                filter,
            })
        })
        .collect()
}

/// Convert engine paths (Vec<Vec<NodeId>>) to proto Path messages.
fn paths_to_proto(paths: &[Vec<NodeId>]) -> Vec<proto::Path> {
    paths
        .iter()
        .map(|p| proto::Path {
            node_ids: p.iter().map(|n| n.0).collect(),
        })
        .collect()
}

/// Convert FrameTier to a string representation for proto.
fn tier_to_string(tier: FrameTier) -> String {
    match tier {
        FrameTier::Hot => "Hot".to_string(),
        FrameTier::Warm => "Warm".to_string(),
        FrameTier::Cold => "Cold".to_string(),
    }
}

// ── KrabnetService implementation ────────────────────────────────────

type SubscribeStream = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<proto::FrameUpdate, Status>>
            + Send
            + 'static,
    >,
>;

#[tonic::async_trait]
impl KrabnetService for KrabnetServer {
    async fn ingest_event(
        &self,
        request: Request<IngestEventRequest>,
    ) -> Result<Response<IngestEventResponse>, Status> {
        let req = request.into_inner();
        let event = proto_event_to_event(&req)?;

        let epoch = {
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine.ingest(event.clone())
        };

        // Persist to WAL if configured (after engine ingest assigns epoch)
        if let Some(ref wal) = self.wal_writer {
            let mut writer = wal
                .lock()
                .map_err(|_| Status::internal("WAL writer lock poisoned"))?;
            writer
                .append(epoch, &event)
                .map_err(|e| Status::internal(format!("WAL write failed: {}", e)))?;
        }

        // Post-ingest: broadcast FrameUpdates and dispatch Tier 3 results
        {
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            let frames = engine.list_frames();
            for (fid, anchor, _tier, _tuple_count) in &frames {
                if let Some(paths) = engine.query_frame(*fid) {
                    // Broadcast FrameUpdate for SubscribeFrame clients (GRPC-03)
                    let update = proto::FrameUpdate {
                        frame_id: *fid,
                        paths: paths_to_proto(&paths),
                        epoch: epoch.0,
                    };
                    let _ = self.frame_tx.send(update);

                    // Dispatch Tier 2 result to Tier 3 worker (TIER3-01..04)
                    if let Some(ref sender) = self.tier3_sender {
                        let path_count = paths.len();
                        let result = Tier2Result {
                            frame_id: *fid,
                            anchor: *anchor,
                            paths,
                            epoch,
                            tier2_summary: format!(
                                "{} paths materialized",
                                path_count
                            ),
                        };
                        let _ = sender.try_send(result);
                    }
                }
            }
        }

        Ok(Response::new(IngestEventResponse { epoch: epoch.0 }))
    }

    async fn register_frame(
        &self,
        request: Request<RegisterFrameRequest>,
    ) -> Result<Response<RegisterFrameResponse>, Status> {
        let req = request.into_inner();
        let anchor = NodeId(req.anchor_node_id);
        let pattern = hopspecs_from_proto(&req.pattern)?;
        let epoch = Epoch(req.epoch);

        let frame_id = {
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine.register_frame(anchor, pattern, epoch)
        };

        Ok(Response::new(RegisterFrameResponse { frame_id }))
    }

    async fn query_frame(
        &self,
        request: Request<QueryFrameRequest>,
    ) -> Result<Response<QueryFrameResponse>, Status> {
        let req = request.into_inner();

        let paths = {
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine
                .query_frame(req.frame_id)
                .ok_or_else(|| Status::not_found(format!("frame {} not found", req.frame_id)))?
        };

        Ok(Response::new(QueryFrameResponse {
            paths: paths_to_proto(&paths),
        }))
    }

    type SubscribeFrameStream = SubscribeStream;

    async fn subscribe_frame(
        &self,
        request: Request<SubscribeFrameRequest>,
    ) -> Result<Response<Self::SubscribeFrameStream>, Status> {
        let frame_id = request.into_inner().frame_id;
        let mut rx = self.frame_tx.subscribe();

        let stream = async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        if update.frame_id == frame_id {
                            yield Ok(update);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Skip lagged messages, continue receiving
                        eprintln!("subscribe_frame: lagged by {n} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::SubscribeFrameStream))
    }

    async fn list_frames(
        &self,
        _request: Request<ListFramesRequest>,
    ) -> Result<Response<ListFramesResponse>, Status> {
        let frames = {
            let engine = self
                .engine
                .read()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine
                .list_frames()
                .into_iter()
                .map(|(id, anchor, tier, tuple_count)| FrameInfo {
                    id,
                    anchor: anchor.0,
                    tier: tier_to_string(tier),
                    tuple_count: tuple_count as u64,
                })
                .collect::<Vec<_>>()
        };

        Ok(Response::new(ListFramesResponse { frames }))
    }

    async fn evict_frame(
        &self,
        request: Request<EvictFrameRequest>,
    ) -> Result<Response<EvictFrameResponse>, Status> {
        let req = request.into_inner();

        let success = {
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine.evict_frame(req.frame_id)
        };

        Ok(Response::new(EvictFrameResponse { success }))
    }

    async fn register_embryonic_template(
        &self,
        request: Request<RegisterTemplateRequest>,
    ) -> Result<Response<RegisterTemplateResponse>, Status> {
        let req = request.into_inner();
        let pattern = hopspecs_from_proto(&req.pattern)?;

        let template = PatternTemplate {
            id: req.id,
            pattern,
            threshold: req.threshold,
            max_candidates: req.max_candidates as usize,
            stale_window: req.stale_window,
            success_count: 0,
            failure_count: 0,
            active: true,
        };

        {
            let mut engine = self
                .engine
                .write()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine.register_template(template);
        }

        Ok(Response::new(RegisterTemplateResponse { success: true }))
    }

    async fn get_stats(
        &self,
        _request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let stats = {
            let engine = self
                .engine
                .read()
                .map_err(|_| Status::internal("engine lock poisoned"))?;
            engine.stats()
        };

        Ok(Response::new(GetStatsResponse {
            node_count: stats.node_count as u64,
            edge_count: stats.edge_count as u64,
            frame_count: stats.frame_count as u64,
            hot_frames: stats.hot_frames as u64,
            warm_frames: stats.warm_frames as u64,
            cold_frames: stats.cold_frames as u64,
            total_tuples: stats.total_tuples as u64,
            embryonic_candidates: stats.embryonic_candidates as u64,
            embryonic_templates: stats.embryonic_templates as u64,
        }))
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::proto;
    use super::proto::krabnet_service_client::KrabnetServiceClient;
    use super::proto::*;
    use super::KrabnetServer;
    use crate::engine::Engine;
    use std::sync::{Arc, RwLock};

    /// TEST-18: gRPC ingest-and-query roundtrip test.
    ///
    /// Starts a gRPC server on a random port, connects a client, ingests
    /// events, registers a frame, queries it, and verifies stats.
    #[tokio::test]
    async fn test_grpc_ingest_and_query() {
        // 1. Create engine with Arc<RwLock>
        let engine = Arc::new(RwLock::new(Engine::new(64)));
        let server = KrabnetServer::new(Arc::clone(&engine));

        // 2. Start server on [::1]:0 (random port)
        let listener = tokio::net::TcpListener::bind("[::1]:0")
            .await
            .expect("failed to bind");
        let addr = listener.local_addr().expect("no local addr");

        let svc = server.into_service();
        let server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    listener,
                ))
                .await
                .expect("server error");
        });

        // Give server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 3. Connect client
        let mut client =
            KrabnetServiceClient::connect(format!("http://[::1]:{}", addr.port()))
                .await
                .expect("failed to connect");

        // 4. IngestEvent: NodeAdded x2
        client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::NodeAdded(NodeAddedEvent {
                    node_id: 1,
                    type_id: 10,
                })),
            })
            .await
            .expect("ingest node 1 failed");

        client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::NodeAdded(NodeAddedEvent {
                    node_id: 2,
                    type_id: 20,
                })),
            })
            .await
            .expect("ingest node 2 failed");

        // IngestEvent: EdgeAdded x1
        let ingest_resp = client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::EdgeAdded(EdgeAddedEvent {
                    edge_id: 0,
                    source: 1,
                    target: 2,
                    type_id: 100,
                })),
            })
            .await
            .expect("ingest edge failed");

        let epoch = ingest_resp.into_inner().epoch;
        assert!(epoch > 0, "epoch should be > 0 after 3 events");

        // 5. RegisterFrame
        let reg_resp = client
            .register_frame(RegisterFrameRequest {
                anchor_node_id: 1,
                pattern: vec![proto::HopSpec {
                    direction: Direction::Outgoing as i32,
                    edge_type: Some(100),
                    target_type: Some(20),
                    filter: Some(proto::Filter {
                        filter_type: Some(filter::FilterType::None(true)),
                    }),
                }],
                epoch,
            })
            .await
            .expect("register frame failed");

        let frame_id = reg_resp.into_inner().frame_id;

        // 6. QueryFrame -> verify paths
        let query_resp = client
            .query_frame(QueryFrameRequest { frame_id })
            .await
            .expect("query frame failed");

        let paths = query_resp.into_inner().paths;
        assert_eq!(paths.len(), 1, "should have 1 path");
        assert_eq!(paths[0].node_ids, vec![1, 2], "path should be [1, 2]");

        // 7. GetStats -> verify counts
        let stats_resp = client
            .get_stats(GetStatsRequest {})
            .await
            .expect("get stats failed");

        let stats = stats_resp.into_inner();
        assert_eq!(stats.node_count, 2, "should have 2 nodes");
        assert_eq!(stats.edge_count, 1, "should have 1 edge");
        assert_eq!(stats.frame_count, 1, "should have 1 frame");

        // 8. ListFrames
        let list_resp = client
            .list_frames(ListFramesRequest {})
            .await
            .expect("list frames failed");

        let frames = list_resp.into_inner().frames;
        assert_eq!(frames.len(), 1, "should list 1 frame");
        assert_eq!(frames[0].id, frame_id);
        assert_eq!(frames[0].anchor, 1);

        // 9. EvictFrame
        let evict_resp = client
            .evict_frame(EvictFrameRequest { frame_id })
            .await
            .expect("evict frame failed");

        assert!(evict_resp.into_inner().success);

        // Verify frame is gone
        let list_resp2 = client
            .list_frames(ListFramesRequest {})
            .await
            .expect("list frames after evict failed");
        assert_eq!(list_resp2.into_inner().frames.len(), 0);

        // 10. RegisterEmbryonicTemplate
        let template_resp = client
            .register_embryonic_template(RegisterTemplateRequest {
                id: 1,
                pattern: vec![proto::HopSpec {
                    direction: Direction::Outgoing as i32,
                    edge_type: Some(100),
                    target_type: None,
                    filter: None,
                }],
                threshold: 0.5,
                max_candidates: 100,
                stale_window: 10,
            })
            .await
            .expect("register template failed");

        assert!(template_resp.into_inner().success);

        // Verify template is registered via stats
        let stats2 = client
            .get_stats(GetStatsRequest {})
            .await
            .expect("get stats 2 failed")
            .into_inner();
        assert_eq!(stats2.embryonic_templates, 1);

        // Shutdown server
        server_handle.abort();
    }

    /// TEST-22: End-to-end ingest broadcast and Tier 3 dispatch.
    ///
    /// Verifies that after ingest_event(), SubscribeFrame clients receive
    /// FrameUpdate messages and the Tier3Worker processes Tier2Results.
    #[tokio::test]
    async fn test_ingest_broadcasts_and_tier3() {
        use crate::tier3::{MockLlmClient, Tier3Worker};
        use std::time::Duration;

        // 1. Create engine
        let engine = Arc::new(RwLock::new(Engine::new(64)));

        // 2. Create Tier 3 worker with mock LLM
        let mock = MockLlmClient::new(vec!["test-analysis".to_string()]);
        let (worker, tier3_sender) = Tier3Worker::new(Box::new(mock));
        let tier3_results = worker.results_handle();

        // 3. Spawn worker thread (processes Tier2Results until sender is dropped)
        let worker_handle = std::thread::spawn(move || {
            worker.run();
        });

        // 4. Create server with Tier3Sender (no WAL needed for test)
        let server = KrabnetServer::with_tier3(Arc::clone(&engine), tier3_sender);

        // 5. Start gRPC server on random port
        let listener = tokio::net::TcpListener::bind("[::1]:0")
            .await
            .expect("failed to bind");
        let addr = listener.local_addr().expect("no local addr");

        let svc = server.into_service();
        let server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    listener,
                ))
                .await
                .expect("server error");
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // 6. Connect client for ingestion RPCs
        let mut client =
            KrabnetServiceClient::connect(format!("http://[::1]:{}", addr.port()))
                .await
                .expect("failed to connect");

        // 7. Ingest nodes + edge to build graph
        client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::NodeAdded(NodeAddedEvent {
                    node_id: 1,
                    type_id: 10,
                })),
            })
            .await
            .expect("ingest node 1 failed");

        client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::NodeAdded(NodeAddedEvent {
                    node_id: 2,
                    type_id: 20,
                })),
            })
            .await
            .expect("ingest node 2 failed");

        let edge_resp = client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::EdgeAdded(EdgeAddedEvent {
                    edge_id: 0,
                    source: 1,
                    target: 2,
                    type_id: 100,
                })),
            })
            .await
            .expect("ingest edge failed");
        let reg_epoch = edge_resp.into_inner().epoch;

        // 8. Register frame (1-hop outgoing from node 1)
        let reg_resp = client
            .register_frame(RegisterFrameRequest {
                anchor_node_id: 1,
                pattern: vec![proto::HopSpec {
                    direction: Direction::Outgoing as i32,
                    edge_type: Some(100),
                    target_type: Some(20),
                    filter: Some(proto::Filter {
                        filter_type: Some(filter::FilterType::None(true)),
                    }),
                }],
                epoch: reg_epoch,
            })
            .await
            .expect("register frame failed");
        let frame_id = reg_resp.into_inner().frame_id;

        // 9. Subscribe to frame updates on a SEPARATE client connection.
        //    This avoids HTTP/2 stream interleaving issues within a single
        //    connection where the subscribe stream can block other RPCs.
        let mut sub_client =
            KrabnetServiceClient::connect(format!("http://[::1]:{}", addr.port()))
                .await
                .expect("failed to connect sub client");
        let mut subscribe_stream = sub_client
            .subscribe_frame(SubscribeFrameRequest { frame_id })
            .await
            .expect("subscribe frame failed")
            .into_inner();

        // Small delay to ensure subscription stream is fully established
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 10. Ingest another event to trigger broadcast (frame exists now)
        client
            .ingest_event(IngestEventRequest {
                event: Some(ingest_event_request::Event::NodeAdded(NodeAddedEvent {
                    node_id: 3,
                    type_id: 30,
                })),
            })
            .await
            .expect("ingest node 3 failed");

        // 11. Wait for FrameUpdate on subscribe stream
        let update = tokio::time::timeout(
            Duration::from_secs(5),
            subscribe_stream.message(),
        )
        .await
        .expect("timeout waiting for FrameUpdate")
        .expect("stream error")
        .expect("stream ended");

        assert_eq!(update.frame_id, frame_id);
        assert!(!update.paths.is_empty(), "FrameUpdate should have paths");

        // 12. Verify Tier 3 received and processed results.
        //     The worker processes results in a background thread. Give it
        //     a brief moment to consume from the channel, then check the
        //     shared results handle directly (no need to join the worker).
        tokio::time::sleep(Duration::from_millis(500)).await;
        let results = tier3_results.lock().unwrap();
        assert!(!results.is_empty(), "Tier 3 should have processed at least one result");
        assert_eq!(results[0].frame_id, frame_id);
        drop(results);

        // 13. Shutdown server and worker
        server_handle.abort();
        // Worker thread will eventually exit when all senders are dropped;
        // detach the handle rather than blocking on join.
        drop(worker_handle);
    }
}
