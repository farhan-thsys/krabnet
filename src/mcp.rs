//! MCP (Model Context Protocol) JSON-RPC 2.0 server over stdio.
//!
//! Exposes 5 tools to AI agents via the standard MCP protocol:
//! - `krabnet_ingest` -- Ingest a graph mutation event
//! - `krabnet_register_frame` -- Register a new frame with anchor and pattern
//! - `krabnet_query_frame` -- Query a frame's current materialized paths
//! - `krabnet_stats` -- Get engine statistics
//! - `krabnet_register_template` -- Register an embryonic pattern template
//!
//! # Protocol
//!
//! The server reads JSON-RPC 2.0 messages from stdin (one per line) and writes
//! responses to stdout. It handles three method types:
//! - `initialize` -- Returns server info and capabilities
//! - `tools/list` -- Returns the list of available tools
//! - `tools/call` -- Executes a specific tool
//!
//! # Architecture
//!
//! The [`McpServer`] owns the [`Engine`] directly (not `Arc<RwLock>`) because
//! it operates single-threaded over stdio. No concurrent access is needed.
//!
//! # Usage
//!
//! ```no_run
//! use krabnet::engine::Engine;
//! use krabnet::mcp::McpServer;
//!
//! let engine = Engine::with_config(1024, Some(10_000), Some(16), Some(1000));
//! let mut server = McpServer::new(engine);
//! server.run().unwrap();
//! ```

use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::embryonic::PatternTemplate;
use crate::engine::Engine;
use crate::types::{Direction, EdgeId, Epoch, Event, Filter, HopSpec, NodeId, TypeId};
use crate::wal::WalWriter;

/// Incoming JSON-RPC 2.0 request.
#[derive(Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<serde_json::Value>,
    method: String,
    params: Option<serde_json::Value>,
}

/// Outgoing JSON-RPC 2.0 response.
#[derive(Serialize)]
pub struct JsonRpcResponse {
    jsonrpc: String,
    id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

/// JSON-RPC 2.0 error object.
#[derive(Serialize)]
pub struct JsonRpcError {
    code: i64,
    message: String,
}

/// MCP JSON-RPC 2.0 server over stdio.
///
/// Owns the [`Engine`] directly for single-threaded stdio operation.
/// Handles `initialize`, `tools/list`, and `tools/call` methods.
/// An optional [`WalWriter`] provides crash-recovery durability.
pub struct McpServer {
    engine: Engine,
    /// Optional WAL writer for durable event persistence.
    /// When set, every `krabnet_ingest` tool call appends to the WAL.
    wal_writer: Option<WalWriter>,
}

impl McpServer {
    /// Create a new MCP server wrapping the given engine (no WAL).
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            wal_writer: None,
        }
    }

    /// Create a new MCP server with WAL persistence.
    ///
    /// Every `krabnet_ingest` tool call will append to the WAL before
    /// responding, ensuring durability for crash recovery.
    pub fn with_wal(engine: Engine, wal_writer: WalWriter) -> Self {
        Self {
            engine,
            wal_writer: Some(wal_writer),
        }
    }

    /// Run the stdio event loop, reading JSON-RPC requests from stdin
    /// and writing responses to stdout.
    pub fn run(&mut self) -> std::io::Result<()> {
        let stdin = std::io::stdin();
        let stdout = std::io::stdout();

        for line in stdin.lock().lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            let request: JsonRpcRequest = match serde_json::from_str(&line) {
                Ok(req) => req,
                Err(e) => {
                    let err_resp = JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: None,
                        result: None,
                        error: Some(JsonRpcError {
                            code: -32700,
                            message: format!("Parse error: {}", e),
                        }),
                    };
                    let out = serde_json::to_string(&err_resp)?;
                    let mut lock = stdout.lock();
                    writeln!(lock, "{}", out)?;
                    lock.flush()?;
                    continue;
                }
            };

            let response = self.handle_request(request);
            let out = serde_json::to_string(&response)?;
            let mut lock = stdout.lock();
            writeln!(lock, "{}", out)?;
            lock.flush()?;
        }

        // Flush WAL on exit to ensure all buffered events are persisted
        if let Some(ref mut wal) = self.wal_writer {
            wal.flush().ok();
        }

        Ok(())
    }

    /// Handle a single JSON-RPC request and produce a response.
    ///
    /// Dispatches to the appropriate handler based on the method name.
    /// This is separated from the run loop for testability.
    pub fn handle_request(&mut self, request: JsonRpcRequest) -> JsonRpcResponse {
        match request.method.as_str() {
            "initialize" => self.handle_initialize(request.id),
            "tools/list" => self.handle_tools_list(request.id),
            "tools/call" => self.handle_tools_call(request.id, request.params),
            _ => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: format!("Method not found: {}", request.method),
                }),
            },
        }
    }

    /// Handle `initialize` -- returns server info and capabilities.
    fn handle_initialize(&self, id: Option<serde_json::Value>) -> JsonRpcResponse {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "krabnet-mcp",
                    "version": "0.1.0"
                }
            })),
            error: None,
        }
    }

    /// Handle `tools/list` -- returns all 5 available tools with JSON Schema input definitions.
    fn handle_tools_list(&self, id: Option<serde_json::Value>) -> JsonRpcResponse {
        let tools = json!({
            "tools": [
                {
                    "name": "krabnet_ingest",
                    "description": "Ingest a graph mutation event",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "event_type": {
                                "type": "string",
                                "enum": ["NodeAdded", "NodeRemoved", "EdgeAdded", "EdgeRemoved", "PropertyChanged"]
                            },
                            "node_id": { "type": "integer", "description": "Node ID (for NodeAdded, NodeRemoved, PropertyChanged)" },
                            "type_id": { "type": "integer", "description": "Type ID (for NodeAdded, EdgeAdded)" },
                            "edge_id": { "type": "integer", "description": "Edge ID (for EdgeAdded, EdgeRemoved)" },
                            "source": { "type": "integer", "description": "Source node ID (for EdgeAdded, EdgeRemoved)" },
                            "target": { "type": "integer", "description": "Target node ID (for EdgeAdded, EdgeRemoved)" },
                            "key": { "type": "integer", "description": "Property key ID (for PropertyChanged)" },
                            "value_type": { "type": "string", "enum": ["Integer", "Float", "Text", "Boolean"], "description": "Property value type (for PropertyChanged)" },
                            "value": { "description": "Property value (for PropertyChanged)" }
                        },
                        "required": ["event_type"]
                    }
                },
                {
                    "name": "krabnet_register_frame",
                    "description": "Register a new frame with anchor and pattern",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "anchor_node_id": { "type": "integer", "description": "Anchor node ID for the frame" },
                            "pattern": {
                                "type": "array",
                                "description": "Array of hop specifications",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "direction": { "type": "string", "enum": ["Outgoing", "Incoming", "Any"] },
                                        "edge_type": { "type": ["integer", "null"], "description": "Edge type ID filter (null for any)" },
                                        "target_type": { "type": ["integer", "null"], "description": "Target node type ID filter (null for any)" }
                                    },
                                    "required": ["direction"]
                                }
                            },
                            "epoch": { "type": "integer", "description": "Registration epoch" }
                        },
                        "required": ["anchor_node_id", "pattern", "epoch"]
                    }
                },
                {
                    "name": "krabnet_query_frame",
                    "description": "Query a frame's current materialized paths",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "frame_id": { "type": "integer", "description": "Frame ID to query" }
                        },
                        "required": ["frame_id"]
                    }
                },
                {
                    "name": "krabnet_stats",
                    "description": "Get engine statistics",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                },
                {
                    "name": "krabnet_register_template",
                    "description": "Register an embryonic pattern template",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "integer", "description": "Template ID" },
                            "pattern": {
                                "type": "array",
                                "description": "Array of hop specifications",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "direction": { "type": "string", "enum": ["Outgoing", "Incoming", "Any"] },
                                        "edge_type": { "type": ["integer", "null"], "description": "Edge type ID filter (null for any)" },
                                        "target_type": { "type": ["integer", "null"], "description": "Target node type ID filter (null for any)" }
                                    },
                                    "required": ["direction"]
                                }
                            },
                            "threshold": { "type": "number", "description": "Completion ratio for promotion (0.0-1.0)" },
                            "max_candidates": { "type": "integer", "description": "Maximum candidates per template" },
                            "stale_window": { "type": "integer", "description": "Epochs without progress before pruning" }
                        },
                        "required": ["id", "pattern", "threshold", "max_candidates", "stale_window"]
                    }
                }
            ]
        });

        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(tools),
            error: None,
        }
    }

    /// Handle `tools/call` -- dispatch to the appropriate tool handler.
    fn handle_tools_call(
        &mut self,
        id: Option<serde_json::Value>,
        params: Option<serde_json::Value>,
    ) -> JsonRpcResponse {
        let params = match params {
            Some(p) => p,
            None => {
                return JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32602,
                        message: "Missing params".to_string(),
                    }),
                };
            }
        };

        let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

        let tool_result = match tool_name {
            "krabnet_ingest" => self.tool_ingest(&arguments),
            "krabnet_register_frame" => self.tool_register_frame(&arguments),
            "krabnet_query_frame" => self.tool_query_frame(&arguments),
            "krabnet_stats" => self.tool_stats(),
            "krabnet_register_template" => self.tool_register_template(&arguments),
            _ => Err(format!("Unknown tool: {}", tool_name)),
        };

        match tool_result {
            Ok(content) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: Some(json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string(&content).unwrap_or_default()
                    }]
                })),
                error: None,
            },
            Err(msg) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id,
                result: Some(json!({
                    "content": [{
                        "type": "text",
                        "text": msg
                    }],
                    "isError": true
                })),
                error: None,
            },
        }
    }

    // ── Tool implementations ───────────────────────────────────────────

    /// Ingest a graph mutation event.
    fn tool_ingest(&mut self, args: &serde_json::Value) -> Result<serde_json::Value, String> {
        let event_type = args
            .get("event_type")
            .and_then(|v| v.as_str())
            .ok_or("missing event_type")?;

        let event = match event_type {
            "NodeAdded" => {
                let node_id = args
                    .get("node_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing node_id")?;
                let type_id = args
                    .get("type_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing type_id")?;
                Event::NodeAdded {
                    node_id: NodeId(node_id),
                    type_id: TypeId(type_id as u32),
                }
            }
            "NodeRemoved" => {
                let node_id = args
                    .get("node_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing node_id")?;
                Event::NodeRemoved {
                    node_id: NodeId(node_id),
                }
            }
            "EdgeAdded" => {
                let edge_id = args
                    .get("edge_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing edge_id")?;
                let source = args
                    .get("source")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing source")?;
                let target = args
                    .get("target")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing target")?;
                let type_id = args
                    .get("type_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing type_id")?;
                Event::EdgeAdded {
                    edge_id: EdgeId(edge_id),
                    source: NodeId(source),
                    target: NodeId(target),
                    type_id: TypeId(type_id as u32),
                }
            }
            "EdgeRemoved" => {
                let edge_id = args
                    .get("edge_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing edge_id")?;
                let source = args
                    .get("source")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing source")?;
                let target = args
                    .get("target")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing target")?;
                Event::EdgeRemoved {
                    edge_id: EdgeId(edge_id),
                    source: NodeId(source),
                    target: NodeId(target),
                }
            }
            "PropertyChanged" => {
                let node_id = args
                    .get("node_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing node_id")?;
                let key = args
                    .get("key")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing key")?;
                let value = parse_property_value(args)?;
                Event::PropertyChanged {
                    node_id: NodeId(node_id),
                    key: key as u32,
                    value,
                }
            }
            other => return Err(format!("unknown event_type: {}", other)),
        };

        let epoch = self.engine.ingest(event.clone());

        // Persist to WAL if configured
        if let Some(ref mut wal) = self.wal_writer {
            if let Err(e) = wal.append(epoch, &event) {
                eprintln!("WAL write failed: {}", e);
            }
        }

        Ok(json!({ "epoch": epoch.0 }))
    }

    /// Register a new frame with anchor, pattern, and epoch.
    fn tool_register_frame(
        &mut self,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let anchor = args
            .get("anchor_node_id")
            .and_then(|v| v.as_u64())
            .ok_or("missing anchor_node_id")?;
        let epoch = args
            .get("epoch")
            .and_then(|v| v.as_u64())
            .ok_or("missing epoch")?;
        let pattern_arr = args
            .get("pattern")
            .and_then(|v| v.as_array())
            .ok_or("missing or invalid pattern")?;

        let pattern = parse_hop_specs(pattern_arr)?;
        let frame_id = self
            .engine
            .register_frame(NodeId(anchor), pattern, Epoch(epoch));
        Ok(json!({ "frame_id": frame_id }))
    }

    /// Query a frame's current materialized paths.
    fn tool_query_frame(
        &mut self,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let frame_id = args
            .get("frame_id")
            .and_then(|v| v.as_u64())
            .ok_or("missing frame_id")?;

        match self.engine.query_frame(frame_id) {
            Some(paths) => {
                let path_arrays: Vec<Vec<u64>> = paths
                    .into_iter()
                    .map(|p| p.into_iter().map(|n| n.0).collect())
                    .collect();
                Ok(json!({ "paths": path_arrays }))
            }
            None => Err(format!("frame {} not found", frame_id)),
        }
    }

    /// Get engine statistics.
    fn tool_stats(&self) -> Result<serde_json::Value, String> {
        let stats = self.engine.stats();
        let mut result = json!({
            "node_count": stats.node_count,
            "edge_count": stats.edge_count,
            "frame_count": stats.frame_count,
            "hot_frames": stats.hot_frames,
            "warm_frames": stats.warm_frames,
            "cold_frames": stats.cold_frames,
            "total_tuples": stats.total_tuples,
            "embryonic_candidates": stats.embryonic_candidates,
            "embryonic_templates": stats.embryonic_templates,
        });

        if let Some(cs) = self.engine.compaction_stats() {
            let obj = result.as_object_mut().unwrap();
            obj.insert("compactions_completed".to_string(), json!(cs.compactions_completed));
            obj.insert("compaction_tuples_before".to_string(), json!(cs.tuples_before));
            obj.insert("compaction_tuples_after".to_string(), json!(cs.tuples_after));
            obj.insert("total_compaction_time_us".to_string(), json!(cs.total_compaction_time_us));
        }

        Ok(result)
    }

    /// Register an embryonic pattern template.
    fn tool_register_template(
        &mut self,
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let id = args
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or("missing id")?;
        let threshold = args
            .get("threshold")
            .and_then(|v| v.as_f64())
            .ok_or("missing threshold")?;
        let max_candidates = args
            .get("max_candidates")
            .and_then(|v| v.as_u64())
            .ok_or("missing max_candidates")? as usize;
        let stale_window = args
            .get("stale_window")
            .and_then(|v| v.as_u64())
            .ok_or("missing stale_window")?;
        let pattern_arr = args
            .get("pattern")
            .and_then(|v| v.as_array())
            .ok_or("missing or invalid pattern")?;

        let pattern = parse_hop_specs(pattern_arr)?;
        let template = PatternTemplate {
            id,
            pattern,
            threshold,
            max_candidates,
            stale_window,
            success_count: 0,
            failure_count: 0,
            active: true,
        };
        self.engine.register_template(template);
        Ok(json!({ "registered": true }))
    }
}

// ── Helper functions ─────────────────────────────────────────────────

/// Parse an array of hop spec JSON objects into `Vec<HopSpec>`.
fn parse_hop_specs(arr: &[serde_json::Value]) -> Result<Vec<HopSpec>, String> {
    arr.iter()
        .map(|hop| {
            let direction = hop
                .get("direction")
                .and_then(|v| v.as_str())
                .ok_or("missing direction in hop")?;
            let direction = match direction {
                "Outgoing" => Direction::Outgoing,
                "Incoming" => Direction::Incoming,
                "Any" => Direction::Any,
                other => return Err(format!("unknown direction: {}", other)),
            };
            let edge_type = hop
                .get("edge_type")
                .and_then(|v| v.as_u64())
                .map(|v| TypeId(v as u32));
            let target_type = hop
                .get("target_type")
                .and_then(|v| v.as_u64())
                .map(|v| TypeId(v as u32));

            Ok(HopSpec {
                direction,
                edge_type,
                target_type,
                filter: Filter::None,
            })
        })
        .collect()
}

/// Parse a property value from tool arguments.
fn parse_property_value(args: &serde_json::Value) -> Result<crate::types::PropertyValue, String> {
    let value_type = args
        .get("value_type")
        .and_then(|v| v.as_str())
        .unwrap_or("Integer");
    let value = args.get("value").ok_or("missing value")?;

    match value_type {
        "Integer" => {
            let v = value.as_i64().ok_or("value must be integer")?;
            Ok(crate::types::PropertyValue::Integer(v))
        }
        "Float" => {
            let v = value.as_f64().ok_or("value must be float")?;
            Ok(crate::types::PropertyValue::Float(v))
        }
        "Text" => {
            let v = value.as_u64().ok_or("text value must be u32 interned ID")? as u32;
            Ok(crate::types::PropertyValue::Text(v))
        }
        "Boolean" => {
            let v = value.as_bool().ok_or("value must be boolean")?;
            Ok(crate::types::PropertyValue::Boolean(v))
        }
        other => Err(format!("unknown value_type: {}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_tools_list() {
        let engine = Engine::new(64);
        let mut server = McpServer::new(engine);

        // Simulate tools/list request
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/list".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 5);

        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"krabnet_ingest"));
        assert!(names.contains(&"krabnet_register_frame"));
        assert!(names.contains(&"krabnet_query_frame"));
        assert!(names.contains(&"krabnet_stats"));
        assert!(names.contains(&"krabnet_register_template"));
    }

    #[test]
    fn test_mcp_initialize() {
        let engine = Engine::new(64);
        let mut server = McpServer::new(engine);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "initialize".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        assert_eq!(result["serverInfo"]["name"], "krabnet-mcp");
        assert_eq!(result["protocolVersion"], "2024-11-05");
    }

    #[test]
    fn test_mcp_ingest_and_query() {
        let engine = Engine::new(64);
        let mut server = McpServer::new(engine);

        // Ingest NodeAdded events
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_ingest",
                "arguments": {
                    "event_type": "NodeAdded",
                    "node_id": 1,
                    "type_id": 10
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(2)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_ingest",
                "arguments": {
                    "event_type": "NodeAdded",
                    "node_id": 2,
                    "type_id": 20
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        // Ingest edge
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(3)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_ingest",
                "arguments": {
                    "event_type": "EdgeAdded",
                    "edge_id": 0,
                    "source": 1,
                    "target": 2,
                    "type_id": 100
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        // Register a frame
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(4)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_register_frame",
                "arguments": {
                    "anchor_node_id": 1,
                    "pattern": [{
                        "direction": "Outgoing",
                        "edge_type": 100,
                        "target_type": 20
                    }],
                    "epoch": 3
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        let content_text = result["content"][0]["text"].as_str().unwrap();
        let frame_result: serde_json::Value = serde_json::from_str(content_text).unwrap();
        let frame_id = frame_result["frame_id"].as_u64().unwrap();

        // Query the frame
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(5)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_query_frame",
                "arguments": {
                    "frame_id": frame_id
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        let content_text = result["content"][0]["text"].as_str().unwrap();
        let query_result: serde_json::Value = serde_json::from_str(content_text).unwrap();
        let paths = query_result["paths"].as_array().unwrap();
        assert_eq!(paths.len(), 1);

        // Get stats
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(6)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_stats",
                "arguments": {}
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        let content_text = result["content"][0]["text"].as_str().unwrap();
        let stats: serde_json::Value = serde_json::from_str(content_text).unwrap();
        assert_eq!(stats["node_count"], 2);
        assert_eq!(stats["edge_count"], 1);
        assert_eq!(stats["frame_count"], 1);
    }

    #[test]
    fn test_mcp_register_template() {
        let engine = Engine::new(64);
        let mut server = McpServer::new(engine);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_register_template",
                "arguments": {
                    "id": 1,
                    "pattern": [{
                        "direction": "Outgoing",
                        "edge_type": 100
                    }],
                    "threshold": 0.5,
                    "max_candidates": 10,
                    "stale_window": 100
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());
        let result = response.result.unwrap();
        let content_text = result["content"][0]["text"].as_str().unwrap();
        let reg_result: serde_json::Value = serde_json::from_str(content_text).unwrap();
        assert_eq!(reg_result["registered"], true);
    }

    #[test]
    fn test_mcp_unknown_method() {
        let engine = Engine::new(64);
        let mut server = McpServer::new(engine);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "unknown/method".to_string(),
            params: None,
        };

        let response = server.handle_request(request);
        assert!(response.error.is_some());
        assert_eq!(response.error.unwrap().code, -32601);
    }

    /// DEBT-04: MCP krabnet_stats includes compaction metrics when compaction worker is configured.
    ///
    /// Creates an engine with compaction enabled, creates an McpServer, calls
    /// krabnet_stats, and verifies that all 4 compaction fields are present.
    #[test]
    fn test_mcp_stats_include_compaction_fields() {
        let engine = Engine::with_config(64, Some(10_000), None, None);
        let mut server = McpServer::new(engine);

        // Call krabnet_stats
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_stats",
                "arguments": {}
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        let result = response.result.unwrap();
        let content_text = result["content"][0]["text"].as_str().unwrap();
        let stats: serde_json::Value = serde_json::from_str(content_text).unwrap();

        // Verify compaction fields are present with initial zero values
        assert_eq!(
            stats["compactions_completed"], 0,
            "compactions_completed should be 0 for fresh engine with compaction enabled"
        );
        assert_eq!(
            stats["compaction_tuples_before"], 0,
            "compaction_tuples_before should be 0"
        );
        assert_eq!(
            stats["compaction_tuples_after"], 0,
            "compaction_tuples_after should be 0"
        );
        assert_eq!(
            stats["total_compaction_time_us"], 0,
            "total_compaction_time_us should be 0"
        );
    }

    /// DEBT-05 + DEBT-06: MCP WAL persistence and replay.
    ///
    /// Creates an McpServer::with_wal(), ingests 3 events via handle_request(),
    /// drops the server to flush the WAL, then replays via WalReader to verify
    /// all 3 events were persisted and can be recovered.
    #[test]
    fn test_mcp_wal_persistence_and_replay() {
        use crate::wal::{WalReader, WalWriter};

        let temp_dir = std::env::temp_dir().join(format!(
            "krabnet_mcp_wal_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&temp_dir);
        let wal_path = temp_dir.join("test.wal");
        let _ = std::fs::remove_file(&wal_path);

        // Create engine and WAL writer with sync_interval=1 (immediate flush)
        let engine = Engine::new(64);
        let wal_writer = WalWriter::new(&wal_path, 1).unwrap();

        // Create MCP server with WAL
        let mut server = McpServer::with_wal(engine, wal_writer);

        // Ingest 2 NodeAdded events
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_ingest",
                "arguments": {
                    "event_type": "NodeAdded",
                    "node_id": 1,
                    "type_id": 10
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(2)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_ingest",
                "arguments": {
                    "event_type": "NodeAdded",
                    "node_id": 2,
                    "type_id": 20
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        // Ingest 1 EdgeAdded event
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(3)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "krabnet_ingest",
                "arguments": {
                    "event_type": "EdgeAdded",
                    "edge_id": 0,
                    "source": 1,
                    "target": 2,
                    "type_id": 100
                }
            })),
        };
        let response = server.handle_request(request);
        assert!(response.error.is_none());

        // Drop the server (BufWriter flush on drop ensures data is written)
        drop(server);

        // Verify WAL file exists and has content
        assert!(wal_path.exists(), "WAL file should exist after ingest");
        let metadata = std::fs::metadata(&wal_path).unwrap();
        assert!(metadata.len() > 0, "WAL file should not be empty");

        // Replay WAL and verify 3 events were persisted
        let entries = WalReader::replay(&wal_path).unwrap();
        assert_eq!(entries.len(), 3, "should replay all 3 ingested events");

        // Verify event types match what was ingested
        match &entries[0].1 {
            crate::types::Event::NodeAdded { node_id, type_id } => {
                assert_eq!(node_id.0, 1);
                assert_eq!(type_id.0, 10);
            }
            other => panic!("expected NodeAdded, got {:?}", other),
        }
        match &entries[1].1 {
            crate::types::Event::NodeAdded { node_id, type_id } => {
                assert_eq!(node_id.0, 2);
                assert_eq!(type_id.0, 20);
            }
            other => panic!("expected NodeAdded, got {:?}", other),
        }
        match &entries[2].1 {
            crate::types::Event::EdgeAdded {
                edge_id,
                source,
                target,
                type_id,
            } => {
                assert_eq!(edge_id.0, 0);
                assert_eq!(source.0, 1);
                assert_eq!(target.0, 2);
                assert_eq!(type_id.0, 100);
            }
            other => panic!("expected EdgeAdded, got {:?}", other),
        }

        // Verify epochs are sequential (engine starts at epoch 0)
        let e0 = entries[0].0.0;
        assert_eq!(entries[1].0.0, e0 + 1, "second event should be epoch+1");
        assert_eq!(entries[2].0.0, e0 + 2, "third event should be epoch+2");

        // Cleanup
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_mcp_unknown_tool() {
        let engine = Engine::new(64);
        let mut server = McpServer::new(engine);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(serde_json::json!(1)),
            method: "tools/call".to_string(),
            params: Some(json!({
                "name": "nonexistent_tool",
                "arguments": {}
            })),
        };

        let response = server.handle_request(request);
        assert!(response.error.is_none()); // MCP returns tool errors in content, not JSON-RPC error
        let result = response.result.unwrap();
        assert_eq!(result["isError"], true);
    }
}
