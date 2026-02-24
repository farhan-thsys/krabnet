---
phase: 12-production-interface
plan: 02
subsystem: api
tags: [mcp, json-rpc, stdio, ai-agents, model-context-protocol]

# Dependency graph
requires:
  - phase: 12-production-interface-01
    provides: "Engine with list_frames(), evict_frame(), current_epoch() helpers and gRPC server"
provides:
  - "McpServer JSON-RPC 2.0 server with 5 tools over stdio"
  - "krabnet-mcp binary entry point for AI agent connections"
  - "TEST-19: MCP tools/list integration test verifying 5 tools"
affects: [12-production-interface-04, 13-scale-and-optimize]

# Tech tracking
tech-stack:
  added: []
  patterns: [mcp-json-rpc-stdio, single-threaded-engine-ownership, tool-dispatch-pattern]

key-files:
  created:
    - src/mcp.rs
    - src/bin/krabnet-mcp.rs
  modified:
    - src/lib.rs
    - Cargo.toml

key-decisions:
  - "McpServer owns Engine directly (not Arc<RwLock>) -- single-threaded stdio, no concurrent access needed"
  - "MCP tool errors returned as content with isError flag, not JSON-RPC error objects (per MCP spec)"
  - "JsonRpcRequest/JsonRpcResponse types made pub for testability of handle_request method"

patterns-established:
  - "MCP tool dispatch pattern: tools/call routes to tool_* methods via name matching"
  - "Hop spec JSON parsing shared between register_frame and register_template tools"
  - "MCP tool results wrapped in content array with type:text per MCP protocol"

requirements-completed: [MCP-01, MCP-02, MCP-03, TEST-19, BIN-02]

# Metrics
duration: 6min
completed: 2026-02-25
---

# Phase 12 Plan 02: MCP Server with 5 Tools Summary

**MCP JSON-RPC 2.0 server over stdio with 5 tools (ingest, register_frame, query_frame, stats, register_template) and krabnet-mcp binary entry point**

## Performance

- **Duration:** 6 min
- **Started:** 2026-02-24T21:50:29Z
- **Completed:** 2026-02-24T21:56:43Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- MCP server handles initialize, tools/list, and tools/call JSON-RPC methods over stdio
- 5 tools exposed: krabnet_ingest, krabnet_register_frame, krabnet_query_frame, krabnet_stats, krabnet_register_template
- krabnet-mcp binary compiles and starts the MCP stdio loop
- 6 MCP integration tests pass: tools_list (5 tools), initialize, ingest-and-query roundtrip, register_template, unknown_method, unknown_tool
- All 144 lib tests pass, 42 doc tests pass, 0 clippy warnings

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement MCP JSON-RPC server with 5 tools** - `784ccc3` (feat)
2. **Task 2: Create krabnet-mcp binary entry point** - `092cd72` (feat)

## Files Created/Modified
- `src/mcp.rs` - McpServer struct with JSON-RPC 2.0 handling, 5 tool implementations, hop spec parsing, 6 integration tests
- `src/bin/krabnet-mcp.rs` - Binary entry point initializing Engine(1024) and running MCP stdio loop
- `src/lib.rs` - Added mcp module declaration and McpServer re-export
- `Cargo.toml` - Added [[bin]] target for krabnet-mcp

## Decisions Made
- McpServer owns Engine directly (not Arc<RwLock>) because MCP operates single-threaded over stdio -- no concurrent access needed, simpler ownership model
- MCP tool errors returned as content with isError:true flag rather than JSON-RPC error objects, following MCP protocol convention where tool execution errors are not transport errors
- JsonRpcRequest/JsonRpcResponse/JsonRpcError types made pub to resolve private_interfaces warnings since handle_request is a pub method for testability

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- MCP server ready for integration into unified server binary (Plan 12-04)
- McpServer::new(engine) constructor takes owned Engine for simple wiring
- Binary can be tested by piping JSON to stdin: `echo '{"jsonrpc":"2.0","id":1,"method":"initialize"}' | krabnet-mcp`
- All 5 tools functional and tested with roundtrip verification

---
## Self-Check: PASSED

- FOUND: src/mcp.rs
- FOUND: src/bin/krabnet-mcp.rs
- FOUND: src/lib.rs
- FOUND: Cargo.toml
- FOUND: .planning/phases/12-production-interface/12-02-SUMMARY.md
- FOUND: commit 784ccc3
- FOUND: commit 092cd72

---
*Phase: 12-production-interface*
*Completed: 2026-02-25*
