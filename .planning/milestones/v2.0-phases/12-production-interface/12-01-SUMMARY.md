---
phase: 12-production-interface
plan: 01
subsystem: api
tags: [grpc, tonic, prost, protobuf, tokio, streaming, broadcast]

# Dependency graph
requires:
  - phase: 11-harden-engine
    provides: "Engine with compaction, coalescing, fanout, hysteresis, Arc<RwLock<Frame>>"
provides:
  - "KrabnetService gRPC server with 8 RPCs (IngestEvent, RegisterFrame, QueryFrame, SubscribeFrame, ListFrames, EvictFrame, RegisterEmbryonicTemplate, GetStats)"
  - "Proto schema (proto/krabnet.proto) with all message types"
  - "build.rs with protox-based proto compilation (no protoc required)"
  - "Engine helper methods: list_frames(), evict_frame(), current_epoch()"
  - "TEST-18: gRPC ingest-and-query roundtrip integration test"
affects: [12-production-interface-02, 12-production-interface-04, 13-scale-and-optimize]

# Tech tracking
tech-stack:
  added: [tokio-1.38.1, tonic-0.12, prost-0.13, serde-1, serde_json-1, tokio-stream-0.1, async-stream-0.3, protox-0.7]
  patterns: [arc-rwlock-engine-grpc, broadcast-frame-streaming, protox-no-protoc-build, proto-to-domain-conversion]

key-files:
  created:
    - proto/krabnet.proto
    - build.rs
    - src/grpc.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - src/engine.rs
    - src/lib.rs

key-decisions:
  - "Pin tokio=1.38.1 to avoid windows-sys 0.60+/dlltool incompatibility on GNU toolchain"
  - "Use protox for proto compilation instead of requiring protoc binary"
  - "Allow clippy::result_large_err for tonic::Status return types (standard gRPC pattern)"
  - "KrabnetServer wraps Engine via Arc<RwLock<Engine>> -- write lock for mutations, read lock for queries"
  - "SubscribeFrame uses tokio::sync::broadcast with 1024 capacity"
  - "async-stream crate for ergonomic server-streaming SubscribeFrame implementation"

patterns-established:
  - "Proto-to-domain conversion pattern: proto_event_to_event, hopspecs_from_proto, paths_to_proto"
  - "gRPC server pattern: Arc<RwLock<Engine>> with per-method lock granularity"
  - "Protox build pattern: protox::compile + tonic_build::compile_fds for protoc-free builds"

requirements-completed: [GRPC-01, GRPC-02, GRPC-03, GRPC-04, TEST-18]

# Metrics
duration: 13min
completed: 2026-02-25
---

# Phase 12 Plan 01: gRPC Server with 8 RPCs Summary

**KrabnetService gRPC server with 8 RPCs (IngestEvent, RegisterFrame, QueryFrame, SubscribeFrame, ListFrames, EvictFrame, RegisterEmbryonicTemplate, GetStats) plus protobuf schema and protox-based build pipeline**

## Performance

- **Duration:** 13 min
- **Started:** 2026-02-24T21:33:25Z
- **Completed:** 2026-02-24T21:46:40Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Protobuf schema with 8 RPC methods and all message types compiles via protox+tonic-build (no protoc required)
- gRPC server implements all 8 methods operating on Arc<RwLock<Engine>>
- SubscribeFrame uses tokio::sync::broadcast for real-time frame update streaming
- TEST-18 roundtrip test passes: ingest events via gRPC, register frame, query, verify paths, get stats, list frames, evict, register template
- All 135 lib tests pass, 39 doc tests pass, 0 clippy warnings

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Phase 12 deps, proto schema, build.rs** - `eba7020` (feat)
2. **Task 2: Implement gRPC server with all 8 methods and integration test** - `45ab541` (feat)

## Files Created/Modified
- `proto/krabnet.proto` - Protobuf service definition with 8 RPCs and all message types
- `build.rs` - Protox-based proto compilation (no protoc binary required)
- `src/grpc.rs` - KrabnetServer struct implementing all 8 gRPC methods, proto-to-domain conversion, roundtrip test
- `Cargo.toml` - Phase 12 dependencies (tokio, tonic, prost, serde, serde_json, tokio-stream, async-stream, protox, tonic-build)
- `Cargo.lock` - Updated lockfile with new dependencies
- `src/engine.rs` - Added list_frames(), evict_frame(), current_epoch() helper methods
- `src/lib.rs` - Added grpc module and KrabnetServer re-export

## Decisions Made
- Pinned tokio=1.38.1 to avoid windows-sys 0.60+/0.61+ which require dlltool (unavailable on this GNU toolchain). Tokio 1.38 uses mio 0.8.x which uses windows-sys 0.48/0.52.
- Used protox crate for pure-Rust proto parsing instead of requiring protoc binary. Build.rs calls protox::compile() then tonic_build::compile_fds() for seamless no-external-tools compilation.
- Allowed clippy::result_large_err on proto_event_to_event and hopspecs_from_proto since tonic::Status is the standard error type for gRPC services.
- KrabnetServer uses Arc<RwLock<Engine>> instead of Mutex because Engine has both read-only (stats, list_frames) and write (ingest, register) operations.
- SubscribeFrame returns a broadcast-filtered stream using async_stream::stream! macro for ergonomic async generator syntax.
- Pinned tempfile=3.10.1 in build-deps to avoid getrandom 0.4 which pulls in windows-targets 0.53 (dlltool dependency).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Pinned tokio version to avoid dlltool/windows-sys incompatibility**
- **Found during:** Task 1 (dependency setup)
- **Issue:** tokio "full" features and latest versions pull in windows-sys 0.60+/0.61+ which require dlltool.exe for raw-dylib linking -- dlltool fails with CreateProcess error on this GNU toolchain
- **Fix:** Pinned tokio=1.38.1 with selective features (no parking_lot), pinned tempfile=3.10.1 in build-deps
- **Files modified:** Cargo.toml
- **Verification:** cargo build succeeds, all tests pass
- **Committed in:** eba7020 (Task 1)

**2. [Rule 3 - Blocking] Used protox instead of protoc for proto compilation**
- **Found during:** Task 1 (proto compilation)
- **Issue:** protoc binary not installed on system, tonic-build/prost-build requires it
- **Fix:** Added protox build-dep, changed build.rs to use protox::compile() + tonic_build::compile_fds()
- **Files modified:** Cargo.toml, build.rs
- **Verification:** cargo build compiles proto schema successfully
- **Committed in:** eba7020 (Task 1)

**3. [Rule 1 - Bug] Added async-stream dependency for SubscribeFrame streaming**
- **Found during:** Task 2 (SubscribeFrame implementation)
- **Issue:** async_stream::stream! macro used but crate not in dependencies
- **Fix:** Added async-stream = "0.3" to Cargo.toml
- **Files modified:** Cargo.toml
- **Verification:** Build succeeds, all methods compile
- **Committed in:** 45ab541 (Task 2)

---

**Total deviations:** 3 auto-fixed (2 blocking, 1 bug)
**Impact on plan:** All fixes necessary for compilation on this platform. No scope creep.

## Issues Encountered
None beyond the auto-fixed blocking issues above.

## User Setup Required
None - no external service configuration required. Protoc is not needed thanks to protox.

## Next Phase Readiness
- gRPC server ready for integration into krabnet-server binary (Plan 12-04)
- KrabnetServer::new() constructor takes Arc<RwLock<Engine>> for easy wiring
- Proto types available via krabnet::grpc::proto for client code
- broadcast channel available for real-time frame subscriptions
- Engine list_frames() and evict_frame() methods available for MCP server (Plan 12-02)

---
## Self-Check: PASSED

- FOUND: proto/krabnet.proto
- FOUND: build.rs
- FOUND: src/grpc.rs
- FOUND: src/engine.rs
- FOUND: src/lib.rs
- FOUND: Cargo.toml
- FOUND: .planning/phases/12-production-interface/12-01-SUMMARY.md
- FOUND: commit eba7020
- FOUND: commit 45ab541

---
*Phase: 12-production-interface*
*Completed: 2026-02-25*
