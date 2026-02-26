# Milestones

## v2.0 Harden, Production Interface, Scale & Optimize (Shipped: 2026-02-26)

**Phases:** 11-15 (5 phases, 12 plans)
**Requirements:** 67/67 complete
**Timeline:** 2026-02-25 → 2026-02-26

**Key accomplishments:**
- Engine hardening: background compaction, mutation coalescing, fan-out limits, hysteresis for concurrent load
- gRPC server with 8 RPCs including real-time SubscribeFrame broadcast and WAL crash recovery
- MCP JSON-RPC server with 5 tools for AI agent integration (hardened with Engine::with_config)
- Enterprise data structures: Set-Trie inverted index, Count-Min Sketch scoring, trunk/leaf detection, custom buffer pool
- Learned template weighting with auto-deactivation for embryonic discovery
- Full integration wiring: post-ingest broadcast, Tier 3 LLM pipeline, hardened MCP binary
- 180 lib tests, 53 doc-tests, 13 benchmarks (including enterprise-scale: 100K nodes, 1M edges)

**Git range:** `702b5c6` (feat(11-01)) → `999d8fc` (feat(15-01))
**Net change:** +14,913 / -441 lines across 53 files

---


## v3.0 Tech Debt Closure + Incremental Path Extension (Shipped: 2026-02-27)

**Phases:** 16-21 (6 phases, 10 plans)
**Requirements:** 30/30 complete
**Timeline:** 2026-02-26 → 2026-02-27

**Key accomplishments:**
- Closed all v2.0 tech debt: AnthropicClient LLM integration, CompactionStats in gRPC/MCP, WAL persistence with crash recovery
- Built correctness oracle baseline: frame maintenance wired into ingest pipeline with full DFS re-traverse + diff oracle (6 scenarios)
- Created PathExtender module (1,799 lines): incremental EdgeAdded path extension via backward prefix + forward extension algorithm
- Implemented incremental removal: EdgeRemoved and NodeRemoved retract affected paths as -1 deltas with parallel-edge survival and DeletionContext
- Added incremental PropertyChanged: hop filter re-evaluation with combined retract+assert for atomicity
- Proved O(affected) scaling: Criterion benchmarks at 3 graph scales + 500K-event stress test at 74K events/sec with oracle verification
- All event types except NodeAdded dispatched incrementally — full DFS re-traverse eliminated for EdgeAdded, EdgeRemoved, NodeRemoved, PropertyChanged

**Git range:** `651d8c0` (feat(16-01)) → `68b0775` (docs(phase-21))
**Net change:** +11,401 / -159 lines across 47 files
**Final codebase:** 17,463 LOC Rust, 244 lib tests, 54 doc-tests, 24 benchmarks

---

