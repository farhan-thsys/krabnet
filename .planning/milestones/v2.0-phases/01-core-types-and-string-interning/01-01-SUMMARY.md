---
phase: 01-core-types-and-string-interning
plan: 01
subsystem: infra
tags: [rust, newtypes, enums, string-interning, types, zero-allocation]

# Dependency graph
requires:
  - phase: none
    provides: "greenfield project -- no prior phase"
provides:
  - "Krabnet crate scaffold (Cargo.toml, lib.rs)"
  - "All shared newtypes: NodeId, EdgeId, TypeId, Epoch, Delta"
  - "All shared enums: PropertyValue, Direction, Filter, HopSpec, Event, DiffTuple, InterpretationTier, FrameTier"
  - "PropertySet type alias"
  - "Bidirectional string interner (Interner) with stable u32 IDs"
  - "Public re-exports via lib.rs for ergonomic use krabnet::*"
affects: [02-epoch-sequencer-ring-buffer, 03-property-graph, 04-differential-mvcc, 05-frame, 06-signal-routing, 07-interpretation-tiering, 08-embryonic, 09-engine, 10-benchmarks]

# Tech tracking
tech-stack:
  added: [crossbeam 0.8, bitvec 1.0, criterion 0.5 (dev)]
  patterns: [newtype-pattern, matklad-interner, zero-allocation-hot-path, init-only-mutability]

key-files:
  created:
    - Cargo.toml
    - src/lib.rs
    - src/types.rs
    - src/interner.rs
    - benches/krabnet_bench.rs
    - .cargo/config.toml
    - .gitignore
  modified: []

key-decisions:
  - "PropertyValue::Text uses u32 interned ID (not String) for zero-allocation hot path"
  - "DiffTuple is generic over T with bounds on impl blocks, not struct definition"
  - "Event does not carry Epoch -- epoch assigned by sequencer in Phase 2"
  - "Installed MSYS2 + MinGW-w64 binutils and switched to stable-x86_64-pc-windows-gnu toolchain to resolve missing MSVC Build Tools"

patterns-established:
  - "Newtype pattern: wrap primitive integers in single-field tuple structs for type safety"
  - "Init-only mutability: intern() takes &mut self, resolve() takes &self"
  - "Zero-allocation hot path: all identifiers are integers after initialization"
  - "Module organization: types.rs for shared vocabulary, interner.rs for string mapping"

requirements-completed: [INFRA-01, INFRA-02]

# Metrics
duration: 13min
completed: 2026-02-24
---

# Phase 1 Plan 1: Core Types and String Interning Summary

**Rust crate scaffold with 14 shared types (5 newtypes, 9 enums/structs/aliases) and bidirectional string interner using matklad HashMap+Vec pattern, all with zero clippy warnings and 24 passing tests**

## Performance

- **Duration:** 13 min
- **Started:** 2026-02-24T18:26:42Z
- **Completed:** 2026-02-24T18:39:24Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Created Krabnet crate from scratch with all foundational types that every subsequent phase imports
- Implemented bidirectional string interner with init-only mutability pattern enforcing zero-allocation hot path
- 17 unit tests + 7 doc-tests all passing, zero clippy warnings, zero build warnings
- Resolved Windows toolchain issues by installing MSYS2 MinGW-w64 and switching to GNU target

## Task Commits

Each task was committed atomically:

1. **Task 1: Create crate scaffold and core types module** - `d73184a` (feat)
2. **Task 2: Create string interner module with comprehensive tests** - `93a62af` (feat)

## Files Created/Modified
- `Cargo.toml` - Crate manifest with krabnet package definition, crossbeam/bitvec deps, criterion dev-dep
- `src/lib.rs` - Crate root with module declarations and public re-exports
- `src/types.rs` - All 14 shared newtypes and enums with doc comments and 6 inline tests
- `src/interner.rs` - Bidirectional string-to-u32 interner with 11 inline tests and 7 doc-tests
- `benches/krabnet_bench.rs` - Placeholder for Phase 10 Criterion benchmarks
- `.cargo/config.toml` - MinGW toolchain configuration note
- `.gitignore` - Excludes /target directory

## Decisions Made
- PropertyValue::Text uses u32 interned ID (not String) -- enforces zero-allocation hot path constraint
- DiffTuple<T> is generic with trait bounds on impl blocks, not on struct definition -- maximizes flexibility for Phase 4
- Event does not carry an Epoch field -- epoch is assigned externally by the sequencer (Phase 2 concern)
- Installed MSYS2 + MinGW-w64 and switched to `stable-x86_64-pc-windows-gnu` toolchain because the MSVC target lacked Windows SDK libraries

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing MSVC Build Tools / Windows SDK**
- **Found during:** Task 1 (cargo build verification)
- **Issue:** The `stable-x86_64-pc-windows-msvc` toolchain could not link because no MSVC linker or Windows SDK import libraries (kernel32.lib, etc.) were installed on the system
- **Fix:** Installed rustup, switched default toolchain to `stable-x86_64-pc-windows-gnu`, installed MSYS2 + mingw-w64-x86_64-binutils to provide the GNU assembler (as.exe) required by dlltool.exe for the windows-sys crate
- **Files modified:** None (system-level toolchain change)
- **Verification:** `cargo build` and `cargo test` both succeed with zero errors
- **Committed in:** Part of both task commits (toolchain fix was prerequisite)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Toolchain fix was necessary to compile any Rust code on this system. No scope creep.

## Issues Encountered
- GNU target's self-contained `dlltool.exe` could not find the GNU assembler (`as.exe`) -- resolved by installing MSYS2 MinGW-w64 binutils package

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All shared types are defined and importable via `use krabnet::*`
- String interner is ready for use by all downstream modules
- Crate compiles cleanly with zero warnings (build and clippy)
- Ready for Phase 2 (Epoch Sequencer and Ring Buffer), Phase 3 (Property Graph), or Phase 4 (Differential MVCC) -- all depend only on Phase 1

## Self-Check: PASSED

All 8 files verified present. Both commit hashes (d73184a, 93a62af) verified in git log.

---
*Phase: 01-core-types-and-string-interning*
*Completed: 2026-02-24*
