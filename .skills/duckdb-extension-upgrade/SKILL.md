---
name: upgrade-duckdb-extension
description: Upgrade the query_condition_cache DuckDB extension to a new DuckDB release. Use when the user asks to upgrade DuckDB, bump the duckdb submodule, sync to a new DuckDB tag (e.g. v1.5.2), or update the duckdb / extension-ci-tools submodules and CI pins together.
---

# Upgrade query_condition_cache to a new DuckDB release

Two submodules and the distribution workflow pins normally move together. Then build, run the extension tests, and note any DuckDB C++ API adjustments needed for this extension.

## Inputs

Before starting, confirm the target DuckDB version (e.g. `v1.5.2`). Everything else is derived from it.

## Workflow

Track these as a checklist; do not skip ahead:

```
- [ ] 1. Pin duckdb submodule to tags/$TARGET
- [ ] 2. Pin extension-ci-tools submodule to $TARGET, or the latest compatible branch/tag if $TARGET is unavailable
- [ ] 4. Build: CMAKE_BUILD_PARALLEL_LEVEL=10 make reldebug
- [ ] 5. Test: make test_reldebug
```

## Reference: historical upgrade commits

- `4a601f5` - `Upgrade duckdb v1.5.2 (#66)`. Minimal: duckdb submodule + small C++ API adjustment.
- `2c6458a` - `Bumpup to DuckDB v1.5.1 (#49)`.
- `cf4bb0c` - `Upgrade to v1.5.0 (#19)`.
