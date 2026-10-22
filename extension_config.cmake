# This file is included by DuckDB's build system. It specifies which extension
# to load

duckdb_extension_load(
  query_condition_cache SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR} EXTENSION_VERSION
  0.2.0 LOAD_TESTS)
duckdb_extension_statically_link(query_condition_cache)
