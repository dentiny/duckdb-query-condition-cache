# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(query_condition_cache
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(icu)
duckdb_extension_load(json)
duckdb_extension_load(tpch)
duckdb_extension_load(tpcds)
duckdb_extension_load(autocomplete)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 74f954001f3a740c909181b02259de6c7b942632
)
