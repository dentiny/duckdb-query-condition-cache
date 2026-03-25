PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=query_condition_cache
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

test_release_all:
	@echo "Running tests from duckdb/test..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/enable_query_condition_cache.json ./build/release/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*"
	@echo "Running tests from test/sql..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/enable_query_condition_cache.json ./build/release/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*"

test_debug_all:
	@echo "Running tests from duckdb/test..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/enable_query_condition_cache.json ./build/debug/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*"
	@echo "Running tests from test/sql..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/enable_query_condition_cache.json ./build/debug/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*"

test_reldebug_all:
	@echo "Running tests from duckdb/test..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/enable_query_condition_cache.json ./build/reldebug/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*"
	@echo "Running tests from test/sql..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/enable_query_condition_cache.json ./build/reldebug/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*"

format-all: format
	find test/unittest -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	cmake-format -i test/unittest/CMakeLists.txt

benchmark_cache_build:
	cmake -DBUILD_BENCHMARKS=ON -B build/release -S duckdb
	cmake --build build/release --target benchmark_cache_build
	./build/release/extension/query_condition_cache/benchmark_cache_build

PHONY: format-all test_release_all test_debug_all test_reldebug_all benchmark_cache_build
