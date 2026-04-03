PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=query_condition_cache
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

SKIP_SLOW ?= 1
SLOW_FILTER := $(if $(filter 1,$(SKIP_SLOW)),~[.],-[.])

test_release_all:
	@echo "Running tests from duckdb/test... (SKIP_SLOW=$(SKIP_SLOW))"
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/use_query_condition_cache.json ./build/release/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*" "$(SLOW_FILTER)"
	@echo "Running tests from test/sql..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/use_query_condition_cache.json ./build/release/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*" "$(SLOW_FILTER)"

test_debug_all:
	@echo "Running tests from duckdb/test... (SKIP_SLOW=$(SKIP_SLOW))"
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/use_query_condition_cache.json ./build/debug/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*" "$(SLOW_FILTER)"
	@echo "Running tests from test/sql..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/use_query_condition_cache.json ./build/debug/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*" "$(SLOW_FILTER)"

test_reldebug_all:
	@echo "Running tests from duckdb/test... (SKIP_SLOW=$(SKIP_SLOW))"
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/use_query_condition_cache.json ./build/reldebug/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*" "$(SLOW_FILTER)"
	@echo "Running tests from test/sql..."
	DUCKDB_TEST_CONFIG=$(PROJ_DIR)test/configs/use_query_condition_cache.json ./build/reldebug/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*" "$(SLOW_FILTER)"

format-all: format
	find test/unittest -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	cmake-format -i test/unittest/CMakeLists.txt

# Benchmark targets
# Usage: make benchmark_tpch [SF=1] [REPEAT=3] [MEMORY_LIMIT=4GB]
SF ?= 1
REPEAT ?= 3
BENCHMARK_OUT ?= benchmark/results_sf$(SF).md
MEMORY_LIMIT ?=

BENCHMARK_MEMORY_FLAG = $(if $(MEMORY_LIMIT),--memory-limit $(MEMORY_LIMIT),)

benchmark_tpch:
	uv run benchmark/run_tpch_benchmark.py --sf $(SF) --repeat $(REPEAT) --out $(BENCHMARK_OUT) $(BENCHMARK_MEMORY_FLAG)

benchmark_tpch_dashboard:
	uv run benchmark/run_tpch_benchmark.py --sf $(SF) --repeat $(REPEAT) --out $(BENCHMARK_OUT) --no-tpch $(BENCHMARK_MEMORY_FLAG)

benchmark_tpch_no_chart:
	uv run benchmark/run_tpch_benchmark.py --sf $(SF) --repeat $(REPEAT) --out $(BENCHMARK_OUT) --no-chart $(BENCHMARK_MEMORY_FLAG)

benchmark_cache_build:
	cmake -DBUILD_BENCHMARKS=ON -B build/release -S duckdb
	cmake --build build/release --target benchmark_cache_build
	./build/release/extension/query_condition_cache/benchmark_cache_build

PHONY: format-all test_release_all test_debug_all test_reldebug_all benchmark_tpch benchmark_tpch_dashboard benchmark_tpch_no_chart benchmark_cache_build
