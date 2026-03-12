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

PHONY: format-all test_release_all test_debug_all test_reldebug_all
