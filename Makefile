PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=query_condition_cache
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

test_reldebug_all:
	@echo "Running tests from test/sql..."
	./build/reldebug/$(TEST_PATH) --test-dir $(PROJ_DIR) "$(TESTS_BASE_DIRECTORY)*"
	@echo "Running tests from duckdb/test..."
	./build/reldebug/$(TEST_PATH) --test-dir $(PROJ_DIR)duckdb "test/*"

format-all: format
	find test/unittest -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	cmake-format -i test/unittest/CMakeLists.txt

PHONY: format-all test_reldebug_all
