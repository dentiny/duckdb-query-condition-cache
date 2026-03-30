include_directories(include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

set(QUERY_CACHE_UNITTEST_OBJECTS
    main.cpp test_bitvector.cpp test_build_cache_entry.cpp test_cache_store.cpp
    test_filter.cpp test_normalize_expression.cpp)

add_executable(unittest_query_condition_cache ${QUERY_CACHE_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest_query_condition_cache test_helpers duckdb
                        ${EXTENSION_NAME})
else()
  target_link_libraries(unittest_query_condition_cache test_helpers
                        duckdb_static ${EXTENSION_NAME})
endif()
