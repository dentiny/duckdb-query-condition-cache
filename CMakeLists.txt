cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME query_condition_cache)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wno-c++17-extensions -Wno-c++20-extensions -Wunused-variable -Wunused-function"
  )
endif()

if(CMAKE_CXX_COMPILER_ID_LOWER MATCHES "clang" OR CMAKE_CXX_COMPILER_LOWER
                                                  MATCHES "clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wthread-safety")
endif()

include_directories(src/include)

set(EXTENSION_SOURCES
    src/cache_invalidation_optimizer.cpp
    src/logical_cache_invalidator.cpp
    src/physical_cache_invalidator.cpp
    src/predicate_key_utils.cpp
    src/query_condition_cache_extension.cpp
    src/query_condition_cache_filter.cpp
    src/query_condition_cache_functions.cpp
    src/query_condition_cache_optimizer.cpp
    src/query_condition_cache_state.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

add_subdirectory(test/unittest)

if(${BUILD_BENCHMARKS})
  add_executable(benchmark_cache_build benchmark/benchmark_cache_build.cpp)
  target_link_libraries(benchmark_cache_build duckdb ${EXTENSION_NAME})
endif()
