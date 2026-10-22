cmake_minimum_required(VERSION 3.10)

set(TARGET_NAME query_condition_cache)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

if(NOT DEFINED CMAKE_CXX_STANDARD OR CMAKE_CXX_STANDARD LESS 17)
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(UNIX OR APPLE)
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -Wno-c++20-extensions -Wunused-variable -Wunused-function"
  )
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
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

if(BUILD_UNITTESTS)
  add_subdirectory(test/unittest)
endif()

if(${BUILD_BENCHMARKS})
  add_executable(benchmark_cache_build benchmark/benchmark_cache_build.cpp)
  if(PREBUILT_BINARY)
    target_link_libraries(benchmark_cache_build duckdb_static)
    link_extension_libraries(benchmark_cache_build "")
  else()
    target_link_libraries(benchmark_cache_build duckdb ${EXTENSION_NAME})
  endif()
endif()
