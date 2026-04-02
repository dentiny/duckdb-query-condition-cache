// Annotated mutex, lock_guard, unique_lock, and DUCKDB_* thread-safety macros.
// Include this (or the individual headers) when you want Clang thread-safety analysis.

#pragma once

#include "concurrency/annotated_lock.hpp"
#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"
