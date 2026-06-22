cmake_minimum_required(VERSION 3.14)
project(oasis-bench CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Release build by default for benchmarking
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Optimization flags for benchmarking
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG -march=native")

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

find_package(DuckDB QUIET)

if(DuckDB_FOUND)
    set(DUCKDB_TARGET DuckDB::DuckDB)
else()
    set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
    set(BUILD_SHELL OFF CACHE BOOL "" FORCE)

    add_subdirectory(duckdb)

    # DuckDB uses directory-scoped include_directories() internally, which do
    # not propagate to consumers via target_link_libraries. Expose the public
    # header directory so our targets can find <duckdb.hpp>. Newer DuckDB
    # versions also reference bundled fmt headers (fmt/core.h) from public
    # headers, so expose third_party/fmt/include too.
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/fmt/include)

    set(DUCKDB_TARGET duckdb)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# Main benchmark executables
add_executable(queries    src/queries.cpp)
add_executable(throughput src/throughput.cpp)
add_executable(verify     src/verify.cpp)

# Link executables
target_link_libraries(queries    PRIVATE ${DUCKDB_TARGET})
target_link_libraries(throughput PRIVATE ${DUCKDB_TARGET})
target_link_libraries(verify     PRIVATE ${DUCKDB_TARGET})

# The Oasis extension pulls in jemalloc via dlopen() at LOAD time. Loaded that
# late, jemalloc fails to claim static TLS ("cannot allocate memory in static
# TLS block"). Linking jemalloc directly into the executables makes it a startup
# dependency (like LD_PRELOAD), so it claims TLS before the surplus runs out.
# --no-as-needed keeps the dependency even though we don't call jemalloc directly.
#
# jemalloc has no CMake config, but ships a jemalloc.pc, so discover it via
# pkg-config (point PKG_CONFIG_PATH at its prefix, e.g. <prefix>/lib/pkgconfig).
# Fall back to a plain library search, overridable with -DJEMALLOC_LIB=<path>.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(JEMALLOC jemalloc)
endif()

if(JEMALLOC_FOUND)
    set(JEMALLOC_LINK ${JEMALLOC_LINK_LIBRARIES})
else()
    find_library(JEMALLOC_LIB jemalloc)
    set(JEMALLOC_LINK ${JEMALLOC_LIB})
endif()

if(JEMALLOC_LINK)
    message(STATUS "Linking jemalloc for oasis extension: ${JEMALLOC_LINK}")
    foreach(tgt queries throughput verify)
        target_link_libraries(${tgt} PRIVATE
            -Wl,--no-as-needed ${JEMALLOC_LINK} -Wl,--as-needed)
    endforeach()
else()
    message(WARNING "jemalloc not found; the oasis source may fail to LOAD its "
                    "extension. Set PKG_CONFIG_PATH to its lib/pkgconfig dir, pass "
                    "-DJEMALLOC_LIB=<path>, or run with LD_PRELOAD=<path>/libjemalloc.so.2.")
endif()
