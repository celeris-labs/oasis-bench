#pragma once

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

#include <duckdb.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <duckdb/main/query_profiler.hpp>

#include "common.hpp"

// ============================================================================
// Statistics
// ============================================================================

struct OperatorMetric {
    double   time        = 0; // CPU seconds summed over all workers
    uint64_t cardinality = 0; // rows returned by the operator

    void add(double t, uint64_t c) {
        time += t;
        cardinality += c;
    }

    OperatorMetric &operator+=(const OperatorMetric &rhs) {
        time += rhs.time;
        cardinality += rhs.cardinality;
        return *this;
    }
};

struct OperatorBreakdown {
    OperatorMetric scan;
    OperatorMetric filter;
    OperatorMetric projection;
    OperatorMetric join;
    OperatorMetric aggregation;
    OperatorMetric sort;
    OperatorMetric other;

    // Within-scan breakdown reported by the Oasis extension via extra_info
    // (CPU seconds summed over all workers; 0 for other sources).
    double scan_filter   = 0;
    double string_decode = 0;

    OperatorBreakdown &operator+=(const OperatorBreakdown &rhs) {
        scan += rhs.scan;
        filter += rhs.filter;
        projection += rhs.projection;
        join += rhs.join;
        aggregation += rhs.aggregation;
        sort += rhs.sort;
        other += rhs.other;
        scan_filter += rhs.scan_filter;
        string_decode += rhs.string_decode;
        return *this;
    }
};

// Named accessors for iterating the per-operator fields when writing JSON.
constexpr std::pair<const char *, OperatorMetric OperatorBreakdown::*> OPERATOR_FIELDS[] = {
    {"scan", &OperatorBreakdown::scan},
    {"filter", &OperatorBreakdown::filter},
    {"projection", &OperatorBreakdown::projection},
    {"join", &OperatorBreakdown::join},
    {"aggregation", &OperatorBreakdown::aggregation},
    {"sort", &OperatorBreakdown::sort},
    {"other", &OperatorBreakdown::other},
};

struct Statistics {
    double            runtime;
    double            latency;
    double            cpu_time;
    size_t            memory_footprint;
    size_t            rows_scanned;
    OperatorBreakdown ops;
};

Statistics median(std::vector<Statistics> values) {
    assert(!values.empty());
    std::sort(values.begin(), values.end(),
              [](const Statistics &a, const Statistics &b) { return a.latency < b.latency; });
    return values[values.size() / 2];
};

// Parse a "%.4fs" duration from extra_info; 0 if the key is absent.
double extra_info_seconds(const duckdb::InsertionOrderPreservingMap<std::string> &extra_info,
                          const std::string                                      &key) {
    auto it = extra_info.find(key);
    return it == extra_info.end() ? 0 : std::stod(it->second);
}

OperatorBreakdown traverse(const duckdb::ProfilingNode &node) {
    OperatorBreakdown stats;
    auto             &info = node.GetOperatorMetrics();

    const std::string &type        = info.name;
    double             cpu_time    = info.time;
    uint64_t           cardinality = info.elements_returned;

    // Oasis scan timers (only the Oasis scan operator sets these keys).
    const auto &extra_info = info.GetExtraInfo();
    stats.scan_filter      = extra_info_seconds(extra_info, "Filter Time");
    stats.string_decode    = extra_info_seconds(extra_info, "String Decode Time");

    if (type == "HASH_JOIN" || type == "LEFT_DELIM_JOIN" || type == "RIGHT_DELIM_JOIN" ||
        type == "NESTED_LOOP_JOIN" || type == "CROSS_PRODUCT" || type == "PIECEWISE_MERGE_JOIN") {
        stats.join.add(cpu_time, cardinality);
    } else if (type == "SEQ_SCAN" || type == "READ_PARQUET" || type == "COLUMN_DATA_SCAN" ||
               type == "DELIM_SCAN" || type == "DUMMY_SCAN" || type == "TABLE_SCAN") {
        stats.scan.add(cpu_time, cardinality);
    } else if (type == "FILTER") {
        stats.filter.add(cpu_time, cardinality);
    } else if (type == "PROJECTION") {
        stats.projection.add(cpu_time, cardinality);
    } else if (type == "HASH_GROUP_BY" || type == "PERFECT_HASH_GROUP_BY" ||
               type == "UNGROUPED_AGGREGATE" || type == "WINDOW") {
        stats.aggregation.add(cpu_time, cardinality);
    } else if (type == "ORDER_BY" || type == "TOP_N") {
        stats.sort.add(cpu_time, cardinality);
    } else if (type == "CTE" || type == "CTE_SCAN" || type == "UNION" ||
               type == "STREAMING_LIMIT") {
        stats.other.add(cpu_time, cardinality);
    } else if (type == "" || type == "RESULT_COLLECTOR") {
        // NOP: empty nodes and the root result collector sink are not real
        // operators and carry no meaningful operator time.
    } else {
        std::cout << "Unknown type " << type << std::endl;
        stats.other.add(cpu_time, cardinality);
    }

    return stats;
};

// ============================================================================
// Timed execution
// ============================================================================

// Run `sql` once (profiling must already be enabled on `con`) and collect the
// run's statistics from the profiler tree.
Statistics measure_once(duckdb::Connection &con, const std::string &sql) {
    Timer timer;
    timer.start();
    auto r = con.Query(sql);

    if (r->HasError()) {
        throw std::runtime_error("Query error: " + r->GetError());
    }

    // Materialize results (must drain fully before profiling tree is available)
    while (r->Fetch()) {}
    double elapsed = timer.stop_seconds();

    // Get statistics.
    auto &profiler = duckdb::QueryProfiler::Get(*con.context);
    if (!profiler.HasRoot()) {
        throw std::runtime_error("Profiling tree is null for query: " + sql);
    }

    Statistics stat;
    stat.runtime          = elapsed;
    stat.latency          = elapsed;
    stat.cpu_time         = 0;
    stat.rows_scanned     = 0;
    stat.memory_footprint = 0;

    for (const auto &entry : profiler.GetTreeMap()) {
        const auto &node = entry.second.get();
        const auto &info = node.GetOperatorMetrics();
        stat.ops += traverse(node);
        stat.cpu_time += info.time;
        stat.rows_scanned += info.rows_scanned;
        stat.memory_footprint =
            std::max<size_t>(stat.memory_footprint, info.system_peak_buffer_manager_memory);
    }

    return stat;
}

// Run `sql` `repetitions` times (profiling must already be enabled on `con`)
// and return the statistics of the median-latency run.
Statistics measure_query(duckdb::Connection &con, const std::string &sql, int repetitions) {
    std::vector<Statistics> stats;
    for (int i = 0; i < repetitions; ++i) {
        stats.push_back(measure_once(con, sql));
    }
    return median(stats);
}

// ============================================================================
// Hardware counters (oasis_stream_profile)
// ============================================================================

// One oasis_stream_profile() snapshot: a row of counters per hardware decoder,
// plus one for the RDMA bypass stream on RDMA bitstreams. Values are
// pre-rendered for JSON output, and non-numeric columns get quoted.
struct HardwareProfile {
    std::vector<std::string>              names;
    std::vector<bool>                     quoted;
    std::vector<std::vector<std::string>> rows;
    std::vector<bool>                     bypass; // per row: the bypass stream (no input counters)
};

HardwareProfile read_stream_profile(duckdb::Connection &con) {
    HardwareProfile profile;

    auto r = con.Query("SELECT * FROM oasis_stream_profile()");
    if (r->HasError()) {
        throw std::runtime_error("Failed to read oasis_stream_profile: " + r->GetError());
    }

    for (const auto &name : r->GetNames()) {
        profile.names.push_back(name.GetIdentifierName());
    }
    for (const auto &type : r->GetTypes()) {
        profile.quoted.push_back(!type.IsNumeric());
    }
    size_t in_handshakes = static_cast<size_t>(
        std::find(profile.names.begin(), profile.names.end(), "in_handshakes_cycles") - profile.names.begin());
    for (const auto &row : fetch_rows(*r)) {
        std::vector<std::string> values;
        for (const auto &value : row) {
            values.push_back(value.ToString());
        }
        profile.rows.push_back(std::move(values));
        profile.bypass.push_back(in_handshakes < row.size() && row[in_handshakes].IsNull());
    }

    return profile;
}

struct OutCycles {
    uint64_t total      = 0; // output cycles (handshakes+starved+stalled+idle) of active decoders
    uint64_t handshakes = 0; // output handshake cycles of active decoders
    uint64_t stalled    = 0; // output stalled cycles of active decoders
    uint64_t idle       = 0; // output idle cycles of active decoders
};

// Sum the output-side cycle counters of a profile snapshot over the bypass row
// or over the decoder rows with stream < num_decoders (0 = all decoders).
// Decoders the scheduler does not use keep stale counters from earlier runs.
OutCycles sum_out_cycles(const HardwareProfile &profile, bool bypass, uint32_t num_decoders = 0) {
    auto col = [&](const std::string &name) {
        auto it = std::find(profile.names.begin(), profile.names.end(), name);
        if (it == profile.names.end()) {
            throw std::runtime_error("Missing column in oasis_stream_profile: " + name);
        }
        return static_cast<size_t>(it - profile.names.begin());
    };
    size_t stream     = col("stream");
    size_t handshakes = col("out_handshakes_cycles");
    size_t starved    = col("out_starved_cycles");
    size_t stalled    = col("out_stalled_cycles");
    size_t idle       = col("out_idle_cycles");

    OutCycles cycles;
    for (size_t i = 0; i < profile.rows.size(); ++i) {
        const auto &row       = profile.rows[i];
        uint64_t    stream_id = std::stoull(row[stream]);
        if (profile.bypass[i] != bypass || (!bypass && num_decoders > 0 && stream_id >= num_decoders)) {
            continue;
        }
        uint64_t h  = std::stoull(row[handshakes]);
        uint64_t sv = std::stoull(row[starved]);
        uint64_t st = std::stoull(row[stalled]);
        uint64_t id = std::stoull(row[idle]);
        cycles.total += h + sv + st + id;
        cycles.handshakes += h;
        cycles.stalled += st;
        cycles.idle += id;
    }
    return cycles;
}
