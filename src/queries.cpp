#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include <duckdb.hpp>
#include <duckdb/main/profiler/profiling_node.hpp>
#include <duckdb/main/query_profiler.hpp>

#include "common.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string data_dir    = "/mnt/ramdisk";
    std::string ip          = ""; // RDMA server IP (rdma:// data dir)
    Benchmark   benchmark   = Benchmark::TPCH;
    Source      source      = Source::PARQUET;
    int         threads     = 1;
    int         repetitions = 5;
    uint32_t    decoders = 0; // oasis_scheduler_num_streams (0 = extension default)
};

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
// Benchmark execution
// ============================================================================

struct QueryResult {
    std::string filename;
    Statistics  stats;
};

std::vector<QueryResult> run_benchmark(const Config                &config,
                                       const std::vector<fs::path> &query_paths) {
    std::vector<QueryResult> results;

    // Initialize DuckDB
    duckdb::DBConfig duck_config;
    duck_config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
    duckdb::DuckDB     db(nullptr, &duck_config); // In-memory database
    duckdb::Connection con(db);

    global_setup(con, is_rdma_dir(config.data_dir), config.source, config.ip,
                 config.decoders);

    // Load data
    std::cout << "Loading data..." << std::endl;
    auto tables = load_data(con, config.data_dir, config.benchmark, config.source);

    std::cout << "Warming metadata cache..." << std::endl;
    warm_metadata_cache(con, tables);

    con.Query("SET threads TO " + std::to_string(config.threads));

    // Enable profiling for all operator types
    con.Query("PRAGMA enable_profiling='no_output'");
    con.Query("PRAGMA profiling_coverage='ALL'");

    Timer timer;

    // Run each query
    for (const auto &query_path : query_paths) {
        std::vector<Statistics> stats;

        // Print filename (matching Python behavior)
        std::cout << "Running query " << query_path.string() << "..." << std::endl;

        // Read query
        std::ifstream f(query_path);
        std::string   sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

        QueryResult result;
        result.filename = query_path.string();

        // Timed runs
        for (int i = 0; i < config.repetitions; ++i) {
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
                throw std::runtime_error("Profiling tree is null for query: " + query_path.string());
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
            stats.push_back(stat);
        }

        // Calculate average
        result.stats = median(stats);
        results.push_back(result);
    }

    return results;
}

// ============================================================================
// Command Line Parsing
// ============================================================================

void print_usage(const char *prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "DuckDB benchmark with different input configurations.\n"
              << "\n"
              << "Options:\n"
              << "  -b, --benchmark BM    Benchmark: tpch, clickbench (default: tpch)\n"
              << "  -s, --source SRC      Input source: parquet, memory, oasis (default: parquet)\n"
              << "  -r, --repetitions N   Number of repetitions per query (default: 5)\n"
              << "  -t, --threads N       Number of threads (default: 1)\n"
              << "  -n, --decoders N      Hardware decoders (oasis_scheduler_num_streams,\n"
              << "                        Oasis only; default: extension default)\n"
              << "  -d, --data-dir DIR    Data directory or \"rdma://\" for a remote RDMA\n"
              << "                        server (default: /mnt/ramdisk)\n"
              << "  -i, --ip ADDR         RDMA server IP (required for rdma:// data dir)\n"
              << "  -h, --help            Show this help message\n";
}

Config parse_args(int argc, char *argv[]) {
    Config config;

    static struct option long_options[] = {{"benchmark", required_argument, 0, 'b'},
                                           {"source", required_argument, 0, 's'},
                                           {"repetitions", required_argument, 0, 'r'},
                                           {"threads", required_argument, 0, 't'},
                                           {"decoders", required_argument, 0, 'n'},
                                           {"data-dir", required_argument, 0, 'd'},
                                           {"ip", required_argument, 0, 'i'},
                                           {"help", no_argument, 0, 'h'},
                                           {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "b:s:r:t:n:d:i:h", long_options, nullptr)) != -1) {
        switch (opt) {
        case 'b':
            config.benchmark = string_to_benchmark(optarg);
            break;
        case 's':
            config.source = string_to_source(optarg);
            break;
        case 'r':
            config.repetitions = std::stoi(optarg);
            break;
        case 't':
            config.threads = std::stoi(optarg);
            break;
        case 'n':
            config.decoders = std::stoi(optarg);
            break;
        case 'd':
            config.data_dir = optarg;
            break;
        case 'i':
            config.ip = optarg;
            break;
        case 'h':
            print_usage(argv[0]);
            exit(0);
        default:
            print_usage(argv[0]);
            exit(1);
        }
    }

    return config;
}

// ============================================================================
// Write JSON
// ============================================================================

void write_json(const Config &config, const std::vector<QueryResult> &results,
                const std::string &output_file) {
    std::ofstream out(output_file);

    // Write results
    out << "[\n";
    for (size_t i = 0; i < results.size(); ++i) {
        const auto &r = results[i];
        out << "    {\n";
        out << "        \"query\": \"" << r.filename << "\",\n";
        out << "        \"runtime_sec\": " << std::fixed << std::setprecision(6) << r.stats.runtime
            << ",\n";
        out << "        \"latency_sec\": " << std::fixed << std::setprecision(6) << r.stats.latency
            << ",\n";
        out << "        \"cpu_time_sec\": " << std::fixed << std::setprecision(6)
            << r.stats.cpu_time << ",\n";
        out << "        \"memory_footprint_bytes\": " << r.stats.memory_footprint << ",\n";
        out << "        \"rows_scanned\": " << r.stats.rows_scanned << ",\n";
        out << "        \"data_dir\": \"" << config.data_dir << "\",\n";
        out << "        \"ip\": \"" << config.ip << "\",\n";
        out << "        \"benchmark\": \"" << benchmark_to_string(config.benchmark) << "\",\n";
        out << "        \"source\": \"" << source_to_string(config.source) << "\",\n";
        out << "        \"threads\": " << config.threads << ",\n";
        out << "        \"decoders\": " << config.decoders << ",\n";
        out << "        \"repetitions\": " << config.repetitions << ",\n";
        out << "        \"operators_time\": {\n";
        for (size_t j = 0; j < std::size(OPERATOR_FIELDS); ++j) {
            const auto &[name, member] = OPERATOR_FIELDS[j];
            out << "            \"" << name << "\": " << std::fixed << std::setprecision(6)
                << (r.stats.ops.*member).time << (j < std::size(OPERATOR_FIELDS) - 1 ? "," : "")
                << "\n";
        }
        out << "        },\n";
        out << "        \"operators_cardinalities\": {\n";
        for (size_t j = 0; j < std::size(OPERATOR_FIELDS); ++j) {
            const auto &[name, member] = OPERATOR_FIELDS[j];
            out << "            \"" << name << "\": " << (r.stats.ops.*member).cardinality
                << (j < std::size(OPERATOR_FIELDS) - 1 ? "," : "") << "\n";
        }
        out << "        }" << (config.source == Source::OASIS ? "," : "") << "\n";
        if (config.source == Source::OASIS) {
            out << "        \"oasis\": {\n";
            out << "            \"scan_filter\": " << std::fixed << std::setprecision(6)
                << r.stats.ops.scan_filter << ",\n";
            out << "            \"string_decode\": " << std::fixed << std::setprecision(6)
                << r.stats.ops.string_decode << "\n";
            out << "        }\n";
        }
        out << "    }" << (i < results.size() - 1 ? "," : "") << "\n";
    }
    out << "]\n";

    out.close();
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    Config config = parse_args(argc, argv);

    // Get queries based on benchmark and source
    std::vector<fs::path> query_paths = get_queries(config.benchmark);

    auto results = run_benchmark(config, query_paths);

    write_json(config, results,
               "measurements/queries/" +
                   std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) +
                   ".json");

    return 0;
}
