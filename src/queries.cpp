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
#include "stats.hpp"

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
    uint32_t    decoders = 4; // oasis_scheduler_num_streams (0 = extension default)
    int         prefetch_depth = 2; // oasis_prefetch_depth (-1 = extension default, 0 = async off)
    int         enable_yield = 1; // oasis_enable_yield (-1 = extension default)
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
                 config.decoders, config.prefetch_depth, config.enable_yield);

    // Load data
    std::cout << "Loading data..." << std::endl;
    auto tables = load_data(con, config.data_dir, config.benchmark, config.source);

    std::cout << "Warming metadata cache..." << std::endl;
    warm_metadata_cache(con, tables);

    con.Query("SET threads TO " + std::to_string(config.threads));

    // Enable profiling for all operator types
    con.Query("PRAGMA enable_profiling='no_output'");
    con.Query("PRAGMA profiling_coverage='ALL'");

    // Run each query
    for (const auto &query_path : query_paths) {
        // Print filename (matching Python behavior)
        std::cout << "Running query " << query_path.string() << "..." << std::endl;

        // Read query
        std::ifstream f(query_path);
        std::string   sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

        QueryResult result;
        result.filename = query_path.string();
        result.stats    = measure_query(con, sql, config.repetitions);
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
              << "                        Oasis only; default: 4)\n"
              << "  -p, --prefetch N      Prefetch depth (oasis_prefetch_depth,\n"
              << "                        Oasis only; default: 2)\n"
              << "  -y, --yield BOOL      Enable yielding (oasis_enable_yield, true/false,\n"
              << "                        Oasis only; default: true)\n"
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
                                           {"prefetch", required_argument, 0, 'p'},
                                           {"yield", required_argument, 0, 'y'},
                                           {"data-dir", required_argument, 0, 'd'},
                                           {"ip", required_argument, 0, 'i'},
                                           {"help", no_argument, 0, 'h'},
                                           {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "b:s:r:t:n:p:y:d:i:h", long_options, nullptr)) != -1) {
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
        case 'p':
            config.prefetch_depth = std::stoi(optarg);
            break;
        case 'y':
            try {
                config.enable_yield = parse_bool_arg(optarg);
            } catch (const std::exception &e) {
                std::cerr << "Error parsing yield arg: " << e.what() << std::endl;
                print_usage(argv[0]);
                exit(1);
            }
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
        out << "        \"prefetch_depth\": " << config.prefetch_depth << ",\n";
        out << "        \"enable_yield\": " << config.enable_yield << ",\n";
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
    // Hardcoded to only run query 4
    query_paths.erase(std::remove_if(query_paths.begin(), query_paths.end(),
                                     [](const fs::path &p) { return p.filename() != "q04.sql"; }),
                      query_paths.end());

    auto results = run_benchmark(config, query_paths);

    fs::create_directories("measurements/queries");
    write_json(config, results,
               "measurements/queries/" +
                   std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) +
                   ".json");

    return 0;
}
