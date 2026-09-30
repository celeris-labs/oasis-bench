#include <chrono>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <duckdb.hpp>

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
    // oasis_scheduler_num_streams; default to a single decoder so per-column
    // measurements isolate one decoder (0 = extension default).
    uint32_t    decoders = 1;
    int         prefetch_depth = 2; // oasis_prefetch_depth (-1 = extension default, 0 = async off)
    int         enable_yield = 1; // oasis_enable_yield (-1 = extension default)
};

// ============================================================================
// Benchmark execution
// ============================================================================

// Sum the output-side cycle counters of oasis_stream_profile() over the active
// decoders: the scheduler drives the first `decoders` streams, and the
// remaining decoders' counters keep accumulating idle cycles across queries.
OutCycles read_out_cycles(duckdb::Connection &con, uint32_t decoders) {
    std::string sql = "SELECT COALESCE(SUM(out_handshakes_cycles + out_starved_cycles + "
                      "out_stalled_cycles + out_idle_cycles), 0), "
                      "COALESCE(SUM(out_handshakes_cycles), 0), "
                      "COALESCE(SUM(out_stalled_cycles), 0), "
                      "COALESCE(SUM(out_idle_cycles), 0) FROM oasis_stream_profile()";
    if (decoders > 0) {
        sql += " WHERE stream < " + std::to_string(decoders);
    }

    auto r = con.Query(sql);
    if (r->HasError()) {
        throw std::runtime_error("Failed to read oasis_stream_profile: " + r->GetError());
    }

    OutCycles cycles;
    cycles.total      = r->GetValue(0, 0).GetValue<uint64_t>();
    cycles.handshakes = r->GetValue(1, 0).GetValue<uint64_t>();
    cycles.stalled    = r->GetValue(2, 0).GetValue<uint64_t>();
    cycles.idle       = r->GetValue(3, 0).GetValue<uint64_t>();
    return cycles;
}

struct ColumnResult {
    std::string table;
    std::string column;
    std::string type;
    double      runtime;
    uint64_t    rows_scanned;
    OutCycles   out_cycles; // decoder output cycle counters (oasis only)
};

// Run `sql` once and return its wall-clock latency in seconds.
double time_query(duckdb::Connection &con, const std::string &sql) {
    Timer timer;
    timer.start();
    auto r = con.Query(sql);
    if (r->HasError()) {
        throw std::runtime_error("Query error: " + r->GetError());
    }
    while (r->Fetch()) {}
    return timer.stop_seconds();
}

std::vector<ColumnResult> run_benchmark(const Config &config) {
    std::vector<ColumnResult> results;

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

    // Scan each column of each table as its own single-projection query so
    // per-column performance can be compared across sources.
    for (const auto &table : tables) {
        auto desc = con.Query("DESCRIBE " + table);
        if (desc->HasError()) {
            throw std::runtime_error("Failed to describe table '" + table +
                                     "': " + desc->GetError());
        }

        // Every single-column query scans the full table, so one COUNT(*)
        // gives the rows scanned for all of its columns.
        auto count = con.Query("SELECT COUNT(*) FROM " + table);
        if (count->HasError()) {
            throw std::runtime_error("Failed to count table '" + table +
                                     "': " + count->GetError());
        }
        uint64_t rows_scanned = count->GetValue(0, 0).GetValue<uint64_t>();

        for (size_t row = 0; row < desc->RowCount(); ++row) {
            std::string column = desc->GetValue(0, row).ToString();
            std::string type   = desc->GetValue(1, row).ToString();
            std::string sql    = "SELECT " + column + " FROM " + table;

            std::cout << "Running " << sql << "..." << std::endl;

            // Snapshot the hardware counters right after each repetition so
            // the profile covers that repetition's scan; keep the median run.
            struct Run {
                double    runtime;
                OutCycles out_cycles;
            };
            std::vector<Run> runs;
            for (int i = 0; i < config.repetitions; ++i) {
                Run run;
                run.runtime = time_query(con, sql);
                if (config.source == Source::OASIS) {
                    run.out_cycles = read_out_cycles(con, config.decoders);
                }
                runs.push_back(run);
            }
            std::sort(runs.begin(), runs.end(),
                      [](const Run &a, const Run &b) { return a.runtime < b.runtime; });
            Run &median_run = runs[runs.size() / 2];

            ColumnResult result;
            result.table        = table;
            result.column       = column;
            result.type         = type;
            result.runtime      = median_run.runtime;
            result.rows_scanned = rows_scanned;
            result.out_cycles   = median_run.out_cycles;
            results.push_back(std::move(result));
        }
    }

    return results;
}

// ============================================================================
// Command Line Parsing
// ============================================================================

void print_usage(const char *prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Scans every column of every table as a single-column SELECT query\n"
              << "(e.g. SELECT l_orderkey FROM lineitem) to compare per-column\n"
              << "performance across input configurations.\n"
              << "\n"
              << "Options:\n"
              << "  -b, --benchmark BM    Benchmark: tpch, tpcds, clickbench (default: tpch)\n"
              << "  -s, --source SRC      Input source: parquet, memory, oasis (default: parquet)\n"
              << "  -r, --repetitions N   Number of repetitions per query (default: 5)\n"
              << "  -t, --threads N       Number of threads (default: 1)\n"
              << "  -n, --decoders N      Hardware decoders (oasis_scheduler_num_streams,\n"
              << "                        Oasis only; default: 1 to isolate a single decoder,\n"
              << "                        0 = extension default)\n"
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

void write_json(const Config &config, const std::vector<ColumnResult> &results,
                const std::string &output_file) {
    std::ofstream out(output_file);

    // Write results
    out << "[\n";
    for (size_t i = 0; i < results.size(); ++i) {
        const auto &r = results[i];
        out << "    {\n";
        out << "        \"table\": \"" << r.table << "\",\n";
        out << "        \"column\": \"" << r.column << "\",\n";
        out << "        \"type\": \"" << r.type << "\",\n";
        out << "        \"runtime_sec\": " << std::fixed << std::setprecision(6) << r.runtime
            << ",\n";
        out << "        \"rows_scanned\": " << r.rows_scanned << ",\n";
        out << "        \"data_dir\": \"" << config.data_dir << "\",\n";
        out << "        \"ip\": \"" << config.ip << "\",\n";
        out << "        \"benchmark\": \"" << benchmark_to_string(config.benchmark) << "\",\n";
        out << "        \"source\": \"" << source_to_string(config.source) << "\",\n";
        out << "        \"threads\": " << config.threads << ",\n";
        out << "        \"decoders\": " << config.decoders << ",\n";
        out << "        \"prefetch_depth\": " << config.prefetch_depth << ",\n";
        out << "        \"enable_yield\": " << config.enable_yield << ",\n";
        out << "        \"repetitions\": " << config.repetitions
            << (config.source == Source::OASIS ? "," : "") << "\n";
        if (config.source == Source::OASIS) {
            // Output-side decoder cycles of the active decoders, from
            // oasis_stream_profile().
            out << "        \"out_cycles\": " << r.out_cycles.total << ",\n";
            out << "        \"out_handshakes_cycles\": " << r.out_cycles.handshakes << ",\n";
            out << "        \"out_stalled_cycles\": " << r.out_cycles.stalled << ",\n";
            out << "        \"out_idle_cycles\": " << r.out_cycles.idle << "\n";
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

    auto results = run_benchmark(config);

    fs::create_directories("measurements/columns");
    write_json(config, results,
               "measurements/columns/" +
                   std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) +
                   ".json");

    return 0;
}
