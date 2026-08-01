#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <duckdb.hpp>

#include "common.hpp"
#include "stats.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string           data_dir    = "/mnt/ramdisk";
    std::string           ip          = ""; // RDMA server IP (rdma:// data dir)
    Benchmark             benchmark   = Benchmark::TPCH;
    Source                source      = Source::PARQUET;
    std::vector<uint32_t> threads{1};
    uint32_t              repetitions = 5;
    uint32_t              streams     = 2; // Number of concurrent query streams (clients)
    uint32_t              decoders    = 4; // oasis_scheduler_num_streams (0 = extension default)
    int                   prefetch_depth = 2; // oasis_prefetch_depth (-1 = extension default,
                                              // 0 = async off)
    int                   enable_yield = 1; // oasis_enable_yield (-1 = extension default)
};

// ============================================================================
// Query Stream Worker
// ============================================================================

class QueryStream {
  private:
    int                      stream_id_;
    duckdb::Connection       con_;
    std::vector<std::string> queries_;

  public:
    QueryStream(int stream_id, std::shared_ptr<duckdb::DuckDB> db,
                const std::vector<std::string> &queries)
        : stream_id_(stream_id), con_(*db), queries_(queries) {
        std::mt19937 g(stream_id);
        std::shuffle(queries_.begin(), queries_.end(), g);
    }

    void run() {
        for (const auto &query : queries_) {
            try {
                auto r = con_.Query(query);
                if (r->HasError()) {
                    std::cerr << "stream " << stream_id_ << ": query failed: " << r->GetError()
                              << std::endl;
                    return;
                }
                // Fetch() re-throws on a materialized error result; guard the whole drain.
                while (auto chunk = r->Fetch()) {
                    if (chunk->size() == 0)
                        break;
                }
            } catch (const std::exception &e) {
                std::cerr << "stream " << stream_id_ << ": query threw: " << e.what() << std::endl;
                return;
            }
        }
    }
};

// ============================================================================
// Benchmark Execution
// ============================================================================

struct ThroughputResult {
    std::vector<double>          runtimes;
    std::vector<HardwareProfile> profiles; // one per repetition (oasis only, else empty)
};

ThroughputResult run_throughput_benchmark(std::shared_ptr<duckdb::DuckDB> db,
                                          uint32_t num_streams, uint32_t num_repetitions,
                                          const std::vector<std::string> &queries,
                                          bool collect_profile) {
    std::vector<QueryStream> streams;

    for (int i = 0; i < num_streams; ++i) {
        streams.push_back(QueryStream(i, db, queries));
    }

    Timer            timer;
    ThroughputResult result;

    for (int i = 0; i < num_repetitions; i++) {
        std::vector<std::thread> threads;

        timer.start();

        for (int j = 0; j < num_streams; j++) {
            threads.emplace_back([&streams, j]() { streams[j].run(); });
        }

        for (auto &thread : threads) {
            thread.join();
        }

        double elapsed = timer.stop_seconds();
        result.runtimes.push_back(elapsed);

        // Snapshot the hardware counters right after each repetition so the
        // profile covers that repetition's scans.
        if (collect_profile) {
            duckdb::Connection profile_con(*db);
            result.profiles.push_back(read_stream_profile(profile_con));
        }
    }

    return result;
}

// ============================================================================
// Command Line Parsing
// ============================================================================

void print_usage(const char *prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Throughput benchmark with concurrent query streams.\n"
              << "\n"
              << "Options:\n"
              << "  -b, --benchmark BM    Benchmark: tpch, clickbench (default: tpch)\n"
              << "  -s, --source SRC      Input source: parquet, memory, oasis (default: parquet)\n"
              << "  -r, --repetitions N   Number of repetitions per query (default: 5)\n"
              << "  -t, --threads N       Number of DuckDB threads as range \"1-8\",\n"
              << "                        list \"1,2,4\", or single value (default: 1)\n"
              << "  -S, --streams N       Number of concurrent query streams (default: 2)\n"
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
                                           {"streams", required_argument, 0, 'S'},
                                           {"decoders", required_argument, 0, 'n'},
                                           {"prefetch", required_argument, 0, 'p'},
                                           {"yield", required_argument, 0, 'y'},
                                           {"data-dir", required_argument, 0, 'd'},
                                           {"ip", required_argument, 0, 'i'},
                                           {"help", no_argument, 0, 'h'},
                                           {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "b:s:r:t:S:n:p:y:d:i:h", long_options, nullptr)) != -1) {
        switch (opt) {
        case 'b':
            config.benchmark = string_to_benchmark(optarg);
            break;
        case 's':
            config.source = string_to_source(optarg);
            break;
        case 't':
            try {
                config.threads = parse_thread_arg(optarg);
            } catch (const std::exception &e) {
                std::cerr << "Error parsing threads arg: " << e.what() << std::endl;
                print_usage(argv[0]);
                exit(1);
            }
            break;
        case 'r':
            config.repetitions = std::stoi(optarg);
            break;
        case 'S':
            config.streams = std::stoi(optarg);
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

void write_json(uint32_t num_threads, const Config &config, const ThroughputResult &result,
                const std::string &output_file) {
    std::ofstream out(output_file);

    std::vector<double> sorted = result.runtimes;
    std::sort(sorted.begin(), sorted.end());

    // Write result.
    out << "{\n";
    out << "    \"runtime_sec\": " << std::fixed << std::setprecision(6)
        << sorted[sorted.size() / 2] << ",\n";
    out << "    \"runtimes\": [";
    for (size_t i = 0; i < result.runtimes.size(); ++i) {
        out << std::fixed << std::setprecision(6) << result.runtimes[i];
        if (i + 1 < result.runtimes.size())
            out << ", ";
    }
    out << "],\n";
    out << "    \"threads\": " << num_threads << ",\n";
    out << "    \"streams\": " << config.streams << ",\n";
    // "num_decoders" so the key doesn't clash with the "decoders" cycle
    // counters below.
    out << "    \"num_decoders\": " << config.decoders << ",\n";
    out << "    \"prefetch_depth\": " << config.prefetch_depth << ",\n";
    out << "    \"enable_yield\": " << config.enable_yield << ",\n";
    out << "    \"data_dir\": \"" << config.data_dir << "\",\n";
    out << "    \"ip\": \"" << config.ip << "\",\n";
    out << "    \"benchmark\": \"" << benchmark_to_string(config.benchmark) << "\",\n";
    out << "    \"source\": \"" << source_to_string(config.source) << "\",\n";
    out << "    \"repetitions\": " << config.repetitions
        << (result.profiles.empty() ? "" : ",") << "\n";
    if (!result.profiles.empty()) {
        // Output-side cycle counters of the median-runtime repetition's
        // profile snapshot, split between the hardware decoders and the
        // bypass stream (the last profile row).
        size_t median_rep =
            std::find(result.runtimes.begin(), result.runtimes.end(), sorted[sorted.size() / 2]) -
            result.runtimes.begin();
        const HardwareProfile &profile = result.profiles[median_rep];
        uint32_t               bypass  = profile.rows.size() - 1;

        auto write_group = [&](const char *name, const OutCycles &cycles, const char *tail) {
            out << "    \"" << name << "\": {\n";
            out << "        \"out_cycles\": " << cycles.total << ",\n";
            out << "        \"out_handshakes_cycles\": " << cycles.handshakes << ",\n";
            out << "        \"out_idle_cycles\": " << cycles.idle << "\n";
            out << "    }" << tail << "\n";
        };
        write_group("decoders", sum_out_cycles(profile, 0, bypass), ",");
        write_group("bypass", sum_out_cycles(profile, bypass, bypass + 1), "");
    }
    out << "}\n";

    out.close();
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    Config config = parse_args(argc, argv);

    // Get queries based on benchmark and source
    std::vector<fs::path>    query_paths = get_queries(config.benchmark);
    std::vector<std::string> queries;
    for (const auto &query_path : query_paths) {
        std::ifstream f(query_path);
        std::string   query((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        queries.push_back(query);
    }

    // Initialize DuckDB instance
    duckdb::DBConfig duck_config;
    duck_config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
    auto               db = std::make_shared<duckdb::DuckDB>(nullptr, &duck_config);
    duckdb::Connection setup_con(*db);

    global_setup(setup_con, is_rdma_dir(config.data_dir), config.source, config.ip,
                 config.decoders, config.prefetch_depth, config.enable_yield);

    std::cout << "Loading data..." << std::endl;
    auto tables = load_data(setup_con, config.data_dir, config.benchmark, config.source);

    std::cout << "Warming metadata cache..." << std::endl;
    warm_metadata_cache(setup_con, tables);

    // Run benchmark
    for (uint32_t num_threads : config.threads) {
        setup_con.Query("SET GLOBAL threads TO " + std::to_string(num_threads));

        std::cout << "Running throughput benchmark for " << std::to_string(num_threads)
                  << " thread(s)..." << std::endl;
        bool collect_profile = config.source == Source::OASIS;
        auto result          = run_throughput_benchmark(db, config.streams, config.repetitions,
                                                        queries, collect_profile);

        fs::create_directories("measurements/throughput");
        std::string output_file =
            "measurements/throughput/" +
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json";
        write_json(num_threads, config, result, output_file);

        std::cout << "Results written to: " << output_file << std::endl;
    }

    return 0;
}