#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include <duckdb.hpp>

#include "common.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string           data_dir    = "/mnt/ramdisk";
    Benchmark             benchmark   = Benchmark::TPCH;
    Source                source      = Source::PARQUET;
    std::vector<uint32_t> threads{1};
    uint32_t              repetitions = 5;
    uint32_t              streams     = 2; // Number of concurrent query streams
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

        con_.Query("SET parquet_metadata_cache TO true");
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

void print_stream_profile(duckdb::Connection &con) {
    auto r = con.Query("SELECT * FROM oasis_stream_profile()");
    if (r->HasError()) {
        std::cerr << "oasis_stream_profile failed: " << r->GetError() << std::endl;
        return;
    }
    r->Print();
}

std::vector<double> run_throughput_benchmark(std::shared_ptr<duckdb::DuckDB> db,
                                             uint32_t num_streams, uint32_t num_repetitions,
                                             const std::vector<std::string> &queries,
                                             bool print_profile) {
    std::vector<QueryStream> streams;

    for (int i = 0; i < num_streams; ++i) {
        streams.push_back(QueryStream(i, db, queries));
    }

    Timer               timer;
    std::vector<double> runtimes;

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
        runtimes.push_back(elapsed);

        if (print_profile) {
            std::cout << "Stream profile after repetition " << (i + 1) << ":" << std::endl;
            duckdb::Connection profile_con(*db);
            print_stream_profile(profile_con);
        }
    }

    return runtimes;
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
              << "  -d, --data-dir DIR    Data directory (default: /mnt/ramdisk)\n"
              << "  -h, --help            Show this help message\n";
}

Config parse_args(int argc, char *argv[]) {
    Config config;

    static struct option long_options[] = {{"benchmark", required_argument, 0, 'b'},
                                           {"source", required_argument, 0, 's'},
                                           {"repetitions", required_argument, 0, 'r'},
                                           {"threads", required_argument, 0, 't'},
                                           {"streams", required_argument, 0, 'S'},
                                           {"data-dir", required_argument, 0, 'd'},
                                           {"help", no_argument, 0, 'h'},
                                           {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "b:s:r:t:S:d:h", long_options, nullptr)) != -1) {
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
        case 'd':
            config.data_dir = optarg;
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

void write_json(uint32_t num_threads, const Config &config, std::vector<double> result,
                const std::string &output_file) {
    std::ofstream out(output_file);

    std::sort(result.begin(), result.end());

    // Write result
    out << "{\n";
    out << "    \"runtime_sec\": " << std::fixed << std::setprecision(6)
        << result[result.size() / 2] << ",\n";
    out << "    \"runtimes\": [";
    for (size_t i = 0; i < result.size(); ++i) {
        out << std::fixed << std::setprecision(6) << result[i];
        if (i + 1 < result.size())
            out << ", ";
    }
    out << "],\n";
    out << "    \"threads\": " << num_threads << ",\n";
    out << "    \"streams\": " << config.streams << ",\n";
    out << "    \"data_dir\": \"" << config.data_dir << "\",\n";
    out << "    \"benchmark\": \"" << benchmark_to_string(config.benchmark) << "\",\n";
    out << "    \"source\": \"" << source_to_string(config.source) << "\",\n";
    out << "    \"repetitions\": " << config.repetitions << "\n";
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

    // Load oasis extension (provides read_oasis)
    if (config.source == Source::OASIS) {
        auto ext = setup_con.Query(
            "LOAD '/local/home/jodann/oasis-bench/extension/oasis.duckdb_extension'");
        if (ext->HasError()) {
            std::cerr << "Failed to load oasis extension: " << ext->GetError() << std::endl;
            exit(1);
        }
    }

    std::cout << "Loading data..." << std::endl;
    load_data(setup_con, config.data_dir, config.benchmark, config.source);

    // Run benchmark
    for (uint32_t num_threads : config.threads) {
        setup_con.Query("SET GLOBAL threads TO " + std::to_string(num_threads));

        std::cout << "Running throughput benchmark for " << std::to_string(num_threads)
                  << " thread(s)..." << std::endl;
        auto result = run_throughput_benchmark(db, config.streams, config.repetitions, queries,
                                               config.source == Source::OASIS);

        std::string output_file =
            "measurements/throughput/" +
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json";
        write_json(num_threads, config, result, output_file);

        std::cout << "Results written to: " << output_file << std::endl;
    }

    return 0;
}