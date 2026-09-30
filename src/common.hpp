#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>

#include <duckdb.hpp>

namespace fs = std::filesystem;

// ============================================================================
// Benchmark types
// ============================================================================

enum class Benchmark { TPCH = 0, TPCDS = 1, CLICKBENCH = 2 };

const char *benchmark_to_string(Benchmark b) {
    switch (b) {
    case Benchmark::TPCH:
        return "tpch";
    case Benchmark::TPCDS:
        return "tpcds";
    case Benchmark::CLICKBENCH:
        return "clickbench";
    }
    throw std::runtime_error("Unknown benchmark!");
}

Benchmark string_to_benchmark(const std::string &s) {
    if (s == "tpch")
        return Benchmark::TPCH;
    if (s == "tpcds")
        return Benchmark::TPCDS;
    if (s == "clickbench")
        return Benchmark::CLICKBENCH;
    throw std::runtime_error("Unknown benchmark: " + s);
}

// ============================================================================
// Source types
// ============================================================================

enum class Source { PARQUET = 0, MEMORY = 1, OASIS = 2 };

const char *source_to_string(Source s) {
    switch (s) {
    case Source::PARQUET:
        return "parquet";
    case Source::MEMORY:
        return "memory";
    case Source::OASIS:
        return "oasis";
    }
    throw std::runtime_error("Unknown source!");
}

Source string_to_source(const std::string &s) {
    if (s == "parquet")
        return Source::PARQUET;
    if (s == "memory")
        return Source::MEMORY;
    if (s == "oasis")
        return Source::OASIS;
    throw std::runtime_error("Unknown source: " + s);
}

bool is_rdma_dir(const std::string &data_dir) {
    return data_dir.rfind("rdma://", 0) == 0;
}

// ============================================================================
// High-resolution timer
// ============================================================================

class Timer {
  public:
    void start() { start_time_ = std::chrono::high_resolution_clock::now(); }

    double stop_seconds() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time_).count();
        return duration / 1e9;
    }

  private:
    std::chrono::high_resolution_clock::time_point start_time_;
};

// ============================================================================
// Global Setup
// ============================================================================

// decoders == 0 leaves oasis_scheduler_num_streams at the extension default,
// prefetch_depth < 0 leaves oasis_prefetch_depth at the extension default (0 is
// a valid depth that turns off async), and enable_yield < 0 leaves
// oasis_enable_yield at the extension default.
void global_setup(duckdb::Connection &con, bool rdma, Source source,
                  const std::string &rdma_ip = "", uint32_t decoders = 0,
                  int prefetch_depth = -1, int enable_yield = -1) {
    con.Query("SET GLOBAL enable_external_file_cache TO false");
    con.Query("SET GLOBAL parquet_metadata_cache TO true");

    if (source == Source::OASIS || rdma) {
        auto ext =
            con.Query("LOAD '/local/home/jodann/oasis-bench/extension/oasis.duckdb_extension'");
        if (ext->HasError()) {
            throw std::runtime_error("Failed to load oasis extension: " + ext->GetError());
        }

        if (decoders > 0) {
            auto set_s = con.Query("SET oasis_scheduler_num_streams TO " +
                                   std::to_string(decoders));
            if (set_s->HasError()) {
                throw std::runtime_error("Failed to set oasis_scheduler_num_streams: " +
                                         set_s->GetError());
            }
        }

        // GLOBAL so that connections other than this one (the throughput
        // benchmark queries on one connection per stream) see the value:
        // these two options have no SET callback, so a session-local SET
        // would only reach this connection.
        if (prefetch_depth >= 0) {
            auto set_p = con.Query("SET GLOBAL oasis_prefetch_depth TO " +
                                   std::to_string(prefetch_depth));
            if (set_p->HasError()) {
                throw std::runtime_error("Failed to set oasis_prefetch_depth: " +
                                         set_p->GetError());
            }
        }

        if (enable_yield >= 0) {
            auto set_y = con.Query(std::string("SET GLOBAL oasis_enable_yield TO ") +
                                   (enable_yield ? "true" : "false"));
            if (set_y->HasError()) {
                throw std::runtime_error("Failed to set oasis_enable_yield: " +
                                         set_y->GetError());
            }
        }

        if (rdma) {
            if (rdma_ip.empty()) {
                throw std::runtime_error("RDMA data dir requires a server IP address (--ip)");
            }
            auto set_r = con.Query("SET oasis_rdma_server TO '" + rdma_ip + "'");
            if (set_r->HasError()) {
                throw std::runtime_error("Failed to set RDMA server: " + set_r->GetError());
            }
        }
    }
}

// ============================================================================
// Data Loading
// ============================================================================

std::vector<fs::path> get_queries(Benchmark benchmark) {
    fs::path query_dir = fs::path("queries") / benchmark_to_string(benchmark);

    std::vector<fs::path> queries;
    if (fs::exists(query_dir)) {
        for (const auto &entry : fs::directory_iterator(query_dir)) {
            if (entry.path().extension() == ".sql") {
                queries.push_back(entry.path());
            }
        }
    }
    std::sort(queries.begin(), queries.end());

    return queries;
}

// Table names per benchmark. Used for RDMA data dirs, where there is no local
// directory to enumerate.
std::vector<std::string> get_table_names(Benchmark benchmark) {
    switch (benchmark) {
    case Benchmark::TPCH:
        return {"customer", "lineitem", "nation",   "orders",
                "part",     "partsupp", "region",   "supplier"};
    case Benchmark::TPCDS:
        return {"call_center",     "catalog_page",   "catalog_returns", "catalog_sales",
                "customer",        "customer_address", "customer_demographics",
                "date_dim",        "household_demographics", "income_band",
                "inventory",       "item",           "promotion",       "reason",
                "ship_mode",       "store",          "store_returns",   "store_sales",
                "time_dim",        "warehouse",      "web_page",        "web_returns",
                "web_sales",       "web_site"};
    case Benchmark::CLICKBENCH:
        return {"hits"};
    }
    throw std::runtime_error("Unknown benchmark!");
}

std::vector<std::string> load_data(duckdb::Connection &con, const std::string &data_dir,
                                   Benchmark benchmark, Source source) {
    std::string replace_cols =
        (benchmark == Benchmark::CLICKBENCH)
            ? " REPLACE(make_date(EventDate) AS EventDate, epoch_ms(EventTime * 1000) AS "
              "EventTime, epoch_ms(ClientEventTime * 1000) AS ClientEventTime, "
              "epoch_ms(LocalEventTime * 1000) AS LocalEventTime)"
            : "";

    // All sources read from Parquet files
    std::string extension = ".parquet";

    std::vector<std::string> tables;
    std::string              path_prefix;
    if (is_rdma_dir(data_dir)) {
        tables      = get_table_names(benchmark);
        path_prefix = "rdma://";
    } else {
        if (!fs::exists(data_dir) || !fs::is_directory(data_dir)) {
            throw std::runtime_error("Data directory does not exist: " + data_dir);
        }

        for (const auto &entry : fs::directory_iterator(data_dir)) {
            if (entry.path().extension() == extension) {
                tables.push_back(entry.path().stem().string());
            }
        }
        std::sort(tables.begin(), tables.end());

        if (tables.size() == 0) {
            throw std::runtime_error("No input data in: " + data_dir);
        }

        path_prefix = data_dir + "/";
    }

    for (const auto &table : tables) {
        std::string path = path_prefix + table + extension;
        std::string sql;
        switch (source) {
        case Source::PARQUET:
            sql = "CREATE VIEW " + table + " AS SELECT *" + replace_cols + " FROM read_parquet('" +
                  path + "')";
            break;
        case Source::OASIS:
            sql = "CREATE VIEW " + table + " AS SELECT *" + replace_cols + " FROM read_oasis('" +
                  path + "')";
            break;
        default: // MEMORY loads base tables into memory
            sql = "CREATE TABLE " + table + " AS SELECT *" + replace_cols + " FROM read_parquet('" +
                  path + "')";
            break;
        }

        auto r = con.Query(sql);
        if (r->HasError()) {
            throw std::runtime_error("Failed to load table '" + table + "': " + r->GetError());
        }
    }

    return tables;
}

// Run a COUNT(*) on every table to warm the Parquet metadata cache, so the
// first timed query does not pay the metadata read.
void warm_metadata_cache(duckdb::Connection &con, const std::vector<std::string> &tables) {
    for (const auto &table : tables) {
        auto r = con.Query("SELECT COUNT(*) FROM " + table);
        if (r->HasError()) {
            throw std::runtime_error("Failed to warm table '" + table + "': " + r->GetError());
        }
    }
}

// ============================================================================
// Arguments
// ============================================================================

int parse_bool_arg(const std::string &arg) {
    if (arg == "true" || arg == "on" || arg == "1")
        return 1;
    if (arg == "false" || arg == "off" || arg == "0")
        return 0;
    throw std::invalid_argument("Invalid boolean (use true/false): " + arg);
}

std::vector<uint32_t> parse_thread_arg(const std::string &arg) {
    std::vector<uint32_t> threads;

    size_t dash_pos = arg.find('-');
    if (dash_pos != std::string::npos) { // Check if it's a range (contains '-')
        uint32_t start = std::stoi(arg.substr(0, dash_pos));
        uint32_t end   = std::stoi(arg.substr(dash_pos + 1));

        if (start > end || start < 1) {
            throw std::invalid_argument("Invalid thread range: " + arg);
        }

        for (uint32_t i = start; i <= end; ++i) {
            threads.push_back(i);
        }
    } else if (arg.find(',') != std::string::npos) { // Check if it's a comma-separated list
        std::stringstream ss(arg);
        std::string       token;

        while (std::getline(ss, token, ',')) {
            uint32_t val = std::stoi(token);
            if (val < 1) {
                throw std::invalid_argument("Invalid thread count: " + token);
            }
            threads.push_back(val);
        }
    } else { // Single value
        uint32_t val = std::stoi(arg);
        if (val < 1) {
            throw std::invalid_argument("Invalid thread count: " + arg);
        }
        threads.push_back(val);
    }

    return threads;
}
