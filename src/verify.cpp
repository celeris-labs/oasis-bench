#include <algorithm>
#include <cstdint>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <duckdb.hpp>

#include "common.hpp"

// ============================================================================
// verify: run every query against two sources (default parquet vs. oasis) and
// check that they produce identical results. This is a correctness harness, not
// a benchmark -- it exists to catch cases where read_oasis returns different
// data than read_parquet (skipped row groups, predicate pushdown bugs, ...).
// ============================================================================

struct Config {
    std::string data_dir  = "/mnt/ramdisk";
    Benchmark   benchmark = Benchmark::TPCH;
    Source      reference = Source::PARQUET;
    Source      candidate = Source::OASIS;
    bool        verbose   = false;
};

// ============================================================================
// Result serialization
//
// A query without ORDER BY does not guarantee row order, and two scan
// implementations may emit row groups in a different order. To compare results
// independent of row order we serialize every row to a string, then sort the
// rows. Floating point values are rounded to a fixed number of decimals so that
// harmless last-digit differences (different summation order) do not register as
// mismatches.
// ============================================================================

std::string format_value(const duckdb::Value &v) {
    if (v.IsNull()) {
        return "NULL";
    }
    switch (v.type().id()) {
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE: {
        double d = v.GetValue<double>();
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << d;
        return ss.str();
    }
    case duckdb::LogicalTypeId::DECIMAL: {
        // Compare decimals by their double value at fixed precision so that
        // DECIMAL vs. DOUBLE columns from the two sources still line up.
        double d = v.GetValue<double>();
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << d;
        return ss.str();
    }
    default:
        return v.ToString();
    }
}

struct ResultSet {
    size_t                   column_count = 0;
    std::vector<std::string> rows; // sorted
};

ResultSet collect(duckdb::Connection &con, const std::string &sql, bool &ok, std::string &err) {
    ResultSet rs;
    auto      result = con.Query(sql);
    if (result->HasError()) {
        ok  = false;
        err = result->GetError();
        return rs;
    }

    auto &mat = result->Cast<duckdb::MaterializedQueryResult>();
    rs.column_count = mat.ColumnCount();

    for (auto chunk = mat.Fetch(); chunk && chunk->size() != 0; chunk = mat.Fetch()) {
        for (duckdb::idx_t r = 0; r < chunk->size(); ++r) {
            std::string row;
            for (duckdb::idx_t c = 0; c < chunk->ColumnCount(); ++c) {
                if (c != 0) {
                    row += '\x1f'; // unit separator, unlikely to appear in data
                }
                row += format_value(chunk->GetValue(c, r));
            }
            rs.rows.push_back(std::move(row));
        }
    }

    std::sort(rs.rows.begin(), rs.rows.end());
    ok = true;
    return rs;
}

// ============================================================================
// Comparison
// ============================================================================

struct Mismatch {
    enum Kind { ERROR, COLUMN_COUNT, ROW_COUNT, ROW_DATA } kind;
    std::string detail;
};

bool compare(const ResultSet &ref, const ResultSet &cand, Mismatch &out, bool verbose) {
    if (ref.column_count != cand.column_count) {
        out.kind   = Mismatch::COLUMN_COUNT;
        out.detail = "reference has " + std::to_string(ref.column_count) + " columns, candidate has " +
                     std::to_string(cand.column_count);
        return false;
    }
    if (ref.rows.size() != cand.rows.size()) {
        out.kind   = Mismatch::ROW_COUNT;
        out.detail = "reference has " + std::to_string(ref.rows.size()) + " rows, candidate has " +
                     std::to_string(cand.rows.size());
        return false;
    }
    for (size_t i = 0; i < ref.rows.size(); ++i) {
        if (ref.rows[i] != cand.rows[i]) {
            out.kind = Mismatch::ROW_DATA;
            out.detail =
                "first differing row (" + std::to_string(i) + " of " + std::to_string(ref.rows.size()) + ")";
            if (verbose) {
                out.detail += "\n      reference: " + ref.rows[i] + "\n      candidate: " + cand.rows[i];
            }
            return false;
        }
    }
    return true;
}

// ============================================================================
// Connection setup
// ============================================================================

void setup_connection(duckdb::Connection &con, Source source) {
    con.Query("SET parquet_metadata_cache TO true");
    if (source == Source::OASIS) {
        auto ext = con.Query("LOAD 'extension/oasis.duckdb_extension'");
        if (ext->HasError()) {
            throw std::runtime_error("Failed to load oasis extension: " + ext->GetError());
        }
    }
}

// ============================================================================
// Command line parsing
// ============================================================================

void print_usage(const char *prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Run every query against two sources and verify the results match.\n"
              << "\n"
              << "Options:\n"
              << "  -b, --benchmark BM    Benchmark: tpch, tpcds, clickbench (default: tpch)\n"
              << "  -d, --data-dir DIR    Data directory (default: /mnt/ramdisk)\n"
              << "  -R, --reference SRC   Reference source: parquet, memory, oasis (default: parquet)\n"
              << "  -C, --candidate SRC   Candidate source: parquet, memory, oasis (default: oasis)\n"
              << "  -v, --verbose         Print first differing row on mismatch\n"
              << "  -h, --help            Show this help message\n";
}

Config parse_args(int argc, char *argv[]) {
    Config config;

    static struct option long_options[] = {{"benchmark", required_argument, 0, 'b'},
                                            {"data-dir", required_argument, 0, 'd'},
                                            {"reference", required_argument, 0, 'R'},
                                            {"candidate", required_argument, 0, 'C'},
                                            {"verbose", no_argument, 0, 'v'},
                                            {"help", no_argument, 0, 'h'},
                                            {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "b:d:R:C:vh", long_options, nullptr)) != -1) {
        switch (opt) {
        case 'b':
            config.benchmark = string_to_benchmark(optarg);
            break;
        case 'd':
            config.data_dir = optarg;
            break;
        case 'R':
            config.reference = string_to_source(optarg);
            break;
        case 'C':
            config.candidate = string_to_source(optarg);
            break;
        case 'v':
            config.verbose = true;
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
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    Config config = parse_args(argc, argv);

    if (config.reference == config.candidate) {
        std::cerr << "reference and candidate sources are identical ("
                  << source_to_string(config.reference) << "); nothing to verify.\n";
        return 2;
    }

    // One DBConfig/DB suffices; sources differ only in how views are created and
    // which extension is loaded. We use separate connections so the two source
    // sets of views (same table names) don't collide.
    duckdb::DBConfig duck_config;
    duck_config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
    duckdb::DuckDB     db(nullptr, &duck_config);
    duckdb::Connection ref_con(db);
    duckdb::Connection cand_con(db);

    try {
        setup_connection(ref_con, config.reference);
        setup_connection(cand_con, config.candidate);

        std::cout << "Loading data (" << source_to_string(config.reference) << " vs. "
                  << source_to_string(config.candidate) << ")..." << std::endl;

        // Each connection gets its own schema so identical view names coexist.
        ref_con.Query("CREATE SCHEMA ref; SET search_path = 'ref'");
        cand_con.Query("CREATE SCHEMA cand; SET search_path = 'cand'");
        load_data(ref_con, config.data_dir, config.benchmark, config.reference);
        load_data(cand_con, config.data_dir, config.benchmark, config.candidate);
    } catch (const std::exception &e) {
        std::cerr << "Setup failed: " << e.what() << std::endl;
        return 1;
    }

    std::vector<fs::path> query_paths = get_queries(config.benchmark);
    if (query_paths.empty()) {
        std::cerr << "No queries found for benchmark " << benchmark_to_string(config.benchmark)
                  << std::endl;
        return 1;
    }

    size_t passed = 0;
    size_t failed = 0;

    for (const auto &query_path : query_paths) {
        std::ifstream f(query_path);
        std::string   sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

        std::string name = query_path.filename().string();

        bool        ref_ok = false, cand_ok = false;
        std::string ref_err, cand_err;
        ResultSet   ref_rs  = collect(ref_con, sql, ref_ok, ref_err);
        ResultSet   cand_rs = collect(cand_con, sql, cand_ok, cand_err);

        if (!ref_ok || !cand_ok) {
            ++failed;
            std::cout << "FAIL " << name << " : query error\n";
            if (!ref_ok) {
                std::cout << "      " << source_to_string(config.reference) << ": " << ref_err << "\n";
            }
            if (!cand_ok) {
                std::cout << "      " << source_to_string(config.candidate) << ": " << cand_err << "\n";
            }
            continue;
        }

        Mismatch mm;
        if (compare(ref_rs, cand_rs, mm, config.verbose)) {
            ++passed;
            std::cout << "PASS " << name << " (" << ref_rs.rows.size() << " rows)\n";
        } else {
            ++failed;
            std::cout << "FAIL " << name << " : " << mm.detail << "\n";
        }
    }

    std::cout << "\n" << passed << " passed, " << failed << " failed of "
              << (passed + failed) << " queries.\n";

    return failed == 0 ? 0 : 1;
}
