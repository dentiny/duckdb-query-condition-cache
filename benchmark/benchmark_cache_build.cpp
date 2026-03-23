#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

using namespace duckdb;

static double BenchMs(Connection &con, const std::string &sql, int rounds = 5) {
	con.Query(sql); // warmup
	double total = 0;
	for (int r = 0; r < rounds; ++r) {
		auto start = std::chrono::steady_clock::now();
		con.Query(sql);
		auto end = std::chrono::steady_clock::now();
		total += std::chrono::duration<double, std::milli>(end - start).count();
	}
	return total / rounds;
}

static void PrintSection(const char *title, const std::vector<std::pair<std::string, double>> &rows) {
	printf("\n%s\n", title);
	for (auto &[label, ms] : rows) {
		printf("  %-45s %8.1f ms\n", label.c_str(), ms);
	}
}

int main() {
	DuckDB db;
	Connection con(db);

	const int rounds = 5;
	const std::string build = "SELECT * FROM condition_cache_build('t', 'val = 42')";

	// 0. Query baseline vs cache build cost
	con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(2000000) t(i)");
	std::vector<std::pair<std::string, double>> results;
	results.emplace_back("SELECT count(*) WHERE val = 42",
	                     BenchMs(con, "SELECT count(*) FROM t WHERE val = 42", rounds));
	results.emplace_back("SELECT count(*) WHERE val < 50",
	                     BenchMs(con, "SELECT count(*) FROM t WHERE val < 50", rounds));
	results.emplace_back("cache build (val = 42)", BenchMs(con, build, rounds));
	results.emplace_back("cache build (val < 50)",
	                     BenchMs(con, "SELECT * FROM condition_cache_build('t', 'val < 50')", rounds));
	PrintSection("Query baseline vs cache build cost (2M rows, threads=default)", results);

	// 1. Row count scaling
	results.clear();
	for (int64_t n : {100000, 500000, 2000000, 12288000}) {
		con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(" + std::to_string(n) + ") t(i)");
		double ms = BenchMs(con, build, rounds);
		int rgs = (n + 122879) / 122880;
		char label[64];
		snprintf(label, sizeof(label), "%'lld rows (%d RGs)", (long long)n, rgs);
		results.emplace_back(label, ms);
	}
	PrintSection("Row count scaling (threads=default)", results);

	// 2. Thread scaling
	con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(2000000) t(i)");
	results.clear();
	for (int threads : {1, 2, 4, 8}) {
		con.Query("SET threads = " + std::to_string(threads));
		double ms = BenchMs(con, build, rounds);
		char label[32];
		snprintf(label, sizeof(label), "threads = %d", threads);
		results.emplace_back(label, ms);
	}
	PrintSection("Thread scaling (2M rows, 17 RGs)", results);
	con.Query("RESET threads");

	// 3. Column count in predicate
	con.Query(R"(CREATE OR REPLACE TABLE t AS
		SELECT i AS id, i%100 AS c1, i%200 AS c2, i%300 AS c3, i%400 AS c4, i%500 AS c5
		FROM range(2000000) t(i))");
	results.clear();
	std::vector<std::pair<std::string, std::string>> col_tests = {
	    {"1 col", "c1 = 42"},
	    {"3 cols", "c1 = 42 AND c2 < 100 AND c3 > 50"},
	    {"5 cols", "c1 = 42 AND c2 < 100 AND c3 > 50 AND c4 != 0 AND c5 < 250"},
	};
	for (auto &[label, pred] : col_tests) {
		double ms = BenchMs(con, "SELECT * FROM condition_cache_build('t', '" + pred + "')", rounds);
		results.emplace_back(label, ms);
	}
	PrintSection("Column count in predicate (2M rows, threads=default)", results);

	// 4. Selectivity
	con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(2000000) t(i)");
	results.clear();
	std::vector<std::pair<std::string, std::string>> sel_tests = {
	    {"1% match (val = 42)", "val = 42"},
	    {"50% match (val < 50)", "val < 50"},
	    {"100% match (val >= 0)", "val >= 0"},
	    {"0% match (id < 0)", "id < 0"},
	};
	for (auto &[label, pred] : sel_tests) {
		double ms = BenchMs(con, "SELECT * FROM condition_cache_build('t', '" + pred + "')", rounds);
		results.emplace_back(label, ms);
	}
	PrintSection("Selectivity (2M rows, threads=default)", results);

	return 0;
}
