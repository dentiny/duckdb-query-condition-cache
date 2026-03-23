#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <cstdio>

#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

using namespace duckdb;

namespace {

// Run sql `rounds` times, return average elapsed time in milliseconds.
double BenchMs(Connection &con, const string &sql, int rounds = 5) {
	double total = 0;
	for (int r = 0; r < rounds; ++r) {
		auto start = std::chrono::steady_clock::now();
		con.Query(sql);
		auto end = std::chrono::steady_clock::now();
		total += std::chrono::duration<double, std::milli>(end - start).count();
	}
	return total / rounds;
}

void PrintSection(const char *title, const vector<std::pair<string, double>> &rows) {
	printf("\n%s\n", title);
	for (const auto &[label, ms] : rows) {
		printf("  %-45s %8.1f ms\n", label.c_str(), ms);
	}
}

} // namespace

int main() {
	DuckDB db;
	Connection con(db);

	const int rounds = 5;
	const string build_val42 = "SELECT * FROM condition_cache_build('t', 'val = 42')";
	const string build_val50 = "SELECT * FROM condition_cache_build('t', 'val < 50')";

	// 0. Query baseline vs cache build cost
	con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(2000000) t(i)");
	vector<std::pair<string, double>> results;
	results.emplace_back("SELECT count(*) WHERE val = 42",
	                     BenchMs(con, "SELECT count(*) FROM t WHERE val = 42", rounds));
	results.emplace_back("SELECT count(*) WHERE val < 50",
	                     BenchMs(con, "SELECT count(*) FROM t WHERE val < 50", rounds));
	results.emplace_back("cache build (val = 42)", BenchMs(con, build_val42, rounds));
	results.emplace_back("cache build (val < 50)", BenchMs(con, build_val50, rounds));
	PrintSection("Query baseline vs cache build cost (2M rows, threads=default)", results);

	// 1. Row count scaling
	results.clear();
	for (int64_t n : {100000, 500000, 2000000, 12288000}) {
		con.Query(StringUtil::Format(
		    "CREATE OR REPLACE TABLE t AS SELECT i AS id, i%%100 AS val FROM range(%lld) t(i)", n));
		double ms = BenchMs(con, build_val42, rounds);
		int64_t rgs = (n + 122879) / 122880;
		results.emplace_back(StringUtil::Format("%lld rows (%lld RGs)", n, rgs), ms);
	}
	PrintSection("Row count scaling (threads=default)", results);

	// 2. Thread scaling
	con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(2000000) t(i)");
	results.clear();
	for (int threads : {1, 2, 4, 8}) {
		con.Query(StringUtil::Format("SET threads = %d", threads));
		double ms = BenchMs(con, build_val42, rounds);
		results.emplace_back(StringUtil::Format("threads = %d", threads), ms);
	}
	PrintSection("Thread scaling (2M rows, 17 RGs)", results);
	con.Query("RESET threads");

	// 3. Column count in predicate
	con.Query(R"(CREATE OR REPLACE TABLE t AS
		SELECT i AS id, i%100 AS c1, i%200 AS c2, i%300 AS c3, i%400 AS c4, i%500 AS c5
		FROM range(2000000) t(i))");
	results.clear();
	vector<std::pair<string, string>> col_tests = {
	    {"1 col", "c1 = 42"},
	    {"3 cols", "c1 = 42 AND c2 < 100 AND c3 > 50"},
	    {"5 cols", "c1 = 42 AND c2 < 100 AND c3 > 50 AND c4 != 0 AND c5 < 250"},
	};
	for (const auto &[label, pred] : col_tests) {
		double ms = BenchMs(con, StringUtil::Format("SELECT * FROM condition_cache_build('t', '%s')", pred), rounds);
		results.emplace_back(label, ms);
	}
	PrintSection("Column count in predicate (2M rows, threads=default)", results);

	// 4. Selectivity
	con.Query("CREATE OR REPLACE TABLE t AS SELECT i AS id, i%100 AS val FROM range(2000000) t(i)");
	results.clear();
	vector<std::pair<string, string>> sel_tests = {
	    {"1% match (val = 42)", "val = 42"},
	    {"50% match (val < 50)", "val < 50"},
	    {"100% match (val >= 0)", "val >= 0"},
	    {"0% match (id < 0)", "id < 0"},
	};
	for (const auto &[label, pred] : sel_tests) {
		double ms = BenchMs(con, StringUtil::Format("SELECT * FROM condition_cache_build('t', '%s')", pred), rounds);
		results.emplace_back(label, ms);
	}
	PrintSection("Selectivity (2M rows, threads=default)", results);

	return 0;
}
