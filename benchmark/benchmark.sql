-- Benchmark: Query Condition Cache — OR predicates with heavy payload columns
-- Modeled after condition-cache-vector-level-prototype benchmarks
-- Key: OR can't be pushed down, so DuckDB must decompress ALL columns for ALL vectors
-- Cache skips vectors → avoids decompression of heavy payload columns

.timer off

-- ============================================================
-- Benchmark 1: Compound OR — Two LIKE predicates (30M rows)
-- ============================================================
.print '=== Benchmark 1: Compound LIKE OR (30M rows, 9 columns) ==='

CREATE TABLE events_like AS
SELECT
    i AS id,
    CASE WHEN i % 50000 = 0
         THEN 'PREFIX_ERROR_' || md5(i::VARCHAR)
         ELSE md5(i::VARCHAR) || md5((i*7)::VARCHAR)
    END AS text_a,
    CASE WHEN i % 40000 = 0
         THEN 'PREFIX_WARN_' || md5(i::VARCHAR)
         ELSE md5((i*2)::VARCHAR) || md5((i*8)::VARCHAR)
    END AS text_b,
    md5((i*3)::VARCHAR) AS col1,
    md5((i*4)::VARCHAR) AS col2,
    md5((i*5)::VARCHAR) AS col3,
    md5((i*6)::VARCHAR) AS col4,
    repeat('payload-' || (i%1000)::VARCHAR, 10) AS payload1,
    repeat('x', 100) AS payload2
FROM range(30000000) t(i);
CHECKPOINT;

-- Warmup
SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

.print '--- baseline (no cache) ---'
SET enable_query_condition_cache = false;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

.timer off

.print '--- cache (build + hit) ---'
SET enable_query_condition_cache = true;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_like WHERE text_a LIKE '%ERROR%' OR text_b LIKE '%WARN%';

.timer off
DROP TABLE events_like;

-- ============================================================
-- Benchmark 2: Simple OR — Integer predicates (30M rows)
-- Even trivial int comparisons can't use zone maps under OR
-- Bottleneck is decompression of all 9 columns
-- ============================================================
.print ''
.print '=== Benchmark 2: Simple Integer OR (30M rows, 9 columns) ==='

CREATE TABLE events_int AS
SELECT
    i AS id,
    (i % 60000) AS val_a,
    (i % 80000) AS val_b,
    md5((i*2)::VARCHAR) AS col1,
    md5((i*3)::VARCHAR) AS col2,
    md5((i*4)::VARCHAR) AS col3,
    md5((i*5)::VARCHAR) AS col4,
    repeat('payload-' || (i%1000)::VARCHAR, 10) AS payload1,
    repeat('x', 100) AS payload2
FROM range(30000000) t(i);
CHECKPOINT;

-- Warmup
SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

.print '--- baseline (no cache) ---'
SET enable_query_condition_cache = false;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

.timer off

.print '--- cache (build + hit) ---'
SET enable_query_condition_cache = true;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_int WHERE val_a < 50 OR val_b < 30;

.timer off
DROP TABLE events_int;

-- ============================================================
-- Benchmark 3: Mixed AND — Integer + LIKE (30M rows)
-- Zone map handles int, cache handles LIKE
-- ============================================================
.print ''
.print '=== Benchmark 3: Mixed AND - int + LIKE (30M rows, 8 columns) ==='

CREATE TABLE events_mix AS
SELECT
    i AS id,
    (i % 60000) AS val,
    CASE WHEN i % 50000 = 0
         THEN 'PREFIX_ERROR_' || md5(i::VARCHAR)
         ELSE md5(i::VARCHAR) || md5((i*7)::VARCHAR)
    END AS text_col,
    md5((i*3)::VARCHAR) AS col1,
    md5((i*4)::VARCHAR) AS col2,
    md5((i*5)::VARCHAR) AS col3,
    md5((i*6)::VARCHAR) AS col4,
    repeat('payload-' || (i%1000)::VARCHAR, 10) AS payload1,
    repeat('x', 100) AS payload2
FROM range(30000000) t(i);
CHECKPOINT;

-- Warmup
SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

.print '--- baseline (no cache) ---'
SET enable_query_condition_cache = false;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

.timer off

.print '--- cache (build + hit) ---'
SET enable_query_condition_cache = true;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(payload1)), sum(length(payload2))
FROM events_mix WHERE val < 50 AND text_col LIKE '%ERROR%';

.timer off
DROP TABLE events_mix;

-- ============================================================
-- Benchmark 4: Single LIKE (pushable predicate, 30M rows)
-- LIKE gets pushed to table_filters as ExpressionFilter
-- No zone map benefit — cache provides row-group + vector skip
-- ============================================================
.print ''
.print '=== Benchmark 4: Single LIKE (30M rows, 9 columns, pushable) ==='

CREATE TABLE events_single AS
SELECT
    i AS id,
    CASE WHEN i % 50000 = 0
         THEN 'PREFIX_ERROR_' || md5(i::VARCHAR)
         ELSE md5(i::VARCHAR) || md5((i*7)::VARCHAR)
    END AS text_col,
    md5((i*3)::VARCHAR) AS col1,
    md5((i*4)::VARCHAR) AS col2,
    md5((i*5)::VARCHAR) AS col3,
    md5((i*6)::VARCHAR) AS col4,
    md5((i*7)::VARCHAR) AS col5,
    repeat('payload-' || (i%1000)::VARCHAR, 10) AS payload1,
    repeat('x', 100) AS payload2
FROM range(30000000) t(i);
CHECKPOINT;

-- Warmup
SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

.print '--- baseline (no cache) ---'
SET enable_query_condition_cache = false;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

.timer off

.print '--- cache (build + hit) ---'
SET enable_query_condition_cache = true;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%';

.timer off
DROP TABLE events_single;
