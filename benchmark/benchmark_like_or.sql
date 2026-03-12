-- Benchmark: Single LIKE with OR (non-pushable)
-- Same table as benchmark_like but using OR to prevent pushdown
-- This forces full decompression of ALL columns for ALL vectors

.timer off
.print '=== Single LIKE OR (30M rows, 9 columns, non-pushable) ==='

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
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

.print '--- baseline (no cache) ---'
SET use_query_condition_cache = false;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

.timer off

.print '--- cache (build + hit) ---'
SET use_query_condition_cache = true;
.timer on

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

SELECT count(*), sum(length(col1)), sum(length(col2)), sum(length(col3)),
       sum(length(col4)), sum(length(col5)), sum(length(payload1)), sum(length(payload2))
FROM events_single WHERE text_col LIKE '%ERROR%' OR text_col LIKE '%XYZNOTEXIST%';

.timer off
DROP TABLE events_single;
