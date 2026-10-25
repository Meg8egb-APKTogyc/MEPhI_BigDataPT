.timer on
SELECT date_trunc('hour', event_time) AS hour,
       type,
       count(*) AS events
FROM read_parquet('/data/duckdb/parquet/events/**/*.parquet', hive_partitioning = true)
WHERE event_date = DATE '2026-10-22'
GROUP BY hour, type
ORDER BY hour, type;