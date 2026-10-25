SELECT event_date,
       count(*)             AS events,
       count(review_state)  AS with_review_state
FROM read_parquet('/data/duckdb/parquet/events/**/*.parquet',
                  hive_partitioning = true, union_by_name = true)
GROUP BY event_date
ORDER BY event_date;