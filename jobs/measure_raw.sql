.timer on
SELECT date_trunc('hour', CAST(event_time AS TIMESTAMP)) AS hour,
       type,
       count(*) AS events
FROM read_json_auto([
  '/data/raw/stream/dt=*/hour=*/*.jsonl.gz',
  '/data/raw/history/dt=*/*.jsonl.gz'
], union_by_name = true)
WHERE CAST(event_time AS DATE) = DATE '2026-10-22'
GROUP BY hour, type
ORDER BY hour, type;