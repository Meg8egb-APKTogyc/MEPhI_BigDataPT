-- ЛР2: весь слой сырых данных (stream + history) → Parquet.
-- Типы по схеме event.avsc; партиции по event_date = дата события UTC.
-- Повторный запуск перезаписывает партиции (OVERWRITE_OR_IGNORE).

COPY (
  SELECT
    event_key,
    CAST(event_time  AS TIMESTAMP) AS event_time,
    CAST(received_at AS TIMESTAMP) AS received_at,
    source,
    type,
    action,
    actor_login,
    repo_name,
    org_login,
    CAST(pr_number AS INTEGER) AS pr_number,
    CAST(pr_id     AS BIGINT)  AS pr_id,
    pr_state,
    CAST(pr_merged AS BOOLEAN) AS pr_merged,
    CAST(public    AS BOOLEAN) AS public,
    CAST(event_time AS DATE)   AS event_date
  FROM read_json_auto([
    '/data/raw/stream/dt=*/hour=*/*.jsonl.gz',
    '/data/raw/history/dt=*/*.jsonl.gz'
  ], union_by_name = true)
) TO '/data/duckdb/parquet/events'
(FORMAT parquet,
 PARTITION_BY (event_date),
 OVERWRITE_OR_IGNORE true,
 FILENAME_PATTERN 'part-{i}');