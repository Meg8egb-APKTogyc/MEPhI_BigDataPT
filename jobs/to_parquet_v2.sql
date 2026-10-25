-- ЛР2 шаг 9: записать по схеме v2 только новые сутки (>= 2026-10-25).
-- Старые партиции остаются без review_state; при чтении union_by_name = true
-- у старых review_state = null. В raw поля review_state нет — оно
-- заполнится с ЛР6 (produce + v2 в реестре); сейчас NULL.

COPY (
  SELECT
    event_key,
    CAST(event_time  AS TIMESTAMP) AS event_time,
    CAST(received_at AS TIMESTAMP) AS received_at,
    source, type, action, actor_login, repo_name, org_login,
    CAST(pr_number AS INTEGER) AS pr_number,
    CAST(pr_id     AS BIGINT)  AS pr_id,
    pr_state,
    CAST(pr_merged AS BOOLEAN) AS pr_merged,
    CAST(public    AS BOOLEAN) AS public,
    CAST(NULL AS VARCHAR)      AS review_state,
    CAST(event_time AS DATE)   AS event_date
  FROM read_json_auto([
    '/data/raw/stream/dt=*/hour=*/*.jsonl.gz'
  ], union_by_name = true)
  WHERE CAST(event_time AS DATE) >= DATE '2026-10-25'
) TO '/data/duckdb/parquet/events'
(FORMAT parquet, PARTITION_BY (event_date),
 OVERWRITE_OR_IGNORE true, FILENAME_PATTERN 'part-{i}');