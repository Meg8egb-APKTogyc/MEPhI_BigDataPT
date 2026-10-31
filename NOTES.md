# Журнал работ

## ЛР1. Стенд и сбор данных

### Решения по сборщику

- **Источник**: GH Archive, почасовые архивы. Фильтр — по полю `type` на стороне
  сборщика, источник фильтр не умеет.
- **Режимы**: `record` пишет поток в `stream/`, `backfill` — историю в `history/`.
  `backfill` не пишет state, позиция потока не сбивается.
- **Раскладка**: поток — по `received_at` (`dt=/hour=`), история — по `event_time`
  (`dt=`). Поздние события не дописываются в закрытые часы.
- **Ключ и время**: `event_key = id`, `event_time = created_at` (UTC). У review-событий
  `pr_number` берётся из `payload.pull_request.number`.
- **State**: `data/raw/state/last_hour.txt` — последний успешно загруженный час.
  При старте читается state; уже скачанный час пропускается по имени файла
  (`skip_existing_hour`, поиск `rglob` по всему слою, а не только в текущем `hour=`).
- **Дедупликация**: внутри часа — `seen_keys`; между часами — поиск файла часа
  по `rglob`. Перекачка после ручной правки state не создаёт дубль.
- **Retry**: 404 — `hour_not_ready`, пауза `COLLECTOR_POLL_SECONDS`; 429/5xx,
  `socket.timeout`, `URLError` — пауза 2^n (4 попытки); `http.client.HTTPException`
  — `incomplete_file`, tmp удаляется, попытка повторяется.
- **Ротация**: `enforce_limit()` после каждого часа `record`; в `backfill` — один
  вызов после цикла. Удаляются самые старые закрытые файлы.
- **Память**: файл читается потоком через `gzip.GzipFile(fileobj=resp)`, ответ
  не буферизуется целиком.
- **Метрики** (допзадание): `:9101/metrics` — `collector_events_total`,
  `collector_errors_total`, `collector_last_success_timestamp_seconds`.

### Замеры

- **Backfill 7 суток (11–17.10.2026)**: 171 файлов, 15 МБ сжатых, **350 844 события**
  после фильтра из 7 545 763 сырых (доля kept ≈ 4.6%).
- **Поток 18.10.2026 00:00–16:00 UTC** (догон первого запуска): 17 файлов, 2.6 МБ,
  **67 397 событий**.
- **24-часовой интервал шага 10** (22.10 22:00 — 23.10 22:00 UTC):
    - `raw = 121 452`, `kept = 45 025`, доля `kept = 37.07%`;
    - до фильтра: **84.3 соб/мин**; после фильтра: **31.3 соб/мин**;
    - сжатых: **1 873 327 байт/сутки** (~1.87 МБ); несжатых: **15 369 061 байт/сутки**;
    - восемь недель потока: **56 × 1.87 МБ ≈ 105 МБ**;
    - исторический срез 7 суток: **15 МБ**;
    - слой сырых данных целиком: **~120 МБ** при пределе 20 ГБ.
- **Пропускная способность догона**: ~3–4 с/час.
- **Повторов по ключу** в `stream/`: **0**.

### Пропуски и восполнение

| Пропуск                   | Причина                                          | Восполнение                                                                                    |
| ------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| 18.10 19:00 — 22.10 21:00 | ручная остановка сборщика                        | догон `record` с сохранённого state                                                            |
| 18–22.10, часть часов     | ошибочно удалены файлы догона (приняты за дубли) | `backfill` в `history/` перед ЛР3; данные в `stream/` за эти часы восстанавливаются из истории |
| 23.10 18:11               | обновление Docker, SIGTERM                       | graceful restart за 5 с, state прочитан, запись продолжена                                     |
| 24.10 22:23–22:24         | ручной стоп для демонстрации допзадания          | state восстановлен, догон выполнен                                                             |

### Допзадание: счётчики на 9101

- Метрики отдаются в формате Prometheus.
- Демонстрация: блокировка HTTPS внутри netns контейнера
  (`nsenter -t <pid> -n iptables -A OUTPUT -p tcp --dport 443 -j REJECT`).
- Во время блокировки `errors_total` растёт на 1 за каждый неуспешный retry
  (для часа, все попытки которого провалились, — +4); `give_up_hour` и
  `hour_not_ready` счётчик не увеличивают.
- После снятия блокировки `errors_total` остаётся прежним (монотонный),
  `poll` восстановился, `last_success` обновился.
- Полный лог — в `NOTES-appendix.md`.

### Ограничения

- `enforce_limit()` обходит всё дерево `data/raw/` через `rglob` — при миллионах
  файлов станет узким местом; заменить на очередь по `stat`.
- `state/last_hour.txt` не защищён от ручного редактирования: при ошибке сборщик
  пойдёт не с того места. Резервное копирование state перед экспериментами.
  Частичный предохранитель — `skip_existing_hour` по `rglob`.
- История не обновляется автоматически; для восполнения пропусков потокового
  слоя повторять `backfill` за нужные сутки.

---

## ЛР2. Форматы данных

### Схема Avro

- `schemas/event.avsc` (v1) — 14 полей паспорта. Обязательные: `event_key`,
  `event_time`, `received_at`, `source`, `type`. Остальные nullable с default null.
- `event_time` и `received_at` — `long` с `logicalType: timestamp-millis`, UTC.
  Конвертация ISO 8601 → мс в `schemas/check.py:iso_to_ms`.
- `schemas/event_v2.avsc` (v2) добавлена полем `review_state` (`null|string`,
  default null) из `payload.review.state`. Заполняется только у
  `PullRequestReviewEvent`; у `PullRequestEvent` и
  `PullRequestReviewCommentEvent` — null. Значения: `approved`,
  `changes_requested`, `commented`, `dismissed`.
- Сборщик извлекает `review_state` в `normalize()` с 2026-10-25 20:51 UTC;
  raw-файлы до этой правки поля не содержат, `union_by_name = true` даёт NULL.

### DuckDB: типы и повторы

- `read_json_auto` вывел: `event_time` и `received_at` — `VARCHAR` (ISO 8601);
  `pr_number` — `BIGINT`; `pr_merged`, `public` — `BOOLEAN`; `pr_id` — `BIGINT`.
- Отличие от схемы: DuckDB не парсит ISO 8601 сам; `CAST(... AS TIMESTAMP)`
  делается в `to_parquet.sql`, в `check.py` — конверсия ISO → мс.
- `dup_keys = 0` (проверено `count(*) - count(DISTINCT event_key)`).
- Всего событий: **654 340** за 15 дат (11.10–25.10): `history/` — 350 844,
  `stream/` — 303 496.

### Parquet

- `to_parquet.sql` (v1) и `to_parquet_v2.sql` (v2):
  `data/duckdb/parquet/events/event_date=…/part-*.parquet`.
  Партиция — `event_date` по `event_time` UTC (не по `received_at`).
- `OVERWRITE_OR_IGNORE true` + `FILENAME_PATTERN 'part-{i}'` — повторный запуск
  перезаписывает те же файлы в тех же партициях, копий не появляется.
- Объём: raw = **28 МБ** (stream 13 + history 15), parquet = **33 МБ**.
  Parquet (snappy) на 18% больше raw (gzip): snappy слабее gzip на JSON-ключах,
  плюс метаданные на 15 файлов. Выигрыш — в скорости чтения, не в объёме.

### Замеры raw vs Parquet

Запрос: число событий по часам за 22.10.2026 в разрезе `type`.
Медиана трёх прогонов (`.timer on`).

| Путь                                | Медиана, с |
| ----------------------------------- | ---------: |
| raw (gzip-JSON, stream + history)   |  **3.590** |
| Parquet (snappy, hive_partitioning) |  **0.018** |
| ускорение                           |  **~200×** |

Ответы совпали: 72 строки = 24 ч × 3 type. Полные прогоны — в `NOTES-appendix.md`.

### Совместимость

- **v1 → v2 (backward)**: `errors_total = 0`, поле `review_state` = null
  в прочитанных записях (default).
- **v2 → v1 (forward)**: `errors_total = 0`, поле `review_state` отбрасывается
  v1-читателем (Avro не падает на неизвестных полях).
- **Вид совместимости: Full** (добавление поля с default в Avro совместимо
  в обе стороны). Проверено на файле часа 19:xx (2026-10-25), 1005 записей;
  полный вывод — в `NOTES-appendix.md`.

### Шаг 9 — проверено

`read_union.sql` на 15 партициях:

- Даты < 2026-10-25: `with_review_state = 0`, хотя `review_events > 0` —
  поля в raw нет (v1).
- Дата 2026-10-25: партиция смешанная — часы до 20:51 без поля (NULL),
  после — со значениями. `with_review_state = 617` при `review_events = 3316`
  (доля ≈ 18.6% — часы после правки).
- Распределение `review_state`: commented 455, approved 113,
  changes_requested 28, dismissed 21; сумма 617.

Полная таблица по датам — в `NOTES-appendix.md`.

### Проверка «no copies»

Два прогона `to_parquet.sql` подряд: число `.parquet`-файлов 15 → 15,
копий не появилось. Binary-md5 меняется из-за порядка строк и метаданных;
content-hash по партиции (`md5(string_agg(event_key, ',' ORDER BY event_key))`)
совпадает для всех 15 дат до и после прогона. Логическое содержимое стабильно,
повторный запуск идемпотентен. Детали — в `NOTES-appendix.md`.

### Допзадание

Не выполнялось: кодеки (`uncompressed`, `snappy`, `gzip`, `zstd`) и
`ROW_GROUP_SIZE` не сравнивались. Для очищенного слоя ЛР3 используются
параметры по умолчанию (snappy), выбор обоснован замером raw vs Parquet.
