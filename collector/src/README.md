# Сборщик GH Archive — переменные окружения

Все переменные читаются из `.env` стенда.

| Переменная                | Кто задаёт | Смысл                                          |
| ------------------------- | ---------- | ---------------------------------------------- |
| SOURCE                    | .env       | `github`                                       |
| SOURCE_FILTER             | .env       | список типов через запятую                     |
| COLLECTOR_MODE            | оболочка   | `record` / `backfill` / `produce` / `replay`   |
| COLLECTOR_RAW_DIR         | оболочка   | `/data/raw`                                    |
| COLLECTOR_RAW_LIMIT_GB    | .env       | предел объёма слоя сырых данных, ГБ            |
| COLLECTOR_METRICS_PORT    | оболочка   | 9101                                           |
| COLLECTOR_USER_AGENT      | студент    | заголовок User-Agent с контактом               |
| COLLECTOR_POLL_SECONDS    | студент    | период опроса источника (по умолчанию 300)     |
| COLLECTOR_HISTORY_FROM/TO | студент    | период backfill, даты UTC, формат `YYYY-MM-DD` |
