import gzip
import http.client
import json
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# -------- Конфигурация --------
MODE            = os.environ.get("COLLECTOR_MODE", "record")
SOURCE          = os.environ.get("SOURCE", "github")
SOURCE_FILTER   = {t.strip() for t in os.environ.get("SOURCE_FILTER", "").split(",") if t.strip()}
RAW_DIR         = Path(os.environ.get("COLLECTOR_RAW_DIR", "/data/raw"))
RAW_LIMIT_BYTES = int(float(os.environ.get("COLLECTOR_RAW_LIMIT_GB", "20")) * 1024**3)
USER_AGENT      = os.environ.get("COLLECTOR_USER_AGENT", "bigdata-stand/1.0")
POLL_SECONDS    = int(os.environ.get("COLLECTOR_POLL_SECONDS", "300"))
METRICS_PORT    = int(os.environ.get("COLLECTOR_METRICS_PORT", "9101"))
HISTORY_FROM    = os.environ.get("COLLECTOR_HISTORY_FROM", "")
HISTORY_TO      = os.environ.get("COLLECTOR_HISTORY_TO", "")

STREAM_DIR  = RAW_DIR / "stream"
HISTORY_DIR = RAW_DIR / "history"
STATE_DIR   = RAW_DIR / "state"

# -------- Счётчики --------
_c = {"events": 0, "errors": 0, "last_success": 0}
_c_lock = threading.Lock()

def inc(name, n=1):
    with _c_lock:
        _c[name] = _c.get(name, 0) + n

def touch_success():
    with _c_lock:
        _c["last_success"] = int(time.time())

# -------- Журнал --------
def log(level, msg, **kw):
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "level": level,
        "msg": msg,
    }
    rec.update(kw)
    print(json.dumps(rec, ensure_ascii=False), flush=True)

# -------- State --------
def read_state():
    p = STATE_DIR / "last_hour.txt"
    if not p.exists():
        return None
    try:
        s = p.read_text().strip()
        return datetime.strptime(s, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc) if s else None
    except Exception:
        return None

def write_state(dt):
    p = STATE_DIR / "last_hour.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(dt.strftime("%Y-%m-%d-%H"))
    tmp.rename(p)

# -------- Источник --------
def hour_url(hour_dt):
    return f"https://data.gharchive.org/{hour_dt.strftime('%Y-%m-%d')}-{hour_dt.hour}.json.gz"

def fetch_hour(hour_dt, max_retries=4):
    """HTTP GET часа. None — файла ещё нет / все retry исчерпаны."""
    url = hour_url(hour_dt)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(max_retries):
        try:
            return urllib.request.urlopen(req, timeout=300)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 500, 502, 503, 504):
                pause = 2 ** attempt
                log("warn", "http", status=e.code, pause=pause, url=url)
                inc("errors")
                time.sleep(pause)
                continue
            raise
        except urllib.error.URLError as e:
            pause = 2 ** attempt
            log("warn", "url_error", error=str(e.reason), pause=pause)
            inc("errors")
            time.sleep(pause)
    return None

def normalize(ev, received_at_iso):
    pl = ev.get("payload") or {}
    pr = pl.get("pull_request") or {}
    rv = pl.get("review") or {}
    pr_number = pl.get("number")
    if pr_number is None:
        pr_number = pr.get("number")
    return {
        "event_key":   str(ev.get("id", "")),
        "event_time":  ev.get("created_at"),
        "received_at": received_at_iso,
        "source":      SOURCE,
        "type":        ev.get("type"),
        "action":      pl.get("action"),
        "actor_login": (ev.get("actor") or {}).get("login"),
        "repo_name":   (ev.get("repo") or {}).get("name"),
        "org_login":   (ev.get("org") or {}).get("login"),
        "pr_number":   pr_number,
        "pr_id":       pr.get("id"),
        "pr_state":    pr.get("state"),
        "pr_merged":   pr.get("merged"),
        "review_state": rv.get("state"),
        "public":      ev.get("public"),
    }

def process_hour(hour_dt, out_dir, received_at_iso, received_at_dt,
                 layer_root, max_retries=3):
    """Обработать часовой архив. None — файл не готов / не удалось скачать."""
    pattern = f"part-*-{hour_dt.strftime('%Y%m%dT%H')}.jsonl.gz"
    if any(layer_root.rglob(pattern)):
        log("info", "skip_existing_hour",
            hour=hour_dt.isoformat(), where=str(layer_root))
        return (0, 0)

    for attempt in range(max_retries):
        resp = fetch_hour(hour_dt)
        if resp is None:
            return None

        out_dir.mkdir(parents=True, exist_ok=True)
        stem  = f"part-{received_at_dt.strftime('%Y%m%dT%H%M%S')}-{hour_dt.strftime('%Y%m%dT%H')}"
        tmp   = out_dir / f".{stem}.jsonl.gz.tmp"
        final = out_dir / f"{stem}.jsonl.gz"

        raw_count = kept = dups = 0
        seen_keys: set[str] = set()
        try:
            with gzip.open(tmp, "wt", encoding="utf-8") as fout:
                with gzip.GzipFile(fileobj=resp, mode="rb") as src:
                    for line in src:
                        line = line.strip()
                        if not line:
                            continue
                        raw_count += 1
                        try:
                            ev = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if SOURCE_FILTER and ev.get("type") not in SOURCE_FILTER:
                            continue
                        key = str(ev.get("id", ""))
                        if not key:
                            continue
                        if key in seen_keys:
                            dups += 1
                            continue
                        seen_keys.add(key)
                        fout.write(json.dumps(normalize(ev, received_at_iso),
                                            ensure_ascii=False,
                                            separators=(",", ":")) + "\n")
                        kept += 1
            tmp.rename(final)
            if dups:
                log("info", "dedup_in_file", hour=hour_dt.isoformat(), dups=dups)
            inc("events", kept)
            return (kept, raw_count)
        except (EOFError, gzip.BadGzipFile, OSError, http.client.HTTPException) as e:
            # http.client.HTTPException — для IncompleteRead: обрыв TCP
            # посреди тела ответа. Раньше не отлавливался, tmp не удалялся,
            # и исключение уходило в run_record как process_failed.
            tmp.unlink(missing_ok=True)
            pause = 2 ** attempt
            log("warn", "incomplete_file",
                hour=hour_dt.isoformat(), error=str(e),
                attempt=attempt + 1, pause=pause)
            inc("errors")
            time.sleep(pause)
        finally:
            resp.close()

    log("warn", "give_up_hour", hour=hour_dt.isoformat())
    return None

# -------- Ротация --------
def enforce_limit():
    files = []
    total = 0
    for p in RAW_DIR.rglob("*.jsonl.gz"):
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        files.append((st.st_mtime, st.st_size, p))
        total += st.st_size
    if total <= RAW_LIMIT_BYTES:
        return
    files.sort()
    for _, size, p in files:
        if total <= RAW_LIMIT_BYTES:
            break
        try:
            p.unlink()
            total -= size
            log("info", "rotated", path=str(p.relative_to(RAW_DIR)))
        except FileNotFoundError:
            pass

# -------- Метрики --------
class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404); self.end_headers(); return
        with _c_lock:
            body = (
                f'collector_events_total{{mode="{MODE}"}} {_c["events"]}\n'
                f'collector_errors_total{{mode="{MODE}",kind="http"}} {_c["errors"]}\n'
                f'collector_last_success_timestamp_seconds {_c["last_success"]}\n'
            )
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a, **kw):
        pass

def start_metrics():
    try:
        srv = HTTPServer(("0.0.0.0", METRICS_PORT), _MetricsHandler)
    except Exception as e:
        log("warn", "metrics_bind_failed", port=METRICS_PORT, error=str(e))
        return
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log("info", "metrics", port=METRICS_PORT)

# -------- Режимы --------
def run_record():
    log("info", "start", mode="record")
    start_metrics()

    stop = threading.Event()
    def _sig(sig, frame):
        log("info", "shutdown_signal", signal=sig)
        stop.set()
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    last = read_state()
    if last is None:
        next_hour = now - timedelta(hours=2)
        log("info", "init_state", next_hour=next_hour.isoformat())
    else:
        next_hour = last + timedelta(hours=1)

    while not stop.is_set():
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        if next_hour >= now:
            log("info", "wait_for_hour", next_hour=next_hour.isoformat())
            stop.wait(POLL_SECONDS)
            continue

        received_dt  = datetime.now(timezone.utc)
        received_iso = received_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        out_dir = (STREAM_DIR
                   / f"dt={received_dt.strftime('%Y-%m-%d')}"
                   / f"hour={received_dt.strftime('%H')}")

        try:
            result = process_hour(next_hour, out_dir, received_iso, received_dt,
                                  layer_root=STREAM_DIR)
        except Exception as e:
            inc("errors")
            log("error", "process_failed", hour=next_hour.isoformat(), error=str(e))
            stop.wait(min(POLL_SECONDS, 60))
            continue

        if result is None:
            log("info", "hour_not_ready", hour=next_hour.isoformat())
            stop.wait(POLL_SECONDS)
            continue

        kept, raw = result
        log("info", "poll", hour=next_hour.isoformat(), raw=raw, kept=kept)
        write_state(next_hour)
        touch_success()
        enforce_limit()
        next_hour += timedelta(hours=1)

    log("info", "stop", mode="record")

def run_backfill():
    log("info", "start", mode="backfill")
    start_metrics()

    if not HISTORY_FROM or not HISTORY_TO:
        log("error", "history_dates_missing", from_=HISTORY_FROM, to=HISTORY_TO)
        sys.exit(2)

    from_dt = datetime.strptime(HISTORY_FROM, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    to_dt   = datetime.strptime(HISTORY_TO,   "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)

    cur = from_dt
    total_kept = total_raw = 0
    while cur < to_dt:
        received_dt  = datetime.now(timezone.utc)
        received_iso = received_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        out_dir = HISTORY_DIR / f"dt={cur.strftime('%Y-%m-%d')}"

        try:
            result = process_hour(cur, out_dir, received_iso, received_dt,
                                  layer_root=HISTORY_DIR)
        except Exception as e:
            inc("errors")
            log("error", "backfill_failed", hour=cur.isoformat(), error=str(e))
            cur += timedelta(hours=1)
            time.sleep(2)
            continue

        if result is None:
            log("warn", "backfill_missing", hour=cur.isoformat())
            inc("errors")
        else:
            kept, raw = result
            total_kept += kept
            total_raw  += raw
            log("info", "backfill_hour", hour=cur.isoformat(), raw=raw, kept=kept)
            touch_success()

        cur += timedelta(hours=1)

    enforce_limit()

    log("info", "done", mode="backfill", total_kept=total_kept, total_raw=total_raw)

# -------- Вход --------
def main():
    global MODE
    mode = sys.argv[1] if len(sys.argv) > 1 else MODE
    MODE = mode
    log("info", "boot", mode=mode, source=SOURCE, filter=sorted(SOURCE_FILTER))

    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if mode == "record":
        run_record()
    elif mode == "backfill":
        run_backfill()
    else:
        log("error", "unknown_mode", mode=mode)
        sys.exit(2)

if __name__ == "__main__":
    main()