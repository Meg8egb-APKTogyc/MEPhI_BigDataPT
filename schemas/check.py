"""Проверка схемы Avro на часовом файле raw.

Режимы:
  encode  --schema v1|v2 <file>       закодировать и прочитать обратно одной схемой
  cross   --writer v1|v2 --reader v1|v2 <file>   кросс-чтение (совместимость)
Печатает JSON: records, json_bytes, avro_bytes, ratio, errors.
"""
import argparse
import gzip
import json
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path

import fastavro

SCHEMAS = {
    "v1": Path(__file__).parent / "event.avsc",
    "v2": Path(__file__).parent / "event_v2.avsc",
}


def load_schema(ver):
    return json.loads(SCHEMAS[ver].read_text())


def iso_to_ms(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def prepare(rec):
    return {
        "event_key": str(rec["event_key"]),
        "event_time": iso_to_ms(rec["event_time"]),
        "received_at": iso_to_ms(rec["received_at"]),
        "source": rec["source"],
        "type": rec["type"],
        "action": rec.get("action"),
        "actor_login": rec.get("actor_login"),
        "repo_name": rec.get("repo_name"),
        "org_login": rec.get("org_login"),
        "pr_number": rec.get("pr_number"),
        "pr_id": rec.get("pr_id"),
        "pr_state": rec.get("pr_state"),
        "pr_merged": rec.get("pr_merged"),
        "review_state": rec.get("review_state"),
        "public": rec.get("public"),
    }


def encode(rec, schema):
    buf = BytesIO()
    fastavro.schemaless_writer(buf, schema, rec)
    return buf.getvalue()


def decode(data, writer_schema, reader_schema=None):
    return fastavro.schemaless_reader(BytesIO(data), writer_schema, reader_schema)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["encode", "cross"])
    ap.add_argument("file")
    ap.add_argument("--schema", default="v1")
    ap.add_argument("--writer", default="v1")
    ap.add_argument("--reader", default="v2")
    ap.add_argument("--limit", type=int, default=0, help="0 — весь файл")
    args = ap.parse_args()

    writer_schema = load_schema(args.schema if args.mode == "encode" else args.writer)
    reader_schema = load_schema(args.schema if args.mode == "encode" else args.reader)

    records = json_bytes = avro_bytes = 0
    errors = []
    samples = []

    with gzip.open(args.file, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if args.limit and i >= args.limit:
                break
            try:
                raw = json.loads(line)
                rec = prepare(raw)
            except Exception as e:
                errors.append({"i": i, "error": f"prepare: {e}"})
                continue
            try:
                data = encode(rec, writer_schema)
            except Exception as e:
                errors.append({"event_key": rec.get("event_key"), "error": f"encode: {e}"})
                continue
            try:
                back = decode(data, writer_schema, reader_schema)
            except Exception as e:
                errors.append({"event_key": rec.get("event_key"), "error": f"decode: {e}"})
                continue
            if len(samples) < 3:
                samples.append({"event_key": rec["event_key"],
                                "decoded_keys": sorted(back.keys())})
            records += 1
            json_bytes += len(json.dumps(rec, ensure_ascii=False, separators=(",", ":")).encode())
            avro_bytes += len(data)

    report = {
        "file": args.file,
        "mode": args.mode,
        "writer": args.schema if args.mode == "encode" else args.writer,
        "reader": args.schema if args.mode == "encode" else args.reader,
        "records": records,
        "json_bytes": json_bytes,
        "avro_bytes": avro_bytes,
        "ratio": round(json_bytes / avro_bytes, 3) if avro_bytes else None,
        "errors": errors[:10],
        "errors_total": len(errors),
        "samples": samples,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()