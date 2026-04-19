#!/bin/sh
set -eu

ROLE="${ORBITER_ROLE:-api}"
DAG_FILE="${ORBITER_DAG_FILE:-examples/example_dag.py}"
DAG_SYMBOL="${ORBITER_DAG_SYMBOL:-dag}"
DB="${ORBITER_DB:-postgresql://orbiter:orbiter@postgres:5432/orbiter}"
QUEUE_BACKEND="${ORBITER_QUEUE_BACKEND:-store}"
HOST="${ORBITER_HOST:-0.0.0.0}"
PORT="${ORBITER_PORT:-8000}"
CONCURRENCY="${ORBITER_CONCURRENCY:-4}"
POLL_INTERVAL="${ORBITER_POLL_INTERVAL:-0.5}"

wait_for_postgres() {
  if printf '%s' "$DB" | grep -Eq '^postgres(ql)?://'; then
    python3 - <<'PY'
import os
import time

import psycopg

dsn = os.environ["ORBITER_DB"]
deadline = time.time() + 60
last_error = None
while time.time() < deadline:
    try:
        conn = psycopg.connect(dsn, connect_timeout=3)
        conn.close()
        raise SystemExit(0)
    except Exception as exc:  # noqa: BLE001
        last_error = exc
        time.sleep(1)
raise SystemExit(f"PostgreSQL did not become ready in time: {last_error}")
PY
  fi
}

wait_for_postgres

case "$ROLE" in
  api)
    exec orbiter serve-api "$DAG_FILE" \
      --symbol "$DAG_SYMBOL" \
      --host "$HOST" \
      --port "$PORT" \
      --db "$DB" \
      --queue-backend "$QUEUE_BACKEND"
    ;;
  scheduler)
    exec orbiter run-scheduler "$DAG_FILE" \
      --symbol "$DAG_SYMBOL" \
      --db "$DB" \
      --queue-backend "$QUEUE_BACKEND" \
      --poll-interval "$POLL_INTERVAL"
    ;;
  worker)
    exec orbiter run-worker "$DAG_FILE" \
      --symbol "$DAG_SYMBOL" \
      --db "$DB" \
      --queue-backend "$QUEUE_BACKEND" \
      --concurrency "$CONCURRENCY"
    ;;
  all)
    exec orbiter serve "$DAG_FILE" \
      --symbol "$DAG_SYMBOL" \
      --host "$HOST" \
      --port "$PORT" \
      --db "$DB" \
      --queue-backend "$QUEUE_BACKEND" \
      --concurrency "$CONCURRENCY"
    ;;
  *)
    echo "Unsupported ORBITER_ROLE: $ROLE" >&2
    exit 1
    ;;
esac
