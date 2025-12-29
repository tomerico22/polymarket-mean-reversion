#!/usr/bin/env python3
import os, time, subprocess
import psycopg
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

SESSION = os.getenv("MR_TMUX_SESSION", "mr_exits").strip()
SUBMIT_SESSION = os.getenv("MR_TMUX_SUBMIT_SESSION", "mr_submit_live").strip()
POLL_SECS = float(os.getenv("MR_HB_POLL_SECS", "5"))

WORKERS = [
    ("intent", f"{SESSION}:intent"),
    ("sell",   f"{SESSION}:sell"),
    ("settle", f"{SESSION}:settle"),
    ("submit", f"{SUBMIT_SESSION}"),
]

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mr_worker_heartbeats (
  worker     text PRIMARY KEY,
  tmux       text,
  pid        bigint,
  alive      boolean NOT NULL DEFAULT false,
  last_seen  timestamptz NOT NULL DEFAULT now(),
  note       text
);
"""

def _run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)

def _pid_for_tmux_target(target: str):
    try:
        out = _run(["tmux", "list-panes", "-t", target, "-F", "#{pane_pid}"]).strip().splitlines()
        return int(out[0]) if out else None
    except Exception:
        return None

def _alive(pid):
    if not pid:
        return False
    try:
        _run(["ps", "-p", str(pid)])
        return True
    except Exception:
        return False

def main():
    print(f"[hb] started session={SESSION} submit_session={SUBMIT_SESSION} poll={POLL_SECS}s")
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)

        while True:
            with conn.cursor() as cur:
                for name, target in WORKERS:
                    pid = _pid_for_tmux_target(target)
                    ok = _alive(pid)
                    note = None if ok else "missing_tmux_or_dead_pid"
                    cur.execute(
                        """
                        INSERT INTO mr_worker_heartbeats(worker, tmux, pid, alive, last_seen, note)
                        VALUES (%s, %s, %s, %s, now(), %s)
                        ON CONFLICT (worker) DO UPDATE
                          SET tmux=EXCLUDED.tmux,
                              pid=EXCLUDED.pid,
                              alive=EXCLUDED.alive,
                              last_seen=EXCLUDED.last_seen,
                              note=EXCLUDED.note;
                        """,
                        (name, target, pid, ok, note),
                    )
            time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
