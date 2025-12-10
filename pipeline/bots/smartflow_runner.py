import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

DB_URL = os.getenv("DB_URL")

STRATEGY_NAME = os.getenv("STRATEGY", "sm_smartflow_v1")
FLOW_WINDOW_SECS = int(os.getenv("FLOW_WINDOW_SECS", "300"))
LOOP_SLEEP = int(os.getenv("SMARTFLOW_LOOP_SLEEP", "10"))

MIN_SMART_WALLETS = int(os.getenv("SMARTFLOW_MIN_SMART_WALLETS", "2"))
MIN_NET_FLOW = Decimal(os.getenv("SMARTFLOW_MIN_NET_FLOW", "50"))
MIN_SNAPSHOT_FRESHNESS = int(os.getenv("SMARTFLOW_SNAPSHOT_FRESHNESS", "60"))
SCORE_THRESHOLD = float(os.getenv("SMARTFLOW_SCORE_THRESHOLD", "0.6"))
SIGNAL_TTL_SECS = int(os.getenv("SMARTFLOW_SIGNAL_TTL_SECS", "300"))
RUNNER_COOLDOWN_SECS = int(os.getenv("SMARTFLOW_RUNNER_COOLDOWN_SECS", "300"))

LAST_SNAPSHOT_TS = datetime.min.replace(tzinfo=timezone.utc)
# per-market/outcome cooldown tracking
LAST_SIGNAL_TS = {}


def get_conn():
    return connect(DB_URL, row_factory=dict_row)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_signals (
                id             bigserial PRIMARY KEY,
                ts             timestamptz NOT NULL,
                strategy       text        NOT NULL,
                market_id      text        NOT NULL,
                outcome        text        NOT NULL,
                side           text        NOT NULL,
                score          numeric     NOT NULL,
                reason         jsonb       NOT NULL,
                expires_at     timestamptz NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_strat_ts
            ON strategy_signals(strategy, ts DESC);
            """
        )
    conn.commit()


def fetch_recent_snapshots(conn, now_ts):
    freshness_cutoff = now_ts - timedelta(seconds=MIN_SNAPSHOT_FRESHNESS)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (fs.market_id, fs.outcome) fs.*
            FROM flow_snapshots fs
            WHERE fs.window_secs = %s
              AND fs.ts >= %s
            ORDER BY fs.market_id, fs.outcome, fs.ts DESC;
            """,
            (FLOW_WINDOW_SECS, freshness_cutoff),
        )
        return cur.fetchall()


def compute_signal_side_and_score(row):
    net_flow = Decimal(row["net_flow"])
    smart_net = Decimal(row["smart_net_flow"])
    whale_net = Decimal(row["whale_net_flow"])
    smart_wallets = int(row["smart_wallets"])
    top_a_swing_wallets = int(row.get("top_a_swing_wallets") or 0)

    if smart_wallets < MIN_SMART_WALLETS and top_a_swing_wallets < 1:
        return None, 0.0
    if abs(smart_net) < MIN_NET_FLOW:
        return None, 0.0

    side = "buy" if smart_net > 0 else "sell"

    mag_score = min(1.0, float(abs(smart_net) / MIN_NET_FLOW))

    if net_flow == 0:
        dom_score = 1.0
    elif net_flow * smart_net < 0:
        dom_score = 1.0
    else:
        dom_score = float(smart_net / net_flow)
        dom_score = max(0.0, min(1.0, dom_score))

    whale_score = 0.5
    if whale_net != 0 and (whale_net > 0 and smart_net > 0 or whale_net < 0 and smart_net < 0):
        whale_score = 1.0

    score = 0.5 * mag_score + 0.3 * dom_score + 0.2 * whale_score
    score = max(0.0, min(1.0, score))
    return side, score


def insert_signal(conn, now_ts, row, side, score, price=None):
    expires_at = now_ts + timedelta(seconds=SIGNAL_TTL_SECS)
    reason = {
        "source": "smart_flow_v1",
        "net_flow": str(row["net_flow"]),
        "smart_net_flow": str(row["smart_net_flow"]),
        "whale_net_flow": str(row["whale_net_flow"]),
        "wallet_count": row["wallet_count"],
        "smart_wallets": row["smart_wallets"],
        "whale_wallets": row["whale_wallets"],
        "top_a_swing_wallets": row.get("top_a_swing_wallets", 0),
        "window_secs": row["window_secs"],
        "snapshot_ts": row["ts"].isoformat(),
    }
    if price is not None:
        reason["price"] = str(price)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO strategy_signals (
                ts,
                strategy,
                market_id,
                outcome,
                side,
                score,
                reason,
                expires_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb,
                %s
            );
            """,
            (now_ts, STRATEGY_NAME, row["market_id"], row["outcome"],
             side, score, Json(reason), expires_at),
        )
        conn.commit()


def main():
    global LAST_SNAPSHOT_TS
    global LAST_SIGNAL_TS
    print(f"[smartflow_runner] Starting up...")
    print(f"[smartflow_runner] Strategy: {STRATEGY_NAME}")
    print(f"[smartflow_runner] Min smart wallets: {MIN_SMART_WALLETS}")
    print(f"[smartflow_runner] Min net flow: {MIN_NET_FLOW}")
    print(f"[smartflow_runner] Score threshold: {SCORE_THRESHOLD}")
    print(f"[smartflow_runner] Loop sleep: {LOOP_SLEEP}s")
    print(f"[smartflow_runner] Runner cooldown: {RUNNER_COOLDOWN_SECS}s per market/outcome")
    conn = get_conn()
    ensure_tables(conn)
    try:
        while True:
            now_ts = datetime.now(timezone.utc)
            rows = fetch_recent_snapshots(conn, now_ts)
            n_signals = 0
            n_evaluated = 0
            n_filtered_flow = 0
            n_filtered_score = 0
            n_filtered_wallets = 0
            n_filtered_cooldown = 0
            batch_max_ts = LAST_SNAPSHOT_TS
            for r in rows:
                n_evaluated += 1
                if r["ts"] > batch_max_ts:
                    batch_max_ts = r["ts"]
                side, score = compute_signal_side_and_score(r)
                if side is None:
                    if int(r["smart_wallets"]) < MIN_SMART_WALLETS:
                        n_filtered_wallets += 1
                    elif abs(Decimal(r["smart_net_flow"])) < MIN_NET_FLOW:
                        n_filtered_flow += 1
                    continue
                if score < SCORE_THRESHOLD:
                    n_filtered_score += 1
                    continue

                key = (r["market_id"], r["outcome"])
                last_ts = LAST_SIGNAL_TS.get(key)
                if last_ts and (now_ts - last_ts).total_seconds() < RUNNER_COOLDOWN_SECS:
                    n_filtered_cooldown += 1
                    continue

                price_for_reason = None
                try:
                    with conn.cursor() as pcur:
                        pcur.execute(
                            """
                            SELECT price
                            FROM raw_trades
                            WHERE market_id = %s AND outcome = %s
                            ORDER BY ts DESC
                            LIMIT 1
                            """,
                            (r["market_id"], r["outcome"]),
                        )
                        prow = pcur.fetchone()
                        if prow and prow.get("price") is not None:
                            price_for_reason = prow["price"]
                except Exception:
                    price_for_reason = None

                insert_signal(conn, now_ts, r, side, score, price=price_for_reason)
                LAST_SIGNAL_TS[key] = now_ts
                n_signals += 1
            LAST_SNAPSHOT_TS = max(LAST_SNAPSHOT_TS, batch_max_ts)
            if n_signals:
                print(f"[smartflow_runner] {now_ts.isoformat()} - inserted {n_signals} signals")
            else:
                # periodic heartbeat
                pass
            time.sleep(LOOP_SLEEP)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
