import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
WINDOW_SECS = int(os.getenv("FLOW_WINDOW_SECS", "300"))
LOOP_SLEEP = int(os.getenv("FLOW_LOOP_SLEEP", "10"))

# Wallet label gating for flow
SMART_SCORE_THRESHOLD = float(os.getenv("FLOW_SMART_SCORE_THRESHOLD", "60"))
SMART_MAX_INACTIVE_DAYS = int(os.getenv("FLOW_SMART_MAX_INACTIVE_DAYS", "14"))
WHALE_MIN_VOLUME_USD = float(os.getenv("FLOW_WHALE_MIN_VOLUME_USD", "20000"))


def get_conn():
    return connect(DB_URL, row_factory=dict_row)


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS flow_snapshots (
                ts                 timestamptz NOT NULL,
                market_id          text        NOT NULL,
                outcome            text        NOT NULL,
                window_secs        int         NOT NULL,
                net_flow           numeric     NOT NULL,
                gross_flow         numeric     NOT NULL,
                smart_net_flow     numeric     NOT NULL,
                whale_net_flow     numeric     NOT NULL,
                wallet_count       int         NOT NULL,
                smart_wallets      int         NOT NULL,
                whale_wallets      int         NOT NULL,
                top_a_swing_wallets int,
                avg_smart_score    numeric,
                smart_value_usd    numeric,
                PRIMARY KEY (ts, market_id, outcome, window_secs)
            );
            """
        )
    conn.commit()


def compute_snapshots(conn, now_ts):
    ensure_tables(conn)
    win_start = now_ts - timedelta(seconds=WINDOW_SECS)

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH t AS (
                SELECT
                    r.market_id,
                    r.outcome,
                    r.taker AS wallet,
                    CASE
                      WHEN r.side = 'buy' THEN COALESCE(r.value_usd, r.price * r.qty, 0)
                      ELSE -COALESCE(r.value_usd, r.price * r.qty, 0)
                    END AS dir_usd,
                    COALESCE(r.value_usd, r.price * r.qty, 0) AS value_usd
                FROM raw_trades r
                WHERE r.ts > %s
                  AND r.ts <= %s
            ),
            joined AS (
                SELECT
                    t.market_id,
                    t.outcome,
                    t.wallet,
                    t.dir_usd,
                    t.value_usd,
                    COALESCE(wl.is_smart, FALSE) AS is_smart,
                    wl.smart_score,
                    wl.last_updated,
                    wl.last_volume,
                    COALESCE(wl.is_top_a_swing, FALSE) AS is_top_a_swing
                FROM t
                LEFT JOIN wallet_labels wl
                  ON wl.wallet = t.wallet
            )
            SELECT
                %s::timestamptz            AS ts,
                market_id,
                outcome,
                %s::int                    AS window_secs,
                SUM(dir_usd)::numeric               AS net_flow,
                SUM(ABS(dir_usd))::numeric          AS gross_flow,
                SUM(
                  CASE
                    WHEN is_smart = TRUE
                     AND smart_score >= %s
                     AND last_updated >= NOW() - (%s || ' days')::interval
                    THEN dir_usd
                    ELSE 0
                  END
                )::numeric AS smart_net_flow,
                SUM(
                  CASE
                    WHEN last_volume IS NOT NULL
                     AND last_volume >= %s
                    THEN dir_usd
                    ELSE 0
                  END
                )::numeric AS whale_net_flow,
                COUNT(DISTINCT wallet)     AS wallet_count,
                COUNT(
                  DISTINCT CASE
                    WHEN is_smart = TRUE
                     AND smart_score >= %s
                     AND last_updated >= NOW() - (%s || ' days')::interval
                    THEN wallet
                  END
                ) AS smart_wallets,
                COUNT(
                  DISTINCT CASE
                    WHEN last_volume IS NOT NULL
                     AND last_volume >= %s
                    THEN wallet
                  END
                ) AS whale_wallets,
                COUNT(
                  DISTINCT CASE
                    WHEN is_top_a_swing = TRUE
                     AND last_updated >= NOW() - (%s || ' days')::interval
                    THEN wallet
                  END
                ) AS top_a_swing_wallets,
                AVG(
                  CASE
                    WHEN is_smart = TRUE
                     AND smart_score >= %s
                     AND last_updated >= NOW() - (%s || ' days')::interval
                    THEN smart_score
                  END
                )::numeric AS avg_smart_score,
                SUM(
                  CASE
                    WHEN is_smart = TRUE
                     AND smart_score >= %s
                     AND last_updated >= NOW() - (%s || ' days')::interval
                    THEN value_usd
                    ELSE 0
                  END
                )::numeric AS smart_value_usd
            FROM joined
            GROUP BY market_id, outcome;
            """,
            (
                win_start,
                now_ts,
                now_ts,
                WINDOW_SECS,
                SMART_SCORE_THRESHOLD,
                SMART_MAX_INACTIVE_DAYS,
                WHALE_MIN_VOLUME_USD,
                SMART_SCORE_THRESHOLD,
                SMART_MAX_INACTIVE_DAYS,
                WHALE_MIN_VOLUME_USD,
                SMART_MAX_INACTIVE_DAYS,
                SMART_SCORE_THRESHOLD,
                SMART_MAX_INACTIVE_DAYS,
                SMART_SCORE_THRESHOLD,
                SMART_MAX_INACTIVE_DAYS,
            ),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO flow_snapshots (
                    ts, market_id, outcome, window_secs,
                    net_flow, gross_flow, smart_net_flow, whale_net_flow,
                    wallet_count, smart_wallets, whale_wallets,
                    top_a_swing_wallets,
                    avg_smart_score, smart_value_usd
                )
                VALUES (
                    %(ts)s, %(market_id)s, %(outcome)s, %(window_secs)s,
                    %(net_flow)s, %(gross_flow)s, %(smart_net_flow)s, %(whale_net_flow)s,
                    %(wallet_count)s, %(smart_wallets)s, %(whale_wallets)s,
                    %(top_a_swing_wallets)s,
                    %(avg_smart_score)s, %(smart_value_usd)s
                )
                ON CONFLICT (ts, market_id, outcome, window_secs) DO NOTHING;
                """,
                r,
            )
        conn.commit()

    return len(rows)


def main():
    conn = get_conn()
    try:
        while True:
            now_ts = datetime.now(timezone.utc).replace(microsecond=0)
            n = compute_snapshots(conn, now_ts)
            if n > 0:
                print(f"[flow_snapshots] {now_ts.isoformat()} - Aggregated {n} market/outcomes")
            time.sleep(LOOP_SLEEP)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
