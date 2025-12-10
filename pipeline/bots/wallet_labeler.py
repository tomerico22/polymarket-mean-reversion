import os
import math
from datetime import date, timedelta
from decimal import Decimal

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")

LOOKBACK_DAYS = int(os.getenv("WALLET_LABEL_LOOKBACK_DAYS", "21"))
MIN_MARKETS = int(os.getenv("WALLET_LABEL_MIN_MARKETS", "8"))
MIN_TRADES = int(os.getenv("WALLET_LABEL_MIN_TRADES", "30"))
MIN_VOLUME = Decimal(os.getenv("WALLET_LABEL_MIN_VOLUME", "2000"))
MIN_PNL = Decimal(os.getenv("WALLET_LABEL_MIN_PNL", "50"))
MIN_WINRATE = float(os.getenv("WALLET_LABEL_MIN_WINRATE", "20"))
MIN_HOLD_SECS = float(os.getenv("WALLET_LABEL_MIN_HOLD_SECS", "5"))
SMART_SCORE_THRESHOLD = float(os.getenv("WALLET_LABEL_SMART_SCORE_THRESHOLD", "60"))
MAX_INACTIVE_DAYS = int(os.getenv("WALLET_LABEL_MAX_INACTIVE_DAYS", "14"))
WHALE_VOLUME_USD = Decimal(os.getenv("WALLET_LABEL_WHALE_VOLUME_USD", "20000"))


def get_conn():
    if not DB_URL:
        raise SystemExit("DB_URL not set")
    return connect(DB_URL, row_factory=dict_row)


def fetch_wallet_stats(conn):
    """
    Aggregate wallet_stats_daily for the recent window.
    Only keep wallets that meet volume, activity, PnL, and recency minimums.
    """
    start_day = date.today() - timedelta(days=LOOKBACK_DAYS)
    inactive_cutoff = date.today() - timedelta(days=MAX_INACTIVE_DAYS)

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH recent AS (
                SELECT *
                FROM wallet_stats_daily
                WHERE day >= %s
            ),
            agg AS (
                SELECT
                    wallet,
                    MAX(day)                 AS last_day,
                    COUNT(*)                AS days_active,
                    SUM(markets_traded)     AS markets_traded,
                    SUM(trades_count)       AS total_trades,
                    SUM(trades_won)         AS total_wins,
                    SUM(gross_volume)       AS total_volume,
                    SUM(realized_pnl)       AS total_pnl,
                    CASE WHEN SUM(trades_count) > 0
                         THEN SUM(trades_won)::numeric / SUM(trades_count)
                         ELSE NULL
                    END                     AS true_win_rate,
                    percentile_disc(0.5) WITHIN GROUP (
                        ORDER BY median_hold_secs
                    ) AS median_hold_secs
                FROM recent
                GROUP BY wallet
            )
            SELECT
                wallet,
                total_trades,
                total_wins,
                total_volume,
                total_pnl,
                true_win_rate,
                median_hold_secs
            FROM agg
            WHERE total_trades   >= %s
              AND markets_traded >= %s
              AND total_volume   >= %s
              AND total_pnl      >= %s
              AND last_day       >= %s
            """,
            (start_day, MIN_TRADES, MIN_MARKETS, MIN_VOLUME, MIN_PNL, inactive_cutoff),
        )
        return cur.fetchall()


def _roi_score(pnl, volume):
    """
    Map ROI to [0,1] with a hard floor at zero:
    - ROI <= 0        -> 0
    - ROI  0..0.25    -> linear 0..1
    - ROI >= 0.25     -> 1
    """
    if volume is None or volume <= 0:
        return 0.0
    roi = float(pnl) / float(volume)
    if roi <= 0:
        return 0.0
    return min(1.0, roi / 0.25)


def compute_scores(rows):
    if not rows:
        return []

    labeled = []

    vols = [float(r["total_volume"]) for r in rows if r["total_volume"] is not None]
    max_vol = max(vols) if vols else 1.0
    log_max_vol = math.log10(max_vol) if max_vol > 0 else 1.0

    for r in rows:
        wallet = r["wallet"]
        trades = int(r["total_trades"] or 0)
        wins = int(r["total_wins"] or 0)
        vol = Decimal(str(r["total_volume"] or "0"))
        pnl = Decimal(str(r["total_pnl"] or "0"))
        median_hold = float(r["median_hold_secs"] or 0.0)
        win_rate_val = float(r.get("true_win_rate") or 0.0)

        if trades <= 0 or vol <= 0:
            continue

        true_wr = win_rate_val if win_rate_val else (wins / trades if trades > 0 else 0.0)
        # Defensive clamp to avoid >100% due to any upstream counting issues
        true_wr = max(0.0, min(1.0, true_wr))

        if trades < MIN_TRADES:
            continue
        if vol < MIN_VOLUME:
            continue
        if pnl < MIN_PNL:
            continue
        if true_wr * 100 < MIN_WINRATE:
            continue
        if median_hold < MIN_HOLD_SECS:
            continue

        wr_lo, wr_hi = 0.50, 0.70
        wr_score = max(0.0, min(1.0, (true_wr - wr_lo) / (wr_hi - wr_lo)))

        if vol > 0 and log_max_vol > 0:
            vol_score = math.log10(float(vol)) / log_max_vol
            vol_score = max(0.0, min(1.0, vol_score))
        else:
            vol_score = 0.0

        roi_score = _roi_score(pnl, vol)

        smart_score = 100.0 * (0.4 * roi_score + 0.3 * wr_score + 0.3 * vol_score)
        is_smart = smart_score >= SMART_SCORE_THRESHOLD and float(pnl) >= float(MIN_PNL)
        if WHALE_VOLUME_USD > 0:
            is_whale = float(vol) >= float(WHALE_VOLUME_USD)
        else:
            is_whale = False

        labeled.append(
            {
                "wallet": wallet,
                "is_smart": is_smart,
                "is_whale": is_whale,
                "smart_score": smart_score,
                "true_wr": true_wr,
                "total_trades": trades,
                "total_volume": float(vol),
                "total_pnl": float(pnl),
                "median_hold_secs": median_hold,
            }
        )

    return labeled


def upsert_wallet_labels(conn, labeled_rows):
    if not labeled_rows:
        return 0
    today = date.today()
    with conn.cursor() as cur:
        for r in labeled_rows:
            cur.execute(
                """
                INSERT INTO wallet_labels (
                    wallet,
                    is_smart,
                    is_whale,
                    smart_score,
                    median_hold_secs,
                    as_of_day,
                    last_win_rate,
                    last_trades,
                    last_volume,
                    last_pnl,
                    last_avg_hold_secs,
                    last_updated
                )
                VALUES (
                    %(wallet)s,
                    %(is_smart)s,
                    %(is_whale)s,
                    %(smart_score)s,
                    %(median_hold_secs)s,
                    %(as_of_day)s,
                    %(last_win_rate)s,
                    %(last_trades)s,
                    %(last_volume)s,
                    %(last_pnl)s,
                    %(last_avg_hold_secs)s,
                    now()
                )
                ON CONFLICT (wallet) DO UPDATE
                SET is_smart           = EXCLUDED.is_smart,
                    is_whale           = EXCLUDED.is_whale,
                    smart_score        = EXCLUDED.smart_score,
                    median_hold_secs   = EXCLUDED.median_hold_secs,
                    as_of_day          = EXCLUDED.as_of_day,
                    last_win_rate      = EXCLUDED.last_win_rate,
                    last_trades        = EXCLUDED.last_trades,
                    last_volume        = EXCLUDED.last_volume,
                    last_pnl           = EXCLUDED.last_pnl,
                    last_avg_hold_secs = EXCLUDED.last_avg_hold_secs,
                    last_updated       = now();
                """,
                {
                    "wallet": r["wallet"],
                    "is_smart": r["is_smart"],
                    "is_whale": r["is_whale"],
                    "smart_score": r["smart_score"],
                    "median_hold_secs": r["median_hold_secs"],
                    "as_of_day": today,
                    "last_win_rate": min(100.0, r["true_wr"] * 100.0),
                    "last_trades": r["total_trades"],
                    "last_volume": r["total_volume"],
                    "last_pnl": r["total_pnl"],
                    "last_avg_hold_secs": r["median_hold_secs"],
                },
            )
        conn.commit()
    return len(labeled_rows)


def main():
    conn = get_conn()
    try:
        stats = fetch_wallet_stats(conn)
        labeled = compute_scores(stats)
        n = upsert_wallet_labels(conn, labeled)
        print(f"[wallet_labeler] updated {n} wallets")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
