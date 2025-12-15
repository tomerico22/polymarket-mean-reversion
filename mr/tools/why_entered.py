#!/usr/bin/env python3
import os
import argparse
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")

MIN_PRICE = Decimal(os.getenv("MR_MIN_PRICE", "0.05"))
MAX_PRICE = Decimal(os.getenv("MR_MAX_PRICE", "0.95"))
DISLO_THR = Decimal(os.getenv("MR_DISLOCATION_THRESHOLD", "0.20"))
MAX_DISLO = Decimal(os.getenv("MR_MAX_DISLOCATION", "0.45"))
MR_PRICE_STALE_SECS = int(os.getenv("MR_PRICE_STALE_SECS", "3600"))

_tags_env = os.getenv("MR_EXCLUDED_TAGS") or os.getenv("MR_EXCLUDE_TAGS") or "sports,nfl,nba,soccer,mlb,hockey"
EXCLUDED_TAGS = {t.strip().lower() for t in _tags_env.split(",") if t.strip()}

def norm_tags(tags_val):
    if not tags_val:
        return set()
    if isinstance(tags_val, list):
        return {str(t).strip().lower() for t in tags_val if str(t).strip()}
    if isinstance(tags_val, str):
        return {t.strip().lower() for t in tags_val.split(",") if t.strip()}
    return set()

def get_conn():
    conn = connect(DB_URL, row_factory=dict_row)
    conn.autocommit = True
    conn.prepare_threshold = None
    return conn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=80)
    ap.add_argument("--hours", type=int, default=24, help="look back this many hours of positions by entry_ts")
    ap.add_argument("--strategy", type=str, default=STRATEGY)
    args = ap.parse_args()

    now_ts = datetime.now(timezone.utc)
    since_ts = now_ts - timedelta(hours=args.hours)

    conn = get_conn()
    with conn.cursor() as cur:
        # Pull recent positions (open + closed) for this strategy
        cur.execute(
            """
            SELECT id, market_id, outcome, entry_ts, entry_price, size, status, exit_ts, exit_reason, pnl
            FROM mr_positions
            WHERE strategy = %s
              AND entry_ts >= %s
            ORDER BY entry_ts DESC
            LIMIT %s
            """,
            (args.strategy, since_ts, args.limit),
        )
        positions = cur.fetchall() or []

        if not positions:
            print(f"now_utc={now_ts.isoformat()} no positions found for strategy={args.strategy} since={since_ts.isoformat()}")
            return

        # Grab market metadata
        mids = list({p["market_id"] for p in positions})
        cur.execute(
            "SELECT market_id, question, tags FROM markets WHERE market_id = ANY(%s)",
            (mids,),
        )
        meta = {r["market_id"]: r for r in (cur.fetchall() or [])}

        print(f"now_utc={now_ts.isoformat()} strategy={args.strategy} showing={len(positions)} since={since_ts.isoformat()}")
        print(f"filters: min_px={MIN_PRICE} max_px={MAX_PRICE} dislo_thr={DISLO_THR} max_dislo={MAX_DISLO} stale_secs={MR_PRICE_STALE_SECS}")
        print(f"tags_excluded={len(EXCLUDED_TAGS)}")
        print("")

        for i, p in enumerate(positions, 1):
            mid = p["market_id"]
            outcome = str(p["outcome"]).strip()
            entry_ts = p["entry_ts"]
            entry_px = Decimal(str(p["entry_price"]))
            size = Decimal(str(p["size"]))

            m = meta.get(mid, {}) or {}
            q = (m.get("question") or "").strip()
            tags_set = norm_tags(m.get("tags"))
            tags_str = ",".join(sorted(tags_set)) if tags_set else ""

            # --- Reconstruct key gates at entry_ts ---
            # 24h volume ending at entry_ts
            cur.execute(
                """
                SELECT SUM(COALESCE(value_usd, price*qty, 0)) AS vol_24h
                FROM raw_trades
                WHERE market_id = %s
                  AND ts >= %s - interval '24 hours'
                  AND ts < %s
                """,
                (mid, entry_ts, entry_ts),
            )
            vol_24h = Decimal(str((cur.fetchone() or {}).get("vol_24h") or 0))

            # Last trade age at entry_ts for that outcome (stale gate)
            cur.execute(
                """
                SELECT ts, price
                FROM raw_trades
                WHERE market_id=%s AND outcome=%s AND ts < %s
                ORDER BY ts DESC
                LIMIT 1
                """,
                (mid, outcome, entry_ts),
            )
            last = cur.fetchone() or {}
            last_ts = last.get("ts")
            last_px = Decimal(str(last.get("price"))) if last.get("price") is not None else None
            px_age_s = int((entry_ts - last_ts).total_seconds()) if last_ts else None
            is_stale = (px_age_s is None) or (px_age_s > MR_PRICE_STALE_SECS)

            # Avg 18h ending at entry_ts, and dislocation vs last_px
            cur.execute(
                """
                SELECT AVG(price) AS avg_price
                FROM raw_trades
                WHERE market_id=%s AND outcome=%s
                  AND ts >= %s - interval '18 hours'
                  AND ts < %s
                """,
                (mid, outcome, entry_ts, entry_ts),
            )
            avg18 = (cur.fetchone() or {}).get("avg_price")
            avg18 = Decimal(str(avg18)) if avg18 is not None else None

            dislo = None
            if (last_px is not None) and (avg18 is not None) and (avg18 > 0):
                dislo = (last_px - avg18) / avg18

            # Tag exclusion
            blocked_tags = bool(tags_set & EXCLUDED_TAGS)

            # Price bounds (using last_px proxy at entry time)
            blocked_px = (last_px is None) or (last_px < MIN_PRICE) or (last_px > MAX_PRICE)

            # Dislocation gate
            blocked_dislo = False
            blocked_dislo_reason = ""
            if dislo is None:
                blocked_dislo = True
                blocked_dislo_reason = "avg_missing"
            else:
                if dislo >= 0:
                    blocked_dislo = True
                    blocked_dislo_reason = "not_negative"
                elif abs(dislo) < DISLO_THR:
                    blocked_dislo = True
                    blocked_dislo_reason = "too_small"
                elif abs(dislo) > MAX_DISLO:
                    blocked_dislo = True
                    blocked_dislo_reason = "too_big"

            # Compose “why it entered” record
            reasons = []
            if blocked_tags: reasons.append("blocked_tags")
            if is_stale: reasons.append("blocked_stale")
            if blocked_px: reasons.append("blocked_px_bounds")
            if blocked_dislo: reasons.append(f"blocked_dislo_{blocked_dislo_reason}")

            entered_ok = (len(reasons) == 0)

            status = p["status"]
            pnl = p["pnl"]
            pnl_s = f"{Decimal(str(pnl)):.2f}" if pnl is not None else ""
            exit_reason = p.get("exit_reason") or ""

            dislo_pct = f"{(float(dislo)*100):.1f}%" if dislo is not None else "NA"
            last_px_s = f"{last_px:.4f}" if last_px is not None else "NA"
            avg18_s = f"{avg18:.4f}" if avg18 is not None else "NA"
            px_age_s_s = str(px_age_s) if px_age_s is not None else "NA"

            print(
                f"{i:03d} {mid[:16]} outcome={outcome} entry_ts={entry_ts.isoformat()} "
                f"entered_ok={str(entered_ok).lower()} reasons={','.join(reasons) if reasons else 'none'} "
                f"vol24h=${int(vol_24h)} last_px={last_px_s} avg18h={avg18_s} dislo={dislo_pct} px_age_s={px_age_s_s} "
                f"status={status} pnl={pnl_s} exit={exit_reason} "
                f"tags={tags_str} q={q[:120]}"
            )

if __name__ == "__main__":
    main()