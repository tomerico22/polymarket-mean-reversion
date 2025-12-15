#!/usr/bin/env python3
import os
from decimal import Decimal
from datetime import datetime, timezone

from psycopg import connect
from psycopg.rows import dict_row

# -------------------------
# Helpers
# -------------------------
def to_dec(x, default=None):
    try:
        if x is None:
            return default
        return Decimal(str(x))
    except Exception:
        return default

def norm_mid(mid: str) -> str:
    return str(mid).strip()

def norm_text(s: str) -> str:
    return " ".join(str(s or "").lower().strip().split())

def load_env_keywords():
    env_csv = os.getenv("MR_EXCLUDE_KEYWORDS", "") or ""
    kws = [norm_text(x) for x in env_csv.split(",") if norm_text(x)]
    seen, out = set(), []
    for k in kws:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out

def question_block_reason(question: str, kws):
    q = norm_text(question)
    if not q and os.getenv("MR_REQUIRE_QUESTION", "0").strip() == "1":
        return "no_question"
    for kw in kws:
        if kw and kw in q:
            return f"kw:{kw}"
    return None

def jsonb_tags_to_set(tags):
    if tags is None:
        return set()
    if isinstance(tags, list):
        return {str(t).lower().strip() for t in tags if str(t).strip()}
    if isinstance(tags, dict):
        if "tags" in tags and isinstance(tags["tags"], list):
            return {str(t).lower().strip() for t in tags["tags"] if str(t).strip()}
        return {str(k).lower().strip() for k in tags.keys() if str(k).strip()}
    return set()

# -------------------------
# Config (match executor)
# -------------------------
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set (load .env first)")

TOP_MARKETS = int(os.getenv("MR_TOP_MARKETS", "500"))
MIN_VOLUME_24H = to_dec(os.getenv("MR_MIN_VOLUME_24H", "10000"), Decimal("10000"))

MIN_PRICE = to_dec(os.getenv("MR_MIN_PRICE", "0.05"), Decimal("0.05"))
MAX_PRICE = to_dec(os.getenv("MR_MAX_PRICE", "0.95"), Decimal("0.95"))
DISLOCATION_THRESHOLD = to_dec(os.getenv("MR_DISLOCATION_THRESHOLD", "0.20"), Decimal("0.20"))
MAX_DISLOCATION = to_dec(os.getenv("MR_MAX_DISLOCATION", "0.45"), Decimal("0.45"))

MR_PRICE_STALE_SECS = int(os.getenv("MR_PRICE_STALE_SECS", "3600"))
now_ts = datetime.now(timezone.utc)

_tags_env = os.getenv("MR_EXCLUDED_TAGS") or os.getenv("MR_EXCLUDE_TAGS") or ""
EXCLUDED_TAGS = {t.strip().lower() for t in _tags_env.split(",") if t.strip()}

# keywords (env-only, by design - matches your "original kws list")
KWS = load_env_keywords()

# how many to show
N = int(os.getenv("WHY_N", "100"))

def main():
    with connect(DB_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # 1) top markets by 24h volume
            cur.execute("""
                SELECT
                    rt.market_id,
                    SUM(COALESCE(rt.value_usd, rt.price * rt.qty, 0)) AS volume_24h
                FROM raw_trades rt
                WHERE rt.ts >= %s - INTERVAL '24 hours'
                  AND rt.ts < %s
                GROUP BY rt.market_id
                HAVING SUM(COALESCE(rt.value_usd, rt.price * rt.qty, 0)) >= %s
                ORDER BY volume_24h DESC
                LIMIT %s
            """, (now_ts, now_ts, MIN_VOLUME_24H, TOP_MARKETS))
            top = cur.fetchall() or []
            mids = [norm_mid(r["market_id"]) for r in top][:N]
            vol_map = {norm_mid(r["market_id"]): to_dec(r["volume_24h"], Decimal("0")) for r in top}

            if not mids:
                print("No markets found for the current TOP_MARKETS/MIN_VOLUME_24H filters.")
                return

            # 2) pull questions + tags
            cur.execute("""
                SELECT market_id, question, tags
                FROM markets
                WHERE market_id = ANY(%s)
            """, (mids,))
            meta_rows = cur.fetchall() or []
            meta = {norm_mid(r["market_id"]): r for r in meta_rows}

            # 3) risk state (banned / cooldown)
            cur.execute("""
                SELECT market_id, banned, banned_until, banned_reason
                FROM mr_market_risk_state
                WHERE market_id = ANY(%s)
            """, (mids,))
            rs_rows = cur.fetchall() or []
            risk = {norm_mid(r["market_id"]): r for r in rs_rows}

            # 4) last price per (market,outcome) from raw_trades (since market_ticks has no outcome)
            cur.execute("""
                SELECT DISTINCT ON (market_id, outcome)
                    market_id, outcome, price, ts
                FROM raw_trades
                WHERE market_id = ANY(%s)
                ORDER BY market_id, outcome, ts DESC
            """, (mids,))
            ticks = cur.fetchall() or []
            last_px = {}
            for r in ticks:
                last_px[(norm_mid(r["market_id"]), str(r["outcome"]).strip())] = (to_dec(r["price"], None), r["ts"])

            # 5) 18h avg per (market,outcome)
            cur.execute("""
                SELECT market_id, outcome, AVG(price) AS avg_price
                FROM raw_trades
                WHERE market_id = ANY(%s)
                  AND ts >= %s - INTERVAL '18 hours'
                  AND ts < %s
                GROUP BY market_id, outcome
            """, (mids, now_ts, now_ts))
            avgs = cur.fetchall() or []
            avg_map = {(norm_mid(r["market_id"]), str(r["outcome"]).strip()): to_dec(r["avg_price"], None) for r in avgs}

            print(f"now_utc={now_ts.isoformat()} showing={len(mids)} top_markets={TOP_MARKETS} min_vol_24h={MIN_VOLUME_24H}")
            print(f"filters: min_px={MIN_PRICE} max_px={MAX_PRICE} dislo_thr={DISLOCATION_THRESHOLD} max_dislo={MAX_DISLOCATION} stale_secs={MR_PRICE_STALE_SECS}")
            print(f"kws(env)={len(KWS)} tags_excluded={len(EXCLUDED_TAGS)}")
            print("")

            for i, mid in enumerate(mids, 1):
                m = meta.get(mid, {}) or {}
                q = (m.get("question") or "").strip()
                tags_set = jsonb_tags_to_set(m.get("tags"))
                vol = vol_map.get(mid, Decimal("0"))

                # 1) banned
                r = risk.get(mid)
                if r and r.get("banned"):
                    if r.get("banned_until"):
                        reason = f"banned_temp:{r.get('banned_reason') or 'unknown'}"
                    else:
                        reason = f"banned_perm:{r.get('banned_reason') or 'unknown'}"
                    print(f"{i:03d} {mid[:16]} vol=${vol:.0f} status=no reason={reason} q={q[:90]}")
                    continue

                # 2) tags
                hit_tag = next((t for t in tags_set if t in EXCLUDED_TAGS), None)
                if hit_tag:
                    print(f"{i:03d} {mid[:16]} vol=${vol:.0f} status=no reason=tag:{hit_tag} q={q[:90]}")
                    continue

                # 3) keywords (env list)
                kw_reason = question_block_reason(q, KWS)
                if kw_reason:
                    print(f"{i:03d} {mid[:16]} vol=${vol:.0f} status=no reason={kw_reason} q={q[:90]}")
                    continue

                # 4) price/dislocation (enter if ANY outcome passes)
                best = None
                fail_hint = "no_signal_or_stale_or_price"

                for outcome in ("0", "1"):
                    cached = last_px.get((mid, outcome))
                    if not cached:
                        fail_hint = "no_last_trade"
                        continue
                    px, ts = cached
                    if px is None or ts is None:
                        fail_hint = "bad_last_trade"
                        continue

                    # stale?
                    age = (now_ts - ts).total_seconds()
                    if age > MR_PRICE_STALE_SECS:
                        fail_hint = f"stale_last_trade_age={int(age)}s"
                        continue

                    # price guard
                    if px < MIN_PRICE:
                        fail_hint = "price_below_min"
                        continue
                    if px > MAX_PRICE:
                        fail_hint = "price_above_max"
                        continue

                    avg = avg_map.get((mid, outcome))
                    if not avg or avg <= 0:
                        fail_hint = "no_avg_18h"
                        continue

                    dislo = (px - avg) / avg
                    if dislo >= 0:
                        fail_hint = "not_below_avg"
                        continue
                    if abs(dislo) < DISLOCATION_THRESHOLD:
                        fail_hint = "dislo_too_small"
                        continue
                    if abs(dislo) > MAX_DISLOCATION:
                        fail_hint = "dislo_too_big"
                        continue

                    best = (outcome, px, avg, dislo, ts)
                    break

                if best:
                    outcome, px, avg, dislo, ts = best
                    print(f"{i:03d} {mid[:16]} vol=${vol:.0f} status=enter reason=ok outcome={outcome} px={px:.4f} avg18h={avg:.4f} dislo={dislo*100:.1f}% last_ts={ts.isoformat()} q={q[:70]}")
                else:
                    print(f"{i:03d} {mid[:16]} vol=${vol:.0f} status=no reason={fail_hint} q={q[:90]}")

if __name__ == "__main__":
    main()
