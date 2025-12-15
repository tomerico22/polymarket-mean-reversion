#!/usr/bin/env python3
import os
import argparse
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from psycopg import connect
from psycopg.rows import dict_row

# -----------------------
# Helpers
# -----------------------

def to_dec(val, default=None):
    try:
        if val is None:
            return default
        return Decimal(str(val))
    except (InvalidOperation, TypeError):
        return default

def norm_market_id(market_id: str) -> str:
    if market_id is None:
        raise ValueError("market_id cannot be None")
    mid = str(market_id).strip()
    if not mid:
        raise ValueError("market_id cannot be empty")
    return mid

def now_utc():
    return datetime.now(timezone.utc)

def kw_norm(s: str) -> str:
    return " ".join(str(s).lower().strip().split())

def load_keyword_blacklist(path: Path):
    env_csv = os.getenv("MR_EXCLUDE_KEYWORDS", "") or ""
    env_list = [kw_norm(x) for x in env_csv.split(",") if kw_norm(x)]

    file_list = []
    try:
        lines = path.read_text().splitlines()
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            file_list.append(kw_norm(line))
    except Exception:
        file_list = []

    # unique + stable ordering
    return sorted(set([x for x in (file_list + env_list) if x]))

def first_blocking_keyword(question: str, blacklist, require_question: bool) -> str:
    q = kw_norm(question or "")
    if require_question and not q:
        return "__missing_question__"
    for kw in blacklist:
        if kw and kw in q:
            return kw
    return ""

def parse_tags(tags_val):
    if not tags_val:
        return set()
    if isinstance(tags_val, list):
        return {str(t).strip().lower() for t in tags_val if str(t).strip()}
    if isinstance(tags_val, str):
        # allow either "a,b,c" or "{a,b}" style strings
        raw = tags_val.strip().strip("{}")
        parts = [p.strip() for p in raw.split(",")]
        return {p.lower() for p in parts if p}
    return set()

def is_price_stale(px_ts, now_ts, stale_secs: int) -> bool:
    return (now_ts - px_ts).total_seconds() > stale_secs

# -----------------------
# Main
# -----------------------

def main():
    ap = argparse.ArgumentParser(description="Show what would be entered now (why_enter)")
    ap.add_argument("--show", type=int, default=int(os.getenv("WHY_ENTER_SHOW", "80")))
    ap.add_argument("--top-markets", type=int, default=int(os.getenv("MR_TOP_MARKETS", "500")))
    ap.add_argument("--min-vol-24h", type=Decimal, default=to_dec(os.getenv("MR_MIN_VOLUME_24H", "5000"), Decimal("5000")))
    ap.add_argument("--stale-secs", type=int, default=int(os.getenv("MR_PRICE_STALE_SECS", "300")))
    args = ap.parse_args()

    db_url = os.getenv("DB_URL")
    if not db_url:
        raise SystemExit("DB_URL not set")

    # pull same env knobs as executor
    STRATEGY = os.getenv("MR_STRATEGY", "mean_reversion_v1")

    MIN_PRICE = to_dec(os.getenv("MR_MIN_PRICE", "0.05"), Decimal("0.05"))
    MAX_PRICE = to_dec(os.getenv("MR_MAX_PRICE", "0.95"), Decimal("0.95"))

    DISLOCATION_THRESHOLD = to_dec(os.getenv("MR_DISLOCATION_THRESHOLD", "0.20"), Decimal("0.20"))
    MAX_DISLOCATION = to_dec(os.getenv("MR_MAX_DISLOCATION", "0.45"), Decimal("0.45"))

    MAX_OPEN_POSITIONS = int(os.getenv("MR_MAX_OPEN_POSITIONS", "10"))
    MAX_POSITIONS_PER_MARKET = int(os.getenv("MR_MAX_POSITIONS_PER_MARKET", "1"))

    # tag filters
    tags_env = os.getenv("MR_EXCLUDED_TAGS") or os.getenv("MR_EXCLUDE_TAGS") or "sports,nfl,nba,soccer,mlb,hockey"
    EXCLUDED_TAGS = {t.strip().lower() for t in tags_env.split(",") if t.strip()}

    # keyword blacklist
    kw_path = Path(os.getenv("MR_KEYWORD_BLACKLIST_PATH", "/root/polymarket-mean-reversion/mr/config/keyword_blacklist.txt"))
    require_question = (os.getenv("MR_REQUIRE_QUESTION", "0").strip() == "1")
    kw_blacklist = load_keyword_blacklist(kw_path)

    now_ts = now_utc()

    conn = connect(db_url, row_factory=dict_row)
    conn.autocommit = True
    with conn.cursor() as cur:
        # global open cap
        cur.execute("""
            SELECT COUNT(*) AS c
            FROM mr_positions
            WHERE strategy=%s AND status='open'
        """, (STRATEGY,))
        global_open = int((cur.fetchone() or {}).get("c") or 0)

        # per market/outcome open counts
        cur.execute("""
            SELECT market_id, outcome, COUNT(*) AS c
            FROM mr_positions
            WHERE strategy=%s AND status='open'
            GROUP BY market_id, outcome
        """, (STRATEGY,))
        open_counts = {}
        for r in (cur.fetchall() or []):
            open_counts[(norm_market_id(r["market_id"]), str(r["outcome"]).strip())] = int(r["c"] or 0)

        # top markets by 24h volume (robust to value_usd vs price*qty)
        cur.execute("""
            WITH per_outcome AS (
                SELECT
                    rt.market_id,
                    rt.outcome,
                    SUM(COALESCE(rt.value_usd, rt.price * rt.qty, 0)) AS vol_24h,
                    MAX(rt.ts) AS last_ts
                FROM raw_trades rt
                WHERE rt.ts >= %s - INTERVAL '24 hours'
                  AND rt.ts < %s
                  AND rt.outcome IN ('0','1')
                GROUP BY rt.market_id, rt.outcome
            ),
            per_market AS (
                SELECT
                    market_id,
                    SUM(vol_24h) AS volume_24h,
                    MIN(last_ts) AS min_last_ts,
                    COUNT(DISTINCT outcome) AS outcomes_seen
                FROM per_outcome
                GROUP BY market_id
            )
            SELECT
                market_id,
                volume_24h,
                min_last_ts AS last_trade_ts
            FROM per_market
            WHERE volume_24h >= %s
              AND outcomes_seen = 2
            ORDER BY volume_24h DESC
            LIMIT %s
        """, (now_ts, now_ts, args.min_vol_24h, args.top_markets))

        top_rows = cur.fetchall() or []
        market_ids = [norm_market_id(r["market_id"]) for r in top_rows]
        vol_map = {norm_market_id(r["market_id"]): to_dec(r["volume_24h"], Decimal("0")) for r in top_rows}

        # pull metadata from markets table
        meta = {}
        if market_ids:
            try:
                cur.execute("""
                    SELECT market_id, question, tags
                    FROM markets
                    WHERE market_id = ANY(%s)
                """, (market_ids,))
                for r in (cur.fetchall() or []):
                    mid = norm_market_id(r["market_id"])
                    meta[mid] = {
                        "question": r.get("question") or "",
                        "tags_raw": r.get("tags"),
                        "tags": parse_tags(r.get("tags")),
                    }
            except Exception:
                # fail open
                for mid in market_ids:
                    meta[mid] = {"question": "", "tags_raw": None, "tags": set()}

        # pull ban state
        ban = {}
        if market_ids:
            cur.execute("""
                SELECT market_id, banned, banned_until, banned_reason
                FROM mr_market_risk_state
                WHERE strategy=%s AND market_id = ANY(%s)
            """, (STRATEGY, market_ids))
            for r in (cur.fetchall() or []):
                ban[norm_market_id(r["market_id"])] = {
                    "banned": bool(r.get("banned")),
                    "banned_until": r.get("banned_until"),
                    "banned_reason": r.get("banned_reason") or "",
                }

        # latest price snapshot per (market,outcome)
        # also compute avg(18h) per (market,outcome)
        # we do this in 2 queries for clarity
        latest_px = {}
        if market_ids:
            cur.execute("""
                SELECT DISTINCT ON (market_id, outcome)
                    market_id, outcome, price, ts
                FROM raw_trades
                WHERE market_id = ANY(%s)
                  AND outcome IN ('0','1')
                ORDER BY market_id, outcome, ts DESC
            """, (market_ids,))
            for r in (cur.fetchall() or []):
                latest_px[(norm_market_id(r["market_id"]), str(r["outcome"]).strip())] = {
                    "px": to_dec(r.get("price"), None),
                    "ts": r.get("ts"),
                }

        avg_18h = {}
        if market_ids:
            cur.execute("""
                SELECT market_id, outcome, AVG(price) AS avg_price
                FROM raw_trades
                WHERE market_id = ANY(%s)
                  AND outcome IN ('0','1')
                  AND ts >= %s - INTERVAL '18 hours'
                  AND ts < %s
                GROUP BY market_id, outcome
            """, (market_ids, now_ts, now_ts))
            for r in (cur.fetchall() or []):
                avg_18h[(norm_market_id(r["market_id"]), str(r["outcome"]).strip())] = to_dec(r.get("avg_price"), None)

        # evaluate entries
        rows = []
        counters = {
            "markets_seen": 0,
            "outcomes_seen": 0,
            "eligible_pairs": 0,
            "blocked_by_global_cap": 0,
            "blocked_by_pair_cap": 0,
            "blocked_by_tags": 0,
            "blocked_by_kw": 0,
            "blocked_by_ban": 0,
            "blocked_by_stale": 0,
            "blocked_by_px_bounds": 0,
            "blocked_by_avg_missing": 0,
            "blocked_by_dislo_sign": 0,
            "blocked_by_dislo_small": 0,
            "blocked_by_dislo_big": 0,
        }

        counters["markets_seen"] = len(market_ids)

        for mid in market_ids:
            tags = meta.get(mid, {}).get("tags", set())
            question = meta.get(mid, {}).get("question", "")
            kw = first_blocking_keyword(question, kw_blacklist, require_question)

            banned_state = ban.get(mid, {"banned": False, "banned_until": None, "banned_reason": ""})
            is_banned = bool(banned_state.get("banned"))
            banned_until = banned_state.get("banned_until")
            banned_reason = banned_state.get("banned_reason", "")

            # tag exclusion is market-level
            if EXCLUDED_TAGS and (tags & EXCLUDED_TAGS):
                # don’t count as “entered now” for either outcome
                counters["blocked_by_tags"] += 2
                continue

            # keyword exclusion is market-level
            if kw:
                counters["blocked_by_kw"] += 2
                continue

            # ban exclusion is market-level (unless temp ban expired - tool is "now" view)
            if is_banned:
                # if temp ban has expired, treat as not banned
                if banned_until and now_ts >= banned_until:
                    is_banned = False
                else:
                    counters["blocked_by_ban"] += 2
                    continue

            for outcome in ("0", "1"):
                counters["outcomes_seen"] += 1

                pair_open = open_counts.get((mid, outcome), 0)
                pair_cap_hit = (pair_open >= MAX_POSITIONS_PER_MARKET)
                global_cap_hit = (global_open >= MAX_OPEN_POSITIONS)

                pxr = latest_px.get((mid, outcome))
                if not pxr or pxr.get("px") is None or pxr.get("ts") is None:
                    counters["blocked_by_stale"] += 1
                    continue
                px = pxr["px"]
                px_ts = pxr["ts"]

                if is_price_stale(px_ts, now_ts, args.stale_secs):
                    counters["blocked_by_stale"] += 1
                    continue

                if px < MIN_PRICE or px > MAX_PRICE:
                    counters["blocked_by_px_bounds"] += 1
                    continue

                avg = avg_18h.get((mid, outcome))
                if avg is None or avg <= 0:
                    counters["blocked_by_avg_missing"] += 1
                    continue

                dislo = (px - avg) / avg

                # longs-only: need negative dislocation
                if dislo >= 0:
                    counters["blocked_by_dislo_sign"] += 1
                    continue
                if abs(dislo) < DISLOCATION_THRESHOLD:
                    counters["blocked_by_dislo_small"] += 1
                    continue
                if abs(dislo) > MAX_DISLOCATION:
                    counters["blocked_by_dislo_big"] += 1
                    continue

                # at this point, it "wants" to enter. now check caps
                would_enter = (not global_cap_hit) and (not pair_cap_hit)

                if global_cap_hit:
                    counters["blocked_by_global_cap"] += 1
                if pair_cap_hit:
                    counters["blocked_by_pair_cap"] += 1

                if would_enter:
                    counters["eligible_pairs"] += 1

                rows.append({
                    "market_id": mid,
                    "outcome": outcome,
                    "vol_24h": vol_map.get(mid, Decimal("0")),
                    "tags": ",".join(sorted(tags)) if tags else "",
                    "banned": "0",
                    "banned_reason": "",
                    "banned_until": "",
                    "px": px,
                    "px_age_s": int((now_ts - px_ts).total_seconds()),
                    "avg_18h": avg,
                    "dislo_pct": dislo * Decimal("100"),
                    "pair_open": pair_open,
                    "pair_cap": MAX_POSITIONS_PER_MARKET,
                    "global_open": global_open,
                    "global_cap": MAX_OPEN_POSITIONS,
                    "status": "YES" if would_enter else "WOULD_ENTER_BUT_CAPPED",
                    "question": question[:120].replace("\n", " "),
                })

        # sort: real entries first, then would_enter_but_capped, then by volume desc
        def sort_key(r):
            return (
                0 if r["status"] == "YES" else 1,
                -float(r["vol_24h"] or 0),
                r["market_id"],
                r["outcome"],
            )
        rows.sort(key=sort_key)

        print(
            f"now_utc={now_ts.isoformat()} showing={args.show} top_markets={args.top_markets} min_vol_24h={args.min_vol_24h}\n"
            f"filters: min_px={MIN_PRICE} max_px={MAX_PRICE} dislo_thr={DISLOCATION_THRESHOLD} max_dislo={MAX_DISLOCATION} stale_secs={args.stale_secs}\n"
            f"caps: global_open={global_open}/{MAX_OPEN_POSITIONS} per_market_outcome={MAX_POSITIONS_PER_MARKET}\n"
            f"kws(env+file)={len(kw_blacklist)} tags_excluded={len(EXCLUDED_TAGS)}\n"
        )

        # summary counters (pairs-level)
        print(
            "SUMMARY "
            f"markets_seen={counters['markets_seen']} outcomes_seen={counters['outcomes_seen']} "
            f"eligible_pairs={counters['eligible_pairs']} "
            f"blocked_global_cap={counters['blocked_by_global_cap']} "
            f"blocked_pair_cap={counters['blocked_by_pair_cap']} "
            f"blocked_tags={counters['blocked_by_tags']} "
            f"blocked_kw={counters['blocked_by_kw']} "
            f"blocked_ban={counters['blocked_by_ban']} "
            f"blocked_stale={counters['blocked_by_stale']} "
            f"blocked_px_bounds={counters['blocked_by_px_bounds']} "
            f"blocked_avg_missing={counters['blocked_by_avg_missing']} "
            f"blocked_dislo_sign={counters['blocked_by_dislo_sign']} "
            f"blocked_dislo_small={counters['blocked_by_dislo_small']} "
            f"blocked_dislo_big={counters['blocked_by_dislo_big']}"
        )

        # print rows
        for i, r in enumerate(rows[: args.show], start=1):
            vol = r["vol_24h"]
            print(
                f"{i:03d} {r['market_id'][:16]} outcome={r['outcome']} "
                f"status={r['status']} vol=${int(vol):d} "
                f"px={float(r['px']):.4f} avg18h={float(r['avg_18h']):.4f} dislo={float(r['dislo_pct']):.1f}% "
                f"px_age_s={r['px_age_s']} "
                f"pair_open={r['pair_open']}/{r['pair_cap']} global_open={r['global_open']}/{r['global_cap']} "
                f"tags={r['tags'][:60]} "
                f"q={r['question']}"
            )

if __name__ == "__main__":
    main()