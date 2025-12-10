import os
from decimal import Decimal

from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")


def get_conn():
    return connect(DB_URL, row_factory=dict_row)


def fmt_money(x):
    if x is None:
        return "0.00"
    return f"{Decimal(str(x)):.2f}"


def fmt_pct(x):
    if x is None:
        return "0.0%"
    return f"{float(x) * 100:.1f}%"


def main():
    print("[MR_V1_ANALYTICS] Connecting to DB...")
    with get_conn() as conn, conn.cursor() as cur:
        # Overall summary
        cur.execute(
            """
            SELECT 
              COUNT(*) FILTER (WHERE status='closed') AS closed_trades,
              COUNT(*) FILTER (WHERE status='open')   AS open_trades,
              COUNT(*) FILTER (WHERE status='closed' AND pnl > 0) AS winners,
              SUM(pnl) FILTER (WHERE status='closed') AS total_pnl
            FROM mr_positions
            WHERE strategy = 'mean_reversion_v1';
            """
        )
        s = cur.fetchone() or {}
        closed = s.get("closed_trades") or 0
        winners = s.get("winners") or 0
        winrate = (winners / closed) if closed else 0.0

        print("\n=== MR v1 Overall ===")
        print(f"Closed trades : {closed}")
        print(f"Open trades   : {s.get('open_trades') or 0}")
        print(f"Winrate       : {fmt_pct(winrate)}")
        print(f"Total PnL     : {fmt_money(s.get('total_pnl'))}")

        # Dislocation buckets
        cur.execute("SELECT * FROM mr_v1_dislocation_buckets;")
        rows = cur.fetchall()
        print("\n=== Dislocation Buckets (-50% to -20%) ===")
        print("bucket |  min   |  max   | trades | avg_pnl | sum_pnl | winrate")
        for r in rows:
            print(
                f"{r['bucket']:>6} | "
                f"{r['bucket_min']:.3f} | "
                f"{r['bucket_max']:.3f} | "
                f"{r['trades']:>6} | "
                f"{fmt_money(r['avg_pnl']):>7} | "
                f"{fmt_money(r['sum_pnl']):>7} | "
                f"{fmt_pct(r['winrate']):>7}"
            )

        # Market class PnL
        cur.execute("SELECT * FROM mr_v1_class_pnl;")
        rows = cur.fetchall()
        print("\n=== PnL by market_class ===")
        print("class         | trades | avg_dislo | avg_pnl | sum_pnl | winrate")
        for r in rows:
            cls = (r["market_class"] or "").ljust(12)[:12]
            print(
                f"{cls:12} | "
                f"{r['trades']:>6} | "
                f"{r['avg_dislocation']:.3f} | "
                f"{fmt_money(r['avg_pnl']):>7} | "
                f"{fmt_money(r['sum_pnl']):>7} | "
                f"{fmt_pct(r['winrate']):>7}"
            )

        # Worst markets
        cur.execute(
            """
            SELECT *
            FROM mr_v1_market_summary
            ORDER BY sum_pnl ASC
            LIMIT 10;
            """
        )
        rows = cur.fetchall()
        print("\n=== Worst Markets (by PnL) ===")
        for r in rows:
            q = (r["market_name"] or "")[:60]
            print(
                f"{r['market_id'][:10]}… | cls={r['market_class']} | "
                f"trades={r['trades']} | sum_pnl={fmt_money(r['sum_pnl'])} | "
                f"avg_dislo={r['avg_dislocation']:.3f} | winrate={fmt_pct(r['winrate'])} | {q}"
            )

        # Shadow summary
        cur.execute("SELECT * FROM mr_v1_shadow_stats;")
        srow = cur.fetchone() or {}
        print("\n=== Shadow Stats ===")
        print(f"Shadow fills    : {srow.get('n') or 0}")
        print(f"Avg dislocation : {srow.get('avg_dislocation') or 0:.3f}")
        print(f"Avg |dislo|     : {srow.get('avg_abs_dislocation') or 0:.3f}")
        print(f"Avg slip (abs)  : {fmt_money(srow.get('avg_slip_abs'))}")
        print(f"Avg slip (%)    : {fmt_pct((srow.get('avg_slip_pct') or 0) / 100)}")


if __name__ == "__main__":
    main()
