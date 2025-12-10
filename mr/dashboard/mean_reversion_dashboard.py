import os
from datetime import datetime, timezone
from decimal import Decimal

from flask import Flask, render_template_string, request
from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

app = Flask(__name__)

REFRESH_SECS = 30

# available strategy filters
STRATEGIES = [
    "mean_reversion_v1",
    "mean_reversion_v3_32",
    "mean_reversion_strict_v1",
    "mean_reversion_v2",
    "all",
]

HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Mean Reversion Dashboard</title>
  <meta http-equiv="refresh" content="{{ refresh_secs }}">
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; background:#111; color:#eee; }
    h1, h2 { margin-bottom: 0.3rem; }
    h1 { font-size: 1.6rem; }
    h2 { font-size: 1.2rem; margin-top: 1.5rem; }
    .summary { display:flex; gap:1rem; flex-wrap:wrap; margin-bottom:1rem; }
    .card { background:#1d1d1d; padding:0.8rem 1rem; border-radius:8px; min-width:10rem; }
    .card-label { font-size:0.75rem; text-transform:uppercase; color:#aaa; margin-bottom:0.25rem; }
    .card-value { font-size:1.1rem; font-weight:600; }
    table { border-collapse:collapse; width:100%; margin-top:0.5rem; font-size:0.8rem; }
    th, td { border:1px solid #333; padding:0.3rem 0.5rem; text-align:left; }
    th { background:#222; position:sticky; top:0; }
    tr:nth-child(even) { background:#181818; }
    tr:nth-child(odd) { background:#151515; }
    .pnl-pos { color:#32cd32; }
    .pnl-neg { color:#ff4d4d; }
    .side-long { color:#32cd32; font-weight:600; }
    .small { font-size:0.7rem; color:#aaa; }
    .toolbar { margin:0.5rem 0 1rem 0; }
    .toolbar label { margin-right:0.5rem; }
  </style>
  <script>
    const refreshSecs = {{ refresh_secs }};
    let remaining = refreshSecs;
    function tick() {
      const el = document.getElementById("refresh_left");
      if (el) {
        el.textContent = remaining;
      }
      remaining = remaining > 0 ? remaining - 1 : 0;
    }
    document.addEventListener("DOMContentLoaded", () => {
      tick();
      setInterval(tick, 1000);
    });
  </script>
</head>
<body>
  <h1>Mean Reversion Dashboard</h1>
  <div class="small">
    DB: {{ db_url_short }} • Updated at {{ now_utc }} • Strategy: <strong>{{ strategy }}</strong> • 
    Refresh every {{ refresh_secs }}s (in <span id="refresh_left">{{ refresh_secs }}</span>s)
  </div>

  <div class="toolbar">
    <form method="get">
      <label for="strategy" class="small">Strategy:</label>
      <select id="strategy" name="strategy" onchange="this.form.submit()">
        {% for s in strategies %}
          <option value="{{ s }}" {% if s == strategy %}selected{% endif %}>{{ s }}</option>
        {% endfor %}
      </select>
    </form>
  </div>

  <div class="summary">
    <div class="card">
      <div class="card-label">Closed PnL</div>
      <div class="card-value {% if summary.total_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
        {{ "%.2f"|format(summary.total_pnl) }}
      </div>
    </div>
    <div class="card">
      <div class="card-label">Trades (closed)</div>
      <div class="card-value">{{ summary.closed_trades }}</div>
    </div>
    <div class="card">
      <div class="card-label">Trades (open)</div>
      <div class="card-value">{{ summary.open_trades }}</div>
    </div>
    <div class="card">
      <div class="card-label">Winrate</div>
      <div class="card-value">
        {{ "%.1f"|format(summary.winrate * 100 if summary.winrate is not none else 0) }}%
      </div>
    </div>
    <div class="card">
      <div class="card-label">Avg PnL / closed</div>
      <div class="card-value">{{ "%.2f"|format(summary.avg_pnl if summary.avg_pnl is not none else 0) }}</div>
    </div>
    <div class="card">
      <div class="card-label">Today PnL</div>
      <div class="card-value {% if summary.today_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
        {{ "%.2f"|format(summary.today_pnl) }}
      </div>
    </div>
  </div>

  <h2>Open Positions</h2>
  {% if open_positions %}
  <table>
    <thead>
      <tr>
        <th>Entry TS</th>
        <th>Market</th>
        <th>Name</th>
        <th>Tags</th>
        <th>Outcome</th>
        <th>Dislo%</th>
        <th>Size</th>
        <th>Entry Px</th>
        <th>Cost</th>
        <th>Last Px</th>
        <th>Px %</th>
        <th>Unrealized</th>
        <th>Hours</th>
      </tr>
    </thead>
    <tbody>
      {% for p in open_positions %}
      <tr>
        <td>{{ p.entry_ts }}</td>
        <td class="small">{{ p.market_id[:16] }}…</td>
        <td class="small">{{ p.market_name or '' }}</td>
        <td class="small">{{ p.market_tags or '' }}</td>
        <td>{{ p.outcome_label }}</td>
        <td>{{ "%.1f"|format(p.dislocation * 100 if p.dislocation is not none else 0) }}</td>
        <td>{{ "%.2f"|format(p.size or 0) }}</td>
        <td>{{ "%.4f"|format(p.entry_price or 0) }}</td>
        <td>{{ "%.2f"|format(p.cost or 0) }}</td>
        <td>{{ "%.4f"|format(p.last_price or 0) }}</td>
        <td class="{% if p.px_change_pct < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.1f"|format(p.px_change_pct or 0) }}%
        </td>
        <td class="{% if p.unrealized_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(p.unrealized_pnl) }}
        </td>
        <td>{{ "%.1f"|format(p.hours_open) }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <div class="small">No open positions.</div>
  {% endif %}

  <h2>Recent Closed Positions (last 50)</h2>
  {% if closed_positions %}
  <table>
    <thead>
      <tr>
        <th>Entry TS</th>
        <th>Exit TS</th>
        <th>Market</th>
        <th>Name</th>
        <th>Tags</th>
        <th>Outcome</th>
        <th>Dislo%</th>
        <th>Size</th>
        <th>Entry Px</th>
        <th>Cost</th>
        <th>Exit Px</th>
        <th>Px %</th>
        <th>PnL</th>
        <th>Exit</th>
        <th>Hours</th>
      </tr>
    </thead>
    <tbody>
      {% for p in closed_positions %}
      <tr>
        <td>{{ p.entry_ts }}</td>
        <td>{{ p.exit_ts }}</td>
        <td class="small">{{ p.market_id[:16] }}…</td>
        <td class="small">{{ p.market_name or '' }}</td>
        <td class="small">{{ p.market_tags or '' }}</td>
        <td>{{ p.outcome_label }}</td>
        <td>{{ "%.1f"|format(p.dislocation * 100 if p.dislocation is not none else 0) }}</td>
        <td>{{ "%.2f"|format(p.size or 0) }}</td>
        <td>{{ "%.4f"|format(p.entry_price or 0) }}</td>
        <td>{{ "%.2f"|format(p.cost or 0) }}</td>
        <td>{{ "%.4f"|format(p.exit_price or 0) }}</td>
        <td class="{% if p.px_change_pct < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.1f"|format(p.px_change_pct or 0) }}%
        </td>
        <td class="{% if p.pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(p.pnl) }}
        </td>
        <td>{{ p.exit_reason }}</td>
        <td>{{ "%.1f"|format(p.hours_held or 0) }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <div class="small">No closed positions yet.</div>
  {% endif %}

  <h2>Exit Reason Breakdown</h2>
  {% if exit_breakdown %}
  <table>
    <thead>
      <tr><th>Reason</th><th>Count</th><th>Avg PnL</th><th>Sum PnL</th></tr>
    </thead>
    <tbody>
      {% for r in exit_breakdown %}
      <tr>
        <td>{{ r.exit_reason }}</td>
        <td>{{ r.count }}</td>
        <td class="{% if r.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.avg_pnl) }}
        </td>
        <td class="{% if r.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.sum_pnl) }}
        </td>
      </tr>
      {% endfor %}
      <tr>
        <th>Total</th>
        <th>{{ exit_totals.count }}</th>
        <th></th>
        <th class="{% if exit_totals.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(exit_totals.sum_pnl) }}
        </th>
      </tr>
    </tbody>
  </table>
  {% else %}
  <div class="small">No exit data.</div>
  {% endif %}

  <h2>Analytics (v1)</h2>

  <h3>Dislocation Buckets (-50% to -20%)</h3>
  {% if analytics.dislocation_buckets %}
  <table>
    <thead>
      <tr>
        <th>Bucket</th>
        <th>Min dislo</th>
        <th>Max dislo</th>
        <th>Trades</th>
        <th>Avg PnL</th>
        <th>Sum PnL</th>
        <th>Winrate</th>
      </tr>
    </thead>
    <tbody>
      {% for r in analytics.dislocation_buckets %}
      <tr>
        <td>{{ r.bucket }}</td>
        <td>{{ "%.3f"|format(r.bucket_min) }}</td>
        <td>{{ "%.3f"|format(r.bucket_max) }}</td>
        <td>{{ r.trades }}</td>
        <td class="{% if r.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.avg_pnl) }}
        </td>
        <td class="{% if r.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.sum_pnl) }}
        </td>
        <td>{{ "%.1f"|format((r.winrate or 0)*100) }}%</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% endif %}

  <h3>PnL by Market Class</h3>
  {% if analytics.class_pnl %}
  <table>
    <thead>
      <tr>
        <th>Class</th>
        <th>Trades</th>
        <th>Avg dislo</th>
        <th>Avg PnL</th>
        <th>Sum PnL</th>
        <th>Winrate</th>
      </tr>
    </thead>
    <tbody>
      {% for r in analytics.class_pnl %}
      <tr>
        <td>{{ r.market_class }}</td>
        <td>{{ r.trades }}</td>
        <td>{{ "%.3f"|format(r.avg_dislocation) }}</td>
        <td class="{% if r.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.avg_pnl) }}
        </td>
        <td class="{% if r.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.sum_pnl) }}
        </td>
        <td>{{ "%.1f"|format((r.winrate or 0)*100) }}%</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% endif %}

  <h3>Worst Markets (by PnL)</h3>
  {% if analytics.worst_markets %}
  <table>
    <thead>
      <tr>
        <th>Market</th>
        <th>Class</th>
        <th>Trades</th>
        <th>Sum PnL</th>
        <th>Avg dislo</th>
        <th>Winrate</th>
        <th>Name</th>
      </tr>
    </thead>
    <tbody>
      {% for r in analytics.worst_markets %}
      <tr>
        <td class="small">{{ r.market_id[:16] }}…</td>
        <td>{{ r.market_class }}</td>
        <td>{{ r.trades }}</td>
        <td class="{% if r.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.sum_pnl) }}
        </td>
        <td>{{ "%.3f"|format(r.avg_dislocation) }}</td>
        <td>{{ "%.1f"|format((r.winrate or 0)*100) }}%</td>
        <td class="small">{{ r.market_name or '' }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% endif %}

  <h3>Best Markets (by PnL)</h3>
  {% if analytics.best_markets %}
  <table>
    <thead>
      <tr>
        <th>Market</th>
        <th>Class</th>
        <th>Trades</th>
        <th>Sum PnL</th>
        <th>Avg dislo</th>
        <th>Winrate</th>
        <th>Name</th>
      </tr>
    </thead>
    <tbody>
      {% for r in analytics.best_markets %}
      <tr>
        <td class="small">{{ r.market_id[:16] }}…</td>
        <td>{{ r.market_class }}</td>
        <td>{{ r.trades }}</td>
        <td class="{% if r.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(r.sum_pnl) }}
        </td>
        <td>{{ "%.3f"|format(r.avg_dislocation) }}</td>
        <td>{{ "%.1f"|format((r.winrate or 0)*100) }}%</td>
        <td class="small">{{ r.market_name or '' }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% endif %}

  <h3>Shadow Stats</h3>
  {% if analytics.shadow_stats %}
  <div class="summary">
    <div class="card">
      <div class="card-label">Shadow fills</div>
      <div class="card-value">{{ analytics.shadow_stats.n or 0 }}</div>
    </div>
    <div class="card">
      <div class="card-label">Avg dislocation</div>
      <div class="card-value">{{ "%.3f"|format(analytics.shadow_stats.avg_dislocation or 0) }}</div>
    </div>
    <div class="card">
      <div class="card-label">Avg |dislo|</div>
      <div class="card-value">{{ "%.3f"|format(analytics.shadow_stats.avg_abs_dislocation or 0) }}</div>
    </div>
    <div class="card">
      <div class="card-label">Avg slip (abs)</div>
      <div class="card-value">{{ "%.3f"|format(analytics.shadow_stats.avg_slip_abs or 0) }}</div>
    </div>
    <div class="card">
      <div class="card-label">Avg slip (%)</div>
      <div class="card-value">{{ "%.1f"|format((analytics.shadow_stats.avg_slip_pct or 0)) }}%</div>
    </div>
  </div>
  {% endif %}

</body>
</html>
"""


def to_dec(x):
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def get_conn():
    return connect(DB_URL, row_factory=dict_row)


def fetch_analytics(cur):
    analytics = {}

    cur.execute("SELECT * FROM mr_v1_dislocation_buckets;")
    analytics["dislocation_buckets"] = cur.fetchall()

    cur.execute("SELECT * FROM mr_v1_class_pnl;")
    analytics["class_pnl"] = cur.fetchall()

    cur.execute(
        """
        SELECT *
        FROM mr_v1_market_summary
        ORDER BY sum_pnl ASC
        LIMIT 20;
        """
    )
    analytics["worst_markets"] = cur.fetchall()

    cur.execute(
        """
        SELECT *
        FROM mr_v1_market_summary
        ORDER BY sum_pnl DESC
        LIMIT 20;
        """
    )
    analytics["best_markets"] = cur.fetchall()

    cur.execute("SELECT * FROM mr_v1_shadow_stats;")
    analytics["shadow_stats"] = cur.fetchone() or {}

    return analytics


@app.route("/")
def index():
    # pick strategy from query param, default v1
    strategy = request.args.get("strategy", "mean_reversion_v1")
    if strategy not in STRATEGIES:
        strategy = "mean_reversion_v1"

    with get_conn() as conn, conn.cursor() as cur:
        # Summary
        cur.execute(
            """
            SELECT 
              COUNT(*) FILTER (WHERE status='closed') AS closed_trades,
              COUNT(*) FILTER (WHERE status='open')   AS open_trades,
              COUNT(*) FILTER (WHERE status='closed' AND pnl > 0) AS winners,
              AVG(pnl)  FILTER (WHERE status='closed') AS avg_pnl,
              SUM(pnl)  FILTER (WHERE status='closed') AS total_pnl,
              SUM(pnl)  FILTER (WHERE status='closed' AND exit_ts >= CURRENT_DATE) AS today_pnl
            FROM mr_positions
            WHERE (%s = 'all' OR strategy = %s);
            """,
            (strategy, strategy),
        )
        s = cur.fetchone() or {}
        closed_trades = s.get("closed_trades") or 0
        winners = s.get("winners") or 0
        winrate = (winners / closed_trades) if closed_trades else None
        summary = {
            "closed_trades": closed_trades,
            "open_trades": s.get("open_trades") or 0,
            "winrate": winrate,
            "avg_pnl": float(s["avg_pnl"]) if s.get("avg_pnl") is not None else None,
            "total_pnl": float(s["total_pnl"]) if s.get("total_pnl") is not None else 0.0,
            "today_pnl": float(s["today_pnl"]) if s.get("today_pnl") is not None else 0.0,
        }

        # Open positions
        cur.execute(
            """
            SELECT 
              p.*,
              COALESCE(m.question, p.market_id) AS market_name,
              m.tags AS market_tags,
              (SELECT price FROM raw_trades rt 
               WHERE rt.market_id = p.market_id 
                 AND rt.outcome = p.outcome 
               ORDER BY rt.ts DESC LIMIT 1) AS last_price
            FROM mr_positions p
            LEFT JOIN markets m ON m.market_id = p.market_id
            WHERE (%s = 'all' OR p.strategy = %s)
              AND p.status = 'open'
            ORDER BY p.entry_ts DESC;
            """,
            (strategy, strategy),
        )
        open_positions = cur.fetchall()
        for p in open_positions:
            entry = to_dec(p["entry_price"]) or Decimal("0")
            last = to_dec(p["last_price"]) or entry
            p["last_price"] = last
            p["cost"] = float(entry * to_dec(p["size"] or 0))
            p["unrealized_pnl"] = float((last - entry) * to_dec(p["size"] or 0))
            p["px_change_pct"] = float(((last / entry) - 1) * 100) if entry > 0 else 0.0
            p["hours_open"] = (datetime.now(timezone.utc) - p["entry_ts"]).total_seconds() / 3600
            o = str(p.get("outcome"))
            p["outcome_label"] = "Yes" if o == "1" else "No"
            p["market_tags"] = ", ".join(p["market_tags"]) if isinstance(p.get("market_tags"), list) else (p.get("market_tags") or "")

        # Recent closed
        cur.execute(
            """
            SELECT 
              p.*,
              COALESCE(m.question, p.market_id) AS market_name,
              m.tags AS market_tags,
              EXTRACT(EPOCH FROM (p.exit_ts - p.entry_ts))/3600 AS hours_held
            FROM mr_positions p
            LEFT JOIN markets m ON m.market_id = p.market_id
            WHERE (%s = 'all' OR p.strategy = %s)
              AND p.status = 'closed'
            ORDER BY p.exit_ts DESC
            LIMIT 50;
            """,
            (strategy, strategy),
        )
        closed_positions = cur.fetchall()
        for p in closed_positions:
            p["hours_held"] = float(p["hours_held"] or 0)
            entry = to_dec(p["entry_price"]) or Decimal("0")
            p["cost"] = float(entry * to_dec(p["size"] or 0))
            exit_px = to_dec(p["exit_price"]) or entry
            p["px_change_pct"] = float(((exit_px / entry) - 1) * 100) if entry > 0 else 0.0
            o = str(p.get("outcome"))
            p["outcome_label"] = "Yes" if o == "1" else "No"
            p["market_tags"] = ", ".join(p["market_tags"]) if isinstance(p.get("market_tags"), list) else (p.get("market_tags") or "")

        # Exit breakdown
        cur.execute(
            """
            SELECT exit_reason, COUNT(*) AS count, AVG(pnl) AS avg_pnl, SUM(pnl) AS sum_pnl
            FROM mr_positions
            WHERE (%s = 'all' OR strategy = %s)
              AND status = 'closed'
            GROUP BY exit_reason
            ORDER BY count DESC;
            """,
            (strategy, strategy),
        )
        exit_breakdown = cur.fetchall()
        exit_totals = {
            "count": sum(r["count"] for r in exit_breakdown) if exit_breakdown else 0,
            "sum_pnl": sum(float(r["sum_pnl"] or 0) for r in exit_breakdown) if exit_breakdown else 0.0,
        }

        # Analytics aggregates for v1 (only)
        analytics = fetch_analytics(cur)

    db_url_short = DB_URL.split("@")[-1] if "@" in DB_URL else DB_URL
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return render_template_string(
        HTML,
        summary=summary,
        open_positions=open_positions,
        closed_positions=closed_positions,
        exit_breakdown=exit_breakdown,
        exit_totals=exit_totals,
        analytics=analytics,
        db_url_short=db_url_short,
        now_utc=now_utc,
        refresh_secs=REFRESH_SECS,
        strategy=strategy,
        strategies=STRATEGIES,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5001)
