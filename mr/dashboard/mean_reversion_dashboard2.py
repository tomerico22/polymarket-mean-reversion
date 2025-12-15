import os
import re
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev

from flask import Flask, render_template_string, request
from psycopg import connect
from psycopg.rows import dict_row

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise SystemExit("DB_URL not set")

app = Flask(__name__)

REFRESH_SECS = 30

# ----------------------------
# Health thresholds (seconds)
# ----------------------------
INGEST_STALE_SECS = int(os.getenv("DASH_INGEST_STALE_SECS", "120"))
BOT_OK_SECS = int(os.getenv("DASH_BOT_OK_SECS", "180"))
BOT_WARN_SECS = int(os.getenv("DASH_BOT_WARN_SECS", "600"))

LOG_MR_V1 = Path(os.getenv("DASH_LOG_MR_V1", "/root/polymarket-mean-reversion/logs/mr_v1.log"))
LOG_MR_V2 = Path(os.getenv("DASH_LOG_MR_V2", "/root/polymarket-mean-reversion/logs/mr_v2.log"))

DASH_ENABLE_MR_V2 = os.getenv("DASH_ENABLE_MR_V2", "1").strip().lower() in ("1", "true", "yes", "y")
if not DASH_ENABLE_MR_V2:
    LOG_MR_V2 = None

# tmux sessions we expect
EXPECTED_TMUX_SESSIONS = [
    s.strip()
    for s in os.getenv("DASH_TMUX_SESSIONS", "mr_v1,elwa_smartflow_full").split(",")
    if s.strip()
]

# available strategy filters
STRATEGIES = [
    "mean_reversion_v1",
    "mean_reversion_v3_32",
    "mean_reversion_strict_v1",
    "mean_reversion_v2",
    "all",
]

MODES = ["live", "paper", "both"]

# ----------------------------
# Kill switch thresholds
# ----------------------------
LIVE_DAILY_LOSS_LIMIT_USD = Decimal(os.getenv("DASH_DAILY_LOSS_LIMIT_USD", "-200"))
LIVE_WORST_OPEN_LIMIT_USD = Decimal(os.getenv("DASH_WORST_OPEN_LIMIT_USD", "-50"))
LIVE_MAX_GLOBAL_LOSS_STREAK = int(os.getenv("DASH_MAX_GLOBAL_LOSS_STREAK", "5"))
LIVE_MIN_TRADES_24H = int(os.getenv("DASH_MIN_TRADES_24H", "10"))
LIVE_WINRATE_FLOOR_24H = Decimal(os.getenv("DASH_WINRATE_FLOOR_24H", "0.45"))

PAPER_DAILY_LOSS_LIMIT_USD = Decimal(os.getenv("DASH_PAPER_DAILY_LOSS_LIMIT_USD", "-500"))
PAPER_WORST_OPEN_LIMIT_USD = Decimal(os.getenv("DASH_PAPER_WORST_OPEN_LIMIT_USD", "-150"))
PAPER_MAX_GLOBAL_LOSS_STREAK = int(os.getenv("DASH_PAPER_MAX_GLOBAL_LOSS_STREAK", "8"))
PAPER_MIN_TRADES_24H = int(os.getenv("DASH_PAPER_MIN_TRADES_24H", "10"))
PAPER_WINRATE_FLOOR_24H = Decimal(os.getenv("DASH_PAPER_WINRATE_FLOOR_24H", "0.40"))

# Problem position flags
DASH_PROBLEM_AGE_HOURS = Decimal(os.getenv("DASH_PROBLEM_AGE_HOURS", "8"))
DASH_PROBLEM_UNREAL_USD = Decimal(os.getenv("DASH_PROBLEM_UNREAL_USD", "-15"))
DASH_PROBLEM_UNREAL_PCT = Decimal(os.getenv("DASH_PROBLEM_UNREAL_PCT", "-10"))

# Market intel thresholds
DASH_MIN_TRADES_REVIEW = int(os.getenv("DASH_MIN_TRADES_REVIEW", "5"))
DASH_REVIEW_PNL_THRESHOLD = Decimal(os.getenv("DASH_REVIEW_PNL_THRESHOLD", "-50"))
DASH_REVIEW_WR_THRESHOLD = Decimal(os.getenv("DASH_REVIEW_WR_THRESHOLD", "0.35"))
DASH_TOP_PNL_THRESHOLD = Decimal(os.getenv("DASH_TOP_PNL_THRESHOLD", "100"))

# Buckets
DISLO_BUCKET_MIN = Decimal(os.getenv("DASH_DISLO_BUCKET_MIN", "-0.50"))
DISLO_BUCKET_MAX = Decimal(os.getenv("DASH_DISLO_BUCKET_MAX", "-0.20"))
DISLO_BUCKET_STEP = Decimal(os.getenv("DASH_DISLO_BUCKET_STEP", "0.05"))

ENTRY_BUCKET_MIN = Decimal(os.getenv("DASH_ENTRY_BUCKET_MIN", "0.05"))
ENTRY_BUCKET_MAX = Decimal(os.getenv("DASH_ENTRY_BUCKET_MAX", "0.95"))
ENTRY_BUCKET_STEP = Decimal(os.getenv("DASH_ENTRY_BUCKET_STEP", "0.05"))

RECENT_CLOSED_LIMIT = int(os.getenv("DASH_RECENT_CLOSED_LIMIT", "30"))
BEST_WORST_MARKETS_LIMIT = int(os.getenv("DASH_BEST_WORST_MARKETS_LIMIT", "20"))
KILLED_MARKETS_LIMIT = int(os.getenv("DASH_KILLED_MARKETS_LIMIT", "20"))

HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Mean Reversion Dashboard</title>
  <meta http-equiv="refresh" content="{{ refresh_secs }}">
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; background:#111; color:#eee; }
    h1 { font-size: 1.6rem; margin: 0.2rem 0; }
    h2 { font-size: 1.15rem; margin: 1.1rem 0 0.4rem 0; }
    h3 { font-size: 0.95rem; margin: 0.8rem 0 0.35rem 0; color:#ddd; }
    .small { font-size:0.75rem; color:#aaa; }
    .muted { color:#999; }

    .toolbar { margin:0.6rem 0 0.8rem 0; display:flex; gap:1rem; align-items:center; flex-wrap:wrap; }
    .toolbar label { margin-right:0.4rem; }
    select { background:#1d1d1d; color:#eee; border:1px solid #333; border-radius:10px; padding:0.35rem 0.55rem; }

    a.tab { background:#1d1d1d; color:#eee; border:1px solid #333; border-radius:10px; padding:0.35rem 0.55rem; text-decoration:none; }
    a.tab.active { border-color:#32cd32; }

    .healthbar { display:flex; gap:0.6rem; flex-wrap:wrap; margin:0.8rem 0 1rem 0; }
    .pill { display:flex; align-items:center; gap:0.5rem; padding:0.35rem 0.6rem; border-radius:999px; border:1px solid #333; background:#1a1a1a; }
    .dot { width:10px; height:10px; border-radius:50%; background:#666; }
    .pill .label { font-size:0.72rem; color:#bbb; text-transform:uppercase; letter-spacing:0.02em; }
    .pill .value { font-size:0.8rem; color:#eee; }
    .ok .dot { background:#32cd32; }
    .warn .dot { background:#ffd24d; }
    .bad .dot { background:#ff4d4d; }
    .na .dot { background:#666; }
    .pill.bad { border-color:#4a2020; }
    .pill.warn { border-color:#4a3b20; }
    .pill.ok { border-color:#244a24; }
    .pill.na { border-color:#333; }

    .summary { display:flex; gap:1rem; flex-wrap:wrap; margin:0.8rem 0 0.8rem 0; }
    .card { background:#1d1d1d; padding:0.8rem 1rem; border-radius:10px; min-width:10rem; border:1px solid #2a2a2a; }
    .card-label { font-size:0.7rem; text-transform:uppercase; color:#aaa; margin-bottom:0.25rem; letter-spacing:0.02em; }
    .card-value { font-size:1.1rem; font-weight:700; }

    .pnl-pos { color:#32cd32; font-weight:700; }
    .pnl-neg { color:#ff4d4d; font-weight:700; }
    .warn-txt { color:#ffd24d; font-weight:700; }

    table { border-collapse:collapse; width:100%; margin-top:0.5rem; font-size:0.82rem; }
    th, td { border:1px solid #333; padding:0.35rem 0.55rem; text-align:left; vertical-align:top; }
    th { background:#222; position:sticky; top:0; }
    tr:nth-child(even) { background:#181818; }
    tr:nth-child(odd) { background:#151515; }

    .strip { background:#151515; border:1px solid #2a2a2a; border-radius:10px; padding:0.6rem 0.75rem; margin:0.6rem 0; }

    .grid2 { display:grid; grid-template-columns: 1fr 1fr; gap:1rem; }
    @media (max-width: 1000px) { .grid2 { grid-template-columns: 1fr; } }

    .errorbox { margin-top: 1rem; padding: 0.8rem 1rem; background:#1d1d1d; border:1px solid #4a2020; border-radius:10px; color:#ffb3b3; }

    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  </style>
  <script>
    const refreshSecs = {{ refresh_secs }};
    let remaining = refreshSecs;
    function tick() {
      const el = document.getElementById("refresh_left");
      if (el) el.textContent = remaining;
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
    DB: {{ db_url_short }} - Updated at {{ now_utc }} - Strategy: <strong>{{ strategy }}</strong> - Mode: <strong>{{ mode }}</strong> -
    Refresh every {{ refresh_secs }}s (in <span id="refresh_left">{{ refresh_secs }}</span>s)
  </div>

  <div class="toolbar">
    <div>
      <a class="tab {% if view == 'command' %}active{% endif %}" href="/?view=command&strategy={{ strategy }}&mode={{ mode }}">Command Center</a>
      <a class="tab {% if view != 'command' %}active{% endif %}" href="/?view=diagnostics&strategy={{ strategy }}&mode={{ mode }}">Diagnostics</a>
    </div>

    <form method="get">
      <input type="hidden" name="view" value="{{ view }}">
      <label for="strategy" class="small">Strategy:</label>
      <select id="strategy" name="strategy" onchange="this.form.submit()">
        {% for s in strategies %}
          <option value="{{ s }}" {% if s == strategy %}selected{% endif %}>{{ s }}</option>
        {% endfor %}
      </select>

      <label for="mode" class="small">Mode:</label>
      <select id="mode" name="mode" onchange="this.form.submit()">
        {% for m in modes %}
          <option value="{{ m }}" {% if m == mode %}selected{% endif %}>{{ m }}</option>
        {% endfor %}
      </select>
    </form>
  </div>

  {% if page_error %}
    <div class="errorbox">
      Error loading data: {{ page_error }}
    </div>
  {% endif %}

  {% if view == 'command' %}

    <div class="strip">
      <strong>Status:</strong>
      DB:
      <span class="{% if cc.status.db_level == 'ok' %}pnl-pos{% elif cc.status.db_level == 'warn' %}warn-txt{% else %}pnl-neg{% endif %}">{{ cc.status.db_text }}</span>
      |
      Ingest:
      <span class="{% if cc.status.ingest_level == 'ok' %}pnl-pos{% elif cc.status.ingest_level == 'warn' %}warn-txt{% else %}pnl-neg{% endif %}">{{ cc.status.ingest_text }}</span>
      |
      tmux:
      <span class="{% if cc.status.tmux_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.status.tmux_text }}</span>
      |
      Bots:
      <span class="{% if cc.status.bots_level == 'ok' %}pnl-pos{% elif cc.status.bots_level == 'warn' %}warn-txt{% else %}pnl-neg{% endif %}">{{ cc.status.bots_text }}</span>
      |
      Last entry: <span class="muted">{{ cc.status.last_entry_text }}</span>
      |
      Last exit: <span class="muted">{{ cc.status.last_exit_text }}</span>
    </div>

    <h2>Performance Snapshot</h2>
    <table>
      <thead>
        <tr>
          <th>Metric</th>
          <th>Today</th>
          <th>Yesterday</th>
          <th>Last 7d</th>
          <th>All time</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>Total PnL</td>
          <td class="{% if cc.perf.pnls.ptoday < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.ptoday) }}</td>
          <td class="{% if cc.perf.pnls.pyday < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.pyday) }}</td>
          <td class="{% if cc.perf.pnls.p7 < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.p7) }}</td>
          <td class="{% if cc.perf.pnls.pall < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.pall) }}</td>
          <td class="{{ cc.perf.status.pnl_class }}">{{ cc.perf.status.pnl_text }}</td>
        </tr>
        <tr>
          <td>Trades</td>
          <td>{{ cc.perf.trades.ttoday }}</td>
          <td>{{ cc.perf.trades.tyday }}</td>
          <td>{{ cc.perf.trades.t7 }}</td>
          <td>{{ cc.perf.trades.tall }}</td>
          <td class="muted">-</td>
        </tr>
        <tr>
          <td>Win rate</td>
          <td>{{ "%.1f"|format(cc.perf.winrate.wtoday * 100) if cc.perf.winrate.wtoday is not none else "na" }}%</td>
          <td>{{ "%.1f"|format(cc.perf.winrate.wyday * 100) if cc.perf.winrate.wyday is not none else "na" }}%</td>
          <td>{{ "%.1f"|format(cc.perf.winrate.w7 * 100) if cc.perf.winrate.w7 is not none else "na" }}%</td>
          <td>{{ "%.1f"|format(cc.perf.winrate.wall * 100) if cc.perf.winrate.wall is not none else "na" }}%</td>
          <td class="{{ cc.perf.status.wr_class }}">{{ cc.perf.status.wr_text }}</td>
        </tr>
        <tr>
          <td>Avg PnL / trade</td>
          <td>{{ "%.2f"|format(cc.perf.avgpnl.atoday) if cc.perf.avgpnl.atoday is not none else "na" }}</td>
          <td>{{ "%.2f"|format(cc.perf.avgpnl.ayday) if cc.perf.avgpnl.ayday is not none else "na" }}</td>
          <td>{{ "%.2f"|format(cc.perf.avgpnl.a7) if cc.perf.avgpnl.a7 is not none else "na" }}</td>
          <td>{{ "%.2f"|format(cc.perf.avgpnl.aall) if cc.perf.avgpnl.aall is not none else "na" }}</td>
          <td class="muted">-</td>
        </tr>
        <tr>
          <td>Max SL rate</td>
          <td>{{ "%.1f"|format(cc.perf.slrate.stoday * 100) if cc.perf.slrate.stoday is not none else "na" }}%</td>
          <td>{{ "%.1f"|format(cc.perf.slrate.syday * 100) if cc.perf.slrate.syday is not none else "na" }}%</td>
          <td>{{ "%.1f"|format(cc.perf.slrate.s7 * 100) if cc.perf.slrate.s7 is not none else "na" }}%</td>
          <td>{{ "%.1f"|format(cc.perf.slrate.sall * 100) if cc.perf.slrate.sall is not none else "na" }}%</td>
          <td class="{{ cc.perf.status.sl_class }}">{{ cc.perf.status.sl_text }}</td>
        </tr>
        <tr>
          <td>Largest loss</td>
          <td class="pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.ltoday) if cc.perf.largestloss.ltoday is not none else "na" }}</td>
          <td class="pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.lyday) if cc.perf.largestloss.lyday is not none else "na" }}</td>
          <td class="pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.l7) if cc.perf.largestloss.l7 is not none else "na" }}</td>
          <td class="pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.lall) if cc.perf.largestloss.lall is not none else "na" }}</td>
          <td class="muted">-</td>
        </tr>
        <tr>
          <td>Sharpe (trade-level)</td>
          <td>{{ "%.2f"|format(cc.perf.sharpe.shtoday) if cc.perf.sharpe.shtoday is not none else "na" }}</td>
          <td>{{ "%.2f"|format(cc.perf.sharpe.shyday) if cc.perf.sharpe.shyday is not none else "na" }}</td>
          <td>{{ "%.2f"|format(cc.perf.sharpe.sh7) if cc.perf.sharpe.sh7 is not none else "na" }}</td>
          <td>{{ "%.2f"|format(cc.perf.sharpe.shall) if cc.perf.sharpe.shall is not none else "na" }}</td>
          <td class="{{ cc.perf.status.sh_class }}">{{ cc.perf.status.sh_text }}</td>
        </tr>
      </tbody>
    </table>

    <h2>Kill Switch Monitor</h2>
    <table>
      <thead>
        <tr><th>Metric</th><th>Current</th><th>Limit</th><th>Status</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>Daily PnL</td>
          <td class="{% if cc.kill.daily_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.kill.daily_pnl) }}</td>
          <td>{{ "%.2f"|format(cc.kill.daily_limit) }}</td>
          <td class="{% if cc.kill.daily_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.kill.daily_level|upper }}</td>
        </tr>
        <tr>
          <td>Worst Open Unrealized</td>
          <td class="{% if cc.kill.worst_open < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.kill.worst_open) }}</td>
          <td>{{ "%.2f"|format(cc.kill.worst_open_limit) }}</td>
          <td class="{% if cc.kill.worst_open_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.kill.worst_open_level|upper }}</td>
        </tr>
        <tr>
          <td>Global Loss Streak</td>
          <td>{{ cc.kill.loss_streak }}</td>
          <td>{{ cc.kill.loss_streak_limit }}</td>
          <td class="{% if cc.kill.loss_streak_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.kill.loss_streak_level|upper }}</td>
        </tr>
        <tr>
          <td>Winrate (24h)</td>
          <td>
            {% if cc.kill.winrate_24h is none %}
              <span class="muted">na ({{ cc.kill.trades_24h }} trades)</span>
            {% else %}
              {{ "%.1f"|format(cc.kill.winrate_24h * 100) }}% ({{ cc.kill.trades_24h }} trades)
            {% endif %}
          </td>
          <td>{{ "%.1f"|format(cc.kill.winrate_floor * 100) }}% (min {{ cc.kill.min_trades_24h }})</td>
          <td class="{% if cc.kill.winrate_level == 'ok' %}pnl-pos{% elif cc.kill.winrate_level == 'na' %}muted{% else %}warn-txt{% endif %}">
            {{ cc.kill.winrate_level|upper }}
          </td>
        </tr>
        <tr>
          <td><strong>SYSTEM STATUS</strong></td>
          <td colspan="3" class="{% if cc.kill.system_level == 'ok' %}pnl-pos{% elif cc.kill.system_level == 'warn' %}warn-txt{% else %}pnl-neg{% endif %}">
            <strong>{{ cc.kill.system_level|upper }}</strong>
          </td>
        </tr>
      </tbody>
    </table>

    <h2>Filter Performance (Today)</h2>
    <div class="strip mono small">
      Markets scanned: <strong>{{ cc.filters.markets_scanned }}</strong> |
      After base filters: <strong>{{ cc.filters.after_filters }}</strong> |
      Would enter (scan): <strong>{{ cc.filters.scan_entries }}</strong> |
      Trades executed (today): <strong>{{ cc.filters.trades_executed_today }}</strong>
      <br><br>
      Blocked by (latest scan):
      cap_per_market_outcome={{ cc.filters.blocked.cap_per_market_outcome }},
      dislo_not_negative={{ cc.filters.blocked.dislo_not_negative }},
      dislo_too_small={{ cc.filters.blocked.dislo_too_small }},
      dislo_too_big={{ cc.filters.blocked.dislo_too_big }},
      market_banned={{ cc.filters.blocked.market_banned }},
      px_oob={{ cc.filters.blocked.px_oob }},
      stale={{ cc.filters.blocked.stale }}
    </div>

    <h2>Problem Positions</h2>
    {% if cc.problems %}
    <table>
      <thead>
        <tr>
          <th>Age (h)</th>
          <th>Market</th>
          <th>Tags</th>
          <th>Entry - Last</th>
          <th>Dislo%</th>
          <th>Unrealized</th>
          <th>Flag</th>
        </tr>
      </thead>
      <tbody>
        {% for p in cc.problems %}
        <tr>
          <td>{{ "%.1f"|format(p.age_h) }}</td>
          <td class="small">{{ p.market_name }}</td>
          <td class="small">{{ p.tags }}</td>
          <td>{{ "%.4f"|format(p.entry_px) }} - {{ "%.4f"|format(p.last_px) }}</td>
          <td>{{ "%.1f"|format(p.dislo_pct) }}</td>
          <td class="{% if p.unreal < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(p.unreal) }}</td>
          <td class="{% if 'UNDERWATER' in p.flag or 'OLD' in p.flag %}warn-txt{% else %}muted{% endif %}">{{ p.flag }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
      <div class="small muted">No problem positions (or not available in this mode).</div>
    {% endif %}

    <h2>Market Intelligence</h2>
    <div class="grid2">
      <div>
        <h3>Review Candidates</h3>
        {% if cc.intel.review %}
        <table>
          <thead><tr><th>Market</th><th>Trades</th><th>Sum PnL</th><th>WR</th><th>Last</th></tr></thead>
          <tbody>
            {% for r in cc.intel.review %}
            <tr>
              <td class="small">{{ r.market_name }}</td>
              <td>{{ r.trades }}</td>
              <td class="pnl-neg">{{ "%.2f"|format(r.sum_pnl) }}</td>
              <td>{{ "%.0f"|format(r.winrate * 100) }}%</td>
              <td class="muted">{{ r.last_age }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>

      <div>
        <h3>Top Performers</h3>
        {% if cc.intel.top %}
        <table>
          <thead><tr><th>Market</th><th>Trades</th><th>Sum PnL</th><th>WR</th><th>Last</th></tr></thead>
          <tbody>
            {% for r in cc.intel.top %}
            <tr>
              <td class="small">{{ r.market_name }}</td>
              <td>{{ r.trades }}</td>
              <td class="pnl-pos">{{ "%.2f"|format(r.sum_pnl) }}</td>
              <td>{{ "%.0f"|format(r.winrate * 100) }}%</td>
              <td class="muted">{{ r.last_age }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>
    </div>

  {% else %}

    <div class="healthbar">
      <div class="pill {{ health.db.status }}">
        <span class="dot"></span>
        <span class="label">DB</span>
        <span class="value">{{ health.db.text }}</span>
      </div>

      <div class="pill {{ health.ingest.status }}">
        <span class="dot"></span>
        <span class="label">Ingest</span>
        <span class="value">{{ health.ingest.text }}</span>
      </div>

      <div class="pill {{ health.tmux.status }}">
        <span class="dot"></span>
        <span class="label">tmux</span>
        <span class="value">{{ health.tmux.text }}</span>
      </div>

      <div class="pill {{ health.bots.status }}">
        <span class="dot"></span>
        <span class="label">Bots</span>
        <span class="value">{{ health.bots.text }}</span>
      </div>

      <div class="pill {{ health.dashboard.status }}">
        <span class="dot"></span>
        <span class="label">Dashboard</span>
        <span class="value">{{ health.dashboard.text }}</span>
      </div>
    </div>

    <div class="summary">
      <div class="card">
        <div class="card-label">Closed PnL (Today)</div>
        <div class="card-value {% if diag.today_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(diag.today_pnl) }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Closed PnL (24h)</div>
        <div class="card-value {% if diag.pnl_24h < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(diag.pnl_24h) }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Closed PnL (7d)</div>
        <div class="card-value {% if diag.pnl_7d < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(diag.pnl_7d) }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Closed PnL (All)</div>
        <div class="card-value {% if diag.total_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(diag.total_pnl) }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Trades (closed)</div>
        <div class="card-value">{{ diag.closed_trades }}</div>
      </div>
      <div class="card">
        <div class="card-label">Winrate (all)</div>
        <div class="card-value">
          {% if diag.winrate is none %}<span class="muted">na</span>{% else %}{{ "%.1f"|format(diag.winrate * 100) }}%{% endif %}
        </div>
      </div>
    </div>

    <h2>Open Positions</h2>
    {% if diag.open_positions %}
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
        {% for p in diag.open_positions %}
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
      <div class="small muted">No open positions (or not available in this mode).</div>
    {% endif %}

    <h2>Recent Closed Positions (last {{ diag.recent_closed|length }})</h2>
    {% if diag.recent_closed %}
    <table>
      <thead>
        <tr>
          <th>Exit TS</th>
          <th>Market</th>
          <th>Name</th>
          <th>Outcome</th>
          <th>Entry Px</th>
          <th>Exit Px</th>
          <th>PnL</th>
          <th>Exit</th>
          <th>Hours</th>
        </tr>
      </thead>
      <tbody>
        {% for r in diag.recent_closed %}
        <tr>
          <td>{{ r.exit_ts }}</td>
          <td class="small">{{ r.market_id[:16] }}…</td>
          <td class="small">{{ r.market_name }}</td>
          <td>{{ r.outcome_label }}</td>
          <td>{{ "%.4f"|format(r.entry_price) }}</td>
          <td>{{ "%.4f"|format(r.exit_price) }}</td>
          <td class="{% if r.pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(r.pnl) }}</td>
          <td>{{ r.exit_reason }}</td>
          <td>{{ "%.1f"|format(r.hours_held) }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
      <div class="small muted">na</div>
    {% endif %}

    <h2>Killed Markets (last {{ diag.killed_markets|length }})</h2>
    {% if diag.killed_markets %}
    <table>
      <thead>
        <tr>
          <th>Market</th>
          <th>Name</th>
          <th>Exit TS</th>
          <th>Total PnL</th>
        </tr>
      </thead>
      <tbody>
        {% for k in diag.killed_markets %}
        <tr>
          <td class="small">{{ k.market_id[:16] }}… ({{ k.outcome_label }})</td>
          <td class="small">{{ k.market_name }}</td>
          <td>{{ k.exit_ts }}</td>
          <td class="{% if k.total_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(k.total_pnl) }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
      <div class="small muted">na</div>
    {% endif %}

    <div class="grid2">
      <div>
        <h2>Dislocation Buckets ({{ diag.dislo_buckets|length }})</h2>
        {% if diag.dislo_buckets %}
        <table>
          <thead>
            <tr>
              <th>Bucket</th><th>Min</th><th>Max</th><th>Trades</th><th>Avg PnL</th><th>Sum PnL</th><th>Winrate</th>
            </tr>
          </thead>
          <tbody>
            {% for b in diag.dislo_buckets %}
            <tr>
              <td>{{ b.bucket }}</td>
              <td>{{ "%.3f"|format(b.minv) }}</td>
              <td>{{ "%.3f"|format(b.maxv) }}</td>
              <td>{{ b.trades }}</td>
              <td class="{% if b.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.avg_pnl) }}</td>
              <td class="{% if b.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.sum_pnl) }}</td>
              <td>{{ "%.1f"|format(b.winrate * 100) }}%</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>

      <div>
        <h2>Entry Price Buckets ({{ diag.entry_buckets|length }})</h2>
        {% if diag.entry_buckets %}
        <table>
          <thead>
            <tr>
              <th>Bucket</th><th>Min</th><th>Max</th><th>Trades</th><th>Avg PnL</th><th>Sum PnL</th><th>Winrate</th>
            </tr>
          </thead>
          <tbody>
            {% for b in diag.entry_buckets %}
            <tr>
              <td>{{ b.bucket }}</td>
              <td>{{ "%.2f"|format(b.minv) }}</td>
              <td>{{ "%.2f"|format(b.maxv) }}</td>
              <td>{{ b.trades }}</td>
              <td class="{% if b.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.avg_pnl) }}</td>
              <td class="{% if b.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.sum_pnl) }}</td>
              <td>{{ "%.1f"|format(b.winrate * 100) }}%</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>
    </div>

    <div class="grid2">
      <div>
        <h2>Worst Markets (24h) - top {{ diag.worst_markets|length }}</h2>
        {% if diag.worst_markets %}
        <table>
          <thead><tr><th>Market</th><th>Trades</th><th>Sum PnL</th><th>WR</th></tr></thead>
          <tbody>
            {% for m in diag.worst_markets %}
            <tr>
              <td class="small">{{ m.market_name }}</td>
              <td>{{ m.trades }}</td>
              <td class="pnl-neg">{{ "%.2f"|format(m.sum_pnl) }}</td>
              <td>{{ "%.0f"|format(m.winrate * 100) }}%</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>

      <div>
        <h2>Best Markets (24h) - top {{ diag.best_markets|length }}</h2>
        {% if diag.best_markets %}
        <table>
          <thead><tr><th>Market</th><th>Trades</th><th>Sum PnL</th><th>WR</th></tr></thead>
          <tbody>
            {% for m in diag.best_markets %}
            <tr>
              <td class="small">{{ m.market_name }}</td>
              <td>{{ m.trades }}</td>
              <td class="pnl-pos">{{ "%.2f"|format(m.sum_pnl) }}</td>
              <td>{{ "%.0f"|format(m.winrate * 100) }}%</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
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


def _run(cmd, timeout=2):
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
        return res.returncode, (res.stdout or "").strip(), (res.stderr or "").strip()
    except Exception as e:
        return 999, "", str(e)


def _file_age_secs(path: Path):
    try:
        return max(0, (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime))
    except Exception:
        return None


def _age_from_ts(ts):
    if not ts:
        return None
    try:
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except Exception:
        return None


def _bot_level(age_s):
    if age_s is None:
        return "na"
    if age_s <= BOT_OK_SECS:
        return "ok"
    if age_s <= BOT_WARN_SECS:
        return "warn"
    return "bad"


def _fmt_age(age_s):
    if age_s is None:
        return "na"
    age_s = int(age_s)
    if age_s < 60:
        return f"{age_s}s"
    m = age_s // 60
    if m < 60:
        return f"{m}m"
    h = m // 60
    return f"{h}h"


def check_tmux_sessions():
    missing = []
    for s in EXPECTED_TMUX_SESSIONS:
        code, _, _ = _run(["tmux", "has-session", "-t", s], timeout=1)
        if code != 0:
            missing.append(s)
    if not missing:
        return {"status": "ok", "text": "all sessions up"}
    return {"status": "bad", "text": f"missing: {', '.join(missing)}"}


def _safe_fetchall(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception:
        return []


def _safe_fetchone(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchone() or {}
    except Exception:
        return {}


def _positions_table_for_mode(mode):
    if mode == "paper":
        return "paper_positions"
    return "mr_positions"


def _paper_like(mode):
    return mode == "paper"


def _limits_for_mode(mode):
    if _paper_like(mode):
        return {
            "daily_loss": PAPER_DAILY_LOSS_LIMIT_USD,
            "worst_open": PAPER_WORST_OPEN_LIMIT_USD,
            "max_streak": PAPER_MAX_GLOBAL_LOSS_STREAK,
            "min_trades_24h": PAPER_MIN_TRADES_24H,
            "winrate_floor": PAPER_WINRATE_FLOOR_24H,
        }
    return {
        "daily_loss": LIVE_DAILY_LOSS_LIMIT_USD,
        "worst_open": LIVE_WORST_OPEN_LIMIT_USD,
        "max_streak": LIVE_MAX_GLOBAL_LOSS_STREAK,
        "min_trades_24h": LIVE_MIN_TRADES_24H,
        "winrate_floor": LIVE_WINRATE_FLOOR_24H,
    }


def _load_open_positions(cur, strategy, mode):
    tbl = _positions_table_for_mode(mode)

    sql = f"""
        SELECT
          p.*,
          COALESCE(m.question, p.market_id) AS market_name,
          m.tags AS market_tags,
          (SELECT price FROM raw_trades rt
           WHERE rt.market_id = p.market_id
             AND rt.outcome = p.outcome
           ORDER BY rt.ts DESC LIMIT 1) AS last_price
        FROM {tbl} p
        LEFT JOIN markets m ON m.market_id = p.market_id
        WHERE (%s = 'all' OR p.strategy = %s)
          AND COALESCE(p.status, 'open') = 'open'
        ORDER BY p.entry_ts DESC;
    """
    rows = _safe_fetchall(cur, sql, (strategy, strategy))

    out = []
    for p in rows:
        entry = to_dec(p.get("entry_price")) or Decimal("0")
        last = to_dec(p.get("last_price")) or entry
        size = to_dec(p.get("size") or 0) or Decimal("0")

        cost = entry * size
        unreal = (last - entry) * size
        if str(p.get("side", "")).lower() == "short":
            unreal = -unreal

        hours_open = 0.0
        try:
            hours_open = (datetime.now(timezone.utc) - p.get("entry_ts")).total_seconds() / 3600
        except Exception:
            hours_open = 0.0

        o = str(p.get("outcome"))
        outcome_label = "Yes" if o == "1" else "No"

        market_tags = p.get("market_tags")
        tags_txt = ", ".join(market_tags) if isinstance(market_tags, list) else (market_tags or "")

        out.append(
            {
                "entry_ts": p.get("entry_ts"),
                "market_id": p.get("market_id") or "",
                "market_name": p.get("market_name") or p.get("market_id") or "",
                "market_tags": tags_txt,
                "outcome_label": outcome_label,
                "dislocation": to_dec(p.get("dislocation")),
                "size": float(size),
                "entry_price": float(entry),
                "cost": float(cost),
                "last_price": float(last),
                "px_change_pct": float(((last / entry) - 1) * 100) if entry > 0 else 0.0,
                "unrealized_pnl": float(unreal),
                "hours_open": float(hours_open),
            }
        )
    return out


def _load_closed_rollups(cur, strategy, mode):
    tbl = _positions_table_for_mode(mode)
    sql = f"""
        SELECT
          COALESCE(SUM(pnl) FILTER (WHERE COALESCE(status,'closed')='closed' AND exit_ts >= CURRENT_DATE), 0) AS today_pnl,
          COALESCE(SUM(pnl) FILTER (WHERE COALESCE(status,'closed')='closed' AND exit_ts >= (NOW() - INTERVAL '24 hours')), 0) AS pnl_24h,
          COALESCE(SUM(pnl) FILTER (WHERE COALESCE(status,'closed')='closed' AND exit_ts >= (NOW() - INTERVAL '7 days')), 0) AS pnl_7d,
          COALESCE(SUM(pnl) FILTER (WHERE COALESCE(status,'closed')='closed'), 0) AS total_pnl,
          COUNT(*) FILTER (WHERE COALESCE(status,'closed')='closed') AS closed_trades,
          COUNT(*) FILTER (WHERE COALESCE(status,'open')='open') AS open_trades,
          COUNT(*) FILTER (WHERE COALESCE(status,'closed')='closed' AND pnl > 0) AS winners
        FROM {tbl}
        WHERE (%s = 'all' OR strategy = %s);
    """
    r = _safe_fetchone(cur, sql, (strategy, strategy))
    closed = int(r.get("closed_trades") or 0)
    winners = int(r.get("winners") or 0)
    winrate = (winners / closed) if closed else None
    return {
        "today_pnl": float(to_dec(r.get("today_pnl")) or 0),
        "pnl_24h": float(to_dec(r.get("pnl_24h")) or 0),
        "pnl_7d": float(to_dec(r.get("pnl_7d")) or 0),
        "total_pnl": float(to_dec(r.get("total_pnl")) or 0),
        "closed_trades": closed,
        "open_trades": int(r.get("open_trades") or 0),
        "winrate": winrate,
    }


def _is_sl_exit(reason: str) -> bool:
    if not reason:
        return False
    r = str(reason).lower().strip()
    if r in ("sl", "max_sl"):
        return True
    if "max_sl" in r:
        return True
    return False


def _sharpe_from_pnls(pnls):
    if not pnls or len(pnls) < 2:
        return None
    mu = mean(pnls)
    sd = pstdev(pnls)
    if sd == 0:
        return None
    # trade-level sharpe: mean/stdev * sqrt(n)
    return (mu / sd) * (len(pnls) ** 0.5)


def _load_perf_time_range(cur, strategy, mode, where_time_sql: str, params_tail=()):
    """
    Loads perf stats for an arbitrary WHERE time clause.
    where_time_sql should start with 'AND ...'
    """
    tbl = _positions_table_for_mode(mode)
    params = [strategy, strategy]
    params.extend(list(params_tail))

    sql = f"""
        SELECT pnl, exit_reason
        FROM {tbl}
        WHERE (%s='all' OR strategy=%s)
          AND COALESCE(status,'closed')='closed'
          AND pnl IS NOT NULL
          AND exit_ts IS NOT NULL
          {where_time_sql};
    """
    rows = _safe_fetchall(cur, sql, tuple(params))

    pnls = []
    wins = 0
    sls = 0
    largest_loss = None
    for r in rows:
        pnl = to_dec(r.get("pnl"))
        if pnl is None:
            continue
        pv = float(pnl)
        pnls.append(pv)
        if pv > 0:
            wins += 1
        if _is_sl_exit(r.get("exit_reason")):
            sls += 1
        if largest_loss is None:
            largest_loss = pv
        else:
            largest_loss = min(largest_loss, pv)

    n = len(pnls)
    total = float(sum(pnls)) if pnls else 0.0
    wr = (wins / n) if n else None
    avg = (total / n) if n else None
    sl_rate = (sls / n) if n else None
    sharpe = _sharpe_from_pnls(pnls)

    return {
        "trades": n,
        "pnl": total,
        "winrate": wr,
        "avg_pnl": avg,
        "sl_rate": sl_rate,
        "largest_loss": largest_loss,
        "sharpe": sharpe,
    }


def _load_perf_window(cur, strategy, mode, since_interval_sql: str | None):
    tbl = _positions_table_for_mode(mode)
    where_time = ""
    params = [strategy, strategy]
    if since_interval_sql:
        where_time = f" AND exit_ts >= (NOW() - INTERVAL '{since_interval_sql}')"

    sql = f"""
        SELECT pnl, exit_reason
        FROM {tbl}
        WHERE (%s='all' OR strategy=%s)
          AND COALESCE(status,'closed')='closed'
          AND pnl IS NOT NULL
          AND exit_ts IS NOT NULL
          {where_time};
    """
    rows = _safe_fetchall(cur, sql, tuple(params))
    pnls = []
    wins = 0
    sls = 0
    largest_loss = None
    for r in rows:
        pnl = to_dec(r.get("pnl"))
        if pnl is None:
            continue
        pv = float(pnl)
        pnls.append(pv)
        if pv > 0:
            wins += 1
        if _is_sl_exit(r.get("exit_reason")):
            sls += 1
        if largest_loss is None:
            largest_loss = pv
        else:
            largest_loss = min(largest_loss, pv)

    n = len(pnls)
    total = float(sum(pnls)) if pnls else 0.0
    wr = (wins / n) if n else None
    avg = (total / n) if n else None
    sl_rate = (sls / n) if n else None
    sharpe = _sharpe_from_pnls(pnls)

    return {
        "trades": n,
        "pnl": total,
        "winrate": wr,
        "avg_pnl": avg,
        "sl_rate": sl_rate,
        "largest_loss": largest_loss,
        "sharpe": sharpe,
    }


def _merge_perf(a, b):
    trades = (a["trades"] or 0) + (b["trades"] or 0)
    pnl = (a["pnl"] or 0.0) + (b["pnl"] or 0.0)

    def wavg(xa, xb):
        ta = a["trades"] or 0
        tb = b["trades"] or 0
        if ta + tb == 0:
            return None
        va = xa if xa is not None else 0.0
        vb = xb if xb is not None else 0.0
        return (va * ta + vb * tb) / (ta + tb)

    winrate = wavg(a.get("winrate"), b.get("winrate"))
    avg_pnl = (pnl / trades) if trades else None
    sl_rate = wavg(a.get("sl_rate"), b.get("sl_rate"))
    largest_loss = None
    for v in (a.get("largest_loss"), b.get("largest_loss")):
        if v is None:
            continue
        largest_loss = v if largest_loss is None else min(largest_loss, v)

    # sharpe can't be merged properly without raw series; return None
    sharpe = None

    return {
        "trades": trades,
        "pnl": pnl,
        "winrate": winrate,
        "avg_pnl": avg_pnl,
        "sl_rate": sl_rate,
        "largest_loss": largest_loss,
        "sharpe": sharpe,
    }


def _load_performance_snapshot(cur, strategy, mode):
    # Today: exit_ts >= CURRENT_DATE
    # Yesterday: CURRENT_DATE - 1 day <= exit_ts < CURRENT_DATE
    def one_mode_snap(m):
        ptoday = _load_perf_time_range(cur, strategy, m, "AND exit_ts >= CURRENT_DATE")
        pyday = _load_perf_time_range(cur, strategy, m, "AND exit_ts >= (CURRENT_DATE - INTERVAL '1 day') AND exit_ts < CURRENT_DATE")
        p7 = _load_perf_window(cur, strategy, m, "7 days")
        pall = _load_perf_window(cur, strategy, m, None)
        return ptoday, pyday, p7, pall

    if mode in ("live", "paper"):
        ptoday, pyday, p7, pall = one_mode_snap(mode)
    else:
        l_today, l_yday, l7, la = one_mode_snap("live")
        p_today, p_yday, p7x, pa = one_mode_snap("paper")
        ptoday = _merge_perf(l_today, p_today)
        pyday = _merge_perf(l_yday, p_yday)
        p7 = _merge_perf(l7, p7x)
        pall = _merge_perf(la, pa)

    # Use today for status if any trades today, else use yesterday, else use 7d
    ref = ptoday if (ptoday["trades"] or 0) > 0 else (pyday if (pyday["trades"] or 0) > 0 else p7)

    def status_from_perf():
        pnl_class = "pnl-pos" if (ref["pnl"] or 0) >= 0 else "pnl-neg"
        pnl_text = "OK" if (ref["pnl"] or 0) >= 0 else "CHECK"

        wr_ok = (ref["winrate"] is not None and ref["winrate"] >= 0.5)
        wr_class = "pnl-pos" if wr_ok else "warn-txt"
        wr_text = "STABLE" if wr_ok else "CHECK"

        sl_ok = (ref["sl_rate"] is not None and ref["sl_rate"] <= 0.35)
        sl_class = "pnl-pos" if sl_ok else "warn-txt"
        sl_text = "STABLE" if sl_ok else "CHECK"

        sh_ok = (ref["sharpe"] is not None and ref["sharpe"] >= 1.0)
        sh_class = "pnl-pos" if sh_ok else "warn-txt"
        sh_text = "GOOD" if sh_ok else "CHECK"

        return {
            "pnl_class": pnl_class, "pnl_text": pnl_text,
            "wr_class": wr_class, "wr_text": wr_text,
            "sl_class": sl_class, "sl_text": sl_text,
            "sh_class": sh_class, "sh_text": sh_text,
        }

    return {
        "pnls": {"ptoday": ptoday["pnl"], "pyday": pyday["pnl"], "p7": p7["pnl"], "pall": pall["pnl"]},
        "trades": {"ttoday": ptoday["trades"], "tyday": pyday["trades"], "t7": p7["trades"], "tall": pall["trades"]},
        "winrate": {"wtoday": ptoday["winrate"], "wyday": pyday["winrate"], "w7": p7["winrate"], "wall": pall["winrate"]},
        "avgpnl": {"atoday": ptoday["avg_pnl"], "ayday": pyday["avg_pnl"], "a7": p7["avg_pnl"], "aall": pall["avg_pnl"]},
        "slrate": {"stoday": ptoday["sl_rate"], "syday": pyday["sl_rate"], "s7": p7["sl_rate"], "sall": pall["sl_rate"]},
        "largestloss": {"ltoday": ptoday["largest_loss"], "lyday": pyday["largest_loss"], "l7": p7["largest_loss"], "lall": pall["largest_loss"]},
        "sharpe": {"shtoday": ptoday["sharpe"], "shyday": pyday["sharpe"], "sh7": p7["sharpe"], "shall": pall["sharpe"]},
        "status": status_from_perf(),
    }


def _load_recent_closed(cur, strategy, mode, limit_n):
    tbl = _positions_table_for_mode(mode)
    sql = f"""
        SELECT
          p.market_id,
          COALESCE(m.question, p.market_id) AS market_name,
          p.outcome,
          p.entry_price,
          p.exit_price,
          p.entry_ts,
          p.exit_ts,
          p.exit_reason,
          p.pnl
        FROM {tbl} p
        LEFT JOIN markets m ON m.market_id = p.market_id
        WHERE (%s='all' OR p.strategy=%s)
          AND COALESCE(p.status,'closed')='closed'
          AND p.exit_ts IS NOT NULL
        ORDER BY p.exit_ts DESC
        LIMIT {int(limit_n)};
    """
    rows = _safe_fetchall(cur, sql, (strategy, strategy))
    out = []
    for r in rows:
        entry_ts = r.get("entry_ts")
        exit_ts = r.get("exit_ts")
        hours = 0.0
        try:
            hours = (exit_ts - entry_ts).total_seconds() / 3600 if entry_ts and exit_ts else 0.0
        except Exception:
            hours = 0.0
        o = str(r.get("outcome"))
        outcome_label = "Yes" if o == "1" else "No"
        out.append({
            "market_id": r.get("market_id") or "",
            "market_name": r.get("market_name") or r.get("market_id") or "",
            "outcome_label": outcome_label,
            "entry_price": float(to_dec(r.get("entry_price")) or 0),
            "exit_price": float(to_dec(r.get("exit_price")) or 0),
            "exit_ts": r.get("exit_ts"),
            "pnl": float(to_dec(r.get("pnl")) or 0),
            "exit_reason": r.get("exit_reason") or "",
            "hours_held": float(hours),
        })
    return out


def _load_killed_markets(cur, strategy, mode, limit_n):
    """
    Last N kill exits, ordered by most recent exit_ts.
    total_pnl = cumulative closed pnl for that market_id+outcome (same table, same strategy filter).
    """
    tbl = _positions_table_for_mode(mode)

    sql = f"""
        SELECT
          p.market_id,
          COALESCE(m.question, p.market_id) AS market_name,
          p.outcome,
          p.exit_ts,
          (
            SELECT COALESCE(SUM(p2.pnl), 0)
            FROM {tbl} p2
            WHERE (%s='all' OR p2.strategy=%s)
              AND COALESCE(p2.status,'closed')='closed'
              AND p2.pnl IS NOT NULL
              AND p2.market_id = p.market_id
              AND p2.outcome = p.outcome
          ) AS total_pnl
        FROM {tbl} p
        LEFT JOIN markets m ON m.market_id = p.market_id
        WHERE (%s='all' OR p.strategy=%s)
          AND COALESCE(p.status,'closed')='closed'
          AND p.exit_ts IS NOT NULL
          AND p.exit_reason = 'market_kill'
        ORDER BY p.exit_ts DESC
        LIMIT {int(limit_n)};
    """
    rows = _safe_fetchall(cur, sql, (strategy, strategy, strategy, strategy))
    out = []
    for r in rows:
        o = str(r.get("outcome"))
        outcome_label = "Yes" if o == "1" else "No"
        out.append({
            "market_id": r.get("market_id") or "",
            "market_name": r.get("market_name") or r.get("market_id") or "",
            "outcome_label": outcome_label,
            "exit_ts": r.get("exit_ts"),
            "total_pnl": float(to_dec(r.get("total_pnl")) or 0),
        })
    return out


def _bucket_edges(minv: Decimal, maxv: Decimal, step: Decimal):
    edges = []
    x = minv
    while x < maxv:
        edges.append(x)
        x = x + step
    edges.append(maxv)
    return edges


def _make_buckets(rows, key_fn, edges):
    buckets = []
    for i in range(len(edges) - 1):
        buckets.append({
            "bucket": i + 1,
            "minv": float(edges[i]),
            "maxv": float(edges[i + 1]),
            "trades": 0,
            "sum_pnl": 0.0,
            "wins": 0,
        })

    for r in rows:
        k = key_fn(r)
        pnl = float(to_dec(r.get("pnl")) or 0.0)
        if k is None:
            continue
        try:
            kv = Decimal(str(k))
        except Exception:
            continue

        for i in range(len(edges) - 1):
            lo = edges[i]
            hi = edges[i + 1]
            in_bucket = (kv >= lo and kv < hi) if i < len(edges) - 2 else (kv >= lo and kv <= hi)
            if in_bucket:
                b = buckets[i]
                b["trades"] += 1
                b["sum_pnl"] += pnl
                if pnl > 0:
                    b["wins"] += 1
                break

    out = []
    for b in buckets:
        if b["trades"] == 0:
            continue
        out.append({
            "bucket": b["bucket"],
            "minv": b["minv"],
            "maxv": b["maxv"],
            "trades": b["trades"],
            "avg_pnl": b["sum_pnl"] / b["trades"],
            "sum_pnl": b["sum_pnl"],
            "winrate": b["wins"] / b["trades"],
        })
    return out


def _load_closed_rows_for_buckets(cur, strategy, mode):
    tbl = _positions_table_for_mode(mode)
    sql = f"""
        SELECT entry_price, dislocation, pnl
        FROM {tbl}
        WHERE (%s='all' OR strategy=%s)
          AND COALESCE(status,'closed')='closed'
          AND pnl IS NOT NULL
          AND exit_ts IS NOT NULL;
    """
    return _safe_fetchall(cur, sql, (strategy, strategy))


def _load_best_worst_markets_24h(cur, strategy, mode, limit_n):
    tbl = _positions_table_for_mode(mode)
    sql = f"""
        SELECT
          p.market_id,
          COALESCE(m.question, p.market_id) AS market_name,
          COUNT(*) AS trades,
          COALESCE(SUM(p.pnl), 0) AS sum_pnl,
          AVG(CASE WHEN p.pnl > 0 THEN 1.0 ELSE 0.0 END) AS winrate
        FROM {tbl} p
        LEFT JOIN markets m ON m.market_id = p.market_id
        WHERE (%s='all' OR p.strategy=%s)
          AND COALESCE(p.status,'closed')='closed'
          AND p.exit_ts >= (NOW() - INTERVAL '24 hours')
        GROUP BY p.market_id, market_name
        HAVING COUNT(*) >= 1;
    """
    rows = _safe_fetchall(cur, sql, (strategy, strategy))
    items = []
    for r in rows:
        items.append({
            "market_name": r.get("market_name") or r.get("market_id"),
            "trades": int(r.get("trades") or 0),
            "sum_pnl": float(to_dec(r.get("sum_pnl")) or 0),
            "winrate": float(r.get("winrate") or 0.0),
        })
    worst = sorted(items, key=lambda x: x["sum_pnl"])[: int(limit_n)]
    best = sorted(items, key=lambda x: x["sum_pnl"], reverse=True)[: int(limit_n)]
    return worst, best


def _read_tail_lines(path: Path, max_lines=4000):
    try:
        if not path or not path.exists():
            return []
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return lines[-max_lines:]
    except Exception:
        return []


def _parse_latest_scan_from_log(lines):
    top_raw = None
    top_after = None
    scan = {}

    top_re = re.compile(r"TOP_MARKETS\s+raw=(\d+)\s+after_filters=(\d+)")
    scan_re = re.compile(r"SCAN_SUMMARY\s+(.*)$")

    for line in reversed(lines):
        if top_raw is None:
            m = top_re.search(line)
            if m:
                top_raw = int(m.group(1))
                top_after = int(m.group(2))
        if not scan:
            m2 = scan_re.search(line)
            if m2:
                blob = m2.group(1).strip()
                parts = blob.split()
                for p in parts:
                    if "=" in p:
                        k, v = p.split("=", 1)
                        try:
                            scan[k] = int(v)
                        except Exception:
                            scan[k] = v
        if top_raw is not None and scan:
            break

    return {
        "raw": top_raw or 0,
        "after_filters": top_after or 0,
        "scan": scan or {},
    }


def _filters_today_from_log(strategy):
    log = LOG_MR_V1
    if strategy == "mean_reversion_v2" and LOG_MR_V2:
        log = LOG_MR_V2

    lines = _read_tail_lines(log, max_lines=6000)
    parsed = _parse_latest_scan_from_log(lines)
    scan = parsed["scan"]

    return {
        "markets_scanned": parsed["raw"],
        "after_filters": parsed["after_filters"],
        "scan_entries": int(scan.get("entries", 0) or 0),
        "blocked": {
            "cap_per_market_outcome": int(scan.get("cap_per_market_outcome", 0) or 0),
            "dislo_not_negative": int(scan.get("dislo_not_negative", 0) or 0),
            "dislo_too_small": int(scan.get("dislo_too_small", 0) or 0),
            "dislo_too_big": int(scan.get("dislo_too_big", 0) or 0),
            "market_banned": int(scan.get("market_banned", 0) or 0),
            "px_oob": int(scan.get("px_oob", 0) or 0),
            "stale": int(scan.get("stale", 0) or 0),
        }
    }


@app.route("/")
def index():
    view = request.args.get("view", "diagnostics")
    if view not in ("command", "diagnostics"):
        view = "diagnostics"

    strategy = request.args.get("strategy", "mean_reversion_v1")
    if strategy not in STRATEGIES:
        strategy = "mean_reversion_v1"

    mode = request.args.get("mode", "live")
    if mode not in MODES:
        mode = "live"

    now_dt = datetime.now(timezone.utc)
    now_utc = now_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    db_url_short = DB_URL.split("@")[-1] if "@" in DB_URL else DB_URL

    page_error = None

    health = {
        "db": {"status": "na", "text": "unknown"},
        "ingest": {"status": "na", "text": "unknown"},
        "tmux": check_tmux_sessions(),
        "bots": {"status": "na", "text": "na"},
        "dashboard": {"status": "ok", "text": "serving"},
    }

    cc = {
        "status": {},
        "kill": {},
        "perf": {},
        "filters": {
            "markets_scanned": 0,
            "after_filters": 0,
            "scan_entries": 0,
            "trades_executed_today": 0,
            "blocked": {},
        },
        "problems": [],
        "intel": {"review": [], "top": []},
    }

    diag = {
        "today_pnl": 0.0,
        "pnl_24h": 0.0,
        "pnl_7d": 0.0,
        "total_pnl": 0.0,
        "closed_trades": 0,
        "open_trades": 0,
        "winrate": None,
        "open_positions": [],
        "recent_closed": [],
        "killed_markets": [],
        "dislo_buckets": [],
        "entry_buckets": [],
        "worst_markets": [],
        "best_markets": [],
    }

    try:
        with get_conn() as conn, conn.cursor() as cur:
            # DB ping
            _ = _safe_fetchone(cur, "SELECT 1 AS ok;")
            health["db"] = {"status": "ok", "text": "connected"}

            # Ingest freshness
            r = _safe_fetchone(cur, "SELECT MAX(ts) AS max_ts FROM raw_trades;")
            max_ts = r.get("max_ts")
            ingest_lag = None
            if max_ts:
                ingest_lag = (now_dt - max_ts).total_seconds()
                if ingest_lag <= INGEST_STALE_SECS:
                    health["ingest"] = {"status": "ok", "text": f"fresh ({int(ingest_lag)}s lag)"}
                else:
                    health["ingest"] = {"status": "warn", "text": f"stale ({int(ingest_lag)}s lag)"}
            else:
                health["ingest"] = {"status": "bad", "text": "no trades yet"}

            # Activity-based bots health
            latest_flow_ts = _safe_fetchone(cur, "SELECT MAX(ts) AS ts FROM flow_snapshots;").get("ts")
            smartflow_age = _age_from_ts(latest_flow_ts)

            mr_v1_age = _file_age_secs(LOG_MR_V1)
            mr_v2_age = (_file_age_secs(LOG_MR_V2) if LOG_MR_V2 else None)

            levels = [_bot_level(smartflow_age), _bot_level(mr_v1_age), _bot_level(mr_v2_age)]
            if "bad" in levels:
                bots_level = "bad"
            elif "warn" in levels or "na" in levels:
                bots_level = "warn"
            else:
                bots_level = "ok"
            health["bots"] = {
                "status": bots_level,
                "text": f"sf {_fmt_age(smartflow_age)} - mr_v1 {_fmt_age(mr_v1_age)} - mr_v2 {_fmt_age(mr_v2_age)}",
            }

            # Load open positions for selected mode
            if mode in ("live", "paper"):
                open_positions = _load_open_positions(cur, strategy, mode)
            else:
                live_open = _load_open_positions(cur, strategy, "live")
                paper_open = _load_open_positions(cur, strategy, "paper")
                open_positions = live_open + paper_open

            # Diagnostics rollup (selected mode)
            if view != "command":
                if mode in ("live", "paper"):
                    diag_roll = _load_closed_rollups(cur, strategy, mode)
                else:
                    l = _load_closed_rollups(cur, strategy, "live")
                    p = _load_closed_rollups(cur, strategy, "paper")
                    closed_trades = (l["closed_trades"] or 0) + (p["closed_trades"] or 0)
                    winners = 0
                    if l["winrate"] is not None:
                        winners += int(round(l["winrate"] * l["closed_trades"]))
                    if p["winrate"] is not None:
                        winners += int(round(p["winrate"] * p["closed_trades"]))
                    winrate = (winners / closed_trades) if closed_trades else None
                    diag_roll = {
                        "today_pnl": l["today_pnl"] + p["today_pnl"],
                        "pnl_24h": l["pnl_24h"] + p["pnl_24h"],
                        "pnl_7d": l["pnl_7d"] + p["pnl_7d"],
                        "total_pnl": l["total_pnl"] + p["total_pnl"],
                        "closed_trades": closed_trades,
                        "open_trades": (l["open_trades"] or 0) + (p["open_trades"] or 0),
                        "winrate": winrate,
                    }
                diag.update(diag_roll)
                diag["open_positions"] = open_positions

                # Recent closed
                if mode in ("live", "paper"):
                    diag["recent_closed"] = _load_recent_closed(cur, strategy, mode, RECENT_CLOSED_LIMIT)
                    diag["killed_markets"] = _load_killed_markets(cur, strategy, mode, KILLED_MARKETS_LIMIT)
                else:
                    # best effort: show from live
                    diag["recent_closed"] = _load_recent_closed(cur, strategy, "live", RECENT_CLOSED_LIMIT)
                    diag["killed_markets"] = _load_killed_markets(cur, strategy, "live", KILLED_MARKETS_LIMIT)

                # Buckets
                if mode in ("live", "paper"):
                    rows_for_buckets = _load_closed_rows_for_buckets(cur, strategy, mode)
                else:
                    rows_for_buckets = _load_closed_rows_for_buckets(cur, strategy, "live")

                dislo_edges = _bucket_edges(DISLO_BUCKET_MIN, DISLO_BUCKET_MAX, DISLO_BUCKET_STEP)
                diag["dislo_buckets"] = _make_buckets(
                    rows_for_buckets,
                    key_fn=lambda r: r.get("dislocation"),
                    edges=dislo_edges
                )

                entry_edges = _bucket_edges(ENTRY_BUCKET_MIN, ENTRY_BUCKET_MAX, ENTRY_BUCKET_STEP)
                diag["entry_buckets"] = _make_buckets(
                    rows_for_buckets,
                    key_fn=lambda r: r.get("entry_price"),
                    edges=entry_edges
                )

                # Best/Worst markets 24h
                if mode in ("live", "paper"):
                    worst, best = _load_best_worst_markets_24h(cur, strategy, mode, BEST_WORST_MARKETS_LIMIT)
                else:
                    worst, best = _load_best_worst_markets_24h(cur, strategy, "live", BEST_WORST_MARKETS_LIMIT)
                diag["worst_markets"] = worst
                diag["best_markets"] = best

            # Command Center
            if view == "command":
                limits = _limits_for_mode("paper" if mode == "paper" else "live")

                # last entry / exit times
                last_entry_ts = None
                last_exit_ts = None
                if mode in ("live", "paper"):
                    tbl = _positions_table_for_mode(mode)
                    rr = _safe_fetchone(
                        cur,
                        f"""
                        SELECT MAX(entry_ts) AS last_entry, MAX(exit_ts) AS last_exit
                        FROM {tbl}
                        WHERE (%s='all' OR strategy=%s);
                        """,
                        (strategy, strategy),
                    )
                    last_entry_ts = rr.get("last_entry")
                    last_exit_ts = rr.get("last_exit")
                else:
                    rr = _safe_fetchone(
                        cur,
                        """
                        SELECT
                          GREATEST(
                            (SELECT MAX(entry_ts) FROM mr_positions WHERE (%s='all' OR strategy=%s)),
                            (SELECT MAX(entry_ts) FROM paper_positions WHERE (%s='all' OR strategy=%s))
                          ) AS last_entry,
                          GREATEST(
                            (SELECT MAX(exit_ts) FROM mr_positions WHERE (%s='all' OR strategy=%s)),
                            (SELECT MAX(exit_ts) FROM paper_positions WHERE (%s='all' OR strategy=%s))
                          ) AS last_exit;
                        """,
                        (strategy, strategy, strategy, strategy, strategy, strategy, strategy, strategy),
                    )
                    last_entry_ts = rr.get("last_entry")
                    last_exit_ts = rr.get("last_exit")

                last_entry_age = _age_from_ts(last_entry_ts)
                last_exit_age = _age_from_ts(last_exit_ts)

                ingest_level = "bad" if ingest_lag is None else ("ok" if ingest_lag <= INGEST_STALE_SECS else "warn")

                cc["status"] = {
                    "db_level": "ok",
                    "db_text": "OK",
                    "ingest_level": ingest_level,
                    "ingest_text": "no trades" if ingest_lag is None else f"{int(ingest_lag)}s",
                    "tmux_level": health["tmux"]["status"],
                    "tmux_text": "OK" if health["tmux"]["status"] == "ok" else health["tmux"]["text"],
                    "bots_level": bots_level,
                    "bots_text": f"sf {_fmt_age(smartflow_age)} - mr_v1 {_fmt_age(mr_v1_age)} - mr_v2 {_fmt_age(mr_v2_age)}",
                    "last_entry_text": _fmt_age(last_entry_age),
                    "last_exit_text": _fmt_age(last_exit_age),
                }

                # daily pnl and 24h winrate for selected mode (use rollups)
                if mode in ("live", "paper"):
                    roll = _load_closed_rollups(cur, strategy, mode)
                else:
                    l = _load_closed_rollups(cur, strategy, "live")
                    p = _load_closed_rollups(cur, strategy, "paper")
                    roll = {
                        "today_pnl": l["today_pnl"] + p["today_pnl"],
                        "pnl_24h": l["pnl_24h"] + p["pnl_24h"],
                        "pnl_7d": l["pnl_7d"] + p["pnl_7d"],
                        "total_pnl": l["total_pnl"] + p["total_pnl"],
                        "closed_trades": (l["closed_trades"] or 0) + (p["closed_trades"] or 0),
                        "open_trades": (l["open_trades"] or 0) + (p["open_trades"] or 0),
                        "winrate": None,
                    }

                daily_pnl = Decimal(str(roll["today_pnl"]))
                daily_level = "ok" if daily_pnl >= limits["daily_loss"] else "bad"

                # worst open
                worst_open = None
                for op in open_positions:
                    u = to_dec(op.get("unrealized_pnl"))
                    if u is None:
                        continue
                    worst_open = u if worst_open is None else min(worst_open, u)
                if worst_open is None:
                    worst_open = Decimal("0")
                worst_open_level = "ok" if worst_open >= limits["worst_open"] else "bad"

                # global loss streak (live best)
                streak = 0
                if mode == "live":
                    rows = _safe_fetchall(
                        cur,
                        """
                        SELECT pnl
                        FROM mr_positions
                        WHERE (%s='all' OR strategy=%s)
                          AND COALESCE(status,'closed')='closed'
                          AND pnl IS NOT NULL
                        ORDER BY exit_ts DESC
                        LIMIT 50;
                        """,
                        (strategy, strategy),
                    )
                    for r0 in rows:
                        pnl = to_dec(r0.get("pnl"))
                        if pnl is None:
                            continue
                        if pnl < 0:
                            streak += 1
                        else:
                            break
                loss_streak_level = "ok" if streak < limits["max_streak"] else "bad"

                # 24h winrate (live best)
                trades_24h = 0
                wins_24h = 0
                if mode == "live":
                    w = _safe_fetchone(
                        cur,
                        """
                        SELECT COUNT(*) AS trades, COUNT(*) FILTER (WHERE pnl > 0) AS wins
                        FROM mr_positions
                        WHERE (%s='all' OR strategy=%s)
                          AND COALESCE(status,'closed')='closed'
                          AND exit_ts >= (NOW() - INTERVAL '24 hours');
                        """,
                        (strategy, strategy),
                    )
                    trades_24h = int(w.get("trades") or 0)
                    wins_24h = int(w.get("wins") or 0)

                winrate_24h = None
                winrate_level = "na"
                if trades_24h >= limits["min_trades_24h"]:
                    winrate_24h = float(wins_24h / trades_24h) if trades_24h else None
                    if winrate_24h is not None and Decimal(str(winrate_24h)) >= limits["winrate_floor"]:
                        winrate_level = "ok"
                    else:
                        winrate_level = "warn"

                if daily_level == "bad" or worst_open_level == "bad" or loss_streak_level == "bad":
                    system_level = "bad"
                elif winrate_level == "warn" or ingest_level == "warn" or bots_level == "warn":
                    system_level = "warn"
                else:
                    system_level = "ok"

                cc["kill"] = {
                    "daily_pnl": float(daily_pnl),
                    "daily_limit": float(limits["daily_loss"]),
                    "daily_level": daily_level,
                    "worst_open": float(worst_open),
                    "worst_open_limit": float(limits["worst_open"]),
                    "worst_open_level": worst_open_level,
                    "loss_streak": streak,
                    "loss_streak_limit": int(limits["max_streak"]),
                    "loss_streak_level": loss_streak_level,
                    "trades_24h": trades_24h,
                    "min_trades_24h": int(limits["min_trades_24h"]),
                    "winrate_24h": winrate_24h,
                    "winrate_floor": float(limits["winrate_floor"]),
                    "winrate_level": winrate_level,
                    "system_level": system_level,
                }

                # performance snapshot (Today / Yesterday / 7d / All)
                cc["perf"] = _load_performance_snapshot(cur, strategy, mode)

                # Filter performance: parse log + trades executed today
                fp = _filters_today_from_log(strategy)

                executed_today = 0
                if mode in ("live", "paper"):
                    tbl = _positions_table_for_mode(mode)
                    rr = _safe_fetchone(
                        cur,
                        f"""
                        SELECT COUNT(*) AS n
                        FROM {tbl}
                        WHERE (%s='all' OR strategy=%s)
                          AND entry_ts >= CURRENT_DATE;
                        """,
                        (strategy, strategy),
                    )
                    executed_today = int(rr.get("n") or 0)
                else:
                    rr = _safe_fetchone(
                        cur,
                        """
                        SELECT
                          (SELECT COUNT(*) FROM mr_positions WHERE (%s='all' OR strategy=%s) AND entry_ts >= CURRENT_DATE) +
                          (SELECT COUNT(*) FROM paper_positions WHERE (%s='all' OR strategy=%s) AND entry_ts >= CURRENT_DATE)
                          AS n;
                        """,
                        (strategy, strategy, strategy, strategy),
                    )
                    executed_today = int(rr.get("n") or 0)

                cc["filters"] = {
                    "markets_scanned": fp["markets_scanned"],
                    "after_filters": fp["after_filters"],
                    "scan_entries": fp["scan_entries"],
                    "trades_executed_today": executed_today,
                    "blocked": fp["blocked"],
                }

                # Problems
                problems = []
                for op in open_positions:
                    entry = Decimal(str(op.get("entry_price") or 0))
                    last = Decimal(str(op.get("last_price") or entry))
                    age_h = Decimal(str(op.get("hours_open") or 0))
                    unreal = Decimal(str(op.get("unrealized_pnl") or 0))
                    px_pct = Decimal(str(op.get("px_change_pct") or 0))
                    dislo = to_dec(op.get("dislocation")) or Decimal("0")

                    flags = []
                    if age_h >= DASH_PROBLEM_AGE_HOURS:
                        flags.append("OLD")
                    if unreal <= DASH_PROBLEM_UNREAL_USD or px_pct <= DASH_PROBLEM_UNREAL_PCT:
                        flags.append("UNDERWATER")
                    if not flags:
                        continue

                    problems.append({
                        "age_h": float(age_h),
                        "market_name": op.get("market_name") or op.get("market_id"),
                        "tags": op.get("market_tags") or "",
                        "entry_px": float(entry),
                        "last_px": float(last),
                        "dislo_pct": float(dislo * 100),
                        "unreal": float(unreal),
                        "flag": " + ".join(flags),
                    })
                cc["problems"] = sorted(problems, key=lambda x: (x["unreal"], -x["age_h"]))[:10]

                # Market intel (live only - schema known)
                if mode == "live":
                    rows = _safe_fetchall(
                        cur,
                        """
                        SELECT
                          p.market_id,
                          COALESCE(m.question, p.market_id) AS market_name,
                          COUNT(*) AS trades,
                          COALESCE(SUM(p.pnl), 0) AS sum_pnl,
                          AVG(CASE WHEN p.pnl > 0 THEN 1.0 ELSE 0.0 END) AS winrate,
                          MAX(p.exit_ts) AS last_exit_ts
                        FROM mr_positions p
                        LEFT JOIN markets m ON m.market_id = p.market_id
                        WHERE (%s = 'all' OR p.strategy = %s)
                          AND p.status='closed'
                          AND p.exit_ts IS NOT NULL
                        GROUP BY p.market_id, market_name
                        HAVING COUNT(*) >= %s;
                        """,
                        (strategy, strategy, DASH_MIN_TRADES_REVIEW),
                    )
                    review = []
                    top = []
                    for r0 in rows:
                        trades = int(r0.get("trades") or 0)
                        sum_pnl = to_dec(r0.get("sum_pnl")) or Decimal("0")
                        winrate = float(r0.get("winrate") or 0.0)
                        last_exit_ts = r0.get("last_exit_ts")
                        last_age = _fmt_age(_age_from_ts(last_exit_ts))

                        item = {
                            "market_name": r0.get("market_name") or r0.get("market_id"),
                            "trades": trades,
                            "sum_pnl": float(sum_pnl),
                            "winrate": winrate,
                            "last_age": last_age,
                        }
                        if sum_pnl <= DASH_REVIEW_PNL_THRESHOLD or Decimal(str(winrate)) <= DASH_REVIEW_WR_THRESHOLD:
                            review.append(item)
                        if sum_pnl >= DASH_TOP_PNL_THRESHOLD:
                            top.append(item)

                    cc["intel"]["review"] = sorted(review, key=lambda x: x["sum_pnl"])[:5]
                    cc["intel"]["top"] = sorted(top, key=lambda x: x["sum_pnl"], reverse=True)[:5]

    except Exception as e:
        health["db"] = {"status": "bad", "text": "FAILED"}
        health["ingest"] = {"status": "na", "text": "unknown"}
        health["bots"] = {"status": "na", "text": "na"}
        page_error = str(e)

    if health["tmux"]["status"] == "bad":
        health["bots"] = {"status": "bad", "text": "tmux missing sessions"}

    return render_template_string(
        HTML,
        db_url_short=db_url_short,
        now_utc=now_utc,
        refresh_secs=REFRESH_SECS,
        view=view,
        strategy=strategy,
        strategies=STRATEGIES,
        mode=mode,
        modes=MODES,
        health=health,
        page_error=page_error,
        cc=cc,
        diag=diag,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.getenv("DASH_PORT", "5002")), debug=False)