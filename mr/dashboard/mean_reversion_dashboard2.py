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


def _tail_text(p: Path, max_bytes: int = 2_000_000) -> str:
    """
    Read only the tail of a potentially large file to avoid blocking the dashboard.
    Returns up to max_bytes from the end (best-effort).
    """
    try:
        if not p or not p.exists():
            return ""
        with p.open("rb") as f:
            try:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - max_bytes), 0)
            except Exception:
                f.seek(0)
            b = f.read()
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return ""
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

# ------------------------------------------------------------
# DB idle-in-transaction health
# ------------------------------------------------------------
DASH_DB_IDLE_TX_WARN_SECS = int(os.getenv("DASH_DB_IDLE_TX_WARN_SECS", "300"))  # 5m
DASH_DB_IDLE_TX_BAD_SECS = int(os.getenv("DASH_DB_IDLE_TX_BAD_SECS", "600"))  # 10m
DASH_DB_IDLE_TX_BAD_COUNT = int(os.getenv("DASH_DB_IDLE_TX_BAD_COUNT", "2"))

HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Mean Reversion Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{{ refresh_secs }}">
  <style>
    :root{
      --bg:#0f1115;
      --panel:#151924;
      --panel2:#121621;
      --border:#2a3142;
      --text:#e9edf5;
      --muted:#aab2c5;
      --muted2:#8089a3;
      --ok:#2ecc71;
      --warn:#f1c40f;
      --bad:#ff4d4d;
      --link:#9db7ff;
      --shadow: 0 10px 25px rgba(0,0,0,.30);
      --r:14px;
    }
    * { box-sizing: border-box; }
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
      margin: 18px;
      background: var(--bg);
      color: var(--text);
    }
    a { color: var(--link); }
    h1 { font-size: 1.35rem; margin: 0.2rem 0; letter-spacing: 0.01em; }
    h2 { font-size: 1.05rem; margin: 1.1rem 0 0.45rem 0; color: #dfe6ff; }
    h3 { font-size: 0.92rem; margin: 0.9rem 0 0.35rem 0; color: #cfd6ef; }
    .small { font-size: 0.78rem; color: var(--muted); }
    .muted { color: var(--muted2); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

    .topbar {
      position: sticky;
      top: 10px;
      z-index: 50;
      padding: 10px 12px;
      background: rgba(15,17,21,0.75);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(42,49,66,0.6);
      border-radius: var(--r);
      box-shadow: var(--shadow);
      margin-bottom: 12px;
    }
    .row { display:flex; gap: 12px; align-items:center; flex-wrap: wrap; }
    .grow { flex: 1 1 auto; }
    .tabs { display:flex; gap: 10px; flex-wrap: wrap; align-items:center; }
    .tab {
      background: var(--panel2);
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 7px 10px;
      text-decoration: none;
      display: inline-flex;
      gap: 8px;
      align-items: center;
    }
    .tab.active { border-color: rgba(46,204,113,0.65); box-shadow: 0 0 0 3px rgba(46,204,113,0.10); }
    .tab .kbd { font-size: 0.70rem; color: var(--muted); border:1px solid var(--border); padding: 2px 6px; border-radius: 8px; background: rgba(255,255,255,0.03); }

    .toolbar {
      display:flex; gap: 10px; align-items: center; flex-wrap: wrap;
      margin-top: 10px;
    }
    label { color: var(--muted); font-size: 0.78rem; }
    select, input[type="text"]{
      background: var(--panel2);
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 8px 10px;
      outline: none;
    }
    input[type="text"]{ min-width: 240px; }
    .btn {
      background: rgba(157,183,255,0.10);
      border: 1px solid rgba(157,183,255,0.35);
      color: var(--text);
      border-radius: 12px;
      padding: 8px 10px;
      cursor: pointer;
    }
    .btn:hover { background: rgba(157,183,255,0.16); }

    .healthbar { display:flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }
    .pill {
      display:flex; align-items:center; gap: 10px;
      padding: 9px 10px;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--panel);
    }
    .dot { width: 10px; height: 10px; border-radius: 50%; background:#666; }
    .pill .label { font-size: 0.70rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
    .pill .value { font-size: 0.80rem; color: var(--text); }
    .ok .dot { background: var(--ok); }
    .warn .dot { background: var(--warn); }
    .bad .dot { background: var(--bad); }
    .na .dot { background: #666; }
    .pill.ok { box-shadow: 0 0 0 3px rgba(46,204,113,0.10); }
    .pill.warn { box-shadow: 0 0 0 3px rgba(241,196,15,0.10); }
    .pill.bad { box-shadow: 0 0 0 3px rgba(255,77,77,0.10); }

    .grid { display:grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; margin-top: 12px; }
    @media (max-width: 1200px) { .grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
    @media (max-width: 750px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--r);
      padding: 10px 12px;
      box-shadow: var(--shadow);
      min-height: 74px;
    }
    .card-label { font-size: 0.70rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px; }
    .card-value { font-size: 1.05rem; font-weight: 750; }
    .pnl-pos { color: var(--ok); font-weight: 750; }
    .pnl-neg { color: var(--bad); font-weight: 750; }
    .warn-txt { color: var(--warn); font-weight: 750; }

    details.section {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--r);
      box-shadow: var(--shadow);
      margin: 12px 0;
      overflow: hidden;
    }
    details.section > summary {
      list-style: none;
      cursor: pointer;
      padding: 10px 12px;
      display:flex;
      gap: 10px;
      align-items: center;
      border-bottom: 1px solid rgba(42,49,66,0.65);
      user-select: none;
    }
    details.section > summary::-webkit-details-marker { display:none; }
    .sumtitle { font-weight: 750; }
    .sumhint { font-size: 0.78rem; color: var(--muted); }
    .content { padding: 10px 12px 12px 12px; }

    table { border-collapse: separate; border-spacing: 0; width: 100%; margin-top: 8px; font-size: 0.82rem; }
    th, td { border-bottom: 1px solid rgba(42,49,66,0.65); padding: 8px 8px; text-align: left; vertical-align: middle; }
    th {
      color: #dfe6ff;
      font-weight: 700;
      position: sticky;
      top: 5px;                 /* was 70px */
      background: rgba(21,25,36,0.98);
      z-index: 5;
    }
    tr:hover td { background: rgba(157,183,255,0.06); }
    .nowrap { white-space: nowrap; }
    .right { text-align: right; }
    .chip {
      display:inline-flex; gap: 8px; align-items:center;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid rgba(42,49,66,0.85);
      background: rgba(0,0,0,0.12);
      color: var(--muted);
      font-size: 0.75rem;
    }
    .copy {
      border: 1px solid rgba(42,49,66,0.9);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      padding: 2px 7px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 0.74rem;
    }
    .copy:hover { background: rgba(255,255,255,0.07); }

    .errorbox {
      margin-top: 12px;
      padding: 10px 12px;
      background: rgba(255,77,77,0.08);
      border: 1px solid rgba(255,77,77,0.35);
      border-radius: var(--r);
      color: #ffb3b3;
      box-shadow: var(--shadow);
    }
    .subtools { display:flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-top: 8px; }
    .hint { font-size: 0.78rem; color: var(--muted); }

    .kpirow { display:flex; gap: 10px; flex-wrap: wrap; margin-top: 8px; }
    .kpi { padding: 6px 10px; border: 1px solid rgba(42,49,66,0.85); background: rgba(0,0,0,0.12); border-radius: 999px; font-size: 0.78rem; color: var(--muted); }
  </style>
  <script>
    const refreshSecs = {{ refresh_secs }};
    let remaining = refreshSecs;

    function tick() {
      const el = document.getElementById("refresh_left");
      if (el) el.textContent = remaining;
      remaining = remaining > 0 ? remaining - 1 : 0;
    }

    function copyText(txt){
      try {
        navigator.clipboard.writeText(txt);
      } catch(e) {}
    }

    function wireCopyButtons(){
      document.querySelectorAll("[data-copy]").forEach(btn => {
        btn.addEventListener("click", () => copyText(btn.getAttribute("data-copy") || ""));
      });
    }

    function filterTable(tableId, query){
      const q = (query || "").toLowerCase().trim();
      const t = document.getElementById(tableId);
      if (!t) return;
      const rows = t.querySelectorAll("tbody tr");
      rows.forEach(r => {
        const txt = r.innerText.toLowerCase();
        r.style.display = (q === "" || txt.includes(q)) ? "" : "none";
      });
    }

    document.addEventListener("DOMContentLoaded", () => {
      tick();
      setInterval(tick, 1000);
      wireCopyButtons();

      const globalFilter = document.getElementById("global_filter");
      if (globalFilter) {
        globalFilter.addEventListener("input", () => {
          const q = globalFilter.value;
          document.querySelectorAll("table[data-filterable='1']").forEach(tbl => {
            filterTable(tbl.id, q);
          });
        });
      }
    });
  </script>
</head>
<body>

  <div class="topbar">
    <div class="row">
      <div class="grow">
        <h1>Mean Reversion Dashboard</h1>
        <div class="small">
          DB: {{ db_url_short }} - Updated: {{ now_utc }}
          - Strategy: <strong>{{ strategy }}</strong>
          - Mode: <strong>{{ mode }}</strong>
          - Refresh: {{ refresh_secs }}s (in <span id="refresh_left">{{ refresh_secs }}</span>s)
        </div>
      </div>

      <div class="tabs">
        <a class="tab {% if view == 'command' %}active{% endif %}" href="/?view=command&strategy={{ strategy }}&mode={{ mode }}">
          Command <span class="kbd">K</span>
        </a>
        <a class="tab {% if view != 'command' %}active{% endif %}" href="/?view=diagnostics&strategy={{ strategy }}&mode={{ mode }}">
          Diagnostics <span class="kbd">D</span>
        </a>
      </div>
    </div>

    <div class="toolbar">
      <form method="get" class="row">
        <input type="hidden" name="view" value="{{ view }}">

        <label for="strategy">Strategy</label>
        <select id="strategy" name="strategy" onchange="this.form.submit()">
          {% for s in strategies %}
            <option value="{{ s }}" {% if s == strategy %}selected{% endif %}>{{ s }}</option>
          {% endfor %}
        </select>

        <label for="mode">Mode</label>
        <select id="mode" name="mode" onchange="this.form.submit()">
          {% for m in modes %}
            <option value="{{ m }}" {% if m == mode %}selected{% endif %}>{{ m }}</option>
          {% endfor %}
        </select>

        <label for="global_filter">Filter tables</label>
        <input id="global_filter" type="text" placeholder="type to filter rows (client-side)">
      </form>
    </div>

    <div class="healthbar">
      <div class="pill {{ health.db.status }}">
        <span class="dot"></span>
        <span class="label">DB</span>
        <span class="value">{{ health.db.text }}</span>
      </div>
      <div class="pill {{ health.db_tx.status }}">
        <span class="dot"></span>
        <span class="label">DB TX</span>
        <span class="value">{{ health.db_tx.text }}</span>
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
        <span class="label">Dash</span>
        <span class="value">{{ health.dashboard.text }}</span>
      </div>
    </div>
  </div>

  {% if page_error %}
    <div class="errorbox">
      Error loading data: {{ page_error }}
    </div>
  {% endif %}

  {% if view == 'command' %}

    <div class="grid">
      <div class="card">
        <div class="card-label">System</div>
        <div class="card-value {% if cc.kill.system_level == 'ok' %}pnl-pos{% elif cc.kill.system_level == 'warn' %}warn-txt{% else %}pnl-neg{% endif %}">
          {{ cc.kill.system_level|upper }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Daily PnL</div>
        <div class="card-value {% if cc.kill.daily_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(cc.kill.daily_pnl) }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Worst Open</div>
        <div class="card-value {% if cc.kill.worst_open < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">
          {{ "%.2f"|format(cc.kill.worst_open) }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Loss Streak</div>
        <div class="card-value {% if cc.kill.loss_streak_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">
          {{ cc.kill.loss_streak }}
        </div>
      </div>
      <div class="card">
        <div class="card-label">WR (24h)</div>
        <div class="card-value {% if cc.kill.winrate_level == 'ok' %}pnl-pos{% elif cc.kill.winrate_level == 'warn' %}warn-txt{% else %}muted{% endif %}">
          {% if cc.kill.winrate_24h is none %}na{% else %}{{ "%.1f"|format(cc.kill.winrate_24h * 100) }}%{% endif %}
        </div>
      </div>
      <div class="card">
        <div class="card-label">Bots</div>
        <div class="card-value {% if cc.status.bots_level == 'ok' %}pnl-pos{% elif cc.status.bots_level == 'warn' %}warn-txt{% else %}pnl-neg{% endif %}">
          {{ cc.status.bots_text }}
        </div>
      </div>
    </div>

    <details class="section" open>
      <summary>
        <span class="sumtitle">Performance Snapshot</span>
        <span class="sumhint">today / yesterday / 7d / all</span>
      </summary>
      <div class="content">
        <table id="tbl_perf" data-filterable="1">
          <thead>
            <tr>
              <th>Metric</th>
              <th class="right">Today</th>
              <th class="right">Yesterday</th>
              <th class="right">Last 7d</th>
              <th class="right">All time</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Total PnL</td>
              <td class="right {% if cc.perf.pnls.ptoday < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.ptoday) }}</td>
              <td class="right {% if cc.perf.pnls.pyday < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.pyday) }}</td>
              <td class="right {% if cc.perf.pnls.p7 < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.p7) }}</td>
              <td class="right {% if cc.perf.pnls.pall < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.perf.pnls.pall) }}</td>
              <td class="{{ cc.perf.status.pnl_class }}">{{ cc.perf.status.pnl_text }}</td>
            </tr>
            <tr>
              <td>Trades</td>
              <td class="right">{{ cc.perf.trades.ttoday }}</td>
              <td class="right">{{ cc.perf.trades.tyday }}</td>
              <td class="right">{{ cc.perf.trades.t7 }}</td>
              <td class="right">{{ cc.perf.trades.tall }}</td>
              <td class="muted">-</td>
            </tr>
            <tr>
              <td>Win rate</td>
              <td class="right">{{ "%.1f"|format(cc.perf.winrate.wtoday * 100) if cc.perf.winrate.wtoday is not none else "na" }}%</td>
              <td class="right">{{ "%.1f"|format(cc.perf.winrate.wyday * 100) if cc.perf.winrate.wyday is not none else "na" }}%</td>
              <td class="right">{{ "%.1f"|format(cc.perf.winrate.w7 * 100) if cc.perf.winrate.w7 is not none else "na" }}%</td>
              <td class="right">{{ "%.1f"|format(cc.perf.winrate.wall * 100) if cc.perf.winrate.wall is not none else "na" }}%</td>
              <td class="{{ cc.perf.status.wr_class }}">{{ cc.perf.status.wr_text }}</td>
            </tr>
            <tr>
              <td>Avg PnL / trade</td>
              <td class="right">{{ "%.2f"|format(cc.perf.avgpnl.atoday) if cc.perf.avgpnl.atoday is not none else "na" }}</td>
              <td class="right">{{ "%.2f"|format(cc.perf.avgpnl.ayday) if cc.perf.avgpnl.ayday is not none else "na" }}</td>
              <td class="right">{{ "%.2f"|format(cc.perf.avgpnl.a7) if cc.perf.avgpnl.a7 is not none else "na" }}</td>
              <td class="right">{{ "%.2f"|format(cc.perf.avgpnl.aall) if cc.perf.avgpnl.aall is not none else "na" }}</td>
              <td class="muted">-</td>
            </tr>
            <tr>
              <td>Max SL rate</td>
              <td class="right">{{ "%.1f"|format(cc.perf.slrate.stoday * 100) if cc.perf.slrate.stoday is not none else "na" }}%</td>
              <td class="right">{{ "%.1f"|format(cc.perf.slrate.syday * 100) if cc.perf.slrate.syday is not none else "na" }}%</td>
              <td class="right">{{ "%.1f"|format(cc.perf.slrate.s7 * 100) if cc.perf.slrate.s7 is not none else "na" }}%</td>
              <td class="right">{{ "%.1f"|format(cc.perf.slrate.sall * 100) if cc.perf.slrate.sall is not none else "na" }}%</td>
              <td class="{{ cc.perf.status.sl_class }}">{{ cc.perf.status.sl_text }}</td>
            </tr>
            <tr>
              <td>Largest loss</td>
              <td class="right pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.ltoday) if cc.perf.largestloss.ltoday is not none else "na" }}</td>
              <td class="right pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.lyday) if cc.perf.largestloss.lyday is not none else "na" }}</td>
              <td class="right pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.l7) if cc.perf.largestloss.l7 is not none else "na" }}</td>
              <td class="right pnl-neg">{{ "%.2f"|format(cc.perf.largestloss.lall) if cc.perf.largestloss.lall is not none else "na" }}</td>
              <td class="muted">-</td>
            </tr>
            <tr>
              <td>Sharpe (trade-level)</td>
              <td class="right">{{ "%.2f"|format(cc.perf.sharpe.shtoday) if cc.perf.sharpe.shtoday is not none else "na" }}</td>
              <td class="right">{{ "%.2f"|format(cc.perf.sharpe.shyday) if cc.perf.sharpe.shyday is not none else "na" }}</td>
              <td class="right">{{ "%.2f"|format(cc.perf.sharpe.sh7) if cc.perf.sharpe.sh7 is not none else "na" }}</td>
              <td class="right">{{ "%.2f"|format(cc.perf.sharpe.shall) if cc.perf.sharpe.shall is not none else "na" }}</td>
              <td class="{{ cc.perf.status.sh_class }}">{{ cc.perf.status.sh_text }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </details>

    <details class="section" open>
      <summary>
        <span class="sumtitle">Kill Switch Monitor</span>
        <span class="sumhint">limits by mode</span>
      </summary>
      <div class="content">
        <table id="tbl_kill" data-filterable="1">
          <thead>
            <tr><th>Metric</th><th class="right">Current</th><th class="right">Limit</th><th>Status</th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Daily PnL</td>
              <td class="right {% if cc.kill.daily_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.kill.daily_pnl) }}</td>
              <td class="right">{{ "%.2f"|format(cc.kill.daily_limit) }}</td>
              <td class="{% if cc.kill.daily_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.kill.daily_level|upper }}</td>
            </tr>
            <tr>
              <td>Worst Open Unrealized</td>
              <td class="right {% if cc.kill.worst_open < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(cc.kill.worst_open) }}</td>
              <td class="right">{{ "%.2f"|format(cc.kill.worst_open_limit) }}</td>
              <td class="{% if cc.kill.worst_open_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.kill.worst_open_level|upper }}</td>
            </tr>
            <tr>
              <td>Global Loss Streak</td>
              <td class="right">{{ cc.kill.loss_streak }}</td>
              <td class="right">{{ cc.kill.loss_streak_limit }}</td>
              <td class="{% if cc.kill.loss_streak_level == 'ok' %}pnl-pos{% else %}pnl-neg{% endif %}">{{ cc.kill.loss_streak_level|upper }}</td>
            </tr>
            <tr>
              <td>Winrate (24h)</td>
              <td class="right">
                {% if cc.kill.winrate_24h is none %}
                  <span class="muted">na ({{ cc.kill.trades_24h }} trades)</span>
                {% else %}
                  {{ "%.1f"|format(cc.kill.winrate_24h * 100) }}% ({{ cc.kill.trades_24h }} trades)
                {% endif %}
              </td>
              <td class="right">{{ "%.1f"|format(cc.kill.winrate_floor * 100) }}% (min {{ cc.kill.min_trades_24h }})</td>
              <td class="{% if cc.kill.winrate_level == 'ok' %}pnl-pos{% elif cc.kill.winrate_level == 'na' %}muted{% else %}warn-txt{% endif %}">
                {{ cc.kill.winrate_level|upper }}
              </td>
            </tr>
          </tbody>
        </table>

        <div class="kpirow">
          <div class="kpi">DB TX: <span class="mono">{{ cc.status.db_tx_text }}</span></div>
          <div class="kpi">Ingest lag: <span class="mono">{{ cc.status.ingest_text }}</span></div>
          <div class="kpi">tmux: <span class="mono">{{ cc.status.tmux_text }}</span></div>
          <div class="kpi">Last entry: <span class="mono">{{ cc.status.last_entry_text }}</span></div>
          <div class="kpi">Last exit: <span class="mono">{{ cc.status.last_exit_text }}</span></div>
        </div>
      </div>
    </details>

    <details class="section" open>
      <summary>
        <span class="sumtitle">Filter Performance (Today)</span>
        <span class="sumhint">latest scan + executed</span>
      </summary>
      <div class="content">
        <div class="hint mono">
          Markets scanned: <strong>{{ cc.filters.markets_scanned }}</strong> |
          After base filters: <strong>{{ cc.filters.after_filters }}</strong> |
          Would enter (scan): <strong>{{ cc.filters.scan_entries }}</strong> |
          Trades executed (today): <strong>{{ cc.filters.trades_executed_today }}</strong>
        </div>
        <div class="subtools hint mono" style="margin-top:10px;">
          Blocked:
          <span class="chip">cap_per_market_outcome={{ cc.filters.blocked.cap_per_market_outcome }}</span>
          <span class="chip">dislo_not_negative={{ cc.filters.blocked.dislo_not_negative }}</span>
          <span class="chip">dislo_too_small={{ cc.filters.blocked.dislo_too_small }}</span>
          <span class="chip">dislo_too_big={{ cc.filters.blocked.dislo_too_big }}</span>
          <span class="chip">market_banned={{ cc.filters.blocked.market_banned }}</span>
          <span class="chip">px_oob={{ cc.filters.blocked.px_oob }}</span>
          <span class="chip">stale={{ cc.filters.blocked.stale }}</span>
        </div>
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Problem Positions</span>
        <span class="sumhint">old and/or underwater</span>
      </summary>
      <div class="content">
        {% if cc.problems %}
        <table id="tbl_problems" data-filterable="1">
          <thead>
            <tr>
              <th class="right">Age (h)</th>
              <th>Market</th>
              <th>Tags</th>
              <th class="right">Entry</th>
              <th class="right">Last</th>
              <th class="right">Dislo%</th>
              <th class="right">Unreal</th>
              <th>Flag</th>
            </tr>
          </thead>
          <tbody>
            {% for p in cc.problems %}
            <tr>
              <td class="right">{{ "%.1f"|format(p.age_h) }}</td>
              <td class="small">{{ p.market_name }}</td>
              <td class="small">{{ p.tags }}</td>
              <td class="right">{{ "%.4f"|format(p.entry_px) }}</td>
              <td class="right">{{ "%.4f"|format(p.last_px) }}</td>
              <td class="right">{{ "%.1f"|format(p.dislo_pct) }}</td>
              <td class="right {% if p.unreal < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(p.unreal) }}</td>
              <td class="{% if 'UNDERWATER' in p.flag or 'OLD' in p.flag %}warn-txt{% else %}muted{% endif %}">{{ p.flag }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">No problem positions (or not available in this mode).</div>
        {% endif %}
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Market Intelligence</span>
        <span class="sumhint">review candidates + top performers</span>
      </summary>
      <div class="content">
        <div class="row" style="align-items: flex-start;">
          <div style="flex:1 1 420px;">
            <h3>Review Candidates</h3>
            {% if cc.intel.review %}
            <table id="tbl_review" data-filterable="1">
              <thead><tr><th>Market</th><th class="right">Trades</th><th class="right">Sum PnL</th><th class="right">WR</th><th>Last</th></tr></thead>
              <tbody>
                {% for r in cc.intel.review %}
                <tr>
                  <td class="small">{{ r.market_name }}</td>
                  <td class="right">{{ r.trades }}</td>
                  <td class="right pnl-neg">{{ "%.2f"|format(r.sum_pnl) }}</td>
                  <td class="right">{{ "%.0f"|format(r.winrate * 100) }}%</td>
                  <td class="muted">{{ r.last_age }}</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
            {% else %}
              <div class="small muted">na</div>
            {% endif %}
          </div>

          <div style="flex:1 1 420px;">
            <h3>Top Performers</h3>
            {% if cc.intel.top %}
            <table id="tbl_top" data-filterable="1">
              <thead><tr><th>Market</th><th class="right">Trades</th><th class="right">Sum PnL</th><th class="right">WR</th><th>Last</th></tr></thead>
              <tbody>
                {% for r in cc.intel.top %}
                <tr>
                  <td class="small">{{ r.market_name }}</td>
                  <td class="right">{{ r.trades }}</td>
                  <td class="right pnl-pos">{{ "%.2f"|format(r.sum_pnl) }}</td>
                  <td class="right">{{ "%.0f"|format(r.winrate * 100) }}%</td>
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
      </div>
    </details>

  {% else %}

    <div class="grid">
      <div class="card">
        <div class="card-label">Closed PnL (Today)</div>
        <div class="card-value {% if diag.today_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(diag.today_pnl) }}</div>
      </div>
      <div class="card">
        <div class="card-label">Closed PnL (24h)</div>
        <div class="card-value {% if diag.pnl_24h < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(diag.pnl_24h) }}</div>
      </div>
      <div class="card">
        <div class="card-label">Closed PnL (7d)</div>
        <div class="card-value {% if diag.pnl_7d < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(diag.pnl_7d) }}</div>
      </div>
      <div class="card">
        <div class="card-label">Closed PnL (All)</div>
        <div class="card-value {% if diag.total_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(diag.total_pnl) }}</div>
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

    <details class="section" open>
      <summary>
        <span class="sumtitle">Open Positions</span>
        <span class="sumhint">{{ diag.open_positions|length }} rows</span>
      </summary>
      <div class="content">
        {% if diag.open_positions %}
        <table id="tbl_open" data-filterable="1">
          <thead>
            <tr>
              <th class="nowrap">Entry TS</th>
              <th>Market</th>
              <th>Name</th>
              <th>Tags</th>
              <th class="nowrap">Outcome</th>
              <th class="right">Dislo%</th>
              <th class="right">Size</th>
              <th class="right">Entry</th>
              <th class="right">Cost</th>
              <th class="right">Last</th>
              <th class="right">Px%</th>
              <th class="right">Unreal</th>
              <th class="right">Hours</th>
            </tr>
          </thead>
          <tbody>
            {% for p in diag.open_positions %}
            <tr>
              <td class="nowrap small">{{ p.entry_ts }}</td>
              <td class="small">
                {{ p.market_id[:16] }}…
                <button class="copy" data-copy="{{ p.market_id }}">copy</button>
              </td>
              <td class="small">{{ p.market_name or '' }}</td>
              <td class="small">{{ p.market_tags or '' }}</td>
              <td class="nowrap">{{ p.outcome_label }}</td>
              <td class="right">{{ "%.1f"|format(p.dislocation * 100 if p.dislocation is not none else 0) }}</td>
              <td class="right">{{ "%.2f"|format(p.size or 0) }}</td>
              <td class="right">{{ "%.4f"|format(p.entry_price or 0) }}</td>
              <td class="right">{{ "%.2f"|format(p.cost or 0) }}</td>
              <td class="right">{{ "%.4f"|format(p.last_price or 0) }}</td>
              <td class="right {% if p.px_change_pct < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.1f"|format(p.px_change_pct or 0) }}%</td>
              <td class="right {% if p.unrealized_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(p.unrealized_pnl) }}</td>
              <td class="right">{{ "%.1f"|format(p.hours_open) }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">No open positions (or not available in this mode).</div>
        {% endif %}
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Live Orders (strategy_orders)</span>
        <span class="sumhint">{{ live_orders|length }} rows</span>
      </summary>
      <div class="content">
        {% if live_orders %}
        <table id="tbl_orders" data-filterable="1">
          <thead>
            <tr>
              <th>ID</th>
              <th class="nowrap">Created</th>
              <th>Status</th>
              <th>Market</th>
              <th>Outcome</th>
              <th>Side</th>
              <th class="right">Qty</th>
              <th class="right">Limit</th>
              <th class="right">Post Notional</th>
              <th class="right">Fill Qty</th>
              <th class="right">Fill Avg</th>
              <th class="nowrap">Last Fill</th>
              <th>CLOB</th>
            </tr>
          </thead>
          <tbody>
          {% for o in live_orders %}
            <tr>
              <td class="small">{{ o.id }}</td>
              <td class="nowrap small">{{ o.created_at }}</td>
              <td class="small">{{ o.status }}</td>
              <td class="small">
                {{ o.market_id[:16] }}…
                <button class="copy" data-copy="{{ o.market_id }}">copy</button>
              </td>
              <td class="small">{{ o.outcome }}</td>
              <td class="small">{{ o.side }}</td>
              <td class="right small">{{ "%.2f"|format((o.qty or 0)|float) if o.qty is not none else "" }}</td>
              <td class="right small">{{ "%.2f"|format((o.limit_px or 0)|float) if o.limit_px is not none else "" }}</td>
              <td class="right small">{{ "%.2f"|format((o.post_notional or 0)|float) if o.post_notional is not none else "" }}</td>
              <td class="right small">{{ "%.2f"|format((o.fill_qty or 0)|float) if o.fill_qty is not none else "" }}</td>
              <td class="right small">{{ "%.2f"|format((o.fill_avg_px or 0)|float) if o.fill_avg_px is not none else "" }}</td>
              <td class="nowrap small">{{ o.last_fill_ts or "" }}</td>
              <td class="small">{{ o.clob_order_id or "" }}</td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">No live orders.</div>
        {% endif %}
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Recent Closed Positions</span>
        <span class="sumhint">{{ diag.recent_closed|length }} rows</span>
      </summary>
      <div class="content">
        {% if diag.recent_closed %}
        <table id="tbl_closed" data-filterable="1">
          <thead>
            <tr>
              <th class="nowrap">Exit TS</th>
              <th>Market</th>
              <th>Name</th>
              <th>Outcome</th>
              <th class="right">Entry</th>
              <th class="right">Exit</th>
              <th class="right">PnL</th>
              <th>Exit</th>
              <th class="right">Hours</th>
            </tr>
          </thead>
          <tbody>
            {% for r in diag.recent_closed %}
            <tr>
              <td class="nowrap small">{{ r.exit_ts }}</td>
              <td class="small">
                {{ r.market_id[:16] }}…
                <button class="copy" data-copy="{{ r.market_id }}">copy</button>
              </td>
              <td class="small">{{ r.market_name }}</td>
              <td>{{ r.outcome_label }}</td>
              <td class="right">{{ "%.4f"|format(r.entry_price) }}</td>
              <td class="right">{{ "%.4f"|format(r.exit_price) }}</td>
              <td class="right {% if r.pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(r.pnl) }}</td>
              <td>{{ r.exit_reason }}</td>
              <td class="right">{{ "%.1f"|format(r.hours_held) }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Killed Markets</span>
        <span class="sumhint">{{ diag.killed_markets|length }} rows</span>
      </summary>
      <div class="content">
        {% if diag.killed_markets %}
        <table id="tbl_killed" data-filterable="1">
          <thead>
            <tr>
              <th>Market</th>
              <th>Name</th>
              <th class="nowrap">Exit TS</th>
              <th class="right">Total PnL</th>
            </tr>
          </thead>
          <tbody>
            {% for k in diag.killed_markets %}
            <tr>
              <td class="small">
                {{ k.market_id[:16] }}… ({{ k.outcome_label }})
                <button class="copy" data-copy="{{ k.market_id }}">copy</button>
              </td>
              <td class="small">{{ k.market_name }}</td>
              <td class="nowrap">{{ k.exit_ts }}</td>
              <td class="right {% if k.total_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(k.total_pnl) }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        {% else %}
          <div class="small muted">na</div>
        {% endif %}
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Buckets</span>
        <span class="sumhint">dislocation + entry price</span>
      </summary>
      <div class="content">
        <div class="row" style="align-items:flex-start;">
          <div style="flex:1 1 520px;">
            <h3>Dislocation Buckets ({{ diag.dislo_buckets|length }})</h3>
            {% if diag.dislo_buckets %}
            <table id="tbl_dislo" data-filterable="1">
              <thead>
                <tr>
                  <th class="right">Bucket</th><th class="right">Min</th><th class="right">Max</th><th class="right">Trades</th><th class="right">Avg PnL</th><th class="right">Sum PnL</th><th class="right">WR</th>
                </tr>
              </thead>
              <tbody>
                {% for b in diag.dislo_buckets %}
                <tr>
                  <td class="right">{{ b.bucket }}</td>
                  <td class="right">{{ "%.3f"|format(b.minv) }}</td>
                  <td class="right">{{ "%.3f"|format(b.maxv) }}</td>
                  <td class="right">{{ b.trades }}</td>
                  <td class="right {% if b.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.avg_pnl) }}</td>
                  <td class="right {% if b.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.sum_pnl) }}</td>
                  <td class="right">{{ "%.1f"|format(b.winrate * 100) }}%</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
            {% else %}
              <div class="small muted">na</div>
            {% endif %}
          </div>

          <div style="flex:1 1 520px;">
            <h3>Entry Price Buckets ({{ diag.entry_buckets|length }})</h3>
            {% if diag.entry_buckets %}
            <table id="tbl_entry" data-filterable="1">
              <thead>
                <tr>
                  <th class="right">Bucket</th><th class="right">Min</th><th class="right">Max</th><th class="right">Trades</th><th class="right">Avg PnL</th><th class="right">Sum PnL</th><th class="right">WR</th>
                </tr>
              </thead>
              <tbody>
                {% for b in diag.entry_buckets %}
                <tr>
                  <td class="right">{{ b.bucket }}</td>
                  <td class="right">{{ "%.2f"|format(b.minv) }}</td>
                  <td class="right">{{ "%.2f"|format(b.maxv) }}</td>
                  <td class="right">{{ b.trades }}</td>
                  <td class="right {% if b.avg_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.avg_pnl) }}</td>
                  <td class="right {% if b.sum_pnl < 0 %}pnl-neg{% else %}pnl-pos{% endif %}">{{ "%.2f"|format(b.sum_pnl) }}</td>
                  <td class="right">{{ "%.1f"|format(b.winrate * 100) }}%</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
            {% else %}
              <div class="small muted">na</div>
            {% endif %}
          </div>
        </div>
      </div>
    </details>

    <details class="section">
      <summary>
        <span class="sumtitle">Best / Worst Markets (24h)</span>
        <span class="sumhint">top {{ diag.worst_markets|length }} worst + top {{ diag.best_markets|length }} best</span>
      </summary>
      <div class="content">
        <div class="row" style="align-items:flex-start;">
          <div style="flex:1 1 520px;">
            <h3>Worst</h3>
            {% if diag.worst_markets %}
            <table id="tbl_worst" data-filterable="1">
              <thead><tr><th>Market</th><th class="right">Trades</th><th class="right">Sum PnL</th><th class="right">WR</th></tr></thead>
              <tbody>
                {% for m in diag.worst_markets %}
                <tr>
                  <td class="small">{{ m.market_name }}</td>
                  <td class="right">{{ m.trades }}</td>
                  <td class="right pnl-neg">{{ "%.2f"|format(m.sum_pnl) }}</td>
                  <td class="right">{{ "%.0f"|format(m.winrate * 100) }}%</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
            {% else %}
              <div class="small muted">na</div>
            {% endif %}
          </div>

          <div style="flex:1 1 520px;">
            <h3>Best</h3>
            {% if diag.best_markets %}
            <table id="tbl_best" data-filterable="1">
              <thead><tr><th>Market</th><th class="right">Trades</th><th class="right">Sum PnL</th><th class="right">WR</th></tr></thead>
              <tbody>
                {% for m in diag.best_markets %}
                <tr>
                  <td class="small">{{ m.market_name }}</td>
                  <td class="right">{{ m.trades }}</td>
                  <td class="right pnl-pos">{{ "%.2f"|format(m.sum_pnl) }}</td>
                  <td class="right">{{ "%.0f"|format(m.winrate * 100) }}%</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
            {% else %}
              <div class="small muted">na</div>
            {% endif %}
          </div>
        </div>
      </div>
    </details>

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
    conn = connect(
        DB_URL,
        row_factory=dict_row,
        options=(
            "-c statement_timeout=5000 "
            "-c lock_timeout=1000 "
            "-c idle_in_transaction_session_timeout=30000"
        ),
    )
    conn.autocommit = True
    return conn


def _run(cmd, timeout: int = 2):
    """
    Run a command safely with a hard timeout and capped output.
    Returns (returncode, stdout, stderr)
    """
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        out = res.stdout or ""
        err = res.stderr or ""
    except subprocess.TimeoutExpired:
        return 124, "", f"TIMEOUT after {timeout}s"
    except Exception as e:
        return 1, "", f"ERROR: {e}"

    # Cap output to avoid huge logs hanging the request / memory blowups
    MAX_CHARS = 8000
    if len(out) > MAX_CHARS:
        out = out[:MAX_CHARS] + "\n...[truncated]"
    if len(err) > MAX_CHARS:
        err = err[:MAX_CHARS] + "\n...[truncated]"

    return res.returncode, out, err
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


def _outcome_label(v):
    """
    Special casing:
      FI/fi -> Yes
      KT/kt -> No
    plus common numeric/string variants.
    """
    s = str(v).strip()
    if s in ("FI", "fi"):
        return "Yes"
    if s in ("KT", "kt"):
        return "No"
    if s in ("1", "yes", "Yes", "true", "True", "Y", "y"):
        return "Yes"
    if s in ("0", "no", "No", "false", "False", "N", "n", "2"):
        return "No"
    # fallback
    return "Yes" if s == "1" else "No"



# ------------------------------------------------------------
# DB idle-in-transaction probe
# ------------------------------------------------------------
def _load_idle_in_transaction(cur):
    r = _safe_fetchone(
        cur,
        """
        SELECT
          COUNT(*) FILTER (WHERE state = 'idle in transaction') AS cnt,
          MAX(EXTRACT(EPOCH FROM (now() - xact_start)))
            FILTER (WHERE state = 'idle in transaction') AS max_age
        FROM pg_stat_activity
        WHERE datname = current_database();
        """,
    )
    return int(r.get("cnt") or 0), int(r.get("max_age") or 0)


def _load_open_positions(cur, strategy, mode):
    tbl = _positions_table_for_mode(mode)

    sql = f"""
        SELECT
          p.*,
          COALESCE(m.question, p.market_id) AS market_name,
          m.tags AS market_tags,
          rt_last.price AS last_price
        FROM {tbl} p
        LEFT JOIN markets m ON m.market_id = p.market_id
        LEFT JOIN LATERAL (
          SELECT price
          FROM raw_trades rt
          WHERE rt.market_id = p.market_id
            AND rt.outcome = p.outcome
          ORDER BY rt.ts DESC
          LIMIT 1
        ) rt_last ON true
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

        try:
            hours_open = (datetime.now(timezone.utc) - p.get("entry_ts")).total_seconds() / 3600
        except Exception:
            hours_open = 0.0

        market_tags = p.get("market_tags")
        tags_txt = ", ".join(market_tags) if isinstance(market_tags, list) else (market_tags or "")

        out.append(
            {
                "entry_ts": p.get("entry_ts"),
                "market_id": p.get("market_id") or "",
                "market_name": p.get("market_name") or p.get("market_id") or "",
                "market_tags": tags_txt,
                "outcome_label": _outcome_label(p.get("outcome")),
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


def _load_live_orders(cur, strategy, limit_n=200):
    return _safe_fetchall(
        cur,
        f"""
        SELECT
          o.id,
          o.created_at,
          o.strategy,
          o.status,
          o.market_id,
          o.outcome,
          o.side,
          o.qty,
          o.limit_px,
          (o.metadata->>'clob_order_id') AS clob_order_id,
          (o.metadata->>'post_notional') AS post_notional,
          COALESCE(f.fill_qty, 0) AS fill_qty,
          f.fill_avg_px,
          f.last_fill_ts
        FROM strategy_orders o
        LEFT JOIN (
          SELECT
            order_id,
            SUM(qty)::numeric AS fill_qty,
            CASE
              WHEN SUM(qty) > 0 THEN SUM(qty * price) / SUM(qty)
              ELSE NULL
            END AS fill_avg_px,
            MAX(ts) AS last_fill_ts
          FROM strategy_fills
          WHERE paper=false
          GROUP BY order_id
        ) f ON f.order_id = o.id
        WHERE o.paper = false
          AND (%s = 'all' OR o.strategy = %s)
        ORDER BY o.created_at DESC
        LIMIT {int(limit_n)}
        """,
        (strategy, strategy),
    )


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
    return (mu / sd) * (len(pnls) ** 0.5)

def _load_perf_time_range(cur, strategy, mode, where_time_sql: str, params_tail=()):
    """
    Fast aggregate perf stats using SQL only.
    where_time_sql must start with 'AND ...'
    """
    tbl = _positions_table_for_mode(mode)
    params = [strategy, strategy]
    params.extend(list(params_tail))

    sql = f"""
        SELECT
          COUNT(*)::int AS trades,
          COALESCE(SUM(pnl), 0)::numeric AS pnl,
          AVG(CASE WHEN pnl > 0 THEN 1.0 ELSE 0.0 END)::float AS winrate,
          AVG(pnl)::float AS avg_pnl,
          AVG(
            CASE
              WHEN lower(coalesce(exit_reason,'')) IN ('sl','max_sl')
                OR lower(coalesce(exit_reason,'')) LIKE '%max_sl%'
              THEN 1.0 ELSE 0.0
            END
          )::float AS sl_rate,
          MIN(pnl)::numeric AS largest_loss,
          STDDEV_POP(pnl)::float AS sd_pnl
        FROM {tbl}
        WHERE (%s='all' OR strategy=%s)
          AND COALESCE(status,'closed')='closed'
          AND pnl IS NOT NULL
          AND exit_ts IS NOT NULL
          {where_time_sql};
    """
    r = _safe_fetchone(cur, sql, tuple(params))

    n = int(r.get("trades") or 0)
    total = float(to_dec(r.get("pnl")) or 0)
    wr = r.get("winrate")
    avg = r.get("avg_pnl")
    slr = r.get("sl_rate")
    largest_loss = r.get("largest_loss")
    sd = r.get("sd_pnl")

    sharpe = None
    if n >= 2 and avg is not None and sd is not None and sd != 0:
        sharpe = (float(avg) / float(sd)) * (n ** 0.5)

    return {
        "trades": n,
        "pnl": total,
        "winrate": float(wr) if wr is not None else None,
        "avg_pnl": float(avg) if avg is not None else None,
        "sl_rate": float(slr) if slr is not None else None,
        "largest_loss": float(to_dec(largest_loss)) if largest_loss is not None else None,
        "sharpe": float(sharpe) if sharpe is not None else None,
    }

def _load_perf_window(cur, strategy, mode, since_interval_sql: str | None):
    where_time = ""
    if since_interval_sql:
        # since_interval_sql is controlled by this file (not user input)
        where_time = f" AND exit_ts >= (NOW() - INTERVAL '{since_interval_sql}')"
    return _load_perf_time_range(cur, strategy, mode, where_time)

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

    # sharpe not merged (kept None for combined view)
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
    def one_mode_snap(m):
        ptoday = _load_perf_time_range(cur, strategy, m, "AND exit_ts >= CURRENT_DATE")
        pyday = _load_perf_time_range(
            cur,
            strategy,
            m,
            "AND exit_ts >= (CURRENT_DATE - INTERVAL '1 day') AND exit_ts < CURRENT_DATE",
        )
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
            "pnl_class": pnl_class,
            "pnl_text": pnl_text,
            "wr_class": wr_class,
            "wr_text": wr_text,
            "sl_class": sl_class,
            "sl_text": sl_text,
            "sh_class": sh_class,
            "sh_text": sh_text,
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
        try:
            hours = (exit_ts - entry_ts).total_seconds() / 3600 if entry_ts and exit_ts else 0.0
        except Exception:
            hours = 0.0

        out.append(
            {
                "market_id": r.get("market_id") or "",
                "market_name": r.get("market_name") or r.get("market_id") or "",
                "outcome_label": _outcome_label(r.get("outcome")),
                "entry_price": float(to_dec(r.get("entry_price")) or 0),
                "exit_price": float(to_dec(r.get("exit_price")) or 0),
                "exit_ts": r.get("exit_ts"),
                "pnl": float(to_dec(r.get("pnl")) or 0),
                "exit_reason": r.get("exit_reason") or "",
                "hours_held": float(hours),
            }
        )
    return out


def _load_killed_markets(cur, strategy, mode, limit_n):
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
        out.append(
            {
                "market_id": r.get("market_id") or "",
                "market_name": r.get("market_name") or r.get("market_id") or "",
                "outcome_label": _outcome_label(r.get("outcome")),
                "exit_ts": r.get("exit_ts"),
                "total_pnl": float(to_dec(r.get("total_pnl")) or 0),
            }
        )
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
        buckets.append({"bucket": i + 1, "minv": float(edges[i]), "maxv": float(edges[i + 1]), "trades": 0, "sum_pnl": 0.0, "wins": 0})

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
        out.append(
            {
                "bucket": b["bucket"],
                "minv": b["minv"],
                "maxv": b["maxv"],
                "trades": b["trades"],
                "avg_pnl": b["sum_pnl"] / b["trades"],
                "sum_pnl": b["sum_pnl"],
                "winrate": b["wins"] / b["trades"],
            }
        )
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
        items.append(
            {
                "market_name": r.get("market_name") or r.get("market_id"),
                "trades": int(r.get("trades") or 0),
                "sum_pnl": float(to_dec(r.get("sum_pnl")) or 0),
                "winrate": float(r.get("winrate") or 0.0),
            }
        )
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

    return {"raw": top_raw or 0, "after_filters": top_after or 0, "scan": scan or {}}


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
        },
    }


def _load_loss_streak(cur, strategy, mode, lookback=50):
    """
    Consecutive losing trades from most recent closed exits.
    For mode='both' we look across live+paper combined ordered by exit_ts.
    """
    if mode in ("live", "paper"):
        tbl = _positions_table_for_mode(mode)
        rows = _safe_fetchall(
            cur,
            f"""
            SELECT pnl
            FROM {tbl}
            WHERE (%s='all' OR strategy=%s)
              AND COALESCE(status,'closed')='closed'
              AND pnl IS NOT NULL
              AND exit_ts IS NOT NULL
            ORDER BY exit_ts DESC
            LIMIT {int(lookback)};
            """,
            (strategy, strategy),
        )
    else:
        rows = _safe_fetchall(
            cur,
            f"""
            SELECT pnl
            FROM (
              SELECT pnl, exit_ts FROM mr_positions
              WHERE (%s='all' OR strategy=%s)
                AND COALESCE(status,'closed')='closed'
                AND pnl IS NOT NULL
                AND exit_ts IS NOT NULL
              UNION ALL
              SELECT pnl, exit_ts FROM paper_positions
              WHERE (%s='all' OR strategy=%s)
                AND COALESCE(status,'closed')='closed'
                AND pnl IS NOT NULL
                AND exit_ts IS NOT NULL
            ) x
            ORDER BY exit_ts DESC
            LIMIT {int(lookback)};
            """,
            (strategy, strategy, strategy, strategy),
        )

    streak = 0
    for r0 in rows:
        pnl = to_dec(r0.get("pnl"))
        if pnl is None:
            continue
        if pnl < 0:
            streak += 1
        else:
            break
    return streak


def _load_24h_wr(cur, strategy, mode):
    """
    Returns (trades_24h, wins_24h)
    """
    if mode in ("live", "paper"):
        tbl = _positions_table_for_mode(mode)
        w = _safe_fetchone(
            cur,
            f"""
            SELECT COUNT(*) AS trades, COUNT(*) FILTER (WHERE pnl > 0) AS wins
            FROM {tbl}
            WHERE (%s='all' OR strategy=%s)
              AND COALESCE(status,'closed')='closed'
              AND exit_ts >= (NOW() - INTERVAL '24 hours');
            """,
            (strategy, strategy),
        )
        return int(w.get("trades") or 0), int(w.get("wins") or 0)

    w = _safe_fetchone(
        cur,
        """
        SELECT
          (SELECT COUNT(*) FROM mr_positions
            WHERE (%s='all' OR strategy=%s)
              AND COALESCE(status,'closed')='closed'
              AND exit_ts >= (NOW() - INTERVAL '24 hours')) +
          (SELECT COUNT(*) FROM paper_positions
            WHERE (%s='all' OR strategy=%s)
              AND COALESCE(status,'closed')='closed'
              AND exit_ts >= (NOW() - INTERVAL '24 hours')) AS trades,
          (SELECT COUNT(*) FROM mr_positions
            WHERE (%s='all' OR strategy=%s)
              AND COALESCE(status,'closed')='closed'
              AND exit_ts >= (NOW() - INTERVAL '24 hours')
              AND pnl > 0) +
          (SELECT COUNT(*) FROM paper_positions
            WHERE (%s='all' OR strategy=%s)
              AND COALESCE(status,'closed')='closed'
              AND exit_ts >= (NOW() - INTERVAL '24 hours')
              AND pnl > 0) AS wins;
        """,
        (strategy, strategy, strategy, strategy, strategy, strategy, strategy, strategy),
    )
    return int(w.get("trades") or 0), int(w.get("wins") or 0)


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
        "db_tx": {"status": "na", "text": "na"},
        "ingest": {"status": "na", "text": "unknown"},
        "tmux": check_tmux_sessions(),
        "bots": {"status": "na", "text": "na"},
        "dashboard": {"status": "ok", "text": "serving"},
    }

    cc = {
        "status": {},
        "kill": {},
        "perf": {},
        "filters": {"markets_scanned": 0, "after_filters": 0, "scan_entries": 0, "trades_executed_today": 0, "blocked": {}},
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

    idle_cnt = 0
    idle_age = 0
    live_orders = []

    try:
        with get_conn() as conn, conn.cursor() as cur:
            _ = _safe_fetchone(cur, "SELECT 1 AS ok;")
            health["db"] = {"status": "ok", "text": "connected"}

            # DB TX health
            idle_cnt, idle_age = _load_idle_in_transaction(cur)
            if idle_cnt == 0:
                health["db_tx"] = {"status": "ok", "text": "0"}
            elif idle_cnt >= DASH_DB_IDLE_TX_BAD_COUNT or idle_age >= DASH_DB_IDLE_TX_BAD_SECS:
                health["db_tx"] = {"status": "bad", "text": f"{idle_cnt} / {_fmt_age(idle_age)}"}
            elif idle_age >= DASH_DB_IDLE_TX_WARN_SECS:
                health["db_tx"] = {"status": "warn", "text": f"{idle_cnt} / {_fmt_age(idle_age)}"}
            else:
                health["db_tx"] = {"status": "warn", "text": str(idle_cnt)}

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

            # Live orders (strategy_orders) - only needed on diagnostics view
            if view != "command":
                live_orders = _load_live_orders(cur, strategy, 200)
            else:
                live_orders = []

            # Load open positions for selected mode
            if mode in ("live", "paper"):
                open_positions = _load_open_positions(cur, strategy, mode)
            else:
                open_positions = _load_open_positions(cur, strategy, "live") + _load_open_positions(cur, strategy, "paper")

            # Diagnostics rollup (selected mode)
            if view != "command":
                if mode in ("live", "paper"):
                    diag_roll = _load_closed_rollups(cur, strategy, mode)
                    diag.update(diag_roll)
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
                    diag.update(
                        {
                            "today_pnl": l["today_pnl"] + p["today_pnl"],
                            "pnl_24h": l["pnl_24h"] + p["pnl_24h"],
                            "pnl_7d": l["pnl_7d"] + p["pnl_7d"],
                            "total_pnl": l["total_pnl"] + p["total_pnl"],
                            "closed_trades": closed_trades,
                            "open_trades": (l["open_trades"] or 0) + (p["open_trades"] or 0),
                            "winrate": winrate,
                        }
                    )

                diag["open_positions"] = open_positions

                # Recent closed / killed
                if mode in ("live", "paper"):
                    diag["recent_closed"] = _load_recent_closed(cur, strategy, mode, RECENT_CLOSED_LIMIT)
                    diag["killed_markets"] = _load_killed_markets(cur, strategy, mode, KILLED_MARKETS_LIMIT)
                    rows_for_buckets = _load_closed_rows_for_buckets(cur, strategy, mode)
                    worst, best = _load_best_worst_markets_24h(cur, strategy, mode, BEST_WORST_MARKETS_LIMIT)
                else:
                    diag["recent_closed"] = _load_recent_closed(cur, strategy, "live", RECENT_CLOSED_LIMIT)
                    diag["killed_markets"] = _load_killed_markets(cur, strategy, "live", KILLED_MARKETS_LIMIT)
                    rows_for_buckets = _load_closed_rows_for_buckets(cur, strategy, "live")
                    worst, best = _load_best_worst_markets_24h(cur, strategy, "live", BEST_WORST_MARKETS_LIMIT)

                dislo_edges = _bucket_edges(DISLO_BUCKET_MIN, DISLO_BUCKET_MAX, DISLO_BUCKET_STEP)
                diag["dislo_buckets"] = _make_buckets(rows_for_buckets, key_fn=lambda r: r.get("dislocation"), edges=dislo_edges)

                entry_edges = _bucket_edges(ENTRY_BUCKET_MIN, ENTRY_BUCKET_MAX, ENTRY_BUCKET_STEP)
                diag["entry_buckets"] = _make_buckets(rows_for_buckets, key_fn=lambda r: r.get("entry_price"), edges=entry_edges)

                diag["worst_markets"] = worst
                diag["best_markets"] = best

            # Command Center
            if view == "command":
                # Guardrail: keep command view responsive even if a query gets slow
                try:
                    cur.execute("SET statement_timeout = '3000ms'")
                except Exception:
                    pass
                limits = _limits_for_mode("paper" if mode == "paper" else "live")

                # last entry / exit times
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
                    "db_tx_level": health["db_tx"]["status"],
                    "db_tx_text": health["db_tx"]["text"],
                    "ingest_level": ingest_level,
                    "ingest_text": "no trades" if ingest_lag is None else f"{int(ingest_lag)}s",
                    "tmux_level": health["tmux"]["status"],
                    "tmux_text": "OK" if health["tmux"]["status"] == "ok" else health["tmux"]["text"],
                    "bots_level": bots_level,
                    "bots_text": health["bots"]["text"],
                    "last_entry_text": _fmt_age(last_entry_age),
                    "last_exit_text": _fmt_age(last_exit_age),
                }

                # daily pnl (selected mode)
                if mode in ("live", "paper"):
                    try:
                        roll = _load_closed_rollups(cur, strategy, mode)
                    except Exception:
                        roll = {
                            "today_pnl": 0,
                            "pnl_24h": 0,
                            "pnl_7d": 0,
                            "total_pnl": 0,
                            "closed_trades": 0,
                            "open_trades": 0,
                            "winrate": None,
                        }
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

                # global loss streak
                try:
                    streak = _load_loss_streak(cur, strategy, mode, lookback=50)
                except Exception:
                    streak = 0
                loss_streak_level = "ok" if streak < limits["max_streak"] else "bad"

                # 24h winrate
                try:
                    trades_24h, wins_24h = _load_24h_wr(cur, strategy, mode)
                except Exception:
                    trades_24h, wins_24h = 0, 0
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

                cc["perf"] = _load_performance_snapshot(cur, strategy, mode)
                try:
                    # NOTE: log parsing can be very slow on large log files.
                    # Only compute filters when explicitly requested: &filters=1
                    fp = {"markets_scanned": 0, "after_filters": 0, "scan_entries": 0, "trades_executed_today": 0, "blocked": {}}
                    if request.args.get("filters") == "1":
                        fp = _filters_today_from_log(strategy)
                except Exception:
                    fp = {"markets_scanned": 0, "after_filters": 0, "scan_entries": 0, "trades_executed_today": 0, "blocked": {}}
                # executed today
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

                    problems.append(
                        {
                            "age_h": float(age_h),
                            "market_name": op.get("market_name") or op.get("market_id"),
                            "tags": op.get("market_tags") or "",
                            "entry_px": float(entry),
                            "last_px": float(last),
                            "dislo_pct": float(dislo * 100),
                            "unreal": float(unreal),
                            "flag": " + ".join(flags),
                        }
                    )
                cc["problems"] = sorted(problems, key=lambda x: (x["unreal"], -x["age_h"]))[:10]
                # market intel (only for live/paper - keep it fast)
                if mode in ("live", "paper"):
                    try:
                        tbl = _positions_table_for_mode(mode)

                        # Bound work: only look at most recent closed rows
                        rows = _safe_fetchall(
                            cur,
                            f"""
                            WITH recent AS (
                              SELECT *
                              FROM {tbl}
                              WHERE (%s = 'all' OR strategy = %s)
                                AND status='closed'
                                AND exit_ts IS NOT NULL
                              ORDER BY exit_ts DESC
                              LIMIT 5000
                            )
                            SELECT
                              p.market_id,
                              COALESCE(m.question, p.market_id) AS market_name,
                              COUNT(*) AS trades,
                              COALESCE(SUM(p.pnl), 0) AS sum_pnl,
                              AVG(CASE WHEN p.pnl > 0 THEN 1.0 ELSE 0.0 END) AS winrate,
                              MAX(p.exit_ts) AS last_exit_ts
                            FROM recent p
                            LEFT JOIN markets m ON m.market_id = p.market_id
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
                    except Exception:
                        cc["intel"]["review"] = []
                        cc["intel"]["top"] = []
                else:
                    cc["intel"]["review"] = []
                    cc["intel"]["top"] = []

    except Exception as e:
        health["db"] = {"status": "bad", "text": "FAILED"}
        health["db_tx"] = {"status": "na", "text": "na"}
        health["ingest"] = {"status": "na", "text": "unknown"}
        health["bots"] = {"status": "na", "text": "na"}
        page_error = str(e)
        live_orders = []

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
        live_orders=live_orders,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.getenv("DASH_PORT", "5002")), debug=False, threaded=True)