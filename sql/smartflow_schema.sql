-- Smartflow strategy support tables

-- Raw trades ingested from Polymarket streams
CREATE TABLE IF NOT EXISTS raw_trades (
    trade_id     bigserial PRIMARY KEY,
    ts           timestamptz NOT NULL,
    market_id    text        NOT NULL,
    outcome      text        NOT NULL,
    taker        text        NOT NULL,
    maker        text        NOT NULL,
    side         text        NOT NULL,       -- "buy" from taker POV
    qty          numeric     NOT NULL,
    price        numeric     NOT NULL,
    tx_hash      text
);

CREATE INDEX IF NOT EXISTS raw_trades_ts_idx        ON raw_trades(ts);
CREATE INDEX IF NOT EXISTS raw_trades_market_ts_idx ON raw_trades(market_id, ts);
CREATE INDEX IF NOT EXISTS raw_trades_taker_ts_idx  ON raw_trades(taker, ts);
CREATE INDEX IF NOT EXISTS raw_trades_maker_ts_idx  ON raw_trades(maker, ts);

-- Wallet positions (optional if you already track elsewhere)
CREATE TABLE IF NOT EXISTS wallet_positions (
    position_id   bigserial PRIMARY KEY,
    wallet        text        NOT NULL,
    market_id     text        NOT NULL,
    outcome       text        NOT NULL,
    opened_at     timestamptz NOT NULL,
    closed_at     timestamptz,
    entry_px      numeric,
    exit_px       numeric,
    size          numeric,
    realized_pnl  numeric,
    status        text        NOT NULL,
    pnl_samples   int         DEFAULT 0
);

CREATE INDEX IF NOT EXISTS wallet_positions_wallet_status_idx
    ON wallet_positions(wallet, status, opened_at);

-- Daily per-wallet stats
CREATE TABLE IF NOT EXISTS wallet_stats_daily (
    day              date        NOT NULL,
    wallet           text        NOT NULL,
    markets_traded   int         NOT NULL,
    trades_count     int         NOT NULL,
    trades_won       int         DEFAULT 0,
    trades_lost      int         DEFAULT 0,
    gross_volume     numeric     NOT NULL,
    realized_pnl     numeric     NOT NULL,
    win_rate         numeric,
    median_hold_secs numeric,
    max_dd           numeric,
    specialization   text,
    sharpe_like      numeric,
    PRIMARY KEY (day, wallet)
);

-- Wallet labels and grades
CREATE TABLE IF NOT EXISTS wallet_labels (
    wallet            text PRIMARY KEY,
    is_smart          boolean,
    smart_score       numeric,
    is_whale          boolean,
    avg_size          numeric,
    main_vertical     text,
    style             text,
    median_hold_secs  numeric,
    last_updated      timestamptz NOT NULL DEFAULT now()
);

-- Rolling flow snapshots
CREATE TABLE IF NOT EXISTS flow_snapshots (
    ts              timestamptz NOT NULL,
    market_id       text        NOT NULL,
    outcome         text        NOT NULL,
    window_secs     int         NOT NULL,
    net_flow        numeric     NOT NULL,
    gross_flow      numeric     NOT NULL,
    smart_net_flow  numeric     NOT NULL,
    whale_net_flow  numeric     NOT NULL,
    wallet_count    int         NOT NULL,
    smart_wallets   int         NOT NULL,
    whale_wallets   int         NOT NULL,
    PRIMARY KEY (ts, market_id, outcome, window_secs)
);

-- Signals table polled by executor
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

CREATE INDEX IF NOT EXISTS strategy_signals_active_idx
    ON strategy_signals(strategy, market_id, expires_at);

-- Paper trading tables for smartflow executor
CREATE TABLE IF NOT EXISTS paper_positions (
    id             serial PRIMARY KEY,
    strategy       text,
    market_id      text,
    outcome        text,
    side           text,
    entry_price    numeric,
    entry_ts       timestamptz,
    size           numeric,
    score          numeric,
    smart_wallets  integer,
    smart_net_flow numeric,
    status         text DEFAULT 'open',
    exit_price     numeric,
    exit_ts        timestamptz,
    pnl            numeric
);

CREATE TABLE IF NOT EXISTS paper_fills (
    id           serial PRIMARY KEY,
    position_id  integer REFERENCES paper_positions(id),
    price        numeric,
    ts           timestamptz,
    reason       text
);

CREATE INDEX IF NOT EXISTS idx_paper_positions_open
    ON paper_positions(status);
-- Prevent multiple open positions per strategy/market/outcome/side
CREATE UNIQUE INDEX IF NOT EXISTS uq_paper_positions_open
    ON paper_positions(strategy, market_id, outcome, side)
    WHERE status = 'open';

-- Backfill columns for wallet_stats_daily if missing
ALTER TABLE IF EXISTS wallet_stats_daily
    ADD COLUMN IF NOT EXISTS trades_won int,
    ADD COLUMN IF NOT EXISTS trades_lost int,
    ADD COLUMN IF NOT EXISTS max_dd numeric,
    ADD COLUMN IF NOT EXISTS specialization text,
    ADD COLUMN IF NOT EXISTS sharpe_like numeric;
