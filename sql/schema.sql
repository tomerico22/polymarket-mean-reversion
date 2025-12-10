-- Core catalog
CREATE TABLE IF NOT EXISTS events (
  event_id      TEXT PRIMARY KEY,
  title         TEXT,
  category      TEXT,
  close_ts      TIMESTAMPTZ,
  resolve_ts    TIMESTAMPTZ,
  status        TEXT
);

CREATE TABLE IF NOT EXISTS markets (
  market_id     TEXT PRIMARY KEY,
  event_id      TEXT REFERENCES events(event_id),
  question      TEXT,
  collateral    TEXT,
  yes_token_id  TEXT,
  no_token_id   TEXT,
  created_ts    TIMESTAMPTZ,
  resolve_ts    TIMESTAMPTZ,
  resolution    TEXT,
  tags          JSONB DEFAULT '[]'::jsonb,
  vertical      TEXT
);

-- Time series (append-only)
CREATE TABLE IF NOT EXISTS trades (
  trade_id      TEXT PRIMARY KEY,
  market_id     TEXT,
  taker         TEXT,
  maker         TEXT,
  side          TEXT,      -- BUY/SELL of YES
  price         NUMERIC,
  size          NUMERIC,
  value_usd     NUMERIC,
  ts            TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS book_ticks (
  market_id     TEXT,
  token_id      TEXT,
  best_bid      NUMERIC,
  best_ask      NUMERIC,
  mid           NUMERIC,
  bid_size      NUMERIC,
  ask_size      NUMERIC,
  ts            TIMESTAMPTZ,
  PRIMARY KEY (market_id, ts)
);

CREATE TABLE IF NOT EXISTS market_ticks (
  asset_id    TEXT NOT NULL,
  market_id   TEXT,
  event_type  TEXT NOT NULL,   -- last_trade_price / price_change
  price       NUMERIC,
  ts          TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (asset_id, ts, event_type)
);

CREATE TABLE IF NOT EXISTS wallet_activity (
  wallet        TEXT,
  market_id     TEXT,
  side          TEXT,
  price         NUMERIC,
  size          NUMERIC,
  value_usd     NUMERIC,
  role          TEXT,        -- taker/maker
  tx_hash       TEXT,
  ts            TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS holders_snap (
  market_id     TEXT,
  token_id      TEXT,
  wallet        TEXT,
  balance       NUMERIC,
  ts            TIMESTAMPTZ,
  PRIMARY KEY (market_id, token_id, wallet, ts)
);

-- Strategy / bot state -------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_orders (
  id          BIGSERIAL PRIMARY KEY,
  strategy    TEXT NOT NULL,
  market_id   TEXT NOT NULL,
  side        TEXT NOT NULL,            -- YES / NO
  qty         NUMERIC NOT NULL,         -- signed: >0 buy, <0 sell
  limit_px    NUMERIC,
  status      TEXT NOT NULL DEFAULT 'submitted',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  paper       BOOLEAN NOT NULL DEFAULT TRUE,
  metadata    JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_strategy_orders_market ON strategy_orders(market_id);
CREATE INDEX IF NOT EXISTS idx_strategy_orders_strategy ON strategy_orders(strategy);

CREATE TABLE IF NOT EXISTS strategy_fills (
  id         BIGSERIAL PRIMARY KEY,
  order_id   BIGINT REFERENCES strategy_orders(id) ON DELETE CASCADE,
  qty        NUMERIC NOT NULL,
  price      NUMERIC NOT NULL,
  ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  paper      BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_strategy_fills_order ON strategy_fills(order_id);

CREATE TABLE IF NOT EXISTS strategy_positions (
  strategy       TEXT NOT NULL,
  market_id      TEXT NOT NULL,
  side           TEXT NOT NULL,    -- YES / NO
  qty            NUMERIC NOT NULL,
  avg_price      NUMERIC NOT NULL,
  opened_at      TIMESTAMPTZ NOT NULL,
  max_adverse    NUMERIC DEFAULT 0,
  max_favourable NUMERIC DEFAULT 0,
  PRIMARY KEY (strategy, market_id, side)
);

CREATE TABLE IF NOT EXISTS strategy_metrics_daily (
  strategy     TEXT NOT NULL,
  date         DATE NOT NULL,
  trades       INT,
  pnl          NUMERIC,
  winrate      NUMERIC,
  avg_rr       NUMERIC,
  drawdown     NUMERIC,
  paper        BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (strategy, date, paper)
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_trades_market_ts ON trades(market_id, ts);
CREATE INDEX IF NOT EXISTS idx_book_ticks_market_ts ON book_ticks(market_id, ts);
CREATE INDEX IF NOT EXISTS idx_market_ticks_market_ts ON market_ticks(market_id, ts);
CREATE INDEX IF NOT EXISTS idx_market_ticks_asset_ts ON market_ticks(asset_id, ts);
CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet_ts ON wallet_activity(wallet, ts);
CREATE INDEX IF NOT EXISTS idx_holders_snap_market_ts ON holders_snap(market_id, ts);
