-- Smartflow migrations: schema fixes and resets

-- wallet_stats_daily schema adjustments
ALTER TABLE wallet_stats_daily
  ADD COLUMN IF NOT EXISTS trades_won int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS trades_lost int DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_updated timestamptz,
  ALTER COLUMN gross_volume TYPE numeric,
  ALTER COLUMN realized_pnl TYPE numeric,
  ALTER COLUMN win_rate TYPE numeric,
  ALTER COLUMN median_hold_secs TYPE numeric;

-- wallet_labels debug fields
ALTER TABLE wallet_labels
  ADD COLUMN IF NOT EXISTS as_of_day date,
  ADD COLUMN IF NOT EXISTS last_win_rate numeric,
  ADD COLUMN IF NOT EXISTS last_trades int,
  ADD COLUMN IF NOT EXISTS last_volume numeric,
  ADD COLUMN IF NOT EXISTS last_pnl numeric,
  ADD COLUMN IF NOT EXISTS last_avg_hold_secs numeric;

-- raw_trades index on ts
CREATE INDEX IF NOT EXISTS idx_raw_trades_ts ON raw_trades (ts);

-- flow_snapshots definition
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

CREATE INDEX IF NOT EXISTS idx_flow_snapshots_lookup
  ON flow_snapshots (ts DESC, market_id);

-- strategy_signals reset with id primary key
DROP TABLE IF EXISTS strategy_signals;

CREATE TABLE strategy_signals (
    id         bigserial PRIMARY KEY,
    ts         timestamptz NOT NULL,
    strategy   text        NOT NULL,
    market_id  text        NOT NULL,
    outcome    text        NOT NULL,
    side       text        NOT NULL,
    score      numeric     NOT NULL,
    reason     jsonb       NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signals_strat_ts
  ON strategy_signals(strategy, ts DESC);
