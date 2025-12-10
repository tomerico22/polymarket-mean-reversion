-- Create elite_wallets table
CREATE TABLE IF NOT EXISTS elite_wallets (
  wallet              text PRIMARY KEY,
  nickname            text,
  total_positions     int,
  active_positions    int,
  total_wins          numeric,
  total_losses        numeric,
  win_rate            numeric,
  current_value_usd   numeric,
  overall_pnl_usd     numeric,
  source              text NOT NULL DEFAULT 'polymarket_analytics',
  added_at            timestamptz NOT NULL DEFAULT now()
);

-- Tag wallet_labels with is_elite
ALTER TABLE wallet_labels
  ADD COLUMN IF NOT EXISTS is_elite boolean DEFAULT false;

-- Optional: reset then mark elite
-- UPDATE wallet_labels SET is_elite = false;
UPDATE wallet_labels wl
SET is_elite = true
FROM elite_wallets e
WHERE wl.wallet = e.wallet;
