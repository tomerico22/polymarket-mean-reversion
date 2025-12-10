DROP MATERIALIZED VIEW IF EXISTS market_24h;
CREATE MATERIALIZED VIEW market_24h AS
SELECT market_id,
       max(ts) AS last_ts,
       avg(price) FILTER (WHERE side='BUY') AS avg_buy_px,
       sum(value_usd) AS vol_24h,
       count(*) AS trades_24h
FROM trades
WHERE ts >= now() - interval '24 hours'
GROUP BY 1;

-- 24h wallet net flow
DROP MATERIALIZED VIEW IF EXISTS wallet_flow_24h;
CREATE MATERIALIZED VIEW wallet_flow_24h AS
SELECT wallet, market_id,
       sum(CASE WHEN side='BUY' THEN value_usd ELSE -value_usd END) AS net_flow_usd
FROM wallet_activity
WHERE ts >= now() - interval '24 hours'
GROUP BY 1,2;

-- Strategy dashboard view depends on market_spotlight_1h, so drop it first
DROP VIEW IF EXISTS vw_strategy_position_dashboard;

-- Latest asset-level signals (marks + price deltas)
DROP MATERIALIZED VIEW IF EXISTS market_spotlight_1h;
DROP MATERIALIZED VIEW IF EXISTS wallet_netflow_1h;
DROP MATERIALIZED VIEW IF EXISTS asset_signals_1h;

CREATE MATERIALIZED VIEW asset_signals_1h AS
WITH last_trade AS (
  SELECT DISTINCT ON (asset_id)
         asset_id,
         market_id,
         price,
         ts
  FROM market_ticks
  WHERE event_type = 'last_trade_price'
    AND ts >= now() - interval '2 days'
    AND price IS NOT NULL
  ORDER BY asset_id, ts DESC
),
price_delta AS (
  SELECT DISTINCT ON (asset_id)
         asset_id,
         price AS delta_price,
         ts    AS delta_ts
  FROM market_ticks
  WHERE event_type = 'price_change'
    AND ts >= now() - interval '2 days'
  ORDER BY asset_id, ts DESC
)
SELECT lt.market_id,
       lt.asset_id,
       lt.price AS last_price,
       lt.ts    AS last_price_ts,
       pd.delta_price AS price_change,
       pd.delta_ts    AS price_change_ts
FROM last_trade lt
LEFT JOIN price_delta pd USING (asset_id);

CREATE UNIQUE INDEX IF NOT EXISTS asset_signals_1h_asset_idx ON asset_signals_1h(asset_id);
CREATE INDEX IF NOT EXISTS asset_signals_1h_market_idx ON asset_signals_1h(market_id);

CREATE MATERIALIZED VIEW wallet_netflow_1h AS
SELECT market_id,
       wallet,
       SUM(CASE WHEN side='BUY' THEN value_usd ELSE -value_usd END) AS net_flow_sum_1h,
       SUM(value_usd) FILTER (WHERE side='BUY')  AS buy_usd_1h,
       SUM(value_usd) FILTER (WHERE side='SELL') AS sell_usd_1h,
       COUNT(*) FILTER (WHERE side='BUY')  AS buy_trades_1h,
       COUNT(*) FILTER (WHERE side='SELL') AS sell_trades_1h,
       MIN(ts) AS first_ts,
       MAX(ts) AS last_ts
FROM wallet_activity
WHERE ts >= now() - interval '1 hour'
  AND value_usd IS NOT NULL
GROUP BY market_id, wallet;

CREATE UNIQUE INDEX IF NOT EXISTS wallet_netflow_1h_uidx ON wallet_netflow_1h(market_id, wallet);

CREATE MATERIALIZED VIEW market_spotlight_1h AS
WITH trades_1h AS (
  SELECT market_id,
         SUM(value_usd) AS usd_1h,
         COUNT(*)       AS trades_1h
  FROM trades
  WHERE ts >= now() - interval '1 hour'
    AND value_usd IS NOT NULL
  GROUP BY market_id
),
wallet_flow AS (
  SELECT market_id,
         wallet,
         SUM(CASE WHEN side='BUY' THEN value_usd ELSE -value_usd END) AS net_flow
  FROM wallet_activity
  WHERE ts >= now() - interval '1 hour'
    AND value_usd IS NOT NULL
  GROUP BY market_id, wallet
),
flow_agg AS (
  SELECT market_id,
         SUM(net_flow)                                        AS net_flow_sum_1h,
         COUNT(*) FILTER (WHERE net_flow > 0)                 AS buyers_cnt_1h,
         COUNT(*) FILTER (WHERE net_flow < 0)                 AS sellers_cnt_1h
  FROM wallet_flow
  GROUP BY market_id
),
prices AS (
  SELECT m.market_id,
         y.last_price    AS last_price_yes,
         y.last_price_ts AS last_price_yes_ts,
         n.last_price    AS last_price_no,
         n.last_price_ts AS last_price_no_ts
  FROM markets m
  LEFT JOIN asset_signals_1h y ON y.asset_id::text = m.yes_token_id::text
  LEFT JOIN asset_signals_1h n ON n.asset_id::text = m.no_token_id::text
)
SELECT m.market_id,
       LEFT(m.market_id, 12) AS market_id_hex,
       t.usd_1h,
       t.trades_1h,
       p.last_price_yes,
       p.last_price_no,
       GREATEST(p.last_price_yes_ts, p.last_price_no_ts) AS last_price_ts,
       f.net_flow_sum_1h,
       f.buyers_cnt_1h,
       f.sellers_cnt_1h,
       NULL::jsonb AS top_buyers_1h,
       NULL::jsonb AS top_sellers_1h
FROM markets m
LEFT JOIN trades_1h   t  ON t.market_id = m.market_id
LEFT JOIN flow_agg    f  ON f.market_id = m.market_id
LEFT JOIN prices      p  ON p.market_id = m.market_id
WHERE t.usd_1h IS NOT NULL
   OR f.net_flow_sum_1h IS NOT NULL
   OR p.last_price_yes IS NOT NULL
   OR p.last_price_no IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS market_spotlight_1h_uidx ON market_spotlight_1h(market_id);

-- Strategy dashboard helper view
CREATE VIEW vw_strategy_position_dashboard AS
SELECT
  sp.strategy,
  sp.market_id,
  LEFT(sp.market_id, 12) AS mkt,
  sp.side,
  sp.qty,
  sp.avg_price,
  m.vertical,
  m.tags,
  m.resolve_ts,
  ms.usd_1h,
  ms.trades_1h,
  ms.net_flow_sum_1h,
  ms.buyers_cnt_1h,
  ms.sellers_cnt_1h,
  ms.top_buyers_1h,
  ms.top_sellers_1h,
  ms.last_price_yes,
  ms.last_price_no,
  ms.last_price_ts,
  CASE WHEN sp.side = 'YES' THEN ms.last_price_yes ELSE ms.last_price_no END AS mark_px,
  (CASE WHEN sp.side = 'YES' THEN ms.last_price_yes ELSE ms.last_price_no END - sp.avg_price) * sp.qty AS u_pnl
FROM strategy_positions sp
JOIN markets m ON m.market_id = sp.market_id
LEFT JOIN market_spotlight_1h ms ON ms.market_id = sp.market_id
WHERE sp.qty > 0;
