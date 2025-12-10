-- mr_v1_analytics_views.sql
-- Analytics views for mean_reversion_v1

-- 1. Dislocation buckets vs PnL (closed trades only)
CREATE OR REPLACE VIEW mr_v1_dislocation_buckets AS
SELECT
  width_bucket(p.dislocation, -0.50, -0.20, 6) AS bucket,
  MIN(p.dislocation)                            AS bucket_min,
  MAX(p.dislocation)                            AS bucket_max,
  COUNT(*)                                      AS trades,
  AVG(p.pnl)                                    AS avg_pnl,
  SUM(p.pnl)                                    AS sum_pnl,
  AVG( CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END )  AS winrate
FROM mr_positions p
WHERE p.strategy = 'mean_reversion_v1'
  AND p.status = 'closed'
GROUP BY 1
ORDER BY 1;


-- 2. Market class level performance (needs market_class set on entry)
CREATE OR REPLACE VIEW mr_v1_class_pnl AS
SELECT
  COALESCE(p.market_class, '(unknown)')                AS market_class,
  COUNT(*)                                             AS trades,
  AVG(p.dislocation)                                   AS avg_dislocation,
  SUM(p.pnl)                                           AS sum_pnl,
  AVG(p.pnl)                                           AS avg_pnl,
  AVG(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END)           AS winrate
FROM mr_positions p
WHERE p.strategy = 'mean_reversion_v1'
  AND p.status = 'closed'
GROUP BY 1
ORDER BY sum_pnl DESC;


-- 3. Toxic / recurring markets (markets that got hit several times)
CREATE OR REPLACE VIEW mr_v1_market_summary AS
SELECT
  p.market_id,
  COALESCE(m.question, p.market_id)                    AS market_name,
  COALESCE(p.market_class, '(unknown)')                AS market_class,
  COUNT(*)                                             AS trades,
  SUM(p.pnl)                                           AS sum_pnl,
  AVG(p.pnl)                                           AS avg_pnl,
  AVG(p.dislocation)                                   AS avg_dislocation,
  AVG(CASE WHEN p.pnl > 0 THEN 1 ELSE 0 END)           AS winrate
FROM mr_positions p
LEFT JOIN markets m ON m.market_id = p.market_id
WHERE p.strategy = 'mean_reversion_v1'
  AND p.status = 'closed'
GROUP BY p.market_id, COALESCE(m.question, p.market_id), COALESCE(p.market_class, '(unknown)');


-- 4. Shadow vs live comparison summary (after shadow table added)
-- Compares positions and shadow fills joined by strategy+market+outcome+ts
CREATE OR REPLACE VIEW mr_v1_shadow_vs_live AS
SELECT
  p.id                            AS position_id,
  p.entry_ts,
  p.market_id,
  p.outcome,
  p.entry_price,
  s.sim_entry_price,
  (p.entry_price - s.sim_entry_price)                  AS entry_price_diff,
  p.size                                               AS pos_size,
  s.size                                               AS shadow_size,
  (p.size - s.size)                                    AS size_diff,
  p.dislocation                                        AS pos_dislocation,
  s.dislocation                                        AS shadow_dislocation,
  (p.dislocation - s.dislocation)                      AS dislocation_diff
FROM mr_positions p
JOIN mr_shadow_fills s
  ON s.strategy  = p.strategy
 AND s.market_id = p.market_id
 AND s.outcome   = p.outcome
 AND s.ts        = p.entry_ts
WHERE p.strategy = 'mean_reversion_v1';


-- 5. High-level shadow summary
CREATE OR REPLACE VIEW mr_v1_shadow_stats AS
SELECT
  COUNT(*)                                   AS n,
  AVG(s.dislocation)                         AS avg_dislocation,
  AVG(ABS(s.dislocation))                    AS avg_abs_dislocation,
  AVG((s.sim_entry_price - s.signal_price))  AS avg_slip_abs,
  AVG((s.sim_entry_price - s.signal_price)
       / NULLIF(s.signal_price,0)) * 100     AS avg_slip_pct
FROM mr_shadow_fills s
WHERE s.strategy = 'mean_reversion_v1';
