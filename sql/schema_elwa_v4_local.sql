--
-- PostgreSQL database dump
--

\restrict w1K92c2ThcqDI6W2hnVsU3tAa4lTNwOKUwg9IQbC6kU9FPUif4Zt2452iwz3G44

-- Dumped from database version 16.10 (Homebrew)
-- Dumped by pg_dump version 16.10 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: wallet_activity_from_trade(); Type: FUNCTION; Schema: public; Owner: tomermaman
--

CREATE FUNCTION public.wallet_activity_from_trade() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.value_usd IS NOT NULL THEN
    INSERT INTO wallet_activity (wallet, market_id, side, price, size, value_usd, role, tx_hash, ts)
    VALUES (
      COALESCE(NEW.taker, NEW.maker),
      NEW.market_id,
      NEW.side,
      NEW.price,
      NEW.size,
      NEW.value_usd,
      CASE WHEN NEW.taker IS NOT NULL THEN 'taker' ELSE 'maker' END,
      NEW.trade_id,
      NEW.ts
    );
  END IF;
  RETURN NEW;
END;
$$;


ALTER FUNCTION public.wallet_activity_from_trade() OWNER TO tomermaman;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: market_ticks; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.market_ticks (
    tick_id bigint NOT NULL,
    asset_id text NOT NULL,
    market_id text,
    event_type text NOT NULL,
    price numeric,
    ts timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.market_ticks OWNER TO tomermaman;

--
-- Name: asset_signals_1h; Type: MATERIALIZED VIEW; Schema: public; Owner: tomermaman
--

CREATE MATERIALIZED VIEW public.asset_signals_1h AS
 WITH last_trade AS (
         SELECT DISTINCT ON (market_ticks.asset_id) market_ticks.asset_id,
            market_ticks.market_id,
            market_ticks.price,
            market_ticks.ts
           FROM public.market_ticks
          WHERE ((market_ticks.event_type = 'last_trade_price'::text) AND (market_ticks.ts >= (now() - '2 days'::interval)) AND (market_ticks.price IS NOT NULL))
          ORDER BY market_ticks.asset_id, market_ticks.ts DESC
        ), price_delta AS (
         SELECT DISTINCT ON (market_ticks.asset_id) market_ticks.asset_id,
            market_ticks.price AS delta_price,
            market_ticks.ts AS delta_ts
           FROM public.market_ticks
          WHERE ((market_ticks.event_type = 'price_change'::text) AND (market_ticks.ts >= (now() - '2 days'::interval)))
          ORDER BY market_ticks.asset_id, market_ticks.ts DESC
        )
 SELECT lt.market_id,
    lt.asset_id,
    lt.price AS last_price,
    lt.ts AS last_price_ts,
    pd.delta_price AS price_change,
    pd.delta_ts AS price_change_ts
   FROM (last_trade lt
     LEFT JOIN price_delta pd USING (asset_id))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.asset_signals_1h OWNER TO tomermaman;

--
-- Name: book_ticks; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.book_ticks (
    market_id text NOT NULL,
    token_id text,
    best_bid numeric,
    best_ask numeric,
    mid numeric,
    bid_size numeric,
    ask_size numeric,
    ts timestamp with time zone NOT NULL
);


ALTER TABLE public.book_ticks OWNER TO tomermaman;

--
-- Name: elite_wallets; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.elite_wallets (
    wallet text NOT NULL,
    nickname text,
    total_positions integer,
    active_positions integer,
    total_wins numeric,
    total_losses numeric,
    win_rate numeric,
    current_value_usd numeric,
    overall_pnl_usd numeric,
    source text DEFAULT 'polymarket_analytics'::text NOT NULL,
    added_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.elite_wallets OWNER TO tomermaman;

--
-- Name: events; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.events (
    event_id text NOT NULL,
    title text,
    category text,
    close_ts timestamp with time zone,
    resolve_ts timestamp with time zone,
    status text
);


ALTER TABLE public.events OWNER TO tomermaman;

--
-- Name: flow_snapshots; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.flow_snapshots (
    ts timestamp with time zone NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    window_secs integer NOT NULL,
    net_flow numeric NOT NULL,
    gross_flow numeric NOT NULL,
    smart_net_flow numeric NOT NULL,
    whale_net_flow numeric NOT NULL,
    wallet_count integer NOT NULL,
    smart_wallets integer NOT NULL,
    whale_wallets integer NOT NULL,
    avg_smart_score numeric,
    smart_value_usd numeric,
    top_a_swing_wallets integer
);


ALTER TABLE public.flow_snapshots OWNER TO tomermaman;

--
-- Name: holders_snap; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.holders_snap (
    market_id text NOT NULL,
    token_id text NOT NULL,
    wallet text NOT NULL,
    balance numeric,
    ts timestamp with time zone NOT NULL
);


ALTER TABLE public.holders_snap OWNER TO tomermaman;

--
-- Name: trades; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.trades (
    trade_id text NOT NULL,
    market_id text,
    taker text,
    maker text,
    side text,
    price numeric,
    size numeric,
    value_usd numeric,
    ts timestamp with time zone,
    asset_id text,
    outcome_index smallint
);


ALTER TABLE public.trades OWNER TO tomermaman;

--
-- Name: market_24h; Type: MATERIALIZED VIEW; Schema: public; Owner: tomermaman
--

CREATE MATERIALIZED VIEW public.market_24h AS
 SELECT market_id,
    max(ts) AS last_ts,
    avg(price) FILTER (WHERE (side = 'BUY'::text)) AS avg_buy_px,
    sum(value_usd) AS vol_24h,
    count(*) AS trades_24h
   FROM public.trades
  WHERE (ts >= (now() - '24:00:00'::interval))
  GROUP BY market_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.market_24h OWNER TO tomermaman;

--
-- Name: markets; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.markets (
    market_id text NOT NULL,
    event_id text,
    question text,
    collateral text,
    yes_token_id text,
    no_token_id text,
    created_ts timestamp with time zone,
    resolve_ts timestamp with time zone,
    resolution text,
    tags jsonb DEFAULT '[]'::jsonb,
    vertical text
);


ALTER TABLE public.markets OWNER TO tomermaman;

--
-- Name: wallet_activity; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.wallet_activity (
    wallet text,
    market_id text,
    side text,
    price numeric,
    size numeric,
    value_usd numeric,
    role text,
    tx_hash text,
    ts timestamp with time zone
);


ALTER TABLE public.wallet_activity OWNER TO tomermaman;

--
-- Name: market_spotlight_1h; Type: MATERIALIZED VIEW; Schema: public; Owner: tomermaman
--

CREATE MATERIALIZED VIEW public.market_spotlight_1h AS
 WITH trades_1h AS (
         SELECT trades.market_id,
            sum(trades.value_usd) AS usd_1h,
            count(*) AS trades_1h
           FROM public.trades
          WHERE ((trades.ts >= (now() - '01:00:00'::interval)) AND (trades.value_usd IS NOT NULL))
          GROUP BY trades.market_id
        ), wallet_flow AS (
         SELECT wallet_activity.market_id,
            wallet_activity.wallet,
            sum(
                CASE
                    WHEN (wallet_activity.side = 'BUY'::text) THEN wallet_activity.value_usd
                    ELSE (- wallet_activity.value_usd)
                END) AS net_flow
           FROM public.wallet_activity
          WHERE ((wallet_activity.ts >= (now() - '01:00:00'::interval)) AND (wallet_activity.value_usd IS NOT NULL))
          GROUP BY wallet_activity.market_id, wallet_activity.wallet
        ), flow_agg AS (
         SELECT wallet_flow.market_id,
            sum(wallet_flow.net_flow) AS net_flow_sum_1h,
            count(*) FILTER (WHERE (wallet_flow.net_flow > (0)::numeric)) AS buyers_cnt_1h,
            count(*) FILTER (WHERE (wallet_flow.net_flow < (0)::numeric)) AS sellers_cnt_1h
           FROM wallet_flow
          GROUP BY wallet_flow.market_id
        ), prices AS (
         SELECT m_1.market_id,
            y.last_price AS last_price_yes,
            y.last_price_ts AS last_price_yes_ts,
            n.last_price AS last_price_no,
            n.last_price_ts AS last_price_no_ts
           FROM ((public.markets m_1
             LEFT JOIN public.asset_signals_1h y ON ((y.asset_id = m_1.yes_token_id)))
             LEFT JOIN public.asset_signals_1h n ON ((n.asset_id = m_1.no_token_id)))
        )
 SELECT m.market_id,
    "left"(m.market_id, 12) AS market_id_hex,
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
   FROM (((public.markets m
     LEFT JOIN trades_1h t ON ((t.market_id = m.market_id)))
     LEFT JOIN flow_agg f ON ((f.market_id = m.market_id)))
     LEFT JOIN prices p ON ((p.market_id = m.market_id)))
  WHERE ((t.usd_1h IS NOT NULL) OR (f.net_flow_sum_1h IS NOT NULL) OR (p.last_price_yes IS NOT NULL) OR (p.last_price_no IS NOT NULL))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.market_spotlight_1h OWNER TO tomermaman;

--
-- Name: market_ticks_tick_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.market_ticks_tick_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.market_ticks_tick_id_seq OWNER TO tomermaman;

--
-- Name: market_ticks_tick_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.market_ticks_tick_id_seq OWNED BY public.market_ticks.tick_id;


--
-- Name: markets_legacy; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.markets_legacy (
    market_id text,
    event_id text,
    question text,
    collateral text,
    yes_token_id text,
    no_token_id text,
    created_ts timestamp with time zone,
    resolve_ts timestamp with time zone,
    resolution text
);


ALTER TABLE public.markets_legacy OWNER TO tomermaman;

--
-- Name: mr_positions; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.mr_positions (
    id integer NOT NULL,
    strategy text NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    side text NOT NULL,
    entry_price numeric NOT NULL,
    entry_ts timestamp with time zone NOT NULL,
    size numeric NOT NULL,
    avg_price_18h numeric NOT NULL,
    dislocation numeric NOT NULL,
    status text DEFAULT 'open'::text,
    exit_price numeric,
    exit_ts timestamp with time zone,
    exit_reason text,
    pnl numeric,
    market_class text
);


ALTER TABLE public.mr_positions OWNER TO tomermaman;

--
-- Name: mr_positions_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.mr_positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.mr_positions_id_seq OWNER TO tomermaman;

--
-- Name: mr_positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.mr_positions_id_seq OWNED BY public.mr_positions.id;


--
-- Name: mr_shadow_fills; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.mr_shadow_fills (
    id integer NOT NULL,
    strategy text NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    side text NOT NULL,
    ts timestamp with time zone NOT NULL,
    size numeric NOT NULL,
    signal_price numeric NOT NULL,
    sim_entry_price numeric NOT NULL,
    avg_price_18h numeric NOT NULL,
    dislocation numeric NOT NULL,
    notes text
);


ALTER TABLE public.mr_shadow_fills OWNER TO tomermaman;

--
-- Name: mr_shadow_fills_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.mr_shadow_fills_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.mr_shadow_fills_id_seq OWNER TO tomermaman;

--
-- Name: mr_shadow_fills_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.mr_shadow_fills_id_seq OWNED BY public.mr_shadow_fills.id;


--
-- Name: mr_short_positions; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.mr_short_positions (
    id integer NOT NULL,
    strategy text NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    side text NOT NULL,
    entry_price numeric NOT NULL,
    entry_ts timestamp with time zone NOT NULL,
    size numeric NOT NULL,
    avg_price_18h numeric NOT NULL,
    dislocation numeric NOT NULL,
    status text DEFAULT 'open'::text,
    exit_price numeric,
    exit_ts timestamp with time zone,
    exit_reason text,
    pnl numeric
);


ALTER TABLE public.mr_short_positions OWNER TO tomermaman;

--
-- Name: mr_short_positions_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.mr_short_positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.mr_short_positions_id_seq OWNER TO tomermaman;

--
-- Name: mr_short_positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.mr_short_positions_id_seq OWNED BY public.mr_short_positions.id;


--
-- Name: mr_v1_class_pnl; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.mr_v1_class_pnl AS
 SELECT COALESCE(market_class, '(unknown)'::text) AS market_class,
    count(*) AS trades,
    avg(dislocation) AS avg_dislocation,
    sum(pnl) AS sum_pnl,
    avg(pnl) AS avg_pnl,
    avg(
        CASE
            WHEN (pnl > (0)::numeric) THEN 1
            ELSE 0
        END) AS winrate
   FROM public.mr_positions p
  WHERE ((strategy = 'mean_reversion_v1'::text) AND (status = 'closed'::text))
  GROUP BY COALESCE(market_class, '(unknown)'::text)
  ORDER BY (sum(pnl)) DESC;


ALTER VIEW public.mr_v1_class_pnl OWNER TO tomermaman;

--
-- Name: mr_v1_dislocation_buckets; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.mr_v1_dislocation_buckets AS
 SELECT width_bucket(dislocation, '-0.50'::numeric, '-0.20'::numeric, 6) AS bucket,
    min(dislocation) AS bucket_min,
    max(dislocation) AS bucket_max,
    count(*) AS trades,
    avg(pnl) AS avg_pnl,
    sum(pnl) AS sum_pnl,
    avg(
        CASE
            WHEN (pnl > (0)::numeric) THEN 1
            ELSE 0
        END) AS winrate
   FROM public.mr_positions p
  WHERE ((strategy = 'mean_reversion_v1'::text) AND (status = 'closed'::text))
  GROUP BY (width_bucket(dislocation, '-0.50'::numeric, '-0.20'::numeric, 6))
  ORDER BY (width_bucket(dislocation, '-0.50'::numeric, '-0.20'::numeric, 6));


ALTER VIEW public.mr_v1_dislocation_buckets OWNER TO tomermaman;

--
-- Name: mr_v1_market_summary; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.mr_v1_market_summary AS
 SELECT p.market_id,
    COALESCE(m.question, p.market_id) AS market_name,
    COALESCE(p.market_class, '(unknown)'::text) AS market_class,
    count(*) AS trades,
    sum(p.pnl) AS sum_pnl,
    avg(p.pnl) AS avg_pnl,
    avg(p.dislocation) AS avg_dislocation,
    avg(
        CASE
            WHEN (p.pnl > (0)::numeric) THEN 1
            ELSE 0
        END) AS winrate
   FROM (public.mr_positions p
     LEFT JOIN public.markets m ON ((m.market_id = p.market_id)))
  WHERE ((p.strategy = 'mean_reversion_v1'::text) AND (p.status = 'closed'::text))
  GROUP BY p.market_id, COALESCE(m.question, p.market_id), COALESCE(p.market_class, '(unknown)'::text);


ALTER VIEW public.mr_v1_market_summary OWNER TO tomermaman;

--
-- Name: mr_v1_shadow_stats; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.mr_v1_shadow_stats AS
 SELECT count(*) AS n,
    avg(dislocation) AS avg_dislocation,
    avg(abs(dislocation)) AS avg_abs_dislocation,
    avg((sim_entry_price - signal_price)) AS avg_slip_abs,
    (avg(((sim_entry_price - signal_price) / NULLIF(signal_price, (0)::numeric))) * (100)::numeric) AS avg_slip_pct
   FROM public.mr_shadow_fills s
  WHERE (strategy = 'mean_reversion_v1'::text);


ALTER VIEW public.mr_v1_shadow_stats OWNER TO tomermaman;

--
-- Name: mr_v1_shadow_vs_live; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.mr_v1_shadow_vs_live AS
 SELECT p.id AS position_id,
    p.entry_ts,
    p.market_id,
    p.outcome,
    p.entry_price,
    s.sim_entry_price,
    (p.entry_price - s.sim_entry_price) AS entry_price_diff,
    p.size AS pos_size,
    s.size AS shadow_size,
    (p.size - s.size) AS size_diff,
    p.dislocation AS pos_dislocation,
    s.dislocation AS shadow_dislocation,
    (p.dislocation - s.dislocation) AS dislocation_diff
   FROM (public.mr_positions p
     JOIN public.mr_shadow_fills s ON (((s.strategy = p.strategy) AND (s.market_id = p.market_id) AND (s.outcome = p.outcome) AND (s.ts = p.entry_ts))))
  WHERE (p.strategy = 'mean_reversion_v1'::text);


ALTER VIEW public.mr_v1_shadow_vs_live OWNER TO tomermaman;

--
-- Name: paper_fills; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.paper_fills (
    id integer NOT NULL,
    position_id integer,
    price numeric,
    ts timestamp with time zone,
    reason text
);


ALTER TABLE public.paper_fills OWNER TO tomermaman;

--
-- Name: paper_fills_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.paper_fills_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.paper_fills_id_seq OWNER TO tomermaman;

--
-- Name: paper_fills_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.paper_fills_id_seq OWNED BY public.paper_fills.id;


--
-- Name: paper_positions; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.paper_positions (
    id integer NOT NULL,
    strategy text,
    market_id text,
    outcome text,
    side text,
    entry_price numeric,
    entry_ts timestamp with time zone,
    size numeric,
    score numeric,
    smart_wallets integer,
    smart_net_flow numeric,
    status text DEFAULT 'open'::text,
    exit_price numeric,
    exit_ts timestamp with time zone,
    pnl numeric
);


ALTER TABLE public.paper_positions OWNER TO tomermaman;

--
-- Name: paper_positions_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.paper_positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.paper_positions_id_seq OWNER TO tomermaman;

--
-- Name: paper_positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.paper_positions_id_seq OWNED BY public.paper_positions.id;


--
-- Name: raw_trades; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.raw_trades (
    trade_id bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    taker text NOT NULL,
    maker text NOT NULL,
    side text NOT NULL,
    qty numeric NOT NULL,
    price numeric NOT NULL,
    tx_hash text,
    value_usd numeric
);


ALTER TABLE public.raw_trades OWNER TO tomermaman;

--
-- Name: raw_trades_trade_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.raw_trades_trade_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.raw_trades_trade_id_seq OWNER TO tomermaman;

--
-- Name: raw_trades_trade_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.raw_trades_trade_id_seq OWNED BY public.raw_trades.trade_id;


--
-- Name: round_trips; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.round_trips (
    strategy text,
    market_id text,
    mkt text,
    entry_order_id integer,
    exit_order_id integer,
    entry_side text,
    exit_side text,
    entry_ts timestamp with time zone,
    exit_ts timestamp with time zone,
    hold_sec numeric,
    hold_minutes numeric,
    hold_hours numeric
);


ALTER TABLE public.round_trips OWNER TO tomermaman;

--
-- Name: strategy_exit_state_v2; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.strategy_exit_state_v2 (
    strategy text NOT NULL,
    market_id text NOT NULL,
    side text NOT NULL,
    partial_exit_taken boolean DEFAULT false NOT NULL,
    last_partial_ts timestamp with time zone
);


ALTER TABLE public.strategy_exit_state_v2 OWNER TO tomermaman;

--
-- Name: strategy_fills; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.strategy_fills (
    id bigint NOT NULL,
    order_id bigint,
    qty numeric NOT NULL,
    price numeric NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    paper boolean DEFAULT true NOT NULL
);


ALTER TABLE public.strategy_fills OWNER TO tomermaman;

--
-- Name: strategy_fills_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.strategy_fills_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.strategy_fills_id_seq OWNER TO tomermaman;

--
-- Name: strategy_fills_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.strategy_fills_id_seq OWNED BY public.strategy_fills.id;


--
-- Name: strategy_metrics_daily; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.strategy_metrics_daily (
    strategy text NOT NULL,
    date date NOT NULL,
    trades integer,
    pnl numeric,
    winrate numeric,
    avg_rr numeric,
    drawdown numeric,
    paper boolean DEFAULT true NOT NULL
);


ALTER TABLE public.strategy_metrics_daily OWNER TO tomermaman;

--
-- Name: strategy_orders; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.strategy_orders (
    id bigint NOT NULL,
    strategy text NOT NULL,
    market_id text NOT NULL,
    side text NOT NULL,
    qty numeric NOT NULL,
    limit_px numeric,
    status text DEFAULT 'submitted'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    paper boolean DEFAULT true NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb
);


ALTER TABLE public.strategy_orders OWNER TO tomermaman;

--
-- Name: strategy_orders_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.strategy_orders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.strategy_orders_id_seq OWNER TO tomermaman;

--
-- Name: strategy_orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.strategy_orders_id_seq OWNED BY public.strategy_orders.id;


--
-- Name: strategy_positions; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.strategy_positions (
    strategy text NOT NULL,
    market_id text NOT NULL,
    side text NOT NULL,
    qty numeric NOT NULL,
    avg_price numeric NOT NULL,
    opened_at timestamp with time zone NOT NULL,
    max_adverse numeric DEFAULT 0,
    max_favourable numeric DEFAULT 0
);


ALTER TABLE public.strategy_positions OWNER TO tomermaman;

--
-- Name: strategy_signals; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.strategy_signals (
    id bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    strategy text NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    side text NOT NULL,
    score numeric NOT NULL,
    reason jsonb NOT NULL,
    expires_at timestamp with time zone NOT NULL
);


ALTER TABLE public.strategy_signals OWNER TO tomermaman;

--
-- Name: strategy_signals_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.strategy_signals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.strategy_signals_id_seq OWNER TO tomermaman;

--
-- Name: strategy_signals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.strategy_signals_id_seq OWNED BY public.strategy_signals.id;


--
-- Name: strategy_trade_ledger; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.strategy_trade_ledger AS
 SELECT f.id AS fill_id,
    f.order_id,
    o.strategy,
    o.market_id,
    "left"(o.market_id, 12) AS mkt,
    o.side,
    f.qty,
    f.price,
    (f.qty * f.price) AS notional_yes,
    f.ts AS fill_ts
   FROM (public.strategy_fills f
     JOIN public.strategy_orders o ON ((o.id = f.order_id)))
  ORDER BY f.ts;


ALTER VIEW public.strategy_trade_ledger OWNER TO tomermaman;

--
-- Name: top_a_wallets; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.top_a_wallets (
    wallet text NOT NULL,
    tagged_at timestamp with time zone DEFAULT now() NOT NULL,
    last_win_rate numeric,
    last_trades integer,
    last_volume numeric,
    last_pnl numeric,
    last_avg_hold_secs numeric
);


ALTER TABLE public.top_a_wallets OWNER TO tomermaman;

--
-- Name: vw_strategy_position_dashboard; Type: VIEW; Schema: public; Owner: tomermaman
--

CREATE VIEW public.vw_strategy_position_dashboard AS
 SELECT sp.strategy,
    sp.market_id,
    "left"(sp.market_id, 12) AS mkt,
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
        CASE
            WHEN (sp.side = 'YES'::text) THEN ms.last_price_yes
            ELSE ms.last_price_no
        END AS mark_px,
    ((
        CASE
            WHEN (sp.side = 'YES'::text) THEN ms.last_price_yes
            ELSE ms.last_price_no
        END - sp.avg_price) * sp.qty) AS u_pnl
   FROM ((public.strategy_positions sp
     JOIN public.markets m ON ((m.market_id = sp.market_id)))
     LEFT JOIN public.market_spotlight_1h ms ON ((ms.market_id = sp.market_id)))
  WHERE (sp.qty > (0)::numeric);


ALTER VIEW public.vw_strategy_position_dashboard OWNER TO tomermaman;

--
-- Name: wallet_flow_24h; Type: MATERIALIZED VIEW; Schema: public; Owner: tomermaman
--

CREATE MATERIALIZED VIEW public.wallet_flow_24h AS
 SELECT wallet,
    market_id,
    sum(
        CASE
            WHEN (side = 'BUY'::text) THEN value_usd
            ELSE (- value_usd)
        END) AS net_flow_usd
   FROM public.wallet_activity
  WHERE (ts >= (now() - '24:00:00'::interval))
  GROUP BY wallet, market_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.wallet_flow_24h OWNER TO tomermaman;

--
-- Name: wallet_labels; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.wallet_labels (
    wallet text NOT NULL,
    is_smart boolean,
    smart_score numeric,
    is_whale boolean,
    avg_size numeric,
    main_vertical text,
    style text,
    median_hold_secs numeric,
    last_updated timestamp with time zone DEFAULT now() NOT NULL,
    as_of_day date,
    last_win_rate numeric,
    last_trades integer,
    last_volume numeric,
    last_pnl numeric,
    last_avg_hold_secs numeric,
    is_elite boolean DEFAULT false,
    is_top_a boolean DEFAULT false,
    is_top_a_swing boolean DEFAULT false
);


ALTER TABLE public.wallet_labels OWNER TO tomermaman;

--
-- Name: wallet_netflow_1h; Type: MATERIALIZED VIEW; Schema: public; Owner: tomermaman
--

CREATE MATERIALIZED VIEW public.wallet_netflow_1h AS
 SELECT market_id,
    wallet,
    sum(
        CASE
            WHEN (side = 'BUY'::text) THEN value_usd
            ELSE (- value_usd)
        END) AS net_flow_sum_1h,
    sum(value_usd) FILTER (WHERE (side = 'BUY'::text)) AS buy_usd_1h,
    sum(value_usd) FILTER (WHERE (side = 'SELL'::text)) AS sell_usd_1h,
    count(*) FILTER (WHERE (side = 'BUY'::text)) AS buy_trades_1h,
    count(*) FILTER (WHERE (side = 'SELL'::text)) AS sell_trades_1h,
    min(ts) AS first_ts,
    max(ts) AS last_ts
   FROM public.wallet_activity
  WHERE ((ts >= (now() - '01:00:00'::interval)) AND (value_usd IS NOT NULL))
  GROUP BY market_id, wallet
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.wallet_netflow_1h OWNER TO tomermaman;

--
-- Name: wallet_positions; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.wallet_positions (
    position_id bigint NOT NULL,
    wallet text NOT NULL,
    market_id text NOT NULL,
    outcome text NOT NULL,
    opened_at timestamp with time zone NOT NULL,
    closed_at timestamp with time zone,
    entry_px numeric,
    exit_px numeric,
    size numeric,
    realized_pnl numeric,
    status text NOT NULL,
    pnl_samples integer DEFAULT 0
);


ALTER TABLE public.wallet_positions OWNER TO tomermaman;

--
-- Name: wallet_positions_position_id_seq; Type: SEQUENCE; Schema: public; Owner: tomermaman
--

CREATE SEQUENCE public.wallet_positions_position_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.wallet_positions_position_id_seq OWNER TO tomermaman;

--
-- Name: wallet_positions_position_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: tomermaman
--

ALTER SEQUENCE public.wallet_positions_position_id_seq OWNED BY public.wallet_positions.position_id;


--
-- Name: wallet_stats_daily; Type: TABLE; Schema: public; Owner: tomermaman
--

CREATE TABLE public.wallet_stats_daily (
    day date NOT NULL,
    wallet text NOT NULL,
    markets_traded integer NOT NULL,
    trades_count integer NOT NULL,
    gross_volume numeric NOT NULL,
    realized_pnl numeric NOT NULL,
    win_rate numeric,
    median_hold_secs numeric,
    max_dd numeric,
    specialization text,
    sharpe_like numeric,
    trades_won integer,
    trades_lost integer,
    last_updated timestamp with time zone
);


ALTER TABLE public.wallet_stats_daily OWNER TO tomermaman;

--
-- Name: market_ticks tick_id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.market_ticks ALTER COLUMN tick_id SET DEFAULT nextval('public.market_ticks_tick_id_seq'::regclass);


--
-- Name: mr_positions id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.mr_positions ALTER COLUMN id SET DEFAULT nextval('public.mr_positions_id_seq'::regclass);


--
-- Name: mr_shadow_fills id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.mr_shadow_fills ALTER COLUMN id SET DEFAULT nextval('public.mr_shadow_fills_id_seq'::regclass);


--
-- Name: mr_short_positions id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.mr_short_positions ALTER COLUMN id SET DEFAULT nextval('public.mr_short_positions_id_seq'::regclass);


--
-- Name: paper_fills id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.paper_fills ALTER COLUMN id SET DEFAULT nextval('public.paper_fills_id_seq'::regclass);


--
-- Name: paper_positions id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.paper_positions ALTER COLUMN id SET DEFAULT nextval('public.paper_positions_id_seq'::regclass);


--
-- Name: raw_trades trade_id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.raw_trades ALTER COLUMN trade_id SET DEFAULT nextval('public.raw_trades_trade_id_seq'::regclass);


--
-- Name: strategy_fills id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_fills ALTER COLUMN id SET DEFAULT nextval('public.strategy_fills_id_seq'::regclass);


--
-- Name: strategy_orders id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_orders ALTER COLUMN id SET DEFAULT nextval('public.strategy_orders_id_seq'::regclass);


--
-- Name: strategy_signals id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_signals ALTER COLUMN id SET DEFAULT nextval('public.strategy_signals_id_seq'::regclass);


--
-- Name: wallet_positions position_id; Type: DEFAULT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.wallet_positions ALTER COLUMN position_id SET DEFAULT nextval('public.wallet_positions_position_id_seq'::regclass);


--
-- Name: book_ticks book_ticks_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.book_ticks
    ADD CONSTRAINT book_ticks_pkey PRIMARY KEY (market_id, ts);


--
-- Name: elite_wallets elite_wallets_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.elite_wallets
    ADD CONSTRAINT elite_wallets_pkey PRIMARY KEY (wallet);


--
-- Name: events events_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (event_id);


--
-- Name: flow_snapshots flow_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.flow_snapshots
    ADD CONSTRAINT flow_snapshots_pkey PRIMARY KEY (ts, market_id, outcome, window_secs);


--
-- Name: holders_snap holders_snap_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.holders_snap
    ADD CONSTRAINT holders_snap_pkey PRIMARY KEY (market_id, token_id, wallet, ts);


--
-- Name: market_ticks market_ticks_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.market_ticks
    ADD CONSTRAINT market_ticks_pkey PRIMARY KEY (tick_id);


--
-- Name: markets markets_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT markets_pkey PRIMARY KEY (market_id);


--
-- Name: mr_positions mr_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.mr_positions
    ADD CONSTRAINT mr_positions_pkey PRIMARY KEY (id);


--
-- Name: mr_shadow_fills mr_shadow_fills_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.mr_shadow_fills
    ADD CONSTRAINT mr_shadow_fills_pkey PRIMARY KEY (id);


--
-- Name: mr_short_positions mr_short_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.mr_short_positions
    ADD CONSTRAINT mr_short_positions_pkey PRIMARY KEY (id);


--
-- Name: paper_fills paper_fills_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.paper_fills
    ADD CONSTRAINT paper_fills_pkey PRIMARY KEY (id);


--
-- Name: paper_positions paper_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.paper_positions
    ADD CONSTRAINT paper_positions_pkey PRIMARY KEY (id);


--
-- Name: raw_trades raw_trades_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.raw_trades
    ADD CONSTRAINT raw_trades_pkey PRIMARY KEY (trade_id);


--
-- Name: strategy_exit_state_v2 strategy_exit_state_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_exit_state_v2
    ADD CONSTRAINT strategy_exit_state_v2_pkey PRIMARY KEY (strategy, market_id, side);


--
-- Name: strategy_fills strategy_fills_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_fills
    ADD CONSTRAINT strategy_fills_pkey PRIMARY KEY (id);


--
-- Name: strategy_metrics_daily strategy_metrics_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_metrics_daily
    ADD CONSTRAINT strategy_metrics_daily_pkey PRIMARY KEY (strategy, date, paper);


--
-- Name: strategy_orders strategy_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_orders
    ADD CONSTRAINT strategy_orders_pkey PRIMARY KEY (id);


--
-- Name: strategy_positions strategy_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_positions
    ADD CONSTRAINT strategy_positions_pkey PRIMARY KEY (strategy, market_id, side);


--
-- Name: strategy_signals strategy_signals_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_signals
    ADD CONSTRAINT strategy_signals_pkey PRIMARY KEY (id);


--
-- Name: top_a_wallets top_a_wallets_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.top_a_wallets
    ADD CONSTRAINT top_a_wallets_pkey PRIMARY KEY (wallet);


--
-- Name: trades trades_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.trades
    ADD CONSTRAINT trades_pkey PRIMARY KEY (trade_id);


--
-- Name: wallet_labels wallet_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.wallet_labels
    ADD CONSTRAINT wallet_labels_pkey PRIMARY KEY (wallet);


--
-- Name: wallet_positions wallet_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.wallet_positions
    ADD CONSTRAINT wallet_positions_pkey PRIMARY KEY (position_id);


--
-- Name: wallet_stats_daily wallet_stats_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.wallet_stats_daily
    ADD CONSTRAINT wallet_stats_daily_pkey PRIMARY KEY (day, wallet);


--
-- Name: asset_signals_1h_asset_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE UNIQUE INDEX asset_signals_1h_asset_idx ON public.asset_signals_1h USING btree (asset_id);


--
-- Name: asset_signals_1h_market_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX asset_signals_1h_market_idx ON public.asset_signals_1h USING btree (market_id);


--
-- Name: idx_book_ticks_market_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_book_ticks_market_ts ON public.book_ticks USING btree (market_id, ts);


--
-- Name: idx_flow_snapshots_lookup; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_flow_snapshots_lookup ON public.flow_snapshots USING btree (ts DESC, market_id);


--
-- Name: idx_flow_snapshots_ts_desc; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_flow_snapshots_ts_desc ON public.flow_snapshots USING btree (ts DESC);


--
-- Name: idx_holders_snap_market_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_holders_snap_market_ts ON public.holders_snap USING btree (market_id, ts);


--
-- Name: idx_market_ticks_asset_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_market_ticks_asset_ts ON public.market_ticks USING btree (asset_id, ts DESC);


--
-- Name: idx_market_ticks_market_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_market_ticks_market_ts ON public.market_ticks USING btree (market_id, ts);


--
-- Name: idx_mr_positions_open; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_mr_positions_open ON public.mr_positions USING btree (strategy, status, market_id, outcome);


--
-- Name: idx_mr_shadow_fills_main; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_mr_shadow_fills_main ON public.mr_shadow_fills USING btree (strategy, market_id, outcome, ts);


--
-- Name: idx_mr_short_positions_open; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_mr_short_positions_open ON public.mr_short_positions USING btree (strategy, status, market_id, outcome);


--
-- Name: idx_paper_positions_open; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_paper_positions_open ON public.paper_positions USING btree (status);


--
-- Name: idx_raw_trades_market_outcome_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_raw_trades_market_outcome_ts ON public.raw_trades USING btree (market_id, outcome, ts);


--
-- Name: idx_raw_trades_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_raw_trades_ts ON public.raw_trades USING btree (ts);


--
-- Name: idx_signals_strat_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_signals_strat_ts ON public.strategy_signals USING btree (strategy, ts DESC);


--
-- Name: idx_strategy_fills_order; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_strategy_fills_order ON public.strategy_fills USING btree (order_id);


--
-- Name: idx_strategy_orders_market; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_strategy_orders_market ON public.strategy_orders USING btree (market_id);


--
-- Name: idx_strategy_orders_strategy; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_strategy_orders_strategy ON public.strategy_orders USING btree (strategy);


--
-- Name: idx_trades_asset_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_trades_asset_ts ON public.trades USING btree (asset_id, ts);


--
-- Name: idx_trades_market_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_trades_market_ts ON public.trades USING btree (market_id, ts);


--
-- Name: idx_wallet_activity_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_wallet_activity_ts ON public.wallet_activity USING btree (ts);


--
-- Name: idx_wallet_activity_wallet_ts; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX idx_wallet_activity_wallet_ts ON public.wallet_activity USING btree (wallet, ts);


--
-- Name: market_spotlight_1h_uidx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE UNIQUE INDEX market_spotlight_1h_uidx ON public.market_spotlight_1h USING btree (market_id);


--
-- Name: raw_trades_maker_ts_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX raw_trades_maker_ts_idx ON public.raw_trades USING btree (maker, ts);


--
-- Name: raw_trades_market_ts_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX raw_trades_market_ts_idx ON public.raw_trades USING btree (market_id, ts);


--
-- Name: raw_trades_taker_ts_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX raw_trades_taker_ts_idx ON public.raw_trades USING btree (taker, ts);


--
-- Name: raw_trades_ts_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX raw_trades_ts_idx ON public.raw_trades USING btree (ts);


--
-- Name: uq_paper_positions_open; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE UNIQUE INDEX uq_paper_positions_open ON public.paper_positions USING btree (strategy, market_id, outcome, side) WHERE (status = 'open'::text);


--
-- Name: wallet_netflow_1h_uidx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE UNIQUE INDEX wallet_netflow_1h_uidx ON public.wallet_netflow_1h USING btree (market_id, wallet);


--
-- Name: wallet_positions_wallet_status_idx; Type: INDEX; Schema: public; Owner: tomermaman
--

CREATE INDEX wallet_positions_wallet_status_idx ON public.wallet_positions USING btree (wallet, status, opened_at);


--
-- Name: trades trg_wallet_activity_after_insert; Type: TRIGGER; Schema: public; Owner: tomermaman
--

CREATE TRIGGER trg_wallet_activity_after_insert AFTER INSERT ON public.trades FOR EACH ROW EXECUTE FUNCTION public.wallet_activity_from_trade();


--
-- Name: markets markets_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT markets_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.events(event_id);


--
-- Name: paper_fills paper_fills_position_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.paper_fills
    ADD CONSTRAINT paper_fills_position_id_fkey FOREIGN KEY (position_id) REFERENCES public.paper_positions(id);


--
-- Name: strategy_fills strategy_fills_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: tomermaman
--

ALTER TABLE ONLY public.strategy_fills
    ADD CONSTRAINT strategy_fills_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.strategy_orders(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict w1K92c2ThcqDI6W2hnVsU3tAa4lTNwOKUwg9IQbC6kU9FPUif4Zt2452iwz3G44

