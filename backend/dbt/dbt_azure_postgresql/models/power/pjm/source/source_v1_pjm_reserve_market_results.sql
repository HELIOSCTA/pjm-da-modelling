{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Reserve Market Results (normalized)
-- Grain: 1 row per (datetime_beginning_utc, locale, service)
-- Source: PJM Data Miner 2 reserve_market_results feed. Hourly grain,
-- backward-looking only (today and forward dates return empty), first
-- available 2013-06-14.
---------------------------

SELECT
    datetime_beginning_utc
    ,datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
    ,'US/Eastern' AS timezone
    ,datetime_beginning_ept AS datetime_beginning_local
    ,datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
    ,datetime_beginning_ept::DATE AS date
    ,(EXTRACT(HOUR FROM datetime_beginning_ept) + 1)::INT AS hour_ending
    ,locale
    ,service
    ,mcp::NUMERIC AS mcp
    ,mcp_capped::NUMERIC AS mcp_capped
    ,reg_ccp::NUMERIC AS reg_ccp
    ,reg_pcp::NUMERIC AS reg_pcp
    ,as_req_mw::NUMERIC AS as_req_mw
    ,total_mw::NUMERIC AS total_mw
    ,as_mw::NUMERIC AS as_mw
    ,ss_mw::NUMERIC AS ss_mw
    ,tier1_mw::NUMERIC AS tier1_mw
    ,ircmwt2::NUMERIC AS ircmwt2
    ,dsr_as_mw::NUMERIC AS dsr_as_mw
    ,nsr_mw::NUMERIC AS nsr_mw
    ,regd_mw::NUMERIC AS regd_mw
FROM {{ source('pjm_v1', 'reserve_market_results') }}
ORDER BY datetime_ending_utc DESC, locale, service
