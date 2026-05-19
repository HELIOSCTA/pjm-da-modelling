{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Reserve Market Results (pivoted wide)
-- Grain: 1 row per (date, hour_ending) at locale='PJM_RTO' (system-wide)
--
-- Scope: PJM_RTO locale only -- the supply-stack model is system-wide,
-- so the locational MAD (Mid-Atlantic-Dominion) sub-zone is intentionally
-- omitted from v1. Add `_mad`-suffixed columns or a sibling staging when
-- a locational dispatch view is needed.
--
-- Coverage: backward-only feed (today and forward dates return empty).
-- Forward-looking consumers compute a rolling profile by (DOW, HE) from
-- the historical rows in this mart -- the staging does NOT pad forward
-- dates with NULL rows.
--
-- Two derived columns ride at the end -- ``operating_reserve_mw_cleared``
-- (SR + PR + 30MIN total_mw -- the MW PJM actually held out of energy that
-- hour) and ``operating_reserve_requirement_mw`` (SR + PR + 30MIN as_req_mw
-- -- the more stable forward proxy). REG (regulation) is held separately
-- as its own market and is NOT in the operating-reserve sums.
--
-- ``reserve_scarcity_flag`` = TRUE when any of SR / PR / 30MIN cleared with
-- MCP > $10/MWh -- a regime proxy for "scarcity adder active". MCP > $0 by
-- itself fires too eagerly (the bid stack often clears at $0.14 etc.); the
-- $10 threshold isolates meaningful scarcity events (e.g. May 12 2026 HE21
-- @ $196 MCP). PJM's actual trigger is the Reserve Penalty Factor
-- exceedance; this is the regime proxy.
---------------------------

WITH LONG AS (
    SELECT * FROM {{ ref('source_v1_pjm_reserve_market_results') }}
    WHERE locale = 'PJM_RTO'
),

PIVOTED AS (
    SELECT
        MAX(datetime_beginning_utc) AS datetime_beginning_utc
        ,MAX(datetime_ending_utc) AS datetime_ending_utc
        ,MAX(timezone) AS timezone
        ,MAX(datetime_beginning_local) AS datetime_beginning_local
        ,MAX(datetime_ending_local) AS datetime_ending_local
        ,date
        ,hour_ending

        -- Synchronized Reserve (SR)
        ,MAX(CASE WHEN service = 'SR' THEN total_mw    END) AS sr_total_mw
        ,MAX(CASE WHEN service = 'SR' THEN as_req_mw   END) AS sr_requirement_mw
        ,MAX(CASE WHEN service = 'SR' THEN mcp         END) AS sr_mcp
        ,MAX(CASE WHEN service = 'SR' THEN mcp_capped  END) AS sr_mcp_capped

        -- Primary Reserve (PR = sync + non-sync)
        ,MAX(CASE WHEN service = 'PR' THEN total_mw    END) AS pr_total_mw
        ,MAX(CASE WHEN service = 'PR' THEN as_req_mw   END) AS pr_requirement_mw
        ,MAX(CASE WHEN service = 'PR' THEN mcp         END) AS pr_mcp
        ,MAX(CASE WHEN service = 'PR' THEN mcp_capped  END) AS pr_mcp_capped
        ,MAX(CASE WHEN service = 'PR' THEN nsr_mw      END) AS pr_nsr_mw

        -- 30-Minute Reserve
        ,MAX(CASE WHEN service = '30MIN' THEN total_mw   END) AS min30_total_mw
        ,MAX(CASE WHEN service = '30MIN' THEN as_req_mw  END) AS min30_requirement_mw
        ,MAX(CASE WHEN service = '30MIN' THEN mcp        END) AS min30_mcp
        ,MAX(CASE WHEN service = '30MIN' THEN mcp_capped END) AS min30_mcp_capped

        -- Regulation (separate product -- frequency control, NOT in operating-reserve sums)
        ,MAX(CASE WHEN service = 'REG' THEN total_mw    END) AS reg_total_mw
        ,MAX(CASE WHEN service = 'REG' THEN as_req_mw   END) AS reg_requirement_mw
        ,MAX(CASE WHEN service = 'REG' THEN reg_ccp     END) AS reg_ccp
        ,MAX(CASE WHEN service = 'REG' THEN reg_pcp     END) AS reg_pcp
        ,MAX(CASE WHEN service = 'REG' THEN mcp_capped  END) AS reg_mcp_capped
    FROM LONG
    GROUP BY date, hour_ending
),

DERIVED AS (
    SELECT
        *
        -- Operating-reserve totals (SR + PR + 30MIN). NULL service columns
        -- COALESCE to 0 so a missing service doesn't NULL the whole sum.
        ,(COALESCE(sr_total_mw, 0) + COALESCE(pr_total_mw, 0) + COALESCE(min30_total_mw, 0))
            AS operating_reserve_mw_cleared
        ,(COALESCE(sr_requirement_mw, 0) + COALESCE(pr_requirement_mw, 0) + COALESCE(min30_requirement_mw, 0))
            AS operating_reserve_requirement_mw
        ,(GREATEST(
            COALESCE(sr_mcp, 0),
            COALESCE(pr_mcp, 0),
            COALESCE(min30_mcp, 0)
        ) > 10) AS reserve_scarcity_flag
    FROM PIVOTED
)

SELECT * FROM DERIVED
ORDER BY datetime_ending_local DESC
