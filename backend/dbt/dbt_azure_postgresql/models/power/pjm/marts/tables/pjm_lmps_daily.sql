{{
  config(
    materialized='incremental',
    unique_key=['date', 'hub', 'period', 'market'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['hub', 'market', 'period', 'date'], 'type': 'btree'},
      {'columns': ['date'], 'type': 'btree'}
    ]
  )
}}

----------------------------------
-- Daily LMPs (normalized)
-- Grain: 1 row per date x hub x period x market
-- period in ('flat','onpeak','offpeak'); 'onpeak' = HE 8-23 of the day
-- (literal hours, not NERC on-peak — applies the same window across
-- weekdays, weekends, and holidays).
-- market in ('da','rt','dart').
--
-- Forward-dated rule: keep DA rows (priced ahead of delivery), drop
-- RT/DART rows beyond today (haven't realized).
--
-- Sourced from pjm_lmps_hourly (already materialised) so DA/RT source
-- scans happen exactly once per dbt run, not twice.
----------------------------------

{% set onpeak_start = 8 %}
{% set onpeak_end = 23 %}

{% set lookback_filter %}
    {% if is_incremental() %}
    AND date >= (SELECT MAX(date) - INTERVAL '10 days' FROM {{ this }})
    {% endif %}
{% endset %}

WITH HOURLY AS (
    SELECT * FROM {{ ref('pjm_lmps_hourly') }}
    WHERE (market = 'da' OR date < (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE)
    {{ lookback_filter }}
),

FLAT AS (
    SELECT
        date
        ,hub
        ,'flat' AS period
        ,market
        ,AVG(lmp_total)                  AS lmp_total
        ,AVG(lmp_system_energy_price)    AS lmp_system_energy_price
        ,AVG(lmp_congestion_price)       AS lmp_congestion_price
        ,AVG(lmp_marginal_loss_price)    AS lmp_marginal_loss_price
    FROM HOURLY
    GROUP BY date, hub, market
),

ONPEAK AS (
    SELECT
        date
        ,hub
        ,'onpeak' AS period
        ,market
        ,AVG(lmp_total)                  AS lmp_total
        ,AVG(lmp_system_energy_price)    AS lmp_system_energy_price
        ,AVG(lmp_congestion_price)       AS lmp_congestion_price
        ,AVG(lmp_marginal_loss_price)    AS lmp_marginal_loss_price
    FROM HOURLY
    WHERE hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date, hub, market
),

OFFPEAK AS (
    SELECT
        date
        ,hub
        ,'offpeak' AS period
        ,market
        ,AVG(lmp_total)                  AS lmp_total
        ,AVG(lmp_system_energy_price)    AS lmp_system_energy_price
        ,AVG(lmp_congestion_price)       AS lmp_congestion_price
        ,AVG(lmp_marginal_loss_price)    AS lmp_marginal_loss_price
    FROM HOURLY
    WHERE hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date, hub, market
)

SELECT * FROM FLAT
UNION ALL
SELECT * FROM ONPEAK
UNION ALL
SELECT * FROM OFFPEAK
