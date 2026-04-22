{% macro create_pjm_indexes() %}

    {#
        Creates indexes on PJM raw source tables.

        Usage:  dbt run-operation create_pjm_indexes
                (or called via: dbt run-operation create_source_indexes)
    #}

    {% set indexes = [
        {
            "table": "pjm.seven_day_load_forecast_v1_2025_08_13",
            "name": "idx_seven_day_load_forecast_date_area",
            "columns": "(evaluated_at_datetime_ept::DATE), forecast_area"
        },
    ] %}

    {% for idx in indexes %}

        {% set sql %}
            CREATE INDEX IF NOT EXISTS {{ idx.name }}
                ON {{ idx.table }} ({{ idx.columns }});
        {% endset %}

        {{ log("  Creating index: " ~ idx.name ~ " on " ~ idx.table, info=True) }}
        {% do run_query(sql) %}
        {{ log("    Done.", info=True) }}

    {% endfor %}

{% endmacro %}
