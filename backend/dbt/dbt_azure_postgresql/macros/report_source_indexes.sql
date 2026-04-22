{% macro report_source_indexes(show_only_auto=false, include_index_names=true) %}

    {#
        Reports index coverage for every dbt-declared source table.

        Usage:
            dbt run-operation report_source_indexes

        Optional args:
            dbt run-operation report_source_indexes --args '{"show_only_auto": true}'
            dbt run-operation report_source_indexes --args '{"include_index_names": false}'
    #}

    {% if not execute %}
        {{ return("") }}
    {% endif %}

    {% set processed_relations = [] %}
    {% set ns = namespace(
        scanned_sources=0,
        existing_tables=0,
        missing_tables=0,
        with_indexes=0,
        without_indexes=0,
        with_auto_index=0
    ) %}

    {{ log("=== Source Index Report ===", info=True) }}
    {{ log("show_only_auto: " ~ show_only_auto, info=True) }}
    {{ log("include_index_names: " ~ include_index_names, info=True) }}

    {% for source_node in graph.sources.values() | sort(attribute="unique_id") %}

        {% set schema_name = source_node.schema %}
        {% set table_name = source_node.identifier if source_node.identifier else source_node.name %}
        {% set relation_key = (schema_name ~ "." ~ table_name) | lower %}

        {% if relation_key in processed_relations %}
            {% continue %}
        {% endif %}
        {% do processed_relations.append(relation_key) %}
        {% set ns.scanned_sources = ns.scanned_sources + 1 %}

        {% set relation_kind = _get_relation_kind(schema_name, table_name) %}
        {% if relation_kind not in ["r", "p", "m"] %}
            {% set ns.missing_tables = ns.missing_tables + 1 %}
            {% if not show_only_auto %}
                {{ log("MISSING  | " ~ relation_key, info=True) }}
            {% endif %}
            {% continue %}
        {% endif %}

        {% set ns.existing_tables = ns.existing_tables + 1 %}

        {% set sql %}
            SELECT
                COUNT(*) AS index_count,
                COALESCE(BOOL_OR(indexname LIKE 'idx_auto_%'), FALSE) AS has_auto_index,
                COALESCE(STRING_AGG(indexname, ', ' ORDER BY indexname), '') AS index_names
            FROM pg_indexes
            WHERE schemaname = '{{ schema_name | replace("'", "''") }}'
              AND tablename = '{{ table_name | replace("'", "''") }}';
        {% endset %}

        {% set result = run_query(sql) %}
        {% if result is none or result.rows | length == 0 %}
            {% if not show_only_auto %}
                {{ log("ERROR    | " ~ relation_key ~ " | Unable to read pg_indexes.", info=True) }}
            {% endif %}
            {% continue %}
        {% endif %}

        {% set index_count = result.rows[0]["index_count"] | int %}
        {% set has_auto_index = result.rows[0]["has_auto_index"] %}
        {% set index_names = result.rows[0]["index_names"] %}

        {% if index_count > 0 %}
            {% set ns.with_indexes = ns.with_indexes + 1 %}
        {% else %}
            {% set ns.without_indexes = ns.without_indexes + 1 %}
        {% endif %}

        {% if has_auto_index %}
            {% set ns.with_auto_index = ns.with_auto_index + 1 %}
        {% endif %}

        {% if show_only_auto and not has_auto_index %}
            {% continue %}
        {% endif %}

        {% if index_count == 0 %}
            {{ log("NO_INDEX | " ~ relation_key, info=True) }}
        {% elif has_auto_index %}
            {% if include_index_names %}
                {{ log("AUTO     | " ~ relation_key ~ " | " ~ index_names, info=True) }}
            {% else %}
                {{ log("AUTO     | " ~ relation_key ~ " | count=" ~ index_count, info=True) }}
            {% endif %}
        {% else %}
            {% if include_index_names %}
                {{ log("MANUAL   | " ~ relation_key ~ " | " ~ index_names, info=True) }}
            {% else %}
                {{ log("MANUAL   | " ~ relation_key ~ " | count=" ~ index_count, info=True) }}
            {% endif %}
        {% endif %}

    {% endfor %}

    {{ log("=== Source Index Report Summary ===", info=True) }}
    {{ log("Sources scanned: " ~ ns.scanned_sources, info=True) }}
    {{ log("Existing tables: " ~ ns.existing_tables, info=True) }}
    {{ log("Missing tables: " ~ ns.missing_tables, info=True) }}
    {{ log("With indexes: " ~ ns.with_indexes, info=True) }}
    {{ log("Without indexes: " ~ ns.without_indexes, info=True) }}
    {{ log("With auto index: " ~ ns.with_auto_index, info=True) }}

{% endmacro %}
