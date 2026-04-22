{% macro create_source_indexes(max_columns=2, dry_run=false) %}

    {#
        Creates indexes on all dbt-declared raw source tables using metadata-driven
        column selection heuristics.

        Usage:  dbt run-operation create_source_indexes

        Optional args:
            dbt run-operation create_source_indexes --args '{"max_columns": 2, "dry_run": false}'

        This does NOT run during `dbt run` - it only runs when you invoke it manually.
        You only need to run this once (indexes persist until dropped).

        Existing domain-specific index macros still exist and can be invoked directly:
            dbt run-operation create_pjm_indexes
            dbt run-operation create_meteologica_pjm_indexes
    #}

    {{ create_source_indexes_auto(max_columns=max_columns, dry_run=dry_run) }}

{% endmacro %}

{% macro create_source_indexes_auto(max_columns=2, dry_run=false) %}

    {% if not execute %}
        {{ return("") }}
    {% endif %}

    {% if max_columns is not number %}
        {% set max_columns = 2 %}
    {% endif %}

    {% if max_columns < 1 %}
        {% set max_columns = 1 %}
    {% elif max_columns > 4 %}
        {% set max_columns = 4 %}
    {% endif %}

    {% set ns = namespace(
        processed_relations=[],
        scanned_sources=0,
        indexed_tables=0,
        skipped_tables=0,
        created_indexes=0
    ) %}

    {{ log("=== Auto source index creation started ===", info=True) }}
    {{ log("Max columns per index: " ~ max_columns, info=True) }}
    {{ log("Dry run: " ~ dry_run, info=True) }}

    {% for source_node in graph.sources.values() | sort(attribute="unique_id") %}

        {% set schema_name = source_node.schema %}
        {% set table_name = source_node.identifier if source_node.identifier else source_node.name %}
        {% set relation_key = (schema_name ~ "." ~ table_name) | lower %}

        {% if relation_key in ns.processed_relations %}
            {{ log("Skipping duplicate source declaration: " ~ relation_key, info=True) }}
        {% else %}
            {% do ns.processed_relations.append(relation_key) %}
            {% set ns.scanned_sources = ns.scanned_sources + 1 %}

            {% set relation_kind = _get_relation_kind(schema_name, table_name) %}
            {% if relation_kind not in ["r", "p", "m"] %}
                {% set ns.skipped_tables = ns.skipped_tables + 1 %}
                {{ log("Skipping " ~ relation_key ~ " (not a table/materialized view or missing).", info=True) }}
            {% else %}
                {% set selected_columns = _pick_auto_index_columns(schema_name, table_name, max_columns) %}

                {% if selected_columns | length == 0 %}
                    {% set ns.skipped_tables = ns.skipped_tables + 1 %}
                    {{ log("Skipping " ~ relation_key ~ " (no suitable columns found).", info=True) }}
                {% else %}
                    {% set has_matching_index = _has_matching_source_index(schema_name, table_name, selected_columns) %}

                    {% if has_matching_index %}
                        {% set ns.skipped_tables = ns.skipped_tables + 1 %}
                        {{ log("Skipping " ~ relation_key ~ " (matching index already exists on " ~ (selected_columns | join(", ")) ~ ").", info=True) }}
                    {% else %}
                        {% set index_name = _build_auto_index_name(schema_name, table_name, selected_columns) %}
                        {% set quoted_columns = [] %}
                        {% for column_name in selected_columns %}
                            {% do quoted_columns.append(adapter.quote(column_name)) %}
                        {% endfor %}

                        {% set sql %}
                            CREATE INDEX IF NOT EXISTS {{ adapter.quote(index_name) }}
                                ON {{ adapter.quote(schema_name) }}.{{ adapter.quote(table_name) }} ({{ quoted_columns | join(", ") }});
                        {% endset %}

                        {% if dry_run %}
                            {{ log("DRY RUN :: " ~ sql | trim, info=True) }}
                        {% else %}
                            {{ log("Creating index " ~ index_name ~ " on " ~ relation_key ~ " using (" ~ (selected_columns | join(", ")) ~ ")", info=True) }}
                            {% do run_query(sql) %}
                            {% set ns.created_indexes = ns.created_indexes + 1 %}
                        {% endif %}

                        {% set ns.indexed_tables = ns.indexed_tables + 1 %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {{ log("=== Auto source index creation completed ===", info=True) }}
    {{ log("Sources scanned: " ~ ns.scanned_sources, info=True) }}
    {{ log("Tables indexed: " ~ ns.indexed_tables, info=True) }}
    {{ log("Indexes created: " ~ ns.created_indexes, info=True) }}
    {{ log("Tables skipped: " ~ ns.skipped_tables, info=True) }}

{% endmacro %}

{% macro _get_relation_kind(schema_name, table_name) %}

    {% if not execute %}
        {{ return(None) }}
    {% endif %}

    {% set sql %}
        SELECT c.relkind
        FROM pg_class AS c
        INNER JOIN pg_namespace AS n
            ON n.oid = c.relnamespace
        WHERE n.nspname = '{{ schema_name | replace("'", "''") }}'
          AND c.relname = '{{ table_name | replace("'", "''") }}'
        LIMIT 1;
    {% endset %}

    {% set result = run_query(sql) %}
    {% if result is none or result.rows | length == 0 %}
        {{ return(None) }}
    {% endif %}

    {{ return(result.rows[0][0]) }}

{% endmacro %}

{% macro _pick_auto_index_columns(schema_name, table_name, max_columns=2) %}

    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {% set sql %}
        WITH COLUMN_CANDIDATES AS (
            SELECT
                c.column_name,
                c.data_type,
                c.ordinal_position,
                CASE
                    WHEN c.column_name ~* '(^|_)(datetime|timestamp|date|time|hour|interval|issue|publish|updated|created)(_|$)'
                      OR c.data_type IN (
                            'date',
                            'time without time zone',
                            'time with time zone',
                            'timestamp without time zone',
                            'timestamp with time zone'
                        )
                    THEN 1 ELSE 0
                END AS is_temporal,
                CASE
                    WHEN c.column_name ~* '(^|_)(id|key|code|pnode|node|zone|region|area|hub|interface|location|market|fuel|asset|portfolio|account|book|exchange|product|symbol|ticker|name|type)(_|$)'
                    THEN 1 ELSE 0
                END AS is_dimensional,
                (
                    CASE
                        WHEN c.column_name ~* '(^|_)(datetime|timestamp)(_|$)' THEN 120
                        WHEN c.column_name ~* '(^|_)(issue_date|publish_time|forecast_date|report_date|trade_date|delivery_date)(_|$)' THEN 115
                        WHEN c.column_name ~* '(^|_)(date)(_|$)' THEN 110
                        WHEN c.column_name ~* '(^|_)(hour|time)(_|$)' THEN 95
                        ELSE 0
                    END
                    + CASE
                        WHEN c.column_name ~* '(^|_)(id|key|code)$' THEN 85
                        WHEN c.column_name ~* '(^|_)(pnode|node|zone|region|area|hub|interface|location|market|fuel|asset|portfolio|account|book|exchange|product|symbol|ticker|name|type)(_|$)' THEN 65
                        ELSE 0
                    END
                    + CASE
                        WHEN c.data_type IN (
                            'date',
                            'time without time zone',
                            'time with time zone',
                            'timestamp without time zone',
                            'timestamp with time zone'
                        ) THEN 35
                        WHEN c.data_type IN ('bigint', 'integer', 'smallint', 'numeric', 'real', 'double precision') THEN 10
                        ELSE 0
                    END
                ) AS score
            FROM information_schema.columns AS c
            WHERE c.table_schema = '{{ schema_name | replace("'", "''") }}'
              AND c.table_name = '{{ table_name | replace("'", "''") }}'
        )
        SELECT
            column_name,
            is_temporal,
            is_dimensional,
            score
        FROM COLUMN_CANDIDATES
        ORDER BY score DESC, ordinal_position;
    {% endset %}

    {% set result = run_query(sql) %}
    {% if result is none or result.rows | length == 0 %}
        {{ return([]) }}
    {% endif %}

    {% set temporal = [] %}
    {% set dimensional = [] %}
    {% set fallback = [] %}

    {% for row in result.rows %}
        {% set column_name = row["column_name"] %}
        {% set is_temporal = row["is_temporal"] | int %}
        {% set is_dimensional = row["is_dimensional"] | int %}

        {% if column_name not in fallback %}
            {% do fallback.append(column_name) %}
        {% endif %}

        {% if is_temporal > 0 and column_name not in temporal %}
            {% do temporal.append(column_name) %}
        {% endif %}

        {% if is_dimensional > 0 and column_name not in dimensional %}
            {% do dimensional.append(column_name) %}
        {% endif %}
    {% endfor %}

    {% set selected = [] %}

    {% if temporal | length > 0 %}
        {% do selected.append(temporal[0]) %}
    {% elif fallback | length > 0 %}
        {% do selected.append(fallback[0]) %}
    {% endif %}

    {% for column_name in dimensional %}
        {% if selected | length < max_columns and column_name not in selected %}
            {% do selected.append(column_name) %}
        {% endif %}
    {% endfor %}

    {% for column_name in temporal %}
        {% if selected | length < max_columns and column_name not in selected %}
            {% do selected.append(column_name) %}
        {% endif %}
    {% endfor %}

    {% for column_name in fallback %}
        {% if selected | length < max_columns and column_name not in selected %}
            {% do selected.append(column_name) %}
        {% endif %}
    {% endfor %}

    {{ return(selected[:max_columns]) }}

{% endmacro %}

{% macro _has_matching_source_index(schema_name, table_name, columns) %}

    {% if not execute %}
        {{ return(False) }}
    {% endif %}

    {% if columns | length == 0 %}
        {{ return(False) }}
    {% endif %}

    {% set literal_columns = [] %}
    {% for column_name in columns %}
        {% do literal_columns.append("'" ~ (column_name | replace("'", "''")) ~ "'") %}
    {% endfor %}

    {% set sql %}
        SELECT EXISTS (
            WITH index_columns AS (
                SELECT
                    i.indexrelid,
                    array_agg(a.attname::text ORDER BY keys.ordinality) AS key_columns
                FROM pg_class AS t
                INNER JOIN pg_namespace AS n
                    ON n.oid = t.relnamespace
                INNER JOIN pg_index AS i
                    ON i.indrelid = t.oid
                INNER JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
                    ON keys.attnum > 0
                INNER JOIN pg_attribute AS a
                    ON a.attrelid = t.oid
                    AND a.attnum = keys.attnum
                WHERE n.nspname = '{{ schema_name | replace("'", "''") }}'
                  AND t.relname = '{{ table_name | replace("'", "''") }}'
                GROUP BY i.indexrelid
            )
            SELECT 1
            FROM index_columns
            WHERE key_columns[1:{{ columns | length }}] = ARRAY[{{ literal_columns | join(", ") }}]::text[]
        ) AS has_index;
    {% endset %}

    {% set result = run_query(sql) %}
    {% if result is none or result.rows | length == 0 %}
        {{ return(False) }}
    {% endif %}

    {{ return(result.rows[0][0]) }}

{% endmacro %}

{% macro _build_auto_index_name(schema_name, table_name, columns) %}

    {% set safe_table_name = table_name
        | lower
        | replace(".", "_")
        | replace("-", "_")
        | replace(" ", "_")
    %}
    {% set prefix = "idx_auto_" ~ safe_table_name[:42] %}
    {% set hash_input = (schema_name ~ "." ~ table_name ~ "|" ~ (columns | join(","))) | lower %}
    {% set hash_suffix = local_md5(hash_input)[:10] %}
    {% set index_name = (prefix ~ "_" ~ hash_suffix)[:63] %}

    {{ return(index_name) }}

{% endmacro %}
