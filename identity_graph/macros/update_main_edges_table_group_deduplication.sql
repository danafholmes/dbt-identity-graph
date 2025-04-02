{% macro update_main_edges_table_group_deduplication() %}

    {% set run_timestamp = modules.datetime.datetime.now() %}
    {% set run_timestamp = run_timestamp.strftime("%Y-%m-%d %H:%M:%S") %}
    {% set iter = 0 %}

    {% if execute %}
        {% for i in range(1000) %}

            {% set run_timestamp = modules.datetime.datetime.now() %}
            {% set run_timestamp = run_timestamp.strftime("%Y-%m-%d %H:%M:%S") %}
            {% set iter = i %}

            {% set query %}

    update
      {{ this }} e
    set
      group_id = min_group_id,
      dbt_updated_at = (timestamp '{{ run_timestamp }}'),
      iter = '{{ iter }}'

    from (
      select
        int_2,
        anomaly,
        min(group_id) as min_group_id {# 'find the minimum group_id for each int' #}
      from (
        select
          min_group_id as group_id,
          e.int_2,
          e.anomaly
        from
          {{ this }} e
        join (
          select
            int_1,
            anomaly,
            min(group_id) as min_group_id
          from
            {{ this }}
          group by
            int_1,
            anomaly) r
        on
          r.int_1 = e.int_1 ) i
      group by
        int_2,
        anomaly) r
    where
      r.int_2 = e.int_2
      and group_id <> min_group_id {# 'Only update if the current group_id is different from the minimum group_id' #}
      and r.anomaly = 'Normal'
      and e.anomaly = 'Normal';{# 'ignore outliers' #}

            {% endset %}

            {% do run_query(query) %}

            {% set rows %}

    select count(*)
    from {{ this }}
    where iter = '{{ iter }}';

            {% endset %}

            {%- set rows_inserted = dbt_utils.get_single_value(rows) -%}

            {{
                print(
                    modules.datetime.datetime.now().strftime("%H:%M:%S")
                    ~ "  Rows updated with new groups in "
                    ~ this.name
                    ~ " in iteration "
                    ~ i
                    ~ ": "
                    ~ rows_inserted
                )
            }}
            {% if rows_inserted == 0 %}
                {{
                    print(
                        modules.datetime.datetime.utcnow().strftime("%H:%M:%S")
                        ~ "  All groups resolved in "
                        ~ this.name
                        ~ ", exiting loop"
                    )
                }}
                {{ return("success") }}
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}
