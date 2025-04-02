{% macro update_main_edges_table_swap_edges() %}

    {% set run_timestamp = modules.datetime.datetime.now() %}
    {% set run_timestamp = run_timestamp.strftime("%Y-%m-%d %H:%M:%S") %}

    {% set query %}

create temp table swapped_edges as
    select
      distinct "timestamp",
      int_2 as int_1,
      int_1 as int_2,
      edge_id,
      group_id,
      count_of_edges,
      edge_source,
      z_score,
      anomaly,
      iter
    from
      {{ this }} ;

insert into
  {{ this }}

  select
    "timestamp",
    int_1,
    int_2,
    edge_id,
    group_id,
    count_of_edges,
    edge_source,
    z_score,
    anomaly,
    cast('{{ run_timestamp }}' as timestamp) dbt_updated_at,
    iter
  from swapped_edges;

    {% endset %}

    {% do run_query(query) %}

    {% set rows %}

select count(*)
from {{ this }}
where dbt_updated_at = '{{ run_timestamp }}';

    {% endset %}

    {%- set rows_inserted = dbt_utils.get_single_value(rows) -%}

    {{
        print(
            modules.datetime.datetime.utcnow().strftime("%H:%M:%S")
            ~ "  Rows updated with swapped edges in "
            ~ this.name
            ~ ": "
            ~ rows_inserted
        )
    }}

{% endmacro %}
