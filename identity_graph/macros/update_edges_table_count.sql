{% macro update_edges_table_count() %}

    {% set run_timestamp = modules.datetime.datetime.now() %}
    {% set run_timestamp = run_timestamp.strftime("%Y-%m-%d %H:%M:%S") %}

    {% set query %}

update
  {{ this }}
set
  count_of_edges = b.count_of_edges,
  dbt_updated_at = (timestamp '{{ run_timestamp }}')
from (
  select
    d.edge_id,
    d.int_1 as int_1_a,
    d.int_2 as int_2_a,
    case
      when (c.count_of_edges_int_1 < e.count_of_edges_int_2) then e.count_of_edges_int_2
      else c.count_of_edges_int_1
  end
    as count_of_edges
  from
      {{ this }} d
  left join (
    select
      distinct int_1,
      count(*) as count_of_edges_int_1
    from
        {{ this }}
    group by
      int_1 ) as c
  on
    d.int_1 = c.int_1
  left join (
    select
      distinct int_2,
      count(*) as count_of_edges_int_2
    from
        {{ this }}
    group by
      int_2 ) as e
  on
    d.int_2 = e.int_2 ) b
where
  int_1 = b.int_1_a
  and int_2 = b.int_2_a;{% endset %}

    {% do run_query(query) %}

    {% set rows %}

select count(*)
from {{ this }}
where dbt_updated_at = '{{ run_timestamp }}' ;

    {% endset %}

    {%- set rows_inserted = dbt_utils.get_single_value(rows) -%}

    {{
        print(
            modules.datetime.datetime.utcnow().strftime("%H:%M:%S")
            ~ "  Rows updated with edge count in "
            ~ this.name
            ~ ": "
            ~ rows_inserted
        )
    }}

{% endmacro %}
