{% macro update_main_edges_table_groups() %}

    {% set run_timestamp = modules.datetime.datetime.now() %}
    {% set run_timestamp = run_timestamp.strftime("%Y-%m-%d %H:%M:%S") %}

    {% set query %}

update
  {{ this }} e
set
  group_id = r.rn,
  dbt_updated_at = (timestamp '{{ run_timestamp }}')
from (
  select
    distinct min("timestamp") as "timestamp",
    edge_id,
    int_1,
    int_2,
    row_number() over (order by int_1) + (
      case
        when cast(( select MAX(group_id) from {{ this }}) as integer) is null then 0
        else (
      select
        cast(max(group_id) as integer)
      from
        {{ this }})
    end
      ) as rn
  from
    {{ this }}
  group by
    int_1,
    int_2,
    edge_id ) r
where
  r.edge_id = e.edge_id
  and r.int_1 = e.int_1
  and r.int_2 = e.int_2
  and e.group_id is null
  and e.anomaly = 'Normal';

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
            ~ "  Rows updated with groups in "
            ~ this.name
            ~ ": "
            ~ rows_inserted
        )
    }}

{% endmacro %}
