{% macro update_main_edges_table_stats() %}

    {% set run_timestamp = modules.datetime.datetime.now() %}
    {% set run_timestamp = run_timestamp.strftime("%Y-%m-%d %H:%M:%S") %}

    {% set query %}

update
  {{ this }} a
set
  group_id = case when b.anomaly like 'Outlier' then null else a.group_id end, {# Reset the group_id to remove outliers from matching #}
  z_score = b.z_score,
  anomaly = b.anomaly,
  dbt_updated_at = (timestamp '{{ run_timestamp }}')
from (
  with
    stats as (
    select
      avg(cast(count_of_edges as integer)) as mean,
      stddev(cast(count_of_edges as integer)) as stddev
    from
      {{ this }} ),
    z_scores as (
    select
      edge_id,
      int_1,
      int_2,
      case
        when stats.stddev = 0 then 0
        else (cast(count_of_edges as integer) - stats.mean) / stats.stddev {# z_score #}
    end
      as z_score
    from
      {{ this }},
      stats )
  select
    edge_id,
    int_1,
    int_2,
    z_score,
    case
      when ABS(z_score) > 3 then 'Outlier' -- Mark as 'Outlier' if z_score is greater than 3
      else 'Normal' -- Otherwise, mark as 'Normal'
  end
    as anomaly
  from
    z_scores ) b
where
  a.int_1 = b.int_1
  and a.int_2 = b.int_2; -- Match records based on int_1 and int_2

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
            ~ "  Rows updated with stats in "
            ~ this.name
            ~ ": "
            ~ rows_inserted
        )
    }}

{% endmacro %}
