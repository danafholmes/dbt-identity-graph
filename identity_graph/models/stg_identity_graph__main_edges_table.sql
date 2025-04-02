{{ config(materialized="table", post_hook="{{ main_edges_table_post_hook() }}") }}

{% set start_date = modules.datetime.datetime.utcnow() %}
{% set window = var("window", 720) %}

{%- set max_index = 0 -%}
{% set window = window * -1 %}
{% set start_date = start_date + modules.datetime.timedelta(days=window) %}
{% set start_date = start_date.strftime("%Y-%m-%d") %}

with
    edge_ids as (
        select
            *,
            md5(concat(int_1, int_2)) as edge_id,
            cast(null as varchar) group_id,
            cast(null as varchar) count_of_edges,
            cast(null as varchar) z_score,
            cast(null as varchar) anomaly,
            cast(null as varchar) dbt_updated_at,
            cast(null as integer) iter
        from {{ ref("stg_identity_graph__int_encoded_ids") }}
        where cast("timestamp" as date) >= '{{ start_date }}'
    )

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
    dbt_updated_at,
    iter
from edge_ids
