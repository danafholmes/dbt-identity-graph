{{ config(materialized="table") }}

{% set start_date = modules.datetime.datetime.utcnow() %}
{% set window = var("window", 720) %}

{%- set max_index = 0 -%}
{% set window = window * -1 %}
{% set start_date = start_date + modules.datetime.timedelta(days=window) %}
{% set start_date = start_date.strftime("%Y-%m-%d") %}

with
    edges as (
        select "timestamp", node_1 as value, type_1 as type
        from {{ ref("stg_identity_graph__id_edges") }}
        where cast("timestamp" as date) >= '{{ start_date }}'
        union all
        select "timestamp", node_2 as value, type_2 as type
        from {{ ref("stg_identity_graph__id_edges") }}
        where cast("timestamp" as date) >= '{{ start_date }}'
    ),

    min_seen as (
        select min("timestamp") as insertion_time, value, type
        from edges
        group by value, type
    )

select
    insertion_time,
    value,
    row_number() over (order by value) + {{ max_index }} as index,
    type
from min_seen
