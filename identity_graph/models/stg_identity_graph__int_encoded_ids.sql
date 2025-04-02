{{ config(materialized="table") }}

{% set start_date = modules.datetime.datetime.utcnow() %}
{% set window = var("window", 720) %}

{%- set max_index = 0 -%}
{% set window = window * -1 %}
{% set start_date = start_date + modules.datetime.timedelta(days=window) %}
{% set start_date = start_date.strftime("%Y-%m-%d") %}

with
    edges_1 as (
        select
            a."timestamp",
            a.node_1,
            b.index as int_1,
            a.type_1,
            a.node_2,
            a.type_2,
            a.edge_source
        from {{ ref("stg_identity_graph__id_edges") }} as a
        join {{ ref("stg_identity_graph__id_dictionary") }} as b on a.node_1 = b.value

        where cast("timestamp" as date) >= '{{ start_date }}'
    ),

    edges_2 as (
        select
            c."timestamp",
            c.node_1,
            c.int_1,
            c.type_1,
            c.node_2,
            d.index as int_2,
            c.type_2,
            c.edge_source
        from edges_1 as c
        join {{ ref("stg_identity_graph__id_dictionary") }} as d on c.node_2 = d.value
    ),

    edges_3 as

    (select "timestamp", int_1, type_1, int_2, type_2, edge_source from edges_2)

select "timestamp", int_1, type_1, int_2, type_2, edge_source
from edges_3 as e
qualify row_number() over (partition by int_1, int_2 order by timestamp asc) = 1
