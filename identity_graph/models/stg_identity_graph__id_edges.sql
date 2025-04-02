{{ config(materialized="view") }}

select

    "timestamp",
    cast(node_1 as varchar) node_1,
    cast(type_1 as varchar) type_1,
    cast(node_2 as varchar) node_2,
    cast(type_2 as varchar) type_2,
    cast(edge_source as varchar) edge_source

from {{ ref("idr_test") }}
