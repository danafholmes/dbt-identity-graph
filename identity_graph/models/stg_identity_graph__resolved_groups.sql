{{ config(materialized="table") }}

select distinct edges.group_id, dict.value, dict.type

from {{ ref("stg_identity_graph__main_edges_table") }} edges
left join
    {{ ref("stg_identity_graph__id_dictionary") }} dict on edges.int_1 = dict.index

order by edges.group_id asc, dict.type asc, dict.value asc
