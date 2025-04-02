{% macro main_edges_table_post_hook() %}

    {% do update_edges_table_count() %}
    {% do update_main_edges_table_stats() %}
    {% do update_main_edges_table_groups() %}
    {% do update_main_edges_table_swap_edges() %}
    {% do update_main_edges_table_group_deduplication() %}

{% endmacro %}
