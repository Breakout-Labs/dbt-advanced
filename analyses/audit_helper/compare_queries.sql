{% set old_etl_relation=ref('orders_deprecated') %}

-- this is your newly built dbt model 
{% set dbt_relation=ref('orders') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="order_id"
) }}