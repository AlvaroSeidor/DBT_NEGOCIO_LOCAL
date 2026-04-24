{{ config(database='PROD_SILVER' if target.name == 'prod' else 'DEV_SILVER', schema='SILVER_GC_GRUPO', materialized='view', tags=['silver','batch']) }}

{%- set asociados = ['coopecarn', 'fricafor'] -%}

{% for a in asociados %}
    {% if not loop.first %} union all {% endif %}
    select * from {{ ref('salesperson_purchaser_batch_' ~ a) }}
{% endfor %}
