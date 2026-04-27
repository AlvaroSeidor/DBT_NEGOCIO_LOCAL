{{ config(database='PROD_SILVER' if target.name == 'prod' else 'DEV_SILVER', schema='SILVER_GC_GRUPO', materialized='view', tags=['silver','batch']) }}

{%- set asociados = ['fricafor'] -%}

{% for a in asociados %}
    {% if not loop.first %} union all {% endif %}
    select * from {{ ref('lin_orden_prod_transf__batch_' ~ a) }}
{% endfor %}
