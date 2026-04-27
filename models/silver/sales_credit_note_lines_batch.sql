{{ config(database='PROD_SILVER' if target.name == 'prod' else 'DEV_SILVER', schema='SILVER_GC_GRUPO', materialized='view', tags=['silver','batch']) }}

{%- set asociados = ['delisano'] -%}

{% for a in asociados %}
    {% if not loop.first %} union all {% endif %}
    select * from {{ ref('sales_credit_note_lines_batch_' ~ a) }}
{% endfor %}
