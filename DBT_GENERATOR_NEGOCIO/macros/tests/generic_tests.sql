
{% test no_nulls_except(model, exclude=[]) %}
{# Falla si hay al menos un NULL en cualquier columna del modelo, excepto las listadas en exclude. Devuelve column_name y null_count. #}

{% set cols = adapter.get_columns_in_relation(model) %}
{% set exu  = exclude | map('upper') | list %}

{# columnas a testear (sin las excluidas) #}
{% set test_cols = [] %}
{% for c in cols %}
  {% if c.name | upper not in exu %}
    {% do test_cols.append(c.name) %}
  {% endif %}
{% endfor %}

{% if test_cols | length == 0 %}
  -- Nada que testear: pasa
  select 0 where 1=0
{% else %}
  with agg as (
    select object_construct(
      {%- for c in test_cols -%}
        '{{ c }}', sum(case when {{ c }} is null then 1 else 0 end)
        {%- if not loop.last %}, {% endif -%}
      {%- endfor -%}
    ) as null_counts
    from {{ model }}
  ),
  flat as (
    select
      f.key::string   as column_name,
      f.value::number as null_count
    from agg, lateral flatten(input => agg.null_counts) f
  )
  select column_name, null_count
  from flat
  where null_count > 0
{% endif %}
{% endtest %}


----------------------------------------------------------------------------------------------------


{% test single_batch(model, column_name='tec_ts_ingesta') %}
-- Falla si hay >1 batch en la view
select 1
from {{ model }}
having count(distinct {{ column_name }}) > 1
{% endtest %}

----------------------------------------------------------------------------------------------------


{% test no_stale_batch_vs_integ(model, integ_ref, pk_cols, assoc_col='tec_des_empresa', integ_ts_col='tec_ts_ingesta') %}

{# Construimos las condiciones de join por PK: b.col = i.col #}
{% set join_conds = [] %}
{% for col in pk_cols %}
  {% set _ = join_conds.append('b.' ~ col ~ ' = i.' ~ col) %}
{% endfor %}
{% set pk_join_sql = join_conds | join(' AND ') %}

{# SELECT de PKs para el GROUP BY #}
{% set pk_select = pk_cols | join(', ') %}

with integ_max as (
  select {{ pk_select }}, {{ assoc_col }} as {{ assoc_col }}, max({{ integ_ts_col }}) as max_integ_ts
  from {{ ref(integ_ref) }}
  where tec_cod_vigencia > 0
  group by {{ pk_select }}, {{ assoc_col }}
)
select b.*
from {{ model }} b
join integ_max i
  on b.{{ assoc_col }} = i.{{ assoc_col }}
 and {{ pk_join_sql }}
where i.max_integ_ts > b.tec_ts_ingesta
{% endtest %}

----------------------------------------------------------------------------------------------------


{% test scd2_no_overlap(model, pk_cols, start_col='tec_fec_inicio', assoc_col='tec_des_empresa', end_col='tec_fec_fin') %}
{% set pk_select = (pk_cols | map('trim') | list) | join(', ') %}
with ord as (
  select
    {{ pk_select }},
    {{ start_col }} as start_dt,
    {{ end_col }}   as end_dt,
    lead({{ start_col }}) over (partition by {{ pk_select }}, {{ assoc_col }} order by {{ start_col }}) as next_start
  from {{ model }}
),
bad as (
  select 1
  from ord
  where next_start is not null
    and next_start <= end_dt
)
select * from bad
{% endtest %}

----------------------------------------------------------------------------------------------------


{% test scd2_single_current(model, pk_cols, assoc_col='tec_des_empresa', current_col='tec_cod_vigencia') %}
{% set pk_select = (pk_cols | map('trim') | list) | join(', ') %}
with g as (
  select {{ pk_select }}, {{ assoc_col }},
         sum(case when {{ current_col }} > 0 then 1 else 0 end) as curr_cnt
  from {{ model }}
  group by {{ pk_select }}, {{ assoc_col }}
)
select 1 from g where curr_cnt <> 1
{% endtest %}

----------------------------------------------------------------------------------------------------

