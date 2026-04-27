
{% macro join_cfg_equivalencias(tabla, variable, emp_col, codigo_col) %}
left join (
    select * from {{ source('bronze_comun','CFG_EQUIVALENCIAS') }}
    where tabla    = '{{ tabla }}'
      and variable = '{{ variable }}'
      and coalesce(f_inicio, date('1900-01-01')) <= current_date()
      and coalesce(f_final,  date('2999-12-31')) >= current_date()
      and emp in ('{{ emp_col }}', 'ALL')
    qualify row_number() over (
        partition by tabla, variable, codigo
        order by
            case when emp = '{{ emp_col }}' then 0 else 1 end,
            coalesce(f_inicio, date('1900-01-01')) desc
    ) = 1
) eq
  on eq.codigo = {{ codigo_col }}
{% endmacro %}
