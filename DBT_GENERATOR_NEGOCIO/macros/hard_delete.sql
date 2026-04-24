
{% macro hard_delete_against_src(this_model, src_model, keys) %}
delete from {{ this_model }} t
where not exists (
  select 1
  from {{ src_model }} s
  where 
    {% for k in keys %}
    s.{{ k }} is not distinct from t.{{ k }}{% if not loop.last %} and {% endif %}
    {% endfor %}
);
{% endmacro %}
