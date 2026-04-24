
{% materialization incremental_merge_identity, adapter='snowflake' %}

  {%- set target_relation = this -%}

  {# 0) La tabla debe existir (porque tiene IDENTITY) #}
  {%- set existing_relation = load_cached_relation(target_relation) -%}
  {%- if existing_relation is none -%} {# Exception #}
    {{ exceptions.raise_compiler_error(
      "incremental_merge_identity requiere que la tabla destino exista previamente (para preservar IDENTITY). " ~
      "No existe: " ~ target_relation
    ) }}
  {%- endif -%}

  {# 0. Asignamos campos de configuración del modelo #}
  {%- set unique_key = config.get('unique_key') -%} {# Unique key #}
  {%- if unique_key is none -%} {# Exception #}
    {{ exceptions.raise_compiler_error("Falta config 'unique_key' en incremental_merge_identity.") }}
  {%- endif -%}
  {%- if unique_key is not string -%} {# Normaliza solo si es lista #}
    {%- set unique_key = unique_key | list -%}
  {%- endif -%}
  {%- set merge_exclude = (config.get('merge_exclude_columns', []) | map('upper') | list) -%} {# merge_exclude #}
  {%- set identity_cols = (config.get('identity_columns', []) | map('upper') | list) -%} {# identity_cols #}

  {# 1. Creamos staging como tabla temporal con el SQL del modelo #}
  {%- set tmp_relation = make_temp_relation(target_relation) -%} {# Objeto relation que representa una tabla temporal donde volcamos el SELECT del modelo #}
  {% do run_query(create_table_as(True, tmp_relation, sql)) %} {# create temporary table <tmp_relation> as <sql>; True = temporal#}

  {# 2. Identificamos columnas destino y staging #}
  {%- set dest_cols = adapter.get_columns_in_relation(target_relation) -%}
  {%- set src_cols  = adapter.get_columns_in_relation(tmp_relation) -%}
  {%- set src_names = src_cols | map(attribute='name') | map('upper') | list -%} {# Para poder mapear en step 3 (target ∩ source) cols #}

  {# 3. insert_cols = (target ∩ source) - identity #}
  {%- set insert_cols = [] -%}
  {%- for c in dest_cols -%}
    {%- set cn = c.name | upper -%}
    {%- if cn in identity_cols -%}{% continue %}{%- endif -%}
    {%- if cn in src_names -%}{% do insert_cols.append(c.name) %}{%- endif -%}
  {%- endfor -%}
  {%- if insert_cols | length == 0 -%} {# Exception #}
    {{ exceptions.raise_compiler_error("incremental_merge_identity: no hay columnas para insertar (insert_cols vacío).") }}
  {%- endif -%}

  {# 4. update_cols = (target ∩ source) - identity - merge_exclude #}
  {%- set update_cols = [] -%}
  {%- for c in dest_cols -%}
    {%- set cn = c.name | upper -%}
    {%- if cn in identity_cols -%}{% continue %}{%- endif -%}
    {%- if cn in merge_exclude -%}{% continue %}{%- endif -%}
    {%- if cn in src_names -%}{% do update_cols.append(c.name) %}{%- endif -%}
  {%- endfor -%}

  {# 5. ON clause: unique_key string o lista #}
  {%- if unique_key is string -%}
    {%- set on_clause = "t." ~ unique_key ~ " is not distinct from s." ~ unique_key -%}
  {%- else -%}
    {%- set preds = [] -%}
    {%- for k in unique_key -%}
      {%- do preds.append("t." ~ k ~ " is not distinct from s." ~ k) -%}
    {%- endfor -%}
    {%- set on_clause = preds | join(" and ") -%}
  {%- endif -%}

  {# 6. VALUES: s.col1, s.col2, ... #}
  {%- set insert_values = [] -%}
  {%- for c in insert_cols -%}
    {%- do insert_values.append("s." ~ c) -%}
  {%- endfor -%}

  {# 7. UPDATE SET: t.col = s.col, ... #}
  {%- set update_setters = [] -%}
  {%- for c in update_cols -%}
    {%- do update_setters.append("t." ~ c ~ " = s." ~ c) -%}
  {%- endfor -%}

  {# 8. SQL del MERGE #}
  {% set merge_sql %}
    merge into {{ target_relation }} t
    using {{ tmp_relation }} s
      on {{ on_clause }}
    when matched then update set
      {{ update_setters | join(",\n      ") }}
    when not matched then insert ({{ insert_cols | join(", ") }})
    values ({{ insert_values | join(", ") }})
  {% endset %}

  {# 9. Ejecutar el SQL principal dentro de statement('main') #}
  {% call statement('main') %}
    {{ merge_sql }}
  {% endcall %}

  {# 10. Limpieza staging #}
  {% do run_query("drop table if exists " ~ tmp_relation) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
