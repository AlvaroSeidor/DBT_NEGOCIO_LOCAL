
{# =========================
   Auditoría de resultados de dbt tests en una única tabla
   ========================= #}
{# Solo se lanza cuando se está ejecutando un test (test / build) #}
{% macro audit__should_log_tests() %}
  {{ return(flags.WHICH in ['test', 'build']) }} {# flags.WHICH para sacar el comando ejecutado (run/test/build/compile/...) #}
{% endmacro %}


{# Crea la tabla en DDBB y schema especificado en las variables en project.yml, si no se define ninguna, usa target.database y target.schema_AUDIT #}
{% macro audit__audit_relation() %}
  {% set audit_db = var('dbt_test_audit_database', target.database) %}
  {% set audit_schema = var('dbt_test_audit_schema', target.schema ~ '_AUDIT') %}
  {% set audit_table = var('dbt_test_audit_table', 'DBT_TEST_RESULTS') %}
  {{ return(api.Relation.create(database=audit_db, schema=audit_schema, identifier=audit_table)) }} {# Referenciamos mediante relation para que dbt gestione quoting correctamente #}
{% endmacro %}


{# Convierte un valor Jinja a literal SQL seguro (escapa comillas) o NULL #}
{% macro audit__sql_literal(val) %}
  {% if val is none %}
    null
  {% else %}
    '{{ (val | string) | replace("'", "''") }}'
  {% endif %}
{% endmacro %}


{% macro audit__ensure_test_audit_objects() %}
  {% if not audit__should_log_tests() %} {# Scape si no estamos en test/build #}
    {{ return('select 1') }}
  {% endif %}
  {% set rel = audit__audit_relation() %} {# Identificamos la tabla destino #}
  {% if execute %} {# execute=True solo cuando dbt está ejecutando (no en parse). run_query requiere conexión #}
    {% set ddl %} {# Creamos tabla de auditoría si no existe #}
      create table if not exists {{ rel }} (
          inserted_at           timestamp_tz default current_timestamp(), {# TS de inserción #}
          run_started_at        timestamp_tz, {# TS de inicio de esa ejecución #}
          target_name           string, {# prod/dev #}
          target_database       string, {# DDBB #}
          target_schema         string, {# Schema #}
          original_file_path    string, {# Path del fichero #}
          invocation_id         string, {# ID de ejecución #}
          node_unique_id        string, {# ID de test #}
          node_name             string, {# Nombre del modelo test (modelo+columna+test) #}
          test_name             string, {# Nombre del test #}
          depends_nodes         variant, {# De que modelos depende #}
          column_name           string, {# De que campos depende #}
          test_kwargs           variant, {# Argumentos del test #}
          status                string, {# pass, fail, warn, error, skipped #}
          failures              number, {# Nº de fallos #}
          failure_details       variant,
          execution_time_s      float, {# Tiempo de ejecución (s) #}
          message               string {# Mensaje del resultado #}
      )
    {% endset %}
    {% do run_query(ddl) %}
  {% endif %}
  {{ return('select 1') }} {# El hook necesita devolver algo #}
{% endmacro %}


{% macro audit__log_test_results(results) %}
  {% if not audit__should_log_tests() %} {# Si no estamos en test/build, no hacemos nada #}
    {{ return('select 1') }}
  {% endif %}
  {% set rel = audit__audit_relation() %} {# Tabla destino #}
  {% if execute %} {# Solamente si ejecutamos #}
    {% set rows = [] %} {# Construimos un INSERT masivo (UNION ALL) con solo los Result cuyo resource_type == 'test' #}
    {% for res in results %}
      {% if res.node.resource_type == 'test' %}
        {% set test_name = (res.node.test_metadata.name if res.node.test_metadata is not none else none) %}
        {% set depends_nodes = (res.node.depends_on.nodes if res.node.depends_on is not none else []) %}
        {% set test_kwargs = (res.node.test_metadata.kwargs if res.node.test_metadata is not none else {}) %}

        {% set failure_details_sql = "null" %}
        {% if test_name == 'no_nulls_except' and res.status in ['fail','warn'] and res.node.compiled_code is not none %}
          {% set compiled = res.node.compiled_code %}
          {% if compiled.endswith(';') %} {# quitar ; final si existe #}
            {% set compiled = compiled[:-1] %}
          {% endif %}
          {% set failure_details_sql %}
            (
              select array_agg(
                       object_construct(
                         'column_name', column_name::string,
                         'null_count', null_count::number
                       )
                     )
              from ( {{ compiled }} ) t
            )
          {% endset %}
        {% endif %}


        {% set row_sql %}
          select
            {{ audit__sql_literal(run_started_at) }}::timestamp_tz                  as run_started_at,
            {{ audit__sql_literal(target.name) }}                                   as target_name,
            {{ audit__sql_literal(target.database) }}                               as target_database,
            {{ audit__sql_literal(target.schema) }}                                 as target_schema,
            {{ audit__sql_literal(res.node.original_file_path) }}                   as original_file_path,
            {{ audit__sql_literal(invocation_id) }}                                 as invocation_id,
            {{ audit__sql_literal(res.node.unique_id) }}                            as node_unique_id,
            {{ audit__sql_literal(res.node.name) }}                                 as node_name,
            {{ audit__sql_literal(test_name) }}                                     as test_name,
            parse_json({{ audit__sql_literal(tojson(depends_nodes)) }})             as depends_nodes,
            {{ audit__sql_literal(res.node.column_name) }}                          as column_name,
            parse_json({{ audit__sql_literal(tojson(test_kwargs)) }})               as test_kwargs,
            {{ audit__sql_literal(res.status) }}                                    as status,
            {{ audit__sql_literal(res.failures) }}                                  as failures,
            {{ failure_details_sql }}                                               as failure_details,
            {{ res.execution_time }}                                                as execution_time_s,
            {{ audit__sql_literal(res.message) }}                                   as message
        {% endset %}
        {% do rows.append(row_sql) %}
      {% endif %}
    {% endfor %}
    {% if rows | length > 0 %} {# Insertamos solo si hay filas (si no, evitamos SQL inválido) #}
      {% set insert_sql %}
        insert into {{ rel }} (
          run_started_at, target_name, target_database, target_schema, original_file_path, invocation_id, node_unique_id, node_name, test_name,
          depends_nodes, column_name, test_kwargs, status, failures, failure_details, execution_time_s, message
        )
        {{ rows | join("\nunion all\n") }}
      {% endset %}
      {% do run_query(insert_sql) %}
    {% endif %}
  {% endif %}
  {{ return('select 1') }}
{% endmacro %}
