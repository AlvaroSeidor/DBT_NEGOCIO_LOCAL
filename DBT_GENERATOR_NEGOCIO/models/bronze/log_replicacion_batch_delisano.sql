{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'LOG_REPLICACION') %}
{% set log_ref = source('support_param', 'INSERT_LOGS') %}

with next_log as (
    select tec_id_ingesta
    from {{ log_ref }}
    where upper(database)   = upper('{{ src_ref.database }}')
      and upper(schema)     = upper('{{ src_ref.schema }}')
      and upper(table_name) = upper('{{ src_ref.identifier }}')
      and start_watermark <> end_watermark
      and tec_ts_integracion_b is null
    qualify row_number() over(order by tec_ts_ingesta) = 1
),
src as (
    select
        {{ std_cast('"APP_NAME"', 'VARCHAR') }}                                as APP_NAME,
        {{ std_cast('"CREATE_DATE"', 'TIMESTAMP_NTZ') }}                       as CREATE_DATE,
        {{ std_cast('"SCHEMA_NAME"', 'VARCHAR') }}                             as SCHEMA_NAME,
        {{ std_cast('"OBJECT_NAME"', 'VARCHAR') }}                             as OBJECT_NAME,
        {{ std_cast('"TYPE_DESC"', 'VARCHAR') }}                               as TYPE_DESC,
        {{ std_cast('"PARENT_NAME"', 'VARCHAR') }}                             as PARENT_NAME,
        'DLS'                                                                     as tec_des_cod_siglas,
        'DELISANO'                                                             as tec_des_empresa,
        tec_id_ingesta,
        tec_ts_ingesta,
        tec_ts_staging,
        tec_ts_integracion_b
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src