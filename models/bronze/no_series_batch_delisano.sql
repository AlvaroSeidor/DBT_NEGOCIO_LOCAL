{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_DL_SERIES') %}
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
        {{ std_cast('"Code"', 'VARCHAR') }}                                    as CODE,
        {{ std_cast('"Description"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"Default Nos_"', 'INTEGER') }}                            as DEFAULT_NOS,
        {{ std_cast('"Manual Nos_"', 'INTEGER') }}                             as MANUAL_NOS,
        {{ std_cast('"Date Order"', 'INTEGER') }}                              as DATE_ORDER,
        {{ std_cast('"ML_BAJAR_SGI"', 'INTEGER') }}                            as ML_BAJAR_SGI,
        {{ std_cast('"ML_BLOQUEA_ALBARANES"', 'INTEGER') }}                    as ML_BLOQUEA_ALBARANES,
        {{ std_cast('"SERIE_ORMA"', 'INTEGER') }}                              as SERIE_ORMA,
        {{ std_cast('"SERIE_SGI"', 'INTEGER') }}                               as SERIE_SGI,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        NULL                                                                   as TIMESTAMP,
        NULL                                                                   as SII_RECTIFICATIVE_INVOICE,
        NULL                                                                   as SII_SPECIAL_TYPE,
        NULL                                                                   as GS1,
        'DLS'                                                                     as tec_des_cod_siglas
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY code
    ORDER BY tec_ts_ingesta DESC, tec_ts_staging DESC
) = 1
