{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_COOPECARN', materialized='view', tags=['bronze','batch','COOPECARN']) }}

{% set src_ref = source('bronze_coopecarn', 'COOPECARN_ITEM_UNIT_OF_MEASURE') %}
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
        {{ std_cast('"TIMESTAMP"', 'INTEGER') }}                               as TIMESTAMP,
        {{ std_cast('"ITEM_NO_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"CODE"', 'VARCHAR') }}                                    as CODE,
        {{ std_cast('"QTY_PER_UNIT_OF_MEASURE"', 'NUMBER(38,5)') }}            as QTY_PER_UNIT_OF_MEASURE,
        {{ std_cast('"LENGTH"', 'NUMBER(38,5)') }}                             as LENGTH,
        {{ std_cast('"WIDTH"', 'NUMBER(38,5)') }}                              as WIDTH,
        {{ std_cast('"HEIGHT"', 'NUMBER(38,5)') }}                             as HEIGHT,
        {{ std_cast('"CUBAGE"', 'NUMBER(38,5)') }}                             as CUBAGE,
        {{ std_cast('"WEIGHT"', 'NUMBER(38,5)') }}                             as WEIGHT,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'COO'                                                                     as tec_des_cod_siglas
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY item_no, code
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
