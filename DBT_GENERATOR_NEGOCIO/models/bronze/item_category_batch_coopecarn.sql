{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_COOPECARN', materialized='view', tags=['bronze','batch','COOPECARN']) }}

{% set src_ref = source('bronze_coopecarn', 'COOPECARN_ITEM_CATEGORY') %}
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
        {{ std_cast('"CODE"', 'VARCHAR') }}                                    as CODE,
        {{ std_cast('"DESCRIPTION"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"DEF_GEN_PROD_POSTING_GROUP"', 'VARCHAR') }}              as DEF_GEN_PROD_POSTING_GROUP,
        {{ std_cast('"DEF_INVENTORY_POSTING_GROUP"', 'VARCHAR') }}             as DEF_INVENTORY_POSTING_GROUP,
        {{ std_cast('"DEF_TAX_GROUP_CODE"', 'VARCHAR') }}                      as DEF_TAX_GROUP_CODE,
        {{ std_cast('"DEF_COSTING_METHOD"', 'INTEGER') }}                      as DEF_COSTING_METHOD,
        {{ std_cast('"DEF_VAT_PROD_POSTING_GROUP"', 'VARCHAR') }}              as DEF_VAT_PROD_POSTING_GROUP,
        {{ std_cast('"ITEM_TRACKING_CODE"', 'VARCHAR') }}                      as ITEM_TRACKING_CODE,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'COO'                                                                     as tec_des_cod_siglas,
        'COOPECARN'                                                            as tec_des_empresa,
        tec_id_ingesta,
        tec_ts_ingesta,
        tec_ts_staging,
        tec_ts_integracion_b
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src