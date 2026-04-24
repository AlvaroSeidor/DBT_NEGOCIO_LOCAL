{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_COOPECARN', materialized='view', tags=['bronze','batch','COOPECARN']) }}

{% set src_ref = source('bronze_coopecarn', 'COOPECARN_SHIPPING_AGENT') %}
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
        {{ std_cast('"NAME"', 'VARCHAR') }}                                    as NAME,
        {{ std_cast('"INTERNET_ADDRESS"', 'VARCHAR') }}                        as INTERNET_ADDRESS,
        {{ std_cast('"ACCOUNT_NO_"', 'VARCHAR') }}                             as ACCOUNT_NO,
        {{ std_cast('"FACTOR_CONVERSION_VOL_PESO"', 'NUMBER(38,5)') }}         as FACTOR_CONVERSION_VOL_PESO,
        {{ std_cast('"PHONE_NO_"', 'VARCHAR') }}                               as PHONE_NO,
        {{ std_cast('"FAX_NO_"', 'VARCHAR') }}                                 as FAX_NO,
        {{ std_cast('"E_MAIL"', 'VARCHAR') }}                                  as E_MAIL,
        {{ std_cast('"VENDOR_NO_"', 'VARCHAR') }}                              as VENDOR_NO,
        {{ std_cast('"LABEL_TYPE"', 'INTEGER') }}                              as LABEL_TYPE,
        {{ std_cast('"CATEGORIA_VEHICULO"', 'INTEGER') }}                      as CATEGORIA_VEHICULO,
        {{ std_cast('"TONELAJE_VEHICULO"', 'NUMBER(38,5)') }}                  as TONELAJE_VEHICULO,
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