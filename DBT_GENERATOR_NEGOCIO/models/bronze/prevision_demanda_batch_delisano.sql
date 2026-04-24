{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_DL_PREVISION_DEMANDA') %}
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
        {{ std_cast('"Entry No_"', 'INTEGER') }}                               as ENTRY_NO,
        {{ std_cast('"Production Forecast Name"', 'VARCHAR') }}                as PRODUCTION_FORECAST_NAME,
        {{ std_cast('"Item No_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"Forecast Date"', 'TIMESTAMP_NTZ') }}                     as FORECAST_DATE,
        {{ std_cast('"Forecast Quantity"', 'NUMBER(38,5)') }}                  as FORECAST_QUANTITY,
        {{ std_cast('"Unit of Measure Code"', 'VARCHAR') }}                    as UNIT_OF_MEASURE_CODE,
        {{ std_cast('"Qty_ per Unit of Measure"', 'NUMBER(38,5)') }}           as QTY_PER_UNIT_OF_MEASURE,
        {{ std_cast('"Forecast Quantity (Base)"', 'NUMBER(38,5)') }}           as FORECAST_QUANTITY_BASE,
        {{ std_cast('"Location Code"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"Component Forecast"', 'INTEGER') }}                      as COMPONENT_FORECAST,
        {{ std_cast('"Description"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
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