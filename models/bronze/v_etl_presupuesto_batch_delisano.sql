{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_ETL_PRESUPUESTO_DELISANO') %}
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
        {{ std_cast('"COD_EMPRESA"', 'VARCHAR') }}                             as COD_EMPRESA,
        {{ std_cast('"FILE_ROW_NUMBER"', 'INTEGER') }}                         as FILE_ROW_NUMBER,
        {{ std_cast('"COL_01"', 'VARCHAR') }}                                  as COL_01,
        {{ std_cast('"COL_02"', 'VARCHAR') }}                                  as COL_02,
        {{ std_cast('"COL_03"', 'VARCHAR') }}                                  as COL_03,
        {{ std_cast('"COL_04"', 'VARCHAR') }}                                  as COL_04,
        {{ std_cast('"COL_05"', 'VARCHAR') }}                                  as COL_05,
        {{ std_cast('"COL_06"', 'VARCHAR') }}                                  as COL_06,
        {{ std_cast('"COL_07"', 'VARCHAR') }}                                  as COL_07,
        {{ std_cast('"COL_08"', 'VARCHAR') }}                                  as COL_08,
        {{ std_cast('"COL_09"', 'VARCHAR') }}                                  as COL_09,
        {{ std_cast('"COL_10"', 'VARCHAR') }}                                  as COL_10,
        {{ std_cast('"COL_11"', 'VARCHAR') }}                                  as COL_11,
        {{ std_cast('"COL_12"', 'VARCHAR') }}                                  as COL_12,
        {{ std_cast('"COL_13"', 'VARCHAR') }}                                  as COL_13,
        {{ std_cast('"COL_14"', 'VARCHAR') }}                                  as COL_14,
        {{ std_cast('"COL_15"', 'VARCHAR') }}                                  as COL_15,
        {{ std_cast('"COL_16"', 'VARCHAR') }}                                  as COL_16,
        {{ std_cast('"COL_17"', 'VARCHAR') }}                                  as COL_17,
        {{ std_cast('"COL_18"', 'VARCHAR') }}                                  as COL_18,
        {{ std_cast('"COL_19"', 'VARCHAR') }}                                  as COL_19,
        {{ std_cast('"COL_20"', 'VARCHAR') }}                                  as COL_20,
        {{ std_cast('"COL_21"', 'VARCHAR') }}                                  as COL_21,
        {{ std_cast('"COL_22"', 'VARCHAR') }}                                  as COL_22,
        {{ std_cast('"COL_23"', 'VARCHAR') }}                                  as COL_23,
        {{ std_cast('"COL_24"', 'VARCHAR') }}                                  as COL_24,
        {{ std_cast('"COL_25"', 'VARCHAR') }}                                  as COL_25,
        {{ std_cast('"COL_26"', 'VARCHAR') }}                                  as COL_26,
        {{ std_cast('"COL_27"', 'VARCHAR') }}                                  as COL_27,
        {{ std_cast('"COL_28"', 'VARCHAR') }}                                  as COL_28,
        {{ std_cast('"COL_29"', 'VARCHAR') }}                                  as COL_29,
        {{ std_cast('"COL_30"', 'VARCHAR') }}                                  as COL_30,
        {{ std_cast('"COL_31"', 'VARCHAR') }}                                  as COL_31,
        {{ std_cast('"COL_32"', 'VARCHAR') }}                                  as COL_32,
        {{ std_cast('"COL_33"', 'VARCHAR') }}                                  as COL_33,
        {{ std_cast('"COL_34"', 'VARCHAR') }}                                  as COL_34,
        {{ std_cast('"COL_35"', 'VARCHAR') }}                                  as COL_35,
        {{ std_cast('"COL_36"', 'VARCHAR') }}                                  as COL_36,
        {{ std_cast('"COL_37"', 'VARCHAR') }}                                  as COL_37,
        {{ std_cast('"COL_38"', 'VARCHAR') }}                                  as COL_38,
        {{ std_cast('"COL_39"', 'VARCHAR') }}                                  as COL_39,
        {{ std_cast('"COL_40"', 'VARCHAR') }}                                  as COL_40,
        {{ std_cast('"COL_41"', 'VARCHAR') }}                                  as COL_41,
        {{ std_cast('"COL_42"', 'VARCHAR') }}                                  as COL_42,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'DLS'                                                                     as tec_des_cod_siglas,
        'DELISANO'                                                             as tec_des_empresa
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src