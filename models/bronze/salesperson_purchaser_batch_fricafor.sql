{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_SALESPERSON_PURCHASER') %}
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
        {{ std_cast('"COMMISSION_"', 'NUMBER(38,5)') }}                        as COMMISSION,
        {{ std_cast('"PRIVACY_BLOCKED"', 'INTEGER') }}                         as PRIVACY_BLOCKED,
        {{ std_cast('"GLOBAL_DIMENSION_1_CODE"', 'VARCHAR') }}                 as GLOBAL_DIMENSION_1_CODE,
        {{ std_cast('"GLOBAL_DIMENSION_2_CODE"', 'VARCHAR') }}                 as GLOBAL_DIMENSION_2_CODE,
        {{ std_cast('"E_MAIL"', 'VARCHAR') }}                                  as E_MAIL,
        {{ std_cast('"PHONE_NO_"', 'VARCHAR') }}                               as PHONE_NO,
        {{ std_cast('"JOB_TITLE"', 'VARCHAR') }}                               as JOB_TITLE,
        {{ std_cast('"SEARCH_E_MAIL"', 'VARCHAR') }}                           as SEARCH_E_MAIL,
        {{ std_cast('"E_MAIL_2"', 'VARCHAR') }}                                as E_MAIL_2,
        {{ std_cast('"LOCATION_CODE"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"NOMBRE_2"', 'VARCHAR') }}                                as NOMBRE_2,
        {{ std_cast('"ALIAS"', 'VARCHAR') }}                                   as ALIAS,
        {{ std_cast('"DIRECCION"', 'VARCHAR') }}                               as DIRECCION,
        {{ std_cast('"DIRECCION_2"', 'VARCHAR') }}                             as DIRECCION_2,
        {{ std_cast('"CODIGO_POSTAL"', 'VARCHAR') }}                           as CODIGO_POSTAL,
        {{ std_cast('"POBLACION"', 'VARCHAR') }}                               as POBLACION,
        {{ std_cast('"PROVINCIA"', 'VARCHAR') }}                               as PROVINCIA,
        {{ std_cast('"PAIS"', 'VARCHAR') }}                                    as PAIS,
        {{ std_cast('"TELEFONO_2"', 'VARCHAR') }}                              as TELEFONO_2,
        {{ std_cast('"COD_IDIOMA"', 'VARCHAR') }}                              as COD_IDIOMA,
        {{ std_cast('"CIF_NIF"', 'VARCHAR') }}                                 as CIF_NIF,
        {{ std_cast('"COMMISSION_GROUP_CODE"', 'VARCHAR') }}                   as COMMISSION_GROUP_CODE,
        {{ std_cast('"PAY_TO_VENDOR_NO_"', 'VARCHAR') }}                       as PAY_TO_VENDOR_NO,
        {{ std_cast('"COMMISSION_ACCOUNT"', 'VARCHAR') }}                      as COMMISSION_ACCOUNT,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'FRI'                                                                     as tec_des_cod_siglas,
        'FRICAFOR'                                                             as tec_des_empresa,
        tec_id_ingesta,
        tec_ts_ingesta,
        tec_ts_staging,
        tec_ts_integracion_b
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY code
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
