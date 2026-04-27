{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_COOPECARN', materialized='view', tags=['bronze','batch','COOPECARN']) }}

{% set src_ref = source('bronze_coopecarn', 'COOPECARN_SALESPERSON_PURCHASER') %}
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
        {{ std_cast('"GLOBAL_DIMENSION_1_CODE"', 'VARCHAR') }}                 as GLOBAL_DIMENSION_1_CODE,
        {{ std_cast('"GLOBAL_DIMENSION_2_CODE"', 'VARCHAR') }}                 as GLOBAL_DIMENSION_2_CODE,
        {{ std_cast('"E_MAIL"', 'VARCHAR') }}                                  as E_MAIL,
        {{ std_cast('"PHONE_NO_"', 'VARCHAR') }}                               as PHONE_NO,
        {{ std_cast('"JOB_TITLE"', 'VARCHAR') }}                               as JOB_TITLE,
        {{ std_cast('"SEARCH_E_MAIL"', 'VARCHAR') }}                           as SEARCH_E_MAIL,
        {{ std_cast('"E_MAIL_2"', 'VARCHAR') }}                                as E_MAIL_2,
        {{ std_cast('"COMPRADOR"', 'INTEGER') }}                               as COMPRADOR,
        {{ std_cast('"VENDEDOR"', 'INTEGER') }}                                as VENDEDOR,
        {{ std_cast('"JEFE_DE_VENTAS"', 'INTEGER') }}                          as JEFE_DE_VENTAS,
        {{ std_cast('"JEFE_DE_ZONA"', 'INTEGER') }}                            as JEFE_DE_ZONA,
        {{ std_cast('"_IVA"', 'NUMBER(38,5)') }}                               as IVA,
        {{ std_cast('"_IRPF"', 'NUMBER(38,5)') }}                              as IRPF,
        {{ std_cast('"NO_PROVEEDOR"', 'VARCHAR') }}                            as NO_PROVEEDOR,
        {{ std_cast('"FECHA_ULT_VOLCADO_TARIFA"', 'TIMESTAMP_NTZ') }}          as FECHA_ULT_VOLCADO_TARIFA,
        {{ std_cast('"HORA_ULT_VOLCADO_TARIFA"', 'TIMESTAMP_NTZ') }}           as HORA_ULT_VOLCADO_TARIFA,
        {{ std_cast('"FECHA_TARIFA_VOLCADA"', 'TIMESTAMP_NTZ') }}              as FECHA_TARIFA_VOLCADA,
        {{ std_cast('"BALANCE_COMISSION_ACCOUNT"', 'VARCHAR') }}               as BALANCE_COMISSION_ACCOUNT,
        {{ std_cast('"EXPENSES_COMISSION_ACCOUNT"', 'VARCHAR') }}              as EXPENSES_COMISSION_ACCOUNT,
        {{ std_cast('"CALC_COMISION_X_FECHA_COBRO"', 'INTEGER') }}             as CALC_COMISION_X_FECHA_COBRO,
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
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY code
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
