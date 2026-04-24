{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_DL_SALES_PRICE') %}
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
        {{ std_cast('"timestamp"', 'INTEGER') }}                               as TIMESTAMP,
        {{ std_cast('"Item No_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"Sales Type"', 'INTEGER') }}                              as SALES_TYPE,
        {{ std_cast('"Sales Code"', 'VARCHAR') }}                              as SALES_CODE,
        {{ std_cast('"Starting Date"', 'TIMESTAMP_NTZ') }}                     as STARTING_DATE,
        {{ std_cast('"Currency Code"', 'VARCHAR') }}                           as CURRENCY_CODE,
        {{ std_cast('"Variant Code"', 'VARCHAR') }}                            as VARIANT_CODE,
        {{ std_cast('"Unit of Measure Code"', 'VARCHAR') }}                    as UNIT_OF_MEASURE_CODE,
        {{ std_cast('"Minimum Quantity"', 'NUMBER(38,5)') }}                   as MINIMUM_QUANTITY,
        {{ std_cast('"Unit Price"', 'NUMBER(38,5)') }}                         as UNIT_PRICE,
        {{ std_cast('"Price Includes VAT"', 'INTEGER') }}                      as PRICE_INCLUDES_VAT,
        {{ std_cast('"Allow Invoice Disc_"', 'INTEGER') }}                     as ALLOW_INVOICE_DISC,
        {{ std_cast('"VAT Bus_ Posting Gr_ (Price)"', 'VARCHAR') }}            as VAT_BUS_POSTING_GR_PRICE,
        {{ std_cast('"Ending Date"', 'TIMESTAMP_NTZ') }}                       as ENDING_DATE,
        {{ std_cast('"Allow Line Disc_"', 'INTEGER') }}                        as ALLOW_LINE_DISC,
        {{ std_cast('"IF_PRS_FINSERCION"', 'TIMESTAMP_NTZ') }}                 as IF_PRS_FINSERCION,
        {{ std_cast('"IF_PRS_FPROD"', 'TIMESTAMP_NTZ') }}                      as IF_PRS_FPROD,
        {{ std_cast('"IF_PRS_ACCION"', 'INTEGER') }}                           as IF_PRS_ACCION,
        {{ std_cast('"IF_PRS_ESTADO"', 'INTEGER') }}                           as IF_PRS_ESTADO,
        {{ std_cast('"IDPVUM Unit Price"', 'NUMBER(38,5)') }}                  as IDPVUM_UNIT_PRICE,
        {{ std_cast('"IDPVUM Price in VUM"', 'INTEGER') }}                     as IDPVUM_PRICE_IN_VUM,
        {{ std_cast('"IDPVUM VUM Per Unit"', 'NUMBER(38,5)') }}                as IDPVUM_VUM_PER_UNIT,
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