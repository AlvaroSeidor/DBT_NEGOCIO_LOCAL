{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_CURRENCY') %}
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
        {{ std_cast('"LAST_DATE_MODIFIED"', 'TIMESTAMP_NTZ') }}                as LAST_DATE_MODIFIED,
        {{ std_cast('"LAST_DATE_ADJUSTED"', 'TIMESTAMP_NTZ') }}                as LAST_DATE_ADJUSTED,
        {{ std_cast('"UNREALIZED_GAINS_ACC_"', 'VARCHAR') }}                   as UNREALIZED_GAINS_ACC,
        {{ std_cast('"REALIZED_GAINS_ACC_"', 'VARCHAR') }}                     as REALIZED_GAINS_ACC,
        {{ std_cast('"UNREALIZED_LOSSES_ACC_"', 'VARCHAR') }}                  as UNREALIZED_LOSSES_ACC,
        {{ std_cast('"REALIZED_LOSSES_ACC_"', 'VARCHAR') }}                    as REALIZED_LOSSES_ACC,
        {{ std_cast('"INVOICE_ROUNDING_PRECISION"', 'NUMBER(38,5)') }}         as INVOICE_ROUNDING_PRECISION,
        {{ std_cast('"INVOICE_ROUNDING_TYPE"', 'INTEGER') }}                   as INVOICE_ROUNDING_TYPE,
        {{ std_cast('"AMOUNT_ROUNDING_PRECISION"', 'NUMBER(38,5)') }}          as AMOUNT_ROUNDING_PRECISION,
        {{ std_cast('"UNIT_AMOUNT_ROUNDING_PRECISION"', 'NUMBER(38,5)') }}     as UNIT_AMOUNT_ROUNDING_PRECISION,
        {{ std_cast('"DESCRIPTION"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"AMOUNT_DECIMAL_PLACES"', 'VARCHAR') }}                   as AMOUNT_DECIMAL_PLACES,
        {{ std_cast('"UNIT_AMOUNT_DECIMAL_PLACES"', 'VARCHAR') }}              as UNIT_AMOUNT_DECIMAL_PLACES,
        {{ std_cast('"REALIZED_G_L_GAINS_ACCOUNT"', 'VARCHAR') }}              as REALIZED_G_L_GAINS_ACCOUNT,
        {{ std_cast('"REALIZED_G_L_LOSSES_ACCOUNT"', 'VARCHAR') }}             as REALIZED_G_L_LOSSES_ACCOUNT,
        {{ std_cast('"APPLN_ROUNDING_PRECISION"', 'NUMBER(38,5)') }}           as APPLN_ROUNDING_PRECISION,
        {{ std_cast('"EMU_CURRENCY"', 'INTEGER') }}                            as EMU_CURRENCY,
        {{ std_cast('"CURRENCY_FACTOR"', 'NUMBER(38,5)') }}                    as CURRENCY_FACTOR,
        {{ std_cast('"RESIDUAL_GAINS_ACCOUNT"', 'VARCHAR') }}                  as RESIDUAL_GAINS_ACCOUNT,
        {{ std_cast('"RESIDUAL_LOSSES_ACCOUNT"', 'VARCHAR') }}                 as RESIDUAL_LOSSES_ACCOUNT,
        {{ std_cast('"CONV_LCY_RNDG_DEBIT_ACC_"', 'VARCHAR') }}                as CONV_LCY_RNDG_DEBIT_ACC,
        {{ std_cast('"CONV_LCY_RNDG_CREDIT_ACC_"', 'VARCHAR') }}               as CONV_LCY_RNDG_CREDIT_ACC,
        {{ std_cast('"MAX_VAT_DIFFERENCE_ALLOWED"', 'NUMBER(38,5)') }}         as MAX_VAT_DIFFERENCE_ALLOWED,
        {{ std_cast('"VAT_ROUNDING_TYPE"', 'INTEGER') }}                       as VAT_ROUNDING_TYPE,
        {{ std_cast('"PAYMENT_TOLERANCE_"', 'NUMBER(38,5)') }}                 as PAYMENT_TOLERANCE,
        {{ std_cast('"MAX_PAYMENT_TOLERANCE_AMOUNT"', 'NUMBER(38,5)') }}       as MAX_PAYMENT_TOLERANCE_AMOUNT,
        {{ std_cast('"SYMBOL"', 'VARCHAR') }}                                  as SYMBOL,
        {{ std_cast('"BILL_GROUPS_COLLECTION"', 'INTEGER') }}                  as BILL_GROUPS_COLLECTION,
        {{ std_cast('"BILL_GROUPS_DISCOUNT"', 'INTEGER') }}                    as BILL_GROUPS_DISCOUNT,
        {{ std_cast('"PAYMENT_ORDERS"', 'INTEGER') }}                          as PAYMENT_ORDERS,
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
