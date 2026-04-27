{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_DL_CONF_REGISTRO_GRAL') %}
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
        {{ std_cast('"Gen_ Bus_ Posting Group"', 'VARCHAR') }}                 as GEN_BUS_POSTING_GROUP,
        {{ std_cast('"Gen_ Prod_ Posting Group"', 'VARCHAR') }}                as GEN_PROD_POSTING_GROUP,
        {{ std_cast('"Sales Account"', 'VARCHAR') }}                           as SALES_ACCOUNT,
        {{ std_cast('"Sales Line Disc_ Account"', 'VARCHAR') }}                as SALES_LINE_DISC_ACCOUNT,
        {{ std_cast('"Sales Inv_ Disc_ Account"', 'VARCHAR') }}                as SALES_INV_DISC_ACCOUNT,
        {{ std_cast('"Sales Pmt_ Disc_ Debit Acc_"', 'VARCHAR') }}             as SALES_PMT_DISC_DEBIT_ACC,
        {{ std_cast('"Purch_ Account"', 'VARCHAR') }}                          as PURCH_ACCOUNT,
        {{ std_cast('"Purch_ Line Disc_ Account"', 'VARCHAR') }}               as PURCH_LINE_DISC_ACCOUNT,
        {{ std_cast('"Purch_ Inv_ Disc_ Account"', 'VARCHAR') }}               as PURCH_INV_DISC_ACCOUNT,
        {{ std_cast('"Purch_ Pmt_ Disc_ Credit Acc_"', 'VARCHAR') }}           as PURCH_PMT_DISC_CREDIT_ACC,
        {{ std_cast('"COGS Account"', 'VARCHAR') }}                            as COGS_ACCOUNT,
        {{ std_cast('"Inventory Adjmt_ Account"', 'VARCHAR') }}                as INVENTORY_ADJMT_ACCOUNT,
        {{ std_cast('"Job Sales Adjmt_ Account"', 'VARCHAR') }}                as JOB_SALES_ADJMT_ACCOUNT,
        {{ std_cast('"Job Cost Adjmt_ Account"', 'VARCHAR') }}                 as JOB_COST_ADJMT_ACCOUNT,
        {{ std_cast('"Sales Credit Memo Account"', 'VARCHAR') }}               as SALES_CREDIT_MEMO_ACCOUNT,
        {{ std_cast('"Purch_ Credit Memo Account"', 'VARCHAR') }}              as PURCH_CREDIT_MEMO_ACCOUNT,
        {{ std_cast('"Sales Pmt_ Disc_ Credit Acc_"', 'VARCHAR') }}            as SALES_PMT_DISC_CREDIT_ACC,
        {{ std_cast('"Purch_ Pmt_ Disc_ Debit Acc_"', 'VARCHAR') }}            as PURCH_PMT_DISC_DEBIT_ACC,
        {{ std_cast('"Sales Pmt_ Tol_ Debit Acc_"', 'VARCHAR') }}              as SALES_PMT_TOL_DEBIT_ACC,
        {{ std_cast('"Sales Pmt_ Tol_ Credit Acc_"', 'VARCHAR') }}             as SALES_PMT_TOL_CREDIT_ACC,
        {{ std_cast('"Purch_ Pmt_ Tol_ Debit Acc_"', 'VARCHAR') }}             as PURCH_PMT_TOL_DEBIT_ACC,
        {{ std_cast('"Purch_ Pmt_ Tol_ Credit Acc_"', 'VARCHAR') }}            as PURCH_PMT_TOL_CREDIT_ACC,
        {{ std_cast('"Sales Prepayments Account"', 'VARCHAR') }}               as SALES_PREPAYMENTS_ACCOUNT,
        {{ std_cast('"Purch_ Prepayments Account"', 'VARCHAR') }}              as PURCH_PREPAYMENTS_ACCOUNT,
        {{ std_cast('"Description"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"Purch_ FA Disc_ Account"', 'VARCHAR') }}                 as PURCH_FA_DISC_ACCOUNT,
        {{ std_cast('"Invt_ Accrual Acc_ (Interim)"', 'VARCHAR') }}            as INVT_ACCRUAL_ACC_INTERIM,
        {{ std_cast('"COGS Account (Interim)"', 'VARCHAR') }}                  as COGS_ACCOUNT_INTERIM,
        {{ std_cast('"Direct Cost Applied Account"', 'VARCHAR') }}             as DIRECT_COST_APPLIED_ACCOUNT,
        {{ std_cast('"Overhead Applied Account"', 'VARCHAR') }}                as OVERHEAD_APPLIED_ACCOUNT,
        {{ std_cast('"Purchase Variance Account"', 'VARCHAR') }}               as PURCHASE_VARIANCE_ACCOUNT,
        {{ std_cast('"View All Accounts on Lookup"', 'INTEGER') }}             as VIEW_ALL_ACCOUNTS_ON_LOOKUP,
        {{ std_cast('"IDPGND Green Dot Account No_"', 'VARCHAR') }}            as IDPGND_GREEN_DOT_ACCOUNT_NO,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'DLS'                                                                     as tec_des_cod_siglas
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src