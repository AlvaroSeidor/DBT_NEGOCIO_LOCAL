{{ config(
    materialized='incremental',
    database='PROD_SILVER' if target.name == 'prod' else 'DEV_SILVER', schema='SILVER_GC_GRUPO',
    incremental_strategy='append',
    tags=['silver','integ'],
    pre_hook=[
        "{% if is_incremental() %}
        -- (tec_cod_vigencia = 1 --> 0) Marcar como no vigentes registros con match y alguna diferencia. fec_negocio - 1 día (o mismo día si coincide inicio)
        merge into {{ this }} t
        using (
            select
                tec_des_empresa,
                HASH(TIMESTAMP, GEN_BUS_POSTING_GROUP, GEN_PROD_POSTING_GROUP, SALES_ACCOUNT, SALES_LINE_DISC_ACCOUNT, SALES_INV_DISC_ACCOUNT, SALES_PMT_DISC_DEBIT_ACC, PURCH_ACCOUNT, PURCH_LINE_DISC_ACCOUNT, PURCH_INV_DISC_ACCOUNT, PURCH_PMT_DISC_CREDIT_ACC, COGS_ACCOUNT, INVENTORY_ADJMT_ACCOUNT, JOB_SALES_ADJMT_ACCOUNT, JOB_COST_ADJMT_ACCOUNT, SALES_CREDIT_MEMO_ACCOUNT, PURCH_CREDIT_MEMO_ACCOUNT, SALES_PMT_DISC_CREDIT_ACC, PURCH_PMT_DISC_DEBIT_ACC, SALES_PMT_TOL_DEBIT_ACC, SALES_PMT_TOL_CREDIT_ACC, PURCH_PMT_TOL_DEBIT_ACC, PURCH_PMT_TOL_CREDIT_ACC, SALES_PREPAYMENTS_ACCOUNT, PURCH_PREPAYMENTS_ACCOUNT, DESCRIPTION, PURCH_FA_DISC_ACCOUNT, INVT_ACCRUAL_ACC_INTERIM, COGS_ACCOUNT_INTERIM, DIRECT_COST_APPLIED_ACCOUNT, OVERHEAD_APPLIED_ACCOUNT, PURCHASE_VARIANCE_ACCOUNT, VIEW_ALL_ACCOUNTS_ON_LOOKUP, IDPGND_GREEN_DOT_ACCOUNT_NO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('conf_registro_gral_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and 1=1
        when matched and t.tec_hash <> s.tec_hash then
            update set
                tec_cod_vigencia = 0,
                tec_fec_fin = case
                    when s.tec_fec_inicio = t.tec_fec_inicio then s.tec_fec_inicio
                    else dateadd(day, -1, s.tec_fec_inicio)
                end;
        {% endif %}"
    ],
    post_hook=[
        "{% set log_ref = source('support_param', 'INSERT_LOGS') %}{% set asociados = var('empresas', []) %}{% for a in asociados %}
            {% set src_ref = source('bronze_' ~ a, 'V_DL_CONF_REGISTRO_GRAL') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('conf_registro_gral_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('conf_registro_gral_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        gen_bus_posting_group, 
        gen_prod_posting_group, 
        sales_account, 
        sales_line_disc_account, 
        sales_inv_disc_account, 
        sales_pmt_disc_debit_acc, 
        purch_account, 
        purch_line_disc_account, 
        purch_inv_disc_account, 
        purch_pmt_disc_credit_acc, 
        cogs_account, 
        inventory_adjmt_account, 
        job_sales_adjmt_account, 
        job_cost_adjmt_account, 
        sales_credit_memo_account, 
        purch_credit_memo_account, 
        sales_pmt_disc_credit_acc, 
        purch_pmt_disc_debit_acc, 
        sales_pmt_tol_debit_acc, 
        sales_pmt_tol_credit_acc, 
        purch_pmt_tol_debit_acc, 
        purch_pmt_tol_credit_acc, 
        sales_prepayments_account, 
        purch_prepayments_account, 
        description, 
        purch_fa_disc_account, 
        invt_accrual_acc_interim, 
        cogs_account_interim, 
        direct_cost_applied_account, 
        overhead_applied_account, 
        purchase_variance_account, 
        view_all_accounts_on_lookup, 
        idpgnd_green_dot_account_no, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, GEN_BUS_POSTING_GROUP, GEN_PROD_POSTING_GROUP, SALES_ACCOUNT, SALES_LINE_DISC_ACCOUNT, SALES_INV_DISC_ACCOUNT, SALES_PMT_DISC_DEBIT_ACC, PURCH_ACCOUNT, PURCH_LINE_DISC_ACCOUNT, PURCH_INV_DISC_ACCOUNT, PURCH_PMT_DISC_CREDIT_ACC, COGS_ACCOUNT, INVENTORY_ADJMT_ACCOUNT, JOB_SALES_ADJMT_ACCOUNT, JOB_COST_ADJMT_ACCOUNT, SALES_CREDIT_MEMO_ACCOUNT, PURCH_CREDIT_MEMO_ACCOUNT, SALES_PMT_DISC_CREDIT_ACC, PURCH_PMT_DISC_DEBIT_ACC, SALES_PMT_TOL_DEBIT_ACC, SALES_PMT_TOL_CREDIT_ACC, PURCH_PMT_TOL_DEBIT_ACC, PURCH_PMT_TOL_CREDIT_ACC, SALES_PREPAYMENTS_ACCOUNT, PURCH_PREPAYMENTS_ACCOUNT, DESCRIPTION, PURCH_FA_DISC_ACCOUNT, INVT_ACCRUAL_ACC_INTERIM, COGS_ACCOUNT_INTERIM, DIRECT_COST_APPLIED_ACCOUNT, OVERHEAD_APPLIED_ACCOUNT, PURCHASE_VARIANCE_ACCOUNT, VIEW_ALL_ACCOUNTS_ON_LOOKUP, IDPGND_GREEN_DOT_ACCOUNT_NO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('conf_registro_gral_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  1=1
    and t.tec_cod_vigencia    = 1
where 1=1
    or t.tec_hash <> s.tec_hash
{% endif %}
