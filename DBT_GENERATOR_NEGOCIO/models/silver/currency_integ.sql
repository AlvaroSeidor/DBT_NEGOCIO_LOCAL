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
                HASH(TIMESTAMP, CODE, LAST_DATE_MODIFIED, LAST_DATE_ADJUSTED, UNREALIZED_GAINS_ACC, REALIZED_GAINS_ACC, UNREALIZED_LOSSES_ACC, REALIZED_LOSSES_ACC, INVOICE_ROUNDING_PRECISION, INVOICE_ROUNDING_TYPE, AMOUNT_ROUNDING_PRECISION, UNIT_AMOUNT_ROUNDING_PRECISION, DESCRIPTION, AMOUNT_DECIMAL_PLACES, UNIT_AMOUNT_DECIMAL_PLACES, REALIZED_G_L_GAINS_ACCOUNT, REALIZED_G_L_LOSSES_ACCOUNT, APPLN_ROUNDING_PRECISION, EMU_CURRENCY, CURRENCY_FACTOR, RESIDUAL_GAINS_ACCOUNT, RESIDUAL_LOSSES_ACCOUNT, CONV_LCY_RNDG_DEBIT_ACC, CONV_LCY_RNDG_CREDIT_ACC, MAX_VAT_DIFFERENCE_ALLOWED, VAT_ROUNDING_TYPE, PAYMENT_TOLERANCE, MAX_PAYMENT_TOLERANCE_AMOUNT, SYMBOL, BILL_GROUPS_COLLECTION, BILL_GROUPS_DISCOUNT, PAYMENT_ORDERS, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('currency_batch') }}
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
            {% set src_ref = source('bronze_' ~ a, 'FRICAFOR_CURRENCY') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('currency_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('currency_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        code, 
        last_date_modified, 
        last_date_adjusted, 
        unrealized_gains_acc, 
        realized_gains_acc, 
        unrealized_losses_acc, 
        realized_losses_acc, 
        invoice_rounding_precision, 
        invoice_rounding_type, 
        amount_rounding_precision, 
        unit_amount_rounding_precision, 
        description, 
        amount_decimal_places, 
        unit_amount_decimal_places, 
        realized_g_l_gains_account, 
        realized_g_l_losses_account, 
        appln_rounding_precision, 
        emu_currency, 
        currency_factor, 
        residual_gains_account, 
        residual_losses_account, 
        conv_lcy_rndg_debit_acc, 
        conv_lcy_rndg_credit_acc, 
        max_vat_difference_allowed, 
        vat_rounding_type, 
        payment_tolerance, 
        max_payment_tolerance_amount, 
        symbol, 
        bill_groups_collection, 
        bill_groups_discount, 
        payment_orders, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, CODE, LAST_DATE_MODIFIED, LAST_DATE_ADJUSTED, UNREALIZED_GAINS_ACC, REALIZED_GAINS_ACC, UNREALIZED_LOSSES_ACC, REALIZED_LOSSES_ACC, INVOICE_ROUNDING_PRECISION, INVOICE_ROUNDING_TYPE, AMOUNT_ROUNDING_PRECISION, UNIT_AMOUNT_ROUNDING_PRECISION, DESCRIPTION, AMOUNT_DECIMAL_PLACES, UNIT_AMOUNT_DECIMAL_PLACES, REALIZED_G_L_GAINS_ACCOUNT, REALIZED_G_L_LOSSES_ACCOUNT, APPLN_ROUNDING_PRECISION, EMU_CURRENCY, CURRENCY_FACTOR, RESIDUAL_GAINS_ACCOUNT, RESIDUAL_LOSSES_ACCOUNT, CONV_LCY_RNDG_DEBIT_ACC, CONV_LCY_RNDG_CREDIT_ACC, MAX_VAT_DIFFERENCE_ALLOWED, VAT_ROUNDING_TYPE, PAYMENT_TOLERANCE, MAX_PAYMENT_TOLERANCE_AMOUNT, SYMBOL, BILL_GROUPS_COLLECTION, BILL_GROUPS_DISCOUNT, PAYMENT_ORDERS, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('currency_batch') }}
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
