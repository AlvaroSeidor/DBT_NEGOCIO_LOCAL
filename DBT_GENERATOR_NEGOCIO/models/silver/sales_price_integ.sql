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
                HASH(TIMESTAMP, ITEM_NO, SALES_TYPE, SALES_CODE, STARTING_DATE, CURRENCY_CODE, VARIANT_CODE, UNIT_OF_MEASURE_CODE, MINIMUM_QUANTITY, UNIT_PRICE, PRICE_INCLUDES_VAT, ALLOW_INVOICE_DISC, VAT_BUS_POSTING_GR_PRICE, ENDING_DATE, ALLOW_LINE_DISC, IF_PRS_FINSERCION, IF_PRS_FPROD, IF_PRS_ACCION, IF_PRS_ESTADO, IDPVUM_UNIT_PRICE, IDPVUM_PRICE_IN_VUM, IDPVUM_VUM_PER_UNIT, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('sales_price_batch') }}
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
            {% set src_ref = source('bronze_' ~ a, 'V_DL_SALES_PRICE') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('sales_price_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_price_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        item_no, 
        sales_type, 
        sales_code, 
        starting_date, 
        currency_code, 
        variant_code, 
        unit_of_measure_code, 
        minimum_quantity, 
        unit_price, 
        price_includes_vat, 
        allow_invoice_disc, 
        vat_bus_posting_gr_price, 
        ending_date, 
        allow_line_disc, 
        if_prs_finsercion, 
        if_prs_fprod, 
        if_prs_accion, 
        if_prs_estado, 
        idpvum_unit_price, 
        idpvum_price_in_vum, 
        idpvum_vum_per_unit, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, ITEM_NO, SALES_TYPE, SALES_CODE, STARTING_DATE, CURRENCY_CODE, VARIANT_CODE, UNIT_OF_MEASURE_CODE, MINIMUM_QUANTITY, UNIT_PRICE, PRICE_INCLUDES_VAT, ALLOW_INVOICE_DISC, VAT_BUS_POSTING_GR_PRICE, ENDING_DATE, ALLOW_LINE_DISC, IF_PRS_FINSERCION, IF_PRS_FPROD, IF_PRS_ACCION, IF_PRS_ESTADO, IDPVUM_UNIT_PRICE, IDPVUM_PRICE_IN_VUM, IDPVUM_VUM_PER_UNIT, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('sales_price_batch') }}
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
