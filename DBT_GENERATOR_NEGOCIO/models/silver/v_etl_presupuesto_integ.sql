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
                HASH(COD_EMPRESA, FILE_ROW_NUMBER, FILE_NAME, COL_01, COL_02, TEC_TS_INGESTA, COL_03, TEC_TS_INTEGRACION_B, COL_04, COL_05, COL_06, COL_07, COL_08, COL_09, COL_10, COL_11, COL_12, COL_13, COL_14, COL_15, COL_16, COL_17, COL_18, COL_19, COL_20, COL_21, COL_22, COL_23, COL_24, COL_25, COL_26, COL_27, COL_28, COL_29, COL_30, COL_31, COL_32, COL_33, COL_34, COL_35, COL_36, COL_37, COL_38, COL_39, COL_40, COL_41, COL_42, TEC_ID_INGESTA, TEC_TS_STAGING) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('v_etl_presupuesto_batch') }}
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
            {% set src_ref = source('bronze_' ~ a, 'V_ETL_PRESUPUESTO_COOPECARN') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('v_etl_presupuesto_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('v_etl_presupuesto_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        cod_empresa, 
        file_row_number, 
        file_name, 
        col_01, 
        col_02, 
        tec_ts_ingesta, 
        col_03, 
        tec_ts_integracion_b, 
        col_04, 
        col_05, 
        col_06, 
        col_07, 
        col_08, 
        col_09, 
        col_10, 
        col_11, 
        col_12, 
        col_13, 
        col_14, 
        col_15, 
        col_16, 
        col_17, 
        col_18, 
        col_19, 
        col_20, 
        col_21, 
        col_22, 
        col_23, 
        col_24, 
        col_25, 
        col_26, 
        col_27, 
        col_28, 
        col_29, 
        col_30, 
        col_31, 
        col_32, 
        col_33, 
        col_34, 
        col_35, 
        col_36, 
        col_37, 
        col_38, 
        col_39, 
        col_40, 
        col_41, 
        col_42, 
        tec_id_ingesta, 
        tec_ts_staging, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(COD_EMPRESA, FILE_ROW_NUMBER, FILE_NAME, COL_01, COL_02, TEC_TS_INGESTA, COL_03, TEC_TS_INTEGRACION_B, COL_04, COL_05, COL_06, COL_07, COL_08, COL_09, COL_10, COL_11, COL_12, COL_13, COL_14, COL_15, COL_16, COL_17, COL_18, COL_19, COL_20, COL_21, COL_22, COL_23, COL_24, COL_25, COL_26, COL_27, COL_28, COL_29, COL_30, COL_31, COL_32, COL_33, COL_34, COL_35, COL_36, COL_37, COL_38, COL_39, COL_40, COL_41, COL_42, TEC_ID_INGESTA, TEC_TS_STAGING) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('v_etl_presupuesto_batch') }}
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
