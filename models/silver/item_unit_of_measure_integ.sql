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
                ITEM_NO, CODE, tec_des_empresa,
                HASH(TIMESTAMP, ITEM_NO, CODE, QTY_PER_UNIT_OF_MEASURE, LENGTH, WIDTH, HEIGHT, CUBAGE, WEIGHT, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('item_unit_of_measure_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.ITEM_NO is not distinct from s.ITEM_NO and t.CODE is not distinct from s.CODE and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
        "{%- set log_ref = source('support_param', 'INSERT_LOGS') -%}
{% set src_ref_coopecarn = source('bronze_coopecarn', 'COOPECARN_ITEM_UNIT_OF_MEASURE') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_coopecarn.database }}')
              and upper(l.schema)     = upper('{{ src_ref_coopecarn.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_coopecarn.identifier }}')
              and exists (
                  select 1
                  from {{ ref('item_unit_of_measure_batch_coopecarn') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        item_no, 
        code, 
        qty_per_unit_of_measure, 
        length, 
        width, 
        height, 
        cubage, 
        weight, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, ITEM_NO, CODE, QTY_PER_UNIT_OF_MEASURE, LENGTH, WIDTH, HEIGHT, CUBAGE, WEIGHT, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('item_unit_of_measure_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.ITEM_NO is not distinct from s.ITEM_NO and t.CODE is not distinct from s.CODE and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.ITEM_NO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
