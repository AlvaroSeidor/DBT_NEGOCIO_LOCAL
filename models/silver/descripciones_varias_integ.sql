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
                HASH(TIMESTAMP, IDTABLA, CODIGO, CODIGO_2, CODIGO_3, CODIGO_4, IDIOMA, FECHA, NO_LINEA, DESCRIPCION, NOMBRE, TIPO, QUANTITY, MAX_QUANTITY, UNIT_OF_MEASURE_CODE, CODIGO_5, COLUMNA_1, COLUMNA_2, COLUMNA_3, DECIMAL_1, DECIMAL_2, DECIMAL_3, DECIMAL_4, FECHA_2, PVP_ETIQUETA, RELACION_MEDIDA, BOOLEAN_1, TEXTO_1, TEXTO_2, PRECIO, DESCUENTO, BLOQUEADO, DATEFORMULA_1, FECHA_3, PROCESO, COD_PRODUCTO, BOOLEAN_2, BOOLEAN_3, BOOLEAN_4, BOOLEAN_5, BOOLEAN_6, VARIANT_CODE, FECHA_1, FECHA_4, FECHA_5, FECHA_6, FECHA_7, FECHA_8, FECHA_9, DECIMAL_5, DECIMAL_6, DECIMAL_7, DECIMAL_8, DECIMAL_9, COLUMNA_4, COLUMNA_5, COLUMNA_6, COLUMNA_7, COLUMNA_8, COLUMNA_9, BOOLEAN_7, BOOLEAN_8, BOOLEAN_9, ENTERO_1, ENTERO_2, ENTERO_3, ENTERO_4, ENTERO_5, ENTERO_6, ENTERO_7, ENTERO_8, ENTERO_9, CODIGO_6, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('descripciones_varias_batch') }}
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
        "{%- set log_ref = source('support_param', 'INSERT_LOGS') -%}
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_DESCRIPCIONES_VARIAS') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('descripciones_varias_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        idtabla, 
        codigo, 
        codigo_2, 
        codigo_3, 
        codigo_4, 
        idioma, 
        fecha, 
        no_linea, 
        descripcion, 
        nombre, 
        tipo, 
        quantity, 
        max_quantity, 
        unit_of_measure_code, 
        codigo_5, 
        columna_1, 
        columna_2, 
        columna_3, 
        decimal_1, 
        decimal_2, 
        decimal_3, 
        decimal_4, 
        fecha_2, 
        pvp_etiqueta, 
        relacion_medida, 
        boolean_1, 
        texto_1, 
        texto_2, 
        precio, 
        descuento, 
        bloqueado, 
        dateformula_1, 
        fecha_3, 
        proceso, 
        cod_producto, 
        boolean_2, 
        boolean_3, 
        boolean_4, 
        boolean_5, 
        boolean_6, 
        variant_code, 
        fecha_1, 
        fecha_4, 
        fecha_5, 
        fecha_6, 
        fecha_7, 
        fecha_8, 
        fecha_9, 
        decimal_5, 
        decimal_6, 
        decimal_7, 
        decimal_8, 
        decimal_9, 
        columna_4, 
        columna_5, 
        columna_6, 
        columna_7, 
        columna_8, 
        columna_9, 
        boolean_7, 
        boolean_8, 
        boolean_9, 
        entero_1, 
        entero_2, 
        entero_3, 
        entero_4, 
        entero_5, 
        entero_6, 
        entero_7, 
        entero_8, 
        entero_9, 
        codigo_6, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, IDTABLA, CODIGO, CODIGO_2, CODIGO_3, CODIGO_4, IDIOMA, FECHA, NO_LINEA, DESCRIPCION, NOMBRE, TIPO, QUANTITY, MAX_QUANTITY, UNIT_OF_MEASURE_CODE, CODIGO_5, COLUMNA_1, COLUMNA_2, COLUMNA_3, DECIMAL_1, DECIMAL_2, DECIMAL_3, DECIMAL_4, FECHA_2, PVP_ETIQUETA, RELACION_MEDIDA, BOOLEAN_1, TEXTO_1, TEXTO_2, PRECIO, DESCUENTO, BLOQUEADO, DATEFORMULA_1, FECHA_3, PROCESO, COD_PRODUCTO, BOOLEAN_2, BOOLEAN_3, BOOLEAN_4, BOOLEAN_5, BOOLEAN_6, VARIANT_CODE, FECHA_1, FECHA_4, FECHA_5, FECHA_6, FECHA_7, FECHA_8, FECHA_9, DECIMAL_5, DECIMAL_6, DECIMAL_7, DECIMAL_8, DECIMAL_9, COLUMNA_4, COLUMNA_5, COLUMNA_6, COLUMNA_7, COLUMNA_8, COLUMNA_9, BOOLEAN_7, BOOLEAN_8, BOOLEAN_9, ENTERO_1, ENTERO_2, ENTERO_3, ENTERO_4, ENTERO_5, ENTERO_6, ENTERO_7, ENTERO_8, ENTERO_9, CODIGO_6, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('descripciones_varias_batch') }}
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
