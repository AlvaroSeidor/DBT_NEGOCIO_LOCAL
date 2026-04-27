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
                NO_PARTIDA, NO_LINEA, tec_des_empresa,
                HASH(TIMESTAMP, NO_PARTIDA, NO_LINEA, ORDEN_SACRIFICIO, ORDEN_SACRIFICIO_2, ITEM_NO, RAZA, SEXO, COD_CATEGORIA_PESO, NO_PRODUCTO_RESULTADO, GRAS, CERTIFICADO, PESO_1_2_CAN_1, PESO_1_2_CAN_2, TARA, MERMA, PESO_NETO, PESO_PIEL, NO_CROTALO, NO_CROTALO_GUIA, MACHO, PRECIO, NO_LOTE, PESO_BRUTO, DECOMISO, ASICI_JAMON, ASICI_PALETA, CANAL_DESCLASIFICADA, OREO, PESO_OREO, COD_CONFORMACION, COD_PENALIZACION, ERROR_CLASIFICACION, TEXTO_ERROR_CLASIFICACION, HISTORICO, NO_PESADA, PRECIO_PENALIZACION, UNIDAD_MEDIDA_PENALIZACION, IMPORTE_PENALIZACION, GRADO_CONFORMACION, COD_ENGRASAMIENTO, GRADO_GRASA, CATEGORIA_EDAT, GRADO_EDAT, NO_DIB, COD_PENALIZACION_2, PRECIO_PENALIZACION_2, IMPORTE_PENALIZACION_2, VARIAN_CODE, COD_RAZA, FILTRO_PESO_MINIMO, FILTRO_PESO_MAXIMO, NOMBRE_PROVEEDOR, FECHA_SACRIFICIO, FECHA_NACIMIENTO, NO_CUADRA, FECHA_DESCARGA, HORA_DESCARGA, NO_APLICAR_PENALIZACIONES, NO_OT_MATADERO, LOCATION_CODE, ID_INDIVIDUO, ESTADO_INDIVIDUO, BLOQUEADO, COD_ESPECIE, SSCC_1, SSCC_2, TIPO_PULIDO, TIPO_DECOMISO, NO_GUIA_SANITARIA, NO_GRANJA, NO_SERIE_GUIA, MUERTO_EN_EXPLOTACION, MAGRO, DESCUENTO_ADICIONAL, PESO_DESCUENTO_ADICIONAL, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('lin_partida_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.NO_PARTIDA is not distinct from s.NO_PARTIDA and t.NO_LINEA is not distinct from s.NO_LINEA and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_LIN_PARTIDA') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('lin_partida_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        no_partida, 
        no_linea, 
        orden_sacrificio, 
        orden_sacrificio_2, 
        item_no, 
        raza, 
        sexo, 
        cod_categoria_peso, 
        no_producto_resultado, 
        gras, 
        certificado, 
        peso_1_2_can_1, 
        peso_1_2_can_2, 
        tara, 
        merma, 
        peso_neto, 
        peso_piel, 
        no_crotalo, 
        no_crotalo_guia, 
        macho, 
        precio, 
        no_lote, 
        peso_bruto, 
        decomiso, 
        asici_jamon, 
        asici_paleta, 
        canal_desclasificada, 
        oreo, 
        peso_oreo, 
        cod_conformacion, 
        cod_penalizacion, 
        error_clasificacion, 
        texto_error_clasificacion, 
        historico, 
        no_pesada, 
        precio_penalizacion, 
        unidad_medida_penalizacion, 
        importe_penalizacion, 
        grado_conformacion, 
        cod_engrasamiento, 
        grado_grasa, 
        categoria_edat, 
        grado_edat, 
        no_dib, 
        cod_penalizacion_2, 
        precio_penalizacion_2, 
        importe_penalizacion_2, 
        varian_code, 
        cod_raza, 
        filtro_peso_minimo, 
        filtro_peso_maximo, 
        nombre_proveedor, 
        fecha_sacrificio, 
        fecha_nacimiento, 
        no_cuadra, 
        fecha_descarga, 
        hora_descarga, 
        no_aplicar_penalizaciones, 
        no_ot_matadero, 
        location_code, 
        id_individuo, 
        estado_individuo, 
        bloqueado, 
        cod_especie, 
        sscc_1, 
        sscc_2, 
        tipo_pulido, 
        tipo_decomiso, 
        no_guia_sanitaria, 
        no_granja, 
        no_serie_guia, 
        muerto_en_explotacion, 
        magro, 
        descuento_adicional, 
        peso_descuento_adicional, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, NO_PARTIDA, NO_LINEA, ORDEN_SACRIFICIO, ORDEN_SACRIFICIO_2, ITEM_NO, RAZA, SEXO, COD_CATEGORIA_PESO, NO_PRODUCTO_RESULTADO, GRAS, CERTIFICADO, PESO_1_2_CAN_1, PESO_1_2_CAN_2, TARA, MERMA, PESO_NETO, PESO_PIEL, NO_CROTALO, NO_CROTALO_GUIA, MACHO, PRECIO, NO_LOTE, PESO_BRUTO, DECOMISO, ASICI_JAMON, ASICI_PALETA, CANAL_DESCLASIFICADA, OREO, PESO_OREO, COD_CONFORMACION, COD_PENALIZACION, ERROR_CLASIFICACION, TEXTO_ERROR_CLASIFICACION, HISTORICO, NO_PESADA, PRECIO_PENALIZACION, UNIDAD_MEDIDA_PENALIZACION, IMPORTE_PENALIZACION, GRADO_CONFORMACION, COD_ENGRASAMIENTO, GRADO_GRASA, CATEGORIA_EDAT, GRADO_EDAT, NO_DIB, COD_PENALIZACION_2, PRECIO_PENALIZACION_2, IMPORTE_PENALIZACION_2, VARIAN_CODE, COD_RAZA, FILTRO_PESO_MINIMO, FILTRO_PESO_MAXIMO, NOMBRE_PROVEEDOR, FECHA_SACRIFICIO, FECHA_NACIMIENTO, NO_CUADRA, FECHA_DESCARGA, HORA_DESCARGA, NO_APLICAR_PENALIZACIONES, NO_OT_MATADERO, LOCATION_CODE, ID_INDIVIDUO, ESTADO_INDIVIDUO, BLOQUEADO, COD_ESPECIE, SSCC_1, SSCC_2, TIPO_PULIDO, TIPO_DECOMISO, NO_GUIA_SANITARIA, NO_GRANJA, NO_SERIE_GUIA, MUERTO_EN_EXPLOTACION, MAGRO, DESCUENTO_ADICIONAL, PESO_DESCUENTO_ADICIONAL, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('lin_partida_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.NO_PARTIDA is not distinct from s.NO_PARTIDA and t.NO_LINEA is not distinct from s.NO_LINEA and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.NO_PARTIDA is null
    or t.tec_hash <> s.tec_hash
{% endif %}
