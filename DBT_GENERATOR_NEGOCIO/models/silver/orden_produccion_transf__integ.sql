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
                HASH(TIMESTAMP, NO, NO_SERIES, DESCRIPTION, CREATION_USER_ID, CREATION_DATE, CREATION_TIME, STARTING_DATE, ENDING_DATE, COMPLETION_USER_ID, COMPLETION_DATE, COMPLETION_TIME, BLOCKED, POSTING_DATE, COD_ALMACEN_CONSUMOS, COD_ALMACEN_SALIDAS, NO_LOTE_SALIDAS, CONTROL_LOTE_ESPECIF, NO_PLANTILLA, TIPO_ORDEN_PROD_TRANSF, CANTIDAD, SERVICIO, COD_IDIOMA_ETIQUETAJE, BIN_CODE_CONSUMO, BIN_CODE_SALIDAS, NO_PEDIDO_VENTA, NO_LINEA_PEDIDO, BILL_TO_CUSTOMER_NO, BILL_TO_NAME, ITEM_NO, DUE_DATE, ESTADO_TRABAJO, COD_RESPONSABLE_TRABAJO, YOUR_REFERENCE, ITEM_DESCRIPTION, VARIANT_CODE, STARTING_TIME, ENDING_TIME, WORK_CENTER_NO, TIPO_TRANSFORMACION, FASE_ESTADISTICA, FECHA_LOTE, LINEA_FABRICACION, TURNO_TRABAJO, BLOQUEAR_CALCULO_COSTE, DURACION, HORA_INICIO_OPERARIO, HORA_FIN_OPERARIO, REENVASADOS, MERMA_PLASTICO, EXPIRATION_DATE, TIPO_PLANTILLA, LOTE_VENTA, CALCULO_CONSUMO_AUTOMATICO, COD_UNIDAD_MEDIDA_CONS_AUTOM, LINEA_REQUERIDA, MEZCLA_LOTES, CONSUMO_SIN_GENERAR_MERMA, ACONDICIONAMIENTO_DE_RECHAZOS, MANTENER_NO_PESADA, MANTENER_NO_LOTE, FORZAR_SALIDA_DE_OC, TIPO_MEZCLA_LOTE, ACCION_SALIDA_OC, LIMPIEZA_REQUERIDA, PALETIZACION_REQUERIDA, SALIDA_AUTOMATICA, FILTRO_COD_VARIANTE_ENTRADA, CONTROLAR_LOTE_RETENIDO, AJUSTAR_COSTE, TIPO_PALET, TIPO_CAJAS, CODIGO_EMBALAJE_PALET, CODIGO_EMBALAJE_CAJAS, SEMANA_INICIAL, VENDOR_NO, FECHA_VALORACION_COSTE, ESTADO, AL_REGISTRAR_SALIDAS, SOURCE_NO, SOURCE_TYPE, PVP, SIN_MOV_PRODUCTO, CONTROL_POR, PERMITE_SELECCION_PL_ORIGEN, CIERRE_CAJAS_POR, CALCULO_FECHA_CAD, CONTROL_SALIDAS_PLANTA, MARGEN_SALI_RESPECTO_CONS, CLASIF_COMERCIAL_PERMITIDA, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('orden_produccion_transf__batch') }}
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
            {% set src_ref = source('bronze_' ~ a, 'FRICAFOR_ORDEN_PRODUCCION_TRANSF_') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('orden_produccion_transf__batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('orden_produccion_transf__batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        no, 
        no_series, 
        description, 
        creation_user_id, 
        creation_date, 
        creation_time, 
        starting_date, 
        ending_date, 
        completion_user_id, 
        completion_date, 
        completion_time, 
        blocked, 
        posting_date, 
        cod_almacen_consumos, 
        cod_almacen_salidas, 
        no_lote_salidas, 
        control_lote_especif, 
        no_plantilla, 
        tipo_orden_prod_transf, 
        cantidad, 
        servicio, 
        cod_idioma_etiquetaje, 
        bin_code_consumo, 
        bin_code_salidas, 
        no_pedido_venta, 
        no_linea_pedido, 
        bill_to_customer_no, 
        bill_to_name, 
        item_no, 
        due_date, 
        estado_trabajo, 
        cod_responsable_trabajo, 
        your_reference, 
        item_description, 
        variant_code, 
        starting_time, 
        ending_time, 
        work_center_no, 
        tipo_transformacion, 
        fase_estadistica, 
        fecha_lote, 
        linea_fabricacion, 
        turno_trabajo, 
        bloquear_calculo_coste, 
        duracion, 
        hora_inicio_operario, 
        hora_fin_operario, 
        reenvasados, 
        merma_plastico, 
        expiration_date, 
        tipo_plantilla, 
        lote_venta, 
        calculo_consumo_automatico, 
        cod_unidad_medida_cons_autom, 
        linea_requerida, 
        mezcla_lotes, 
        consumo_sin_generar_merma, 
        acondicionamiento_de_rechazos, 
        mantener_no_pesada, 
        mantener_no_lote, 
        forzar_salida_de_oc, 
        tipo_mezcla_lote, 
        accion_salida_oc, 
        limpieza_requerida, 
        paletizacion_requerida, 
        salida_automatica, 
        filtro_cod_variante_entrada, 
        controlar_lote_retenido, 
        ajustar_coste, 
        tipo_palet, 
        tipo_cajas, 
        codigo_embalaje_palet, 
        codigo_embalaje_cajas, 
        semana_inicial, 
        vendor_no, 
        fecha_valoracion_coste, 
        estado, 
        al_registrar_salidas, 
        source_no, 
        source_type, 
        pvp, 
        sin_mov_producto, 
        control_por, 
        permite_seleccion_pl_origen, 
        cierre_cajas_por, 
        calculo_fecha_cad, 
        control_salidas_planta, 
        margen_sali_respecto_cons, 
        clasif_comercial_permitida, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, NO, NO_SERIES, DESCRIPTION, CREATION_USER_ID, CREATION_DATE, CREATION_TIME, STARTING_DATE, ENDING_DATE, COMPLETION_USER_ID, COMPLETION_DATE, COMPLETION_TIME, BLOCKED, POSTING_DATE, COD_ALMACEN_CONSUMOS, COD_ALMACEN_SALIDAS, NO_LOTE_SALIDAS, CONTROL_LOTE_ESPECIF, NO_PLANTILLA, TIPO_ORDEN_PROD_TRANSF, CANTIDAD, SERVICIO, COD_IDIOMA_ETIQUETAJE, BIN_CODE_CONSUMO, BIN_CODE_SALIDAS, NO_PEDIDO_VENTA, NO_LINEA_PEDIDO, BILL_TO_CUSTOMER_NO, BILL_TO_NAME, ITEM_NO, DUE_DATE, ESTADO_TRABAJO, COD_RESPONSABLE_TRABAJO, YOUR_REFERENCE, ITEM_DESCRIPTION, VARIANT_CODE, STARTING_TIME, ENDING_TIME, WORK_CENTER_NO, TIPO_TRANSFORMACION, FASE_ESTADISTICA, FECHA_LOTE, LINEA_FABRICACION, TURNO_TRABAJO, BLOQUEAR_CALCULO_COSTE, DURACION, HORA_INICIO_OPERARIO, HORA_FIN_OPERARIO, REENVASADOS, MERMA_PLASTICO, EXPIRATION_DATE, TIPO_PLANTILLA, LOTE_VENTA, CALCULO_CONSUMO_AUTOMATICO, COD_UNIDAD_MEDIDA_CONS_AUTOM, LINEA_REQUERIDA, MEZCLA_LOTES, CONSUMO_SIN_GENERAR_MERMA, ACONDICIONAMIENTO_DE_RECHAZOS, MANTENER_NO_PESADA, MANTENER_NO_LOTE, FORZAR_SALIDA_DE_OC, TIPO_MEZCLA_LOTE, ACCION_SALIDA_OC, LIMPIEZA_REQUERIDA, PALETIZACION_REQUERIDA, SALIDA_AUTOMATICA, FILTRO_COD_VARIANTE_ENTRADA, CONTROLAR_LOTE_RETENIDO, AJUSTAR_COSTE, TIPO_PALET, TIPO_CAJAS, CODIGO_EMBALAJE_PALET, CODIGO_EMBALAJE_CAJAS, SEMANA_INICIAL, VENDOR_NO, FECHA_VALORACION_COSTE, ESTADO, AL_REGISTRAR_SALIDAS, SOURCE_NO, SOURCE_TYPE, PVP, SIN_MOV_PRODUCTO, CONTROL_POR, PERMITE_SELECCION_PL_ORIGEN, CIERRE_CAJAS_POR, CALCULO_FECHA_CAD, CONTROL_SALIDAS_PLANTA, MARGEN_SALI_RESPECTO_CONS, CLASIF_COMERCIAL_PERMITIDA, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('orden_produccion_transf__batch') }}
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
