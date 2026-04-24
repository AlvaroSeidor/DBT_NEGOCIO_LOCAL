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
                HASH(TIMESTAMP, NO_PESADA, TABLE_ID, DOCUMENT_TYPE, DOCUMENT_NO, LINE_NO, DESTINATION_TABLE_ID, DESTINATION_DOCUMENT_TYPE, DESTINATION_DOCUMENT_NO, DESTINATION_LINE_NO, REF_EXTERNA, HISTORICO, BATCH_NAME, DESTINATION_BATCH_NAME, SOURCE_TABLE_ID, SOURCE_DOCUMENT_NO, SOURCE_DOCUMENT_TYPE, SOURCE_BATCH_NAME, SOURCE_DOCUMENT_LINE_NO, ASSIGN_ENTRY_NO_RELATION, BLOQUEO_FISICO, ITEM_NO, VARIANT_CODE, CODIGO_EMBALAJE, NO_PRESENTACION, LOT_NO, CANTIDAD_EMBALAJE, CANTIDAD_UNIDADES, CANTIDAD_KILOS, CODIGO_EMBALAJE_DO, PARAMETRO_1, PARAMETRO_2, PARAMETRO_3, LOCATION_CODE, TIPO_PALET, TARA_PALETS, CANTIDAD_PALETS, TIPO_CAJAS, TARA_CAJAS, CANTIDAD_CAJAS, TARA_EXTRA, EXPIRATION_DATE, VENDOR_EXPIRATION_DATE, VENDOR_LOT_NO, PARAMETRO_FECHA_1, PARAMETRO_FECHA_2, PARAMETRO_CODIGO_1, PARAMETRO_DATEFORMULA_1, PARAMETRO_BOOLEAN_1, BAR_CODE, REGISTRO_SANITARIO, PARAMETRO_CODIGO_2, CUSTOMER_NO, VENDOR_NO, ESTADO_VALORES, TIPO_EV, NO_EV, COD_VARIANTE_EV, DESCRIPCION_EV, DESCRIPCION_2_EV, COD_ALMACEN_EV, CANTIDAD_UNIDADES_EV, CANTIDAD_KILOS_EV, NO_LOTE_EV, FECHA_CADUCIDAD_EV, TIPO_PALET_EV, TARA_PALETS_EV, CANTIDAD_PALETS_EV, TIPO_CAJAS_EV, TARA_CAJAS_EV, CANTIDAD_CAJAS_EV, TARA_EXTRA_EV, CODIGO_EMBALAJE_EV, CANTIDAD_EMBALAJE_EV, NO_OP_FABR, NO_OP_CURAC, COD_FASE_OC, COD_ALMACEN_OC, PARAMETRO_1_EV, PARAMETRO_2_EV, PARAMETRO_3_EV, TIPO_ASIGNACION, TIPO_AS, NO_AS, COD_VARIANTE_AS, DESCRIPCION_AS, DESCRIPCION_2_AS, COD_ALMACEN_AS, CANTIDAD_UNIDADES_AS, CANTIDAD_KILOS_AS, NO_LOTE_AS, FECHA_CADUCIDAD_AS, TIPO_PALET_AS, TARA_PALETS_AS, CANTIDAD_PALETS_AS, TIPO_CAJAS_AS, TARA_CAJAS_AS, CANTIDAD_CAJAS_AS, TARA_EXTRA_AS, CODIGO_EMBALAJE_AS, CANTIDAD_EMBALAJE_AS, TOTAL_NETO_AS, NO_PESADA_EMBALAJE, NO_PESADA_PALET, TIPO_PESADA, INBOUND_ITEM_ENTRY_NO, OUTBOUND_ITEM_ENTRY_NO, FECHA_CREACION, SSCC_NO, NO_BULTO_ETIQUETA, PARAMETRO_1_AS, PARAMETRO_2_AS, PARAMETRO_3_AS, HORA_CREACION, NO_PESADA_PALET_ORIGEN, APPL_FROM_ITEM_ENTRY, ESTADO, ESTADO_PALETIZADO, REGISTRADO, NO_DOCUMENTO_TRANSF, NO_LINEA_TRANSF, NO_MOV_CURACION, BLOQUEAR_TRANSFORMACION, NO_DOCUMENTO_REG_ENTRADA, NO_LIN_DOC_REG_ENTRADA, NO_DOCUMENTO_REG_SALIDA, NO_LIN_DOC_REG_SALIDA, NO_DOCUMENTO_DEVOLUCION, NO_LIN_DOCUMENTO_DEVOLUCION, NUMCARRO, BIN_CODE, CONGELADO, PARAMETRO_4, NUMALBPROV, UNIDAD_MEDIDA_BASE, NUMNOTA, NUMGABIA, OBSQUALITAT, FECHA_SACRIFICIO, FECHA_CONGELADO, FECHA_2, ID_TAG, HALAL, ID_INDIVIDUO, PARAMETRO_5, FECHA_DESPIECE, TARA_UNIDAD, PARAMETRO_6, TIENE_CERT, OBSERVACIONES_PALET, PRECIO_VALORACION, FECHA_VALORACION, VALORADO, SEXO, FECHA_NACIMIENTO, OBSERVACIONES, COD_CLAS_COMERCIAL, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('no_pesada_batch') }}
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
            {% set src_ref = source('bronze_' ~ a, 'FRICAFOR_NO_PESADA') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('no_pesada_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('no_pesada_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        no_pesada, 
        table_id, 
        document_type, 
        document_no, 
        line_no, 
        destination_table_id, 
        destination_document_type, 
        destination_document_no, 
        destination_line_no, 
        ref_externa, 
        historico, 
        batch_name, 
        destination_batch_name, 
        source_table_id, 
        source_document_no, 
        source_document_type, 
        source_batch_name, 
        source_document_line_no, 
        assign_entry_no_relation, 
        bloqueo_fisico, 
        item_no, 
        variant_code, 
        codigo_embalaje, 
        no_presentacion, 
        lot_no, 
        cantidad_embalaje, 
        cantidad_unidades, 
        cantidad_kilos, 
        codigo_embalaje_do, 
        parametro_1, 
        parametro_2, 
        parametro_3, 
        location_code, 
        tipo_palet, 
        tara_palets, 
        cantidad_palets, 
        tipo_cajas, 
        tara_cajas, 
        cantidad_cajas, 
        tara_extra, 
        expiration_date, 
        vendor_expiration_date, 
        vendor_lot_no, 
        parametro_fecha_1, 
        parametro_fecha_2, 
        parametro_codigo_1, 
        parametro_dateformula_1, 
        parametro_boolean_1, 
        bar_code, 
        registro_sanitario, 
        parametro_codigo_2, 
        customer_no, 
        vendor_no, 
        estado_valores, 
        tipo_ev, 
        no_ev, 
        cod_variante_ev, 
        descripcion_ev, 
        descripcion_2_ev, 
        cod_almacen_ev, 
        cantidad_unidades_ev, 
        cantidad_kilos_ev, 
        no_lote_ev, 
        fecha_caducidad_ev, 
        tipo_palet_ev, 
        tara_palets_ev, 
        cantidad_palets_ev, 
        tipo_cajas_ev, 
        tara_cajas_ev, 
        cantidad_cajas_ev, 
        tara_extra_ev, 
        codigo_embalaje_ev, 
        cantidad_embalaje_ev, 
        no_op_fabr, 
        no_op_curac, 
        cod_fase_oc, 
        cod_almacen_oc, 
        parametro_1_ev, 
        parametro_2_ev, 
        parametro_3_ev, 
        tipo_asignacion, 
        tipo_as, 
        no_as, 
        cod_variante_as, 
        descripcion_as, 
        descripcion_2_as, 
        cod_almacen_as, 
        cantidad_unidades_as, 
        cantidad_kilos_as, 
        no_lote_as, 
        fecha_caducidad_as, 
        tipo_palet_as, 
        tara_palets_as, 
        cantidad_palets_as, 
        tipo_cajas_as, 
        tara_cajas_as, 
        cantidad_cajas_as, 
        tara_extra_as, 
        codigo_embalaje_as, 
        cantidad_embalaje_as, 
        total_neto_as, 
        no_pesada_embalaje, 
        no_pesada_palet, 
        tipo_pesada, 
        inbound_item_entry_no, 
        outbound_item_entry_no, 
        fecha_creacion, 
        sscc_no, 
        no_bulto_etiqueta, 
        parametro_1_as, 
        parametro_2_as, 
        parametro_3_as, 
        hora_creacion, 
        no_pesada_palet_origen, 
        appl_from_item_entry, 
        estado, 
        estado_paletizado, 
        registrado, 
        no_documento_transf, 
        no_linea_transf, 
        no_mov_curacion, 
        bloquear_transformacion, 
        no_documento_reg_entrada, 
        no_lin_doc_reg_entrada, 
        no_documento_reg_salida, 
        no_lin_doc_reg_salida, 
        no_documento_devolucion, 
        no_lin_documento_devolucion, 
        numcarro, 
        bin_code, 
        congelado, 
        parametro_4, 
        numalbprov, 
        unidad_medida_base, 
        numnota, 
        numgabia, 
        obsqualitat, 
        fecha_sacrificio, 
        fecha_congelado, 
        fecha_2, 
        id_tag, 
        halal, 
        id_individuo, 
        parametro_5, 
        fecha_despiece, 
        tara_unidad, 
        parametro_6, 
        tiene_cert, 
        observaciones_palet, 
        precio_valoracion, 
        fecha_valoracion, 
        valorado, 
        sexo, 
        fecha_nacimiento, 
        observaciones, 
        cod_clas_comercial, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, NO_PESADA, TABLE_ID, DOCUMENT_TYPE, DOCUMENT_NO, LINE_NO, DESTINATION_TABLE_ID, DESTINATION_DOCUMENT_TYPE, DESTINATION_DOCUMENT_NO, DESTINATION_LINE_NO, REF_EXTERNA, HISTORICO, BATCH_NAME, DESTINATION_BATCH_NAME, SOURCE_TABLE_ID, SOURCE_DOCUMENT_NO, SOURCE_DOCUMENT_TYPE, SOURCE_BATCH_NAME, SOURCE_DOCUMENT_LINE_NO, ASSIGN_ENTRY_NO_RELATION, BLOQUEO_FISICO, ITEM_NO, VARIANT_CODE, CODIGO_EMBALAJE, NO_PRESENTACION, LOT_NO, CANTIDAD_EMBALAJE, CANTIDAD_UNIDADES, CANTIDAD_KILOS, CODIGO_EMBALAJE_DO, PARAMETRO_1, PARAMETRO_2, PARAMETRO_3, LOCATION_CODE, TIPO_PALET, TARA_PALETS, CANTIDAD_PALETS, TIPO_CAJAS, TARA_CAJAS, CANTIDAD_CAJAS, TARA_EXTRA, EXPIRATION_DATE, VENDOR_EXPIRATION_DATE, VENDOR_LOT_NO, PARAMETRO_FECHA_1, PARAMETRO_FECHA_2, PARAMETRO_CODIGO_1, PARAMETRO_DATEFORMULA_1, PARAMETRO_BOOLEAN_1, BAR_CODE, REGISTRO_SANITARIO, PARAMETRO_CODIGO_2, CUSTOMER_NO, VENDOR_NO, ESTADO_VALORES, TIPO_EV, NO_EV, COD_VARIANTE_EV, DESCRIPCION_EV, DESCRIPCION_2_EV, COD_ALMACEN_EV, CANTIDAD_UNIDADES_EV, CANTIDAD_KILOS_EV, NO_LOTE_EV, FECHA_CADUCIDAD_EV, TIPO_PALET_EV, TARA_PALETS_EV, CANTIDAD_PALETS_EV, TIPO_CAJAS_EV, TARA_CAJAS_EV, CANTIDAD_CAJAS_EV, TARA_EXTRA_EV, CODIGO_EMBALAJE_EV, CANTIDAD_EMBALAJE_EV, NO_OP_FABR, NO_OP_CURAC, COD_FASE_OC, COD_ALMACEN_OC, PARAMETRO_1_EV, PARAMETRO_2_EV, PARAMETRO_3_EV, TIPO_ASIGNACION, TIPO_AS, NO_AS, COD_VARIANTE_AS, DESCRIPCION_AS, DESCRIPCION_2_AS, COD_ALMACEN_AS, CANTIDAD_UNIDADES_AS, CANTIDAD_KILOS_AS, NO_LOTE_AS, FECHA_CADUCIDAD_AS, TIPO_PALET_AS, TARA_PALETS_AS, CANTIDAD_PALETS_AS, TIPO_CAJAS_AS, TARA_CAJAS_AS, CANTIDAD_CAJAS_AS, TARA_EXTRA_AS, CODIGO_EMBALAJE_AS, CANTIDAD_EMBALAJE_AS, TOTAL_NETO_AS, NO_PESADA_EMBALAJE, NO_PESADA_PALET, TIPO_PESADA, INBOUND_ITEM_ENTRY_NO, OUTBOUND_ITEM_ENTRY_NO, FECHA_CREACION, SSCC_NO, NO_BULTO_ETIQUETA, PARAMETRO_1_AS, PARAMETRO_2_AS, PARAMETRO_3_AS, HORA_CREACION, NO_PESADA_PALET_ORIGEN, APPL_FROM_ITEM_ENTRY, ESTADO, ESTADO_PALETIZADO, REGISTRADO, NO_DOCUMENTO_TRANSF, NO_LINEA_TRANSF, NO_MOV_CURACION, BLOQUEAR_TRANSFORMACION, NO_DOCUMENTO_REG_ENTRADA, NO_LIN_DOC_REG_ENTRADA, NO_DOCUMENTO_REG_SALIDA, NO_LIN_DOC_REG_SALIDA, NO_DOCUMENTO_DEVOLUCION, NO_LIN_DOCUMENTO_DEVOLUCION, NUMCARRO, BIN_CODE, CONGELADO, PARAMETRO_4, NUMALBPROV, UNIDAD_MEDIDA_BASE, NUMNOTA, NUMGABIA, OBSQUALITAT, FECHA_SACRIFICIO, FECHA_CONGELADO, FECHA_2, ID_TAG, HALAL, ID_INDIVIDUO, PARAMETRO_5, FECHA_DESPIECE, TARA_UNIDAD, PARAMETRO_6, TIENE_CERT, OBSERVACIONES_PALET, PRECIO_VALORACION, FECHA_VALORACION, VALORADO, SEXO, FECHA_NACIMIENTO, OBSERVACIONES, COD_CLAS_COMERCIAL, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('no_pesada_batch') }}
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
