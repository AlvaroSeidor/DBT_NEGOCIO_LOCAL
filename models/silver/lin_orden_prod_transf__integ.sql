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
                PROD_ORDER_NO, LINE_NO, tec_des_empresa,
                HASH(TIMESTAMP, PROD_ORDER_NO, TIPO_MOVIMIENTO, LINE_NO, TYPE, NO, VARIANT_CODE, DESCRIPTION, DESCRIPTION_2, UNIT_COST, UNIT_OF_MEASURE_CODE, LOCATION_CODE, LOT_NO, CANTIDAD_UNIDADES, CANTIDAD_KILOS, TIPO_PALET, TARA_PALETS, CANTIDAD_PALETS, TIPO_CAJAS, TARA_CAJAS, CANTIDAD_CAJAS, TARA_EXTRA, BLOQUEAR_AJUSTE_AUT_COSTE, EXPIRATION_DATE, SALES_ORDER_NO, SALES_ORDER_LINE_NO, ESTADO_ASIGNACION, TIPO_PLANTILLA, AUTOCONSUMO, COD_TRABAJO, POSTING_DATE, ORDER_ASSIGN_ENTRY_NO, MERMA, COD_OPERARIO, TIPO_MOV_RELACIONADO, LINE_NO_RELATION, OP_CURAC_ENTRY_NO, PARAMETRO_1, PARAMETRO_2, PARAMETRO_3, BIN_CODE, POSTING_TIME, RECHAZO, PRODUCTO_FABRICACION, COD_MOTIVO_RECHAZO, SUMA_A_TARA, KG_HORA, ITEM_NO_PRODUCED, COD_EMBALAJE_PRODUCIDO, CDAD_EMBALAJE_PRODUCIDO, CDAD_UNIDADES_PRODUCIDAS, CDAD_KILOS_PRODUCIDOS, TOTAL_NETO, IDENTIFICADOR_TARA, CANTIDAD_KILOS_INFORMATIVO, PESADA_FINALIZADA, TIPO_COSTE, FASE_ESTADISTICA, COSTE_TOTAL, WORK_CENTER_NO, NO_PESADA, NO_PLANTILLA, NO_LINEA_PLANTILLA, CODIGO_EMBALAJE, CANTIDAD_EMBALAJE, ALTERNATIVE_ITEM_TO_LINE_NO, INDENTATION, ALTERNATIVE_ITEM_TO_ITEM_NO, ID_INDIVIDUO, TIPO_MOV_RELAC_PLANTILLA, TIPO_PLANT_RELAC_PLANTILLA, NO_LINEA_RELAC_PLANTILLA, CONTROL_POR, NO_PESADA_CONSUMO, ITEM_NO_CONSUMPTION, VARIANT_CODE_CONSUMPTION, LOT_NO_CONSUMPTION, DESCRIPCION_ABREVIADA, NUMERO_DE_ETIQUETAS, GENERAR_ORDEN_RUTA, NO_ORDEN_RUTA, TIPO_MOV_ORDEN_RUTA, NO_LINEA_ORDEN_RUTA, LOTE_MAGRO_FINALIZADO, DESPIECE, TARA_UNIDAD, SIN_MOV_PRODUCTO, NO_PLANTILLA_RENDIMIENTO, NO_OT_RENDIMIENTO, RENDIM_NO_OT_RELAC, RENDIM_TIPO_MOV_RELAC, RENDIM_NO_LINEA_RELAC, OBSERVACIONES_PALET, NO_ORDEN_RUTA_AUX, PRODUCT_DESTINED_TO_ITEM_NO, PRODUCT_DESTINED_TO_LINE_NO, OBTENCION_PESO_PROD_DESTINO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('lin_orden_prod_transf__batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.PROD_ORDER_NO is not distinct from s.PROD_ORDER_NO and t.LINE_NO is not distinct from s.LINE_NO and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_LIN_ORDEN_PROD_TRANSF_') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('lin_orden_prod_transf__batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        prod_order_no, 
        tipo_movimiento, 
        line_no, 
        type, 
        no, 
        variant_code, 
        description, 
        description_2, 
        unit_cost, 
        unit_of_measure_code, 
        location_code, 
        lot_no, 
        cantidad_unidades, 
        cantidad_kilos, 
        tipo_palet, 
        tara_palets, 
        cantidad_palets, 
        tipo_cajas, 
        tara_cajas, 
        cantidad_cajas, 
        tara_extra, 
        bloquear_ajuste_aut_coste, 
        expiration_date, 
        sales_order_no, 
        sales_order_line_no, 
        estado_asignacion, 
        tipo_plantilla, 
        autoconsumo, 
        cod_trabajo, 
        posting_date, 
        order_assign_entry_no, 
        merma, 
        cod_operario, 
        tipo_mov_relacionado, 
        line_no_relation, 
        op_curac_entry_no, 
        parametro_1, 
        parametro_2, 
        parametro_3, 
        bin_code, 
        posting_time, 
        rechazo, 
        producto_fabricacion, 
        cod_motivo_rechazo, 
        suma_a_tara, 
        kg_hora, 
        item_no_produced, 
        cod_embalaje_producido, 
        cdad_embalaje_producido, 
        cdad_unidades_producidas, 
        cdad_kilos_producidos, 
        total_neto, 
        identificador_tara, 
        cantidad_kilos_informativo, 
        pesada_finalizada, 
        tipo_coste, 
        fase_estadistica, 
        coste_total, 
        work_center_no, 
        no_pesada, 
        no_plantilla, 
        no_linea_plantilla, 
        codigo_embalaje, 
        cantidad_embalaje, 
        alternative_item_to_line_no, 
        indentation, 
        alternative_item_to_item_no, 
        id_individuo, 
        tipo_mov_relac_plantilla, 
        tipo_plant_relac_plantilla, 
        no_linea_relac_plantilla, 
        control_por, 
        no_pesada_consumo, 
        item_no_consumption, 
        variant_code_consumption, 
        lot_no_consumption, 
        descripcion_abreviada, 
        numero_de_etiquetas, 
        generar_orden_ruta, 
        no_orden_ruta, 
        tipo_mov_orden_ruta, 
        no_linea_orden_ruta, 
        lote_magro_finalizado, 
        despiece, 
        tara_unidad, 
        sin_mov_producto, 
        no_plantilla_rendimiento, 
        no_ot_rendimiento, 
        rendim_no_ot_relac, 
        rendim_tipo_mov_relac, 
        rendim_no_linea_relac, 
        observaciones_palet, 
        no_orden_ruta_aux, 
        product_destined_to_item_no, 
        product_destined_to_line_no, 
        obtencion_peso_prod_destino, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, PROD_ORDER_NO, TIPO_MOVIMIENTO, LINE_NO, TYPE, NO, VARIANT_CODE, DESCRIPTION, DESCRIPTION_2, UNIT_COST, UNIT_OF_MEASURE_CODE, LOCATION_CODE, LOT_NO, CANTIDAD_UNIDADES, CANTIDAD_KILOS, TIPO_PALET, TARA_PALETS, CANTIDAD_PALETS, TIPO_CAJAS, TARA_CAJAS, CANTIDAD_CAJAS, TARA_EXTRA, BLOQUEAR_AJUSTE_AUT_COSTE, EXPIRATION_DATE, SALES_ORDER_NO, SALES_ORDER_LINE_NO, ESTADO_ASIGNACION, TIPO_PLANTILLA, AUTOCONSUMO, COD_TRABAJO, POSTING_DATE, ORDER_ASSIGN_ENTRY_NO, MERMA, COD_OPERARIO, TIPO_MOV_RELACIONADO, LINE_NO_RELATION, OP_CURAC_ENTRY_NO, PARAMETRO_1, PARAMETRO_2, PARAMETRO_3, BIN_CODE, POSTING_TIME, RECHAZO, PRODUCTO_FABRICACION, COD_MOTIVO_RECHAZO, SUMA_A_TARA, KG_HORA, ITEM_NO_PRODUCED, COD_EMBALAJE_PRODUCIDO, CDAD_EMBALAJE_PRODUCIDO, CDAD_UNIDADES_PRODUCIDAS, CDAD_KILOS_PRODUCIDOS, TOTAL_NETO, IDENTIFICADOR_TARA, CANTIDAD_KILOS_INFORMATIVO, PESADA_FINALIZADA, TIPO_COSTE, FASE_ESTADISTICA, COSTE_TOTAL, WORK_CENTER_NO, NO_PESADA, NO_PLANTILLA, NO_LINEA_PLANTILLA, CODIGO_EMBALAJE, CANTIDAD_EMBALAJE, ALTERNATIVE_ITEM_TO_LINE_NO, INDENTATION, ALTERNATIVE_ITEM_TO_ITEM_NO, ID_INDIVIDUO, TIPO_MOV_RELAC_PLANTILLA, TIPO_PLANT_RELAC_PLANTILLA, NO_LINEA_RELAC_PLANTILLA, CONTROL_POR, NO_PESADA_CONSUMO, ITEM_NO_CONSUMPTION, VARIANT_CODE_CONSUMPTION, LOT_NO_CONSUMPTION, DESCRIPCION_ABREVIADA, NUMERO_DE_ETIQUETAS, GENERAR_ORDEN_RUTA, NO_ORDEN_RUTA, TIPO_MOV_ORDEN_RUTA, NO_LINEA_ORDEN_RUTA, LOTE_MAGRO_FINALIZADO, DESPIECE, TARA_UNIDAD, SIN_MOV_PRODUCTO, NO_PLANTILLA_RENDIMIENTO, NO_OT_RENDIMIENTO, RENDIM_NO_OT_RELAC, RENDIM_TIPO_MOV_RELAC, RENDIM_NO_LINEA_RELAC, OBSERVACIONES_PALET, NO_ORDEN_RUTA_AUX, PRODUCT_DESTINED_TO_ITEM_NO, PRODUCT_DESTINED_TO_LINE_NO, OBTENCION_PESO_PROD_DESTINO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('lin_orden_prod_transf__batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.PROD_ORDER_NO is not distinct from s.PROD_ORDER_NO and t.LINE_NO is not distinct from s.LINE_NO and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.PROD_ORDER_NO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
