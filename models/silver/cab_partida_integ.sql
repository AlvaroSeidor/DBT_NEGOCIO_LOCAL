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
                NO_PARTIDA, tec_des_empresa,
                HASH(TIMESTAMP, NO_PARTIDA, FECHA_SACRIFICIO, NO_GUIA_SANITARIA, NO_PROVEEDOR, OBSERVACIONES, APTO_EXPORTACION, EXPLOTACION, NO_CUADRA, NO_LOTE, PARTIDA_ACABADA, TRASPASADO_A_GESTION, LIQUIDACION_CERRADA, NO_PEDIDO, NO_LINEA_PEDIDO, COD_PENALIZACION, PESO_NETO_CAMION, PESO_BRUTO_ANIMALES, HORAS_AYUNO, NO_SERIES, PESO_BRUTO_CAMION, FECHA_DESCARGA, HORA_DESCARGA, DESINFECCION, TIPO_PRODUCTO, HISTORICO, OREO, PESO_OREO, TOTAL_KILOS_PROVEEDOR, NO_PARTIDA_PROVEEDOR, ESTADO_LIQUIDACION, NO_DOCUMENTO_EXTERNO, NO_APLICAR_PENALIZACIONES, ALBARAN_GENERADO, ESTADO, BLOQUEADA, FECHA_PARTIDA, FECHA_HORA_CREACION, NO_CABEZAS_RECIBIDAS, ANO, SEMANA, FECHA_RECEPCION_ESPERADA, HORA_RECEPCION_ESPERADA, LOCATION_CODE, COD_TRANSPORTISTA, NOMBRE_TRANSPORTISTA, ITEM_NO, NO_LOTE_PROVEEDOR, FECHA_ENTREGA, COD_ESPECIE, HORA_FIN_DESCARGA, COD_OPERARIO, NOMBRE_OPERARIO, TURNO, TOTAL_BAJAS, TOTAL_DECOMISOS, DESCUENTO_ADICIONAL, PESO_DESCUENTO_ADICIONAL, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('cab_partida_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.NO_PARTIDA is not distinct from s.NO_PARTIDA and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_CAB_PARTIDA') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('cab_partida_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        no_partida, 
        fecha_sacrificio, 
        no_guia_sanitaria, 
        no_proveedor, 
        observaciones, 
        apto_exportacion, 
        explotacion, 
        no_cuadra, 
        no_lote, 
        partida_acabada, 
        traspasado_a_gestion, 
        liquidacion_cerrada, 
        no_pedido, 
        no_linea_pedido, 
        cod_penalizacion, 
        peso_neto_camion, 
        peso_bruto_animales, 
        horas_ayuno, 
        no_series, 
        peso_bruto_camion, 
        fecha_descarga, 
        hora_descarga, 
        desinfeccion, 
        tipo_producto, 
        historico, 
        oreo, 
        peso_oreo, 
        total_kilos_proveedor, 
        no_partida_proveedor, 
        estado_liquidacion, 
        no_documento_externo, 
        no_aplicar_penalizaciones, 
        albaran_generado, 
        estado, 
        bloqueada, 
        fecha_partida, 
        fecha_hora_creacion, 
        no_cabezas_recibidas, 
        ano, 
        semana, 
        fecha_recepcion_esperada, 
        hora_recepcion_esperada, 
        location_code, 
        cod_transportista, 
        nombre_transportista, 
        item_no, 
        no_lote_proveedor, 
        fecha_entrega, 
        cod_especie, 
        hora_fin_descarga, 
        cod_operario, 
        nombre_operario, 
        turno, 
        total_bajas, 
        total_decomisos, 
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
        HASH(TIMESTAMP, NO_PARTIDA, FECHA_SACRIFICIO, NO_GUIA_SANITARIA, NO_PROVEEDOR, OBSERVACIONES, APTO_EXPORTACION, EXPLOTACION, NO_CUADRA, NO_LOTE, PARTIDA_ACABADA, TRASPASADO_A_GESTION, LIQUIDACION_CERRADA, NO_PEDIDO, NO_LINEA_PEDIDO, COD_PENALIZACION, PESO_NETO_CAMION, PESO_BRUTO_ANIMALES, HORAS_AYUNO, NO_SERIES, PESO_BRUTO_CAMION, FECHA_DESCARGA, HORA_DESCARGA, DESINFECCION, TIPO_PRODUCTO, HISTORICO, OREO, PESO_OREO, TOTAL_KILOS_PROVEEDOR, NO_PARTIDA_PROVEEDOR, ESTADO_LIQUIDACION, NO_DOCUMENTO_EXTERNO, NO_APLICAR_PENALIZACIONES, ALBARAN_GENERADO, ESTADO, BLOQUEADA, FECHA_PARTIDA, FECHA_HORA_CREACION, NO_CABEZAS_RECIBIDAS, ANO, SEMANA, FECHA_RECEPCION_ESPERADA, HORA_RECEPCION_ESPERADA, LOCATION_CODE, COD_TRANSPORTISTA, NOMBRE_TRANSPORTISTA, ITEM_NO, NO_LOTE_PROVEEDOR, FECHA_ENTREGA, COD_ESPECIE, HORA_FIN_DESCARGA, COD_OPERARIO, NOMBRE_OPERARIO, TURNO, TOTAL_BAJAS, TOTAL_DECOMISOS, DESCUENTO_ADICIONAL, PESO_DESCUENTO_ADICIONAL, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('cab_partida_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.NO_PARTIDA is not distinct from s.NO_PARTIDA and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.NO_PARTIDA is null
    or t.tec_hash <> s.tec_hash
{% endif %}
