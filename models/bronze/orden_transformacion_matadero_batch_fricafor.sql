{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_ORDEN_TRANSFORMACION_MATADERO') %}
{% set log_ref = source('support_param', 'INSERT_LOGS') %}

with next_log as (
    select tec_id_ingesta
    from {{ log_ref }}
    where upper(database)   = upper('{{ src_ref.database }}')
      and upper(schema)     = upper('{{ src_ref.schema }}')
      and upper(table_name) = upper('{{ src_ref.identifier }}')
      and start_watermark <> end_watermark
      and tec_ts_integracion_b is null
    qualify row_number() over(order by tec_ts_ingesta) = 1
),
src as (
    select
        {{ std_cast('"TIMESTAMP"', 'INTEGER') }}                               as TIMESTAMP,
        {{ std_cast('"NO_"', 'VARCHAR') }}                                     as NO,
        {{ std_cast('"NO_SERIES"', 'VARCHAR') }}                               as NO_SERIES,
        {{ std_cast('"DESCRIPTION"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"CREATION_USER_ID"', 'VARCHAR') }}                        as CREATION_USER_ID,
        {{ std_cast('"CREATION_DATE"', 'TIMESTAMP_NTZ') }}                     as CREATION_DATE,
        {{ std_cast('"CREATION_TIME"', 'TIMESTAMP_NTZ') }}                     as CREATION_TIME,
        {{ std_cast('"STARTING_DATE"', 'TIMESTAMP_NTZ') }}                     as STARTING_DATE,
        {{ std_cast('"ENDING_DATE"', 'TIMESTAMP_NTZ') }}                       as ENDING_DATE,
        {{ std_cast('"COMPLETION_USER_ID"', 'VARCHAR') }}                      as COMPLETION_USER_ID,
        {{ std_cast('"COMPLETION_DATE"', 'TIMESTAMP_NTZ') }}                   as COMPLETION_DATE,
        {{ std_cast('"COMPLETION_TIME"', 'TIMESTAMP_NTZ') }}                   as COMPLETION_TIME,
        {{ std_cast('"BLOCKED"', 'INTEGER') }}                                 as BLOCKED,
        {{ std_cast('"POSTING_DATE"', 'TIMESTAMP_NTZ') }}                      as POSTING_DATE,
        {{ std_cast('"COD_ALMACEN_CONSUMOS"', 'VARCHAR') }}                    as COD_ALMACEN_CONSUMOS,
        {{ std_cast('"COD_ALMACEN_SALIDAS"', 'VARCHAR') }}                     as COD_ALMACEN_SALIDAS,
        {{ std_cast('"NO_LOTE_SALIDAS"', 'VARCHAR') }}                         as NO_LOTE_SALIDAS,
        {{ std_cast('"CONTROL_LOTE_ESPECIF_"', 'INTEGER') }}                   as CONTROL_LOTE_ESPECIF,
        {{ std_cast('"NO_PLANTILLA"', 'VARCHAR') }}                            as NO_PLANTILLA,
        {{ std_cast('"TIPO_ORDEN_PROD_TRANSF_"', 'INTEGER') }}                 as TIPO_ORDEN_PROD_TRANSF,
        {{ std_cast('"CANTIDAD"', 'NUMBER(38,5)') }}                           as CANTIDAD,
        {{ std_cast('"SERVICIO"', 'INTEGER') }}                                as SERVICIO,
        {{ std_cast('"COD_IDIOMA_ETIQUETAJE"', 'VARCHAR') }}                   as COD_IDIOMA_ETIQUETAJE,
        {{ std_cast('"BIN_CODE_CONSUMO"', 'VARCHAR') }}                        as BIN_CODE_CONSUMO,
        {{ std_cast('"BIN_CODE_SALIDAS"', 'VARCHAR') }}                        as BIN_CODE_SALIDAS,
        {{ std_cast('"NO_PEDIDO_VENTA"', 'VARCHAR') }}                         as NO_PEDIDO_VENTA,
        {{ std_cast('"NO_LINEA_PEDIDO"', 'INTEGER') }}                         as NO_LINEA_PEDIDO,
        {{ std_cast('"BILL_TO_CUSTOMER_NO_"', 'VARCHAR') }}                    as BILL_TO_CUSTOMER_NO,
        {{ std_cast('"BILL_TO_NAME"', 'VARCHAR') }}                            as BILL_TO_NAME,
        {{ std_cast('"ITEM_NO_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"DUE_DATE"', 'TIMESTAMP_NTZ') }}                          as DUE_DATE,
        {{ std_cast('"ESTADO_TRABAJO"', 'INTEGER') }}                          as ESTADO_TRABAJO,
        {{ std_cast('"COD_RESPONSABLE_TRABAJO"', 'VARCHAR') }}                 as COD_RESPONSABLE_TRABAJO,
        {{ std_cast('"YOUR_REFERENCE"', 'VARCHAR') }}                          as YOUR_REFERENCE,
        {{ std_cast('"ITEM_DESCRIPTION"', 'VARCHAR') }}                        as ITEM_DESCRIPTION,
        {{ std_cast('"VARIANT_CODE"', 'VARCHAR') }}                            as VARIANT_CODE,
        {{ std_cast('"STARTING_TIME"', 'TIMESTAMP_NTZ') }}                     as STARTING_TIME,
        {{ std_cast('"ENDING_TIME"', 'TIMESTAMP_NTZ') }}                       as ENDING_TIME,
        {{ std_cast('"WORK_CENTER_NO_"', 'VARCHAR') }}                         as WORK_CENTER_NO,
        {{ std_cast('"TIPO_TRANSFORMACION"', 'VARCHAR') }}                     as TIPO_TRANSFORMACION,
        {{ std_cast('"FASE_ESTADISTICA"', 'VARCHAR') }}                        as FASE_ESTADISTICA,
        {{ std_cast('"FECHA_LOTE"', 'TIMESTAMP_NTZ') }}                        as FECHA_LOTE,
        {{ std_cast('"LINEA_FABRICACION"', 'VARCHAR') }}                       as LINEA_FABRICACION,
        {{ std_cast('"TURNO_TRABAJO"', 'VARCHAR') }}                           as TURNO_TRABAJO,
        {{ std_cast('"BLOQUEAR_CALCULO_COSTE"', 'INTEGER') }}                  as BLOQUEAR_CALCULO_COSTE,
        {{ std_cast('"DURACION"', 'TIMESTAMP_NTZ') }}                          as DURACION,
        {{ std_cast('"HORA_INICIO_OPERARIO"', 'TIMESTAMP_NTZ') }}              as HORA_INICIO_OPERARIO,
        {{ std_cast('"HORA_FIN_OPERARIO"', 'TIMESTAMP_NTZ') }}                 as HORA_FIN_OPERARIO,
        {{ std_cast('"REENVASADOS"', 'NUMBER(38,5)') }}                        as REENVASADOS,
        {{ std_cast('"MERMA_PLASTICO"', 'NUMBER(38,5)') }}                     as MERMA_PLASTICO,
        {{ std_cast('"EXPIRATION_DATE"', 'TIMESTAMP_NTZ') }}                   as EXPIRATION_DATE,
        {{ std_cast('"TIPO_PLANTILLA"', 'INTEGER') }}                          as TIPO_PLANTILLA,
        {{ std_cast('"LOTE_VENTA"', 'INTEGER') }}                              as LOTE_VENTA,
        {{ std_cast('"CALCULO_CONSUMO_AUTOMATICO"', 'INTEGER') }}              as CALCULO_CONSUMO_AUTOMATICO,
        {{ std_cast('"COD_UNIDAD_MEDIDA_CONS_AUTOM"', 'VARCHAR') }}            as COD_UNIDAD_MEDIDA_CONS_AUTOM,
        {{ std_cast('"LINEA_REQUERIDA"', 'INTEGER') }}                         as LINEA_REQUERIDA,
        {{ std_cast('"MEZCLA_LOTES"', 'INTEGER') }}                            as MEZCLA_LOTES,
        {{ std_cast('"GESTION_POR_INDIVIDUO"', 'INTEGER') }}                   as GESTION_POR_INDIVIDUO,
        {{ std_cast('"CONSUMO_SIN_GENERAR_MERMA"', 'INTEGER') }}               as CONSUMO_SIN_GENERAR_MERMA,
        {{ std_cast('"ACONDICIONAMIENTO_DE_RECHAZOS"', 'INTEGER') }}           as ACONDICIONAMIENTO_DE_RECHAZOS,
        {{ std_cast('"MANTENER_NO_PESADA"', 'INTEGER') }}                      as MANTENER_NO_PESADA,
        {{ std_cast('"MANTENER_NO_LOTE"', 'INTEGER') }}                        as MANTENER_NO_LOTE,
        {{ std_cast('"TIPO_MEZCLA_LOTE"', 'INTEGER') }}                        as TIPO_MEZCLA_LOTE,
        {{ std_cast('"LIMPIEZA_REQUERIDA"', 'INTEGER') }}                      as LIMPIEZA_REQUERIDA,
        {{ std_cast('"PALETIZACION_REQUERIDA"', 'INTEGER') }}                  as PALETIZACION_REQUERIDA,
        {{ std_cast('"SALIDA_AUTOMATICA"', 'INTEGER') }}                       as SALIDA_AUTOMATICA,
        {{ std_cast('"FILTRO_COD_VARIANTE_ENTRADA"', 'VARCHAR') }}             as FILTRO_COD_VARIANTE_ENTRADA,
        {{ std_cast('"CONTROLAR_LOTE_RETENIDO"', 'INTEGER') }}                 as CONTROLAR_LOTE_RETENIDO,
        {{ std_cast('"AJUSTAR_COSTE"', 'INTEGER') }}                           as AJUSTAR_COSTE,
        {{ std_cast('"TIPO_PALET"', 'VARCHAR') }}                              as TIPO_PALET,
        {{ std_cast('"TIPO_CAJAS"', 'VARCHAR') }}                              as TIPO_CAJAS,
        {{ std_cast('"CODIGO_EMBALAJE_PALET"', 'VARCHAR') }}                   as CODIGO_EMBALAJE_PALET,
        {{ std_cast('"CODIGO_EMBALAJE_CAJAS"', 'VARCHAR') }}                   as CODIGO_EMBALAJE_CAJAS,
        {{ std_cast('"SEMANA_INICIAL"', 'INTEGER') }}                          as SEMANA_INICIAL,
        {{ std_cast('"VENDOR_NO_"', 'VARCHAR') }}                              as VENDOR_NO,
        {{ std_cast('"FECHA_VALORACION_COSTE"', 'TIMESTAMP_NTZ') }}            as FECHA_VALORACION_COSTE,
        {{ std_cast('"ESTADO"', 'INTEGER') }}                                  as ESTADO,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'FRI'                                                                     as tec_des_cod_siglas,
        'FRICAFOR'                                                             as tec_des_empresa,
        tec_id_ingesta,
        tec_ts_ingesta,
        tec_ts_staging,
        tec_ts_integracion_b
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY no
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
