{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_LIN_ORDEN_TRANSF_MATADERO') %}
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
        {{ std_cast('"PROD_ORDER_NO_"', 'VARCHAR') }}                          as PROD_ORDER_NO,
        {{ std_cast('"TIPO_MOVIMIENTO"', 'INTEGER') }}                         as TIPO_MOVIMIENTO,
        {{ std_cast('"TIPO_PLANTILLA"', 'INTEGER') }}                          as TIPO_PLANTILLA,
        {{ std_cast('"LINE_NO_"', 'INTEGER') }}                                as LINE_NO,
        {{ std_cast('"TYPE"', 'INTEGER') }}                                    as TYPE,
        {{ std_cast('"NO_"', 'VARCHAR') }}                                     as NO,
        {{ std_cast('"VARIANT_CODE"', 'VARCHAR') }}                            as VARIANT_CODE,
        {{ std_cast('"DESCRIPTION"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"DESCRIPTION_2"', 'VARCHAR') }}                           as DESCRIPTION_2,
        {{ std_cast('"UNIT_COST"', 'NUMBER(38,5)') }}                          as UNIT_COST,
        {{ std_cast('"UNIT_OF_MEASURE_CODE"', 'VARCHAR') }}                    as UNIT_OF_MEASURE_CODE,
        {{ std_cast('"LOCATION_CODE"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"LOT_NO_"', 'VARCHAR') }}                                 as LOT_NO,
        {{ std_cast('"CANTIDAD_UNIDADES"', 'NUMBER(38,5)') }}                  as CANTIDAD_UNIDADES,
        {{ std_cast('"CANTIDAD_KILOS"', 'NUMBER(38,5)') }}                     as CANTIDAD_KILOS,
        {{ std_cast('"TIPO_PALET"', 'VARCHAR') }}                              as TIPO_PALET,
        {{ std_cast('"TARA_PALETS"', 'NUMBER(38,5)') }}                        as TARA_PALETS,
        {{ std_cast('"CANTIDAD_PALETS"', 'NUMBER(38,5)') }}                    as CANTIDAD_PALETS,
        {{ std_cast('"TIPO_CAJAS"', 'VARCHAR') }}                              as TIPO_CAJAS,
        {{ std_cast('"TARA_CAJAS"', 'NUMBER(38,5)') }}                         as TARA_CAJAS,
        {{ std_cast('"CANTIDAD_CAJAS"', 'NUMBER(38,5)') }}                     as CANTIDAD_CAJAS,
        {{ std_cast('"TARA_EXTRA"', 'NUMBER(38,5)') }}                         as TARA_EXTRA,
        {{ std_cast('"BLOQUEAR_AJUSTE_AUT_COSTE"', 'INTEGER') }}               as BLOQUEAR_AJUSTE_AUT_COSTE,
        {{ std_cast('"EXPIRATION_DATE"', 'TIMESTAMP_NTZ') }}                   as EXPIRATION_DATE,
        {{ std_cast('"SALES_ORDER_NO_"', 'VARCHAR') }}                         as SALES_ORDER_NO,
        {{ std_cast('"SALES_ORDER_LINE_NO_"', 'INTEGER') }}                    as SALES_ORDER_LINE_NO,
        {{ std_cast('"ESTADO_ASIGNACION"', 'INTEGER') }}                       as ESTADO_ASIGNACION,
        {{ std_cast('"AUTOCONSUMO"', 'INTEGER') }}                             as AUTOCONSUMO,
        {{ std_cast('"COD_TRABAJO"', 'VARCHAR') }}                             as COD_TRABAJO,
        {{ std_cast('"POSTING_DATE"', 'TIMESTAMP_NTZ') }}                      as POSTING_DATE,
        {{ std_cast('"ORDER_ASSIGN_ENTRY_NO_"', 'INTEGER') }}                  as ORDER_ASSIGN_ENTRY_NO,
        {{ std_cast('"MERMA"', 'INTEGER') }}                                   as MERMA,
        {{ std_cast('"COD_OPERARIO"', 'VARCHAR') }}                            as COD_OPERARIO,
        {{ std_cast('"TIPO_MOV_RELACIONADO"', 'INTEGER') }}                    as TIPO_MOV_RELACIONADO,
        {{ std_cast('"LINE_NO_RELATION"', 'INTEGER') }}                        as LINE_NO_RELATION,
        {{ std_cast('"OP_CURAC_ENTRY_NO_"', 'INTEGER') }}                      as OP_CURAC_ENTRY_NO,
        {{ std_cast('"PARAMETRO_1"', 'NUMBER(38,5)') }}                        as PARAMETRO_1,
        {{ std_cast('"PARAMETRO_2"', 'NUMBER(38,5)') }}                        as PARAMETRO_2,
        {{ std_cast('"PARAMETRO_3"', 'INTEGER') }}                             as PARAMETRO_3,
        {{ std_cast('"ALTERNATIVE_ITEM_TO_LINE_NO_"', 'INTEGER') }}            as ALTERNATIVE_ITEM_TO_LINE_NO,
        {{ std_cast('"INDENTATION"', 'INTEGER') }}                             as INDENTATION,
        {{ std_cast('"ALTERNATIVE_ITEM_TO_ITEM_NO_"', 'VARCHAR') }}            as ALTERNATIVE_ITEM_TO_ITEM_NO,
        {{ std_cast('"BIN_CODE"', 'VARCHAR') }}                                as BIN_CODE,
        {{ std_cast('"POSTING_TIME"', 'TIMESTAMP_NTZ') }}                      as POSTING_TIME,
        {{ std_cast('"RECHAZO"', 'INTEGER') }}                                 as RECHAZO,
        {{ std_cast('"PRODUCTO_FABRICACION"', 'INTEGER') }}                    as PRODUCTO_FABRICACION,
        {{ std_cast('"COD_MOTIVO_RECHAZO"', 'VARCHAR') }}                      as COD_MOTIVO_RECHAZO,
        {{ std_cast('"SUMA_A_TARA"', 'INTEGER') }}                             as SUMA_A_TARA,
        {{ std_cast('"DESCRIPCION_ABREVIADA"', 'VARCHAR') }}                   as DESCRIPCION_ABREVIADA,
        {{ std_cast('"KG_HORA"', 'NUMBER(38,5)') }}                            as KG_HORA,
        {{ std_cast('"ITEM_NO_PRODUCED"', 'VARCHAR') }}                        as ITEM_NO_PRODUCED,
        {{ std_cast('"COD_EMBALAJE_PRODUCIDO"', 'VARCHAR') }}                  as COD_EMBALAJE_PRODUCIDO,
        {{ std_cast('"CDAD_EMBALAJE_PRODUCIDO"', 'NUMBER(38,5)') }}            as CDAD_EMBALAJE_PRODUCIDO,
        {{ std_cast('"CDAD_UNIDADES_PRODUCIDAS"', 'NUMBER(38,5)') }}           as CDAD_UNIDADES_PRODUCIDAS,
        {{ std_cast('"CDAD_KILOS_PRODUCIDOS"', 'NUMBER(38,5)') }}              as CDAD_KILOS_PRODUCIDOS,
        {{ std_cast('"TOTAL_NETO"', 'NUMBER(38,5)') }}                         as TOTAL_NETO,
        {{ std_cast('"IDENTIFICADOR_TARA"', 'VARCHAR') }}                      as IDENTIFICADOR_TARA,
        {{ std_cast('"CANTIDAD_KILOS_INFORMATIVO"', 'NUMBER(38,5)') }}         as CANTIDAD_KILOS_INFORMATIVO,
        {{ std_cast('"PESADA_FINALIZADA"', 'INTEGER') }}                       as PESADA_FINALIZADA,
        {{ std_cast('"TIPO_COSTE"', 'VARCHAR') }}                              as TIPO_COSTE,
        {{ std_cast('"FASE_ESTADISTICA"', 'VARCHAR') }}                        as FASE_ESTADISTICA,
        {{ std_cast('"COSTE_TOTAL"', 'NUMBER(38,5)') }}                        as COSTE_TOTAL,
        {{ std_cast('"WORK_CENTER_NO_"', 'VARCHAR') }}                         as WORK_CENTER_NO,
        {{ std_cast('"ID_INDIVIDUO"', 'VARCHAR') }}                            as ID_INDIVIDUO,
        {{ std_cast('"SIN_MOVIMIENTO_PRODUCTO"', 'INTEGER') }}                 as SIN_MOVIMIENTO_PRODUCTO,
        {{ std_cast('"PERMITE_DECOMISO"', 'INTEGER') }}                        as PERMITE_DECOMISO,
        {{ std_cast('"PERMITE_CLASIFICACION"', 'INTEGER') }}                   as PERMITE_CLASIFICACION,
        {{ std_cast('"NUM_ETIQUETAS"', 'INTEGER') }}                           as NUM_ETIQUETAS,
        {{ std_cast('"TIPO_MOV_RELAC_PLANTILLA"', 'INTEGER') }}                as TIPO_MOV_RELAC_PLANTILLA,
        {{ std_cast('"TIPO_PLANT_RELAC_PLANTILLA"', 'INTEGER') }}              as TIPO_PLANT_RELAC_PLANTILLA,
        {{ std_cast('"NO_LINEA_RELAC_PLANTILLA"', 'INTEGER') }}                as NO_LINEA_RELAC_PLANTILLA,
        {{ std_cast('"CONTROL_POR"', 'INTEGER') }}                             as CONTROL_POR,
        {{ std_cast('"PERMITE_PESAJE"', 'INTEGER') }}                          as PERMITE_PESAJE,
        {{ std_cast('"DECOMISO"', 'INTEGER') }}                                as DECOMISO,
        {{ std_cast('"NO_PESADA"', 'VARCHAR') }}                               as NO_PESADA,
        {{ std_cast('"NO_PLANTILLA"', 'VARCHAR') }}                            as NO_PLANTILLA,
        {{ std_cast('"NO_LINEA_PLANTILLA"', 'INTEGER') }}                      as NO_LINEA_PLANTILLA,
        {{ std_cast('"CODIGO_EMBALAJE"', 'VARCHAR') }}                         as CODIGO_EMBALAJE,
        {{ std_cast('"CANTIDAD_EMBALAJE"', 'NUMBER(38,5)') }}                  as CANTIDAD_EMBALAJE,
        {{ std_cast('"OT_DESPIECE_1_2_CANALES"', 'VARCHAR') }}                 as OT_DESPIECE_1_2_CANALES,
        {{ std_cast('"ES_TERNERA"', 'INTEGER') }}                              as ES_TERNERA,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'FRI'                                                                     as tec_des_cod_siglas
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY prod_order_no, line_no
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
