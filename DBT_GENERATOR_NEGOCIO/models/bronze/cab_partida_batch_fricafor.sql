{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_CAB_PARTIDA') %}
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
        {{ std_cast('"NO_PARTIDA"', 'VARCHAR') }}                              as NO_PARTIDA,
        {{ std_cast('"FECHA_SACRIFICIO"', 'TIMESTAMP_NTZ') }}                  as FECHA_SACRIFICIO,
        {{ std_cast('"NO_GUIA_SANITARIA"', 'VARCHAR') }}                       as NO_GUIA_SANITARIA,
        {{ std_cast('"NO_PROVEEDOR"', 'VARCHAR') }}                            as NO_PROVEEDOR,
        {{ std_cast('"OBSERVACIONES"', 'VARCHAR') }}                           as OBSERVACIONES,
        {{ std_cast('"APTO_EXPORTACION"', 'INTEGER') }}                        as APTO_EXPORTACION,
        {{ std_cast('"EXPLOTACION"', 'VARCHAR') }}                             as EXPLOTACION,
        {{ std_cast('"NO_CUADRA"', 'VARCHAR') }}                               as NO_CUADRA,
        {{ std_cast('"NO_LOTE"', 'VARCHAR') }}                                 as NO_LOTE,
        {{ std_cast('"PARTIDA_ACABADA"', 'INTEGER') }}                         as PARTIDA_ACABADA,
        {{ std_cast('"TRASPASADO_A_GESTION"', 'INTEGER') }}                    as TRASPASADO_A_GESTION,
        {{ std_cast('"LIQUIDACION_CERRADA"', 'INTEGER') }}                     as LIQUIDACION_CERRADA,
        {{ std_cast('"NO_PEDIDO"', 'VARCHAR') }}                               as NO_PEDIDO,
        {{ std_cast('"NO_LINEA_PEDIDO"', 'INTEGER') }}                         as NO_LINEA_PEDIDO,
        {{ std_cast('"COD_PENALIZACION"', 'VARCHAR') }}                        as COD_PENALIZACION,
        {{ std_cast('"PESO_NETO_CAMION"', 'NUMBER(38,5)') }}                   as PESO_NETO_CAMION,
        {{ std_cast('"PESO_BRUTO_ANIMALES"', 'NUMBER(38,5)') }}                as PESO_BRUTO_ANIMALES,
        {{ std_cast('"HORAS_AYUNO"', 'INTEGER') }}                             as HORAS_AYUNO,
        {{ std_cast('"NO_SERIES"', 'VARCHAR') }}                               as NO_SERIES,
        {{ std_cast('"PESO_BRUTO_CAMION"', 'NUMBER(38,5)') }}                  as PESO_BRUTO_CAMION,
        {{ std_cast('"FECHA_DESCARGA"', 'TIMESTAMP_NTZ') }}                    as FECHA_DESCARGA,
        {{ std_cast('"HORA_DESCARGA"', 'TIMESTAMP_NTZ') }}                     as HORA_DESCARGA,
        {{ std_cast('"DESINFECCION"', 'INTEGER') }}                            as DESINFECCION,
        {{ std_cast('"TIPO_PRODUCTO"', 'INTEGER') }}                           as TIPO_PRODUCTO,
        {{ std_cast('"HISTORICO"', 'INTEGER') }}                               as HISTORICO,
        {{ std_cast('"_OREO"', 'NUMBER(38,5)') }}                              as OREO,
        {{ std_cast('"PESO_OREO"', 'NUMBER(38,5)') }}                          as PESO_OREO,
        {{ std_cast('"TOTAL_KILOS_PROVEEDOR"', 'NUMBER(38,5)') }}              as TOTAL_KILOS_PROVEEDOR,
        {{ std_cast('"NO_PARTIDA_PROVEEDOR"', 'VARCHAR') }}                    as NO_PARTIDA_PROVEEDOR,
        {{ std_cast('"ESTADO_LIQUIDACION"', 'INTEGER') }}                      as ESTADO_LIQUIDACION,
        {{ std_cast('"NO_DOCUMENTO_EXTERNO"', 'VARCHAR') }}                    as NO_DOCUMENTO_EXTERNO,
        {{ std_cast('"NO_APLICAR_PENALIZACIONES"', 'INTEGER') }}               as NO_APLICAR_PENALIZACIONES,
        {{ std_cast('"ALBARAN_GENERADO"', 'INTEGER') }}                        as ALBARAN_GENERADO,
        {{ std_cast('"ESTADO"', 'INTEGER') }}                                  as ESTADO,
        {{ std_cast('"BLOQUEADA"', 'INTEGER') }}                               as BLOQUEADA,
        {{ std_cast('"FECHA_PARTIDA"', 'TIMESTAMP_NTZ') }}                     as FECHA_PARTIDA,
        {{ std_cast('"FECHA_HORA_CREACION"', 'TIMESTAMP_NTZ') }}               as FECHA_HORA_CREACION,
        {{ std_cast('"NO_CABEZAS_RECIBIDAS"', 'INTEGER') }}                    as NO_CABEZAS_RECIBIDAS,
        {{ std_cast('"ANO"', 'INTEGER') }}                                     as ANO,
        {{ std_cast('"SEMANA"', 'INTEGER') }}                                  as SEMANA,
        {{ std_cast('"FECHA_RECEPCION_ESPERADA"', 'TIMESTAMP_NTZ') }}          as FECHA_RECEPCION_ESPERADA,
        {{ std_cast('"HORA_RECEPCION_ESPERADA"', 'TIMESTAMP_NTZ') }}           as HORA_RECEPCION_ESPERADA,
        {{ std_cast('"LOCATION_CODE"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"COD_TRANSPORTISTA"', 'VARCHAR') }}                       as COD_TRANSPORTISTA,
        {{ std_cast('"NOMBRE_TRANSPORTISTA"', 'VARCHAR') }}                    as NOMBRE_TRANSPORTISTA,
        {{ std_cast('"ITEM_NO_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"NO_LOTE_PROVEEDOR"', 'VARCHAR') }}                       as NO_LOTE_PROVEEDOR,
        {{ std_cast('"FECHA_ENTREGA"', 'TIMESTAMP_NTZ') }}                     as FECHA_ENTREGA,
        {{ std_cast('"COD_ESPECIE"', 'VARCHAR') }}                             as COD_ESPECIE,
        {{ std_cast('"HORA_FIN_DESCARGA"', 'TIMESTAMP_NTZ') }}                 as HORA_FIN_DESCARGA,
        {{ std_cast('"COD_OPERARIO"', 'VARCHAR') }}                            as COD_OPERARIO,
        {{ std_cast('"NOMBRE_OPERARIO"', 'VARCHAR') }}                         as NOMBRE_OPERARIO,
        {{ std_cast('"TURNO"', 'INTEGER') }}                                   as TURNO,
        {{ std_cast('"TOTAL_BAJAS"', 'INTEGER') }}                             as TOTAL_BAJAS,
        {{ std_cast('"TOTAL_DECOMISOS"', 'INTEGER') }}                         as TOTAL_DECOMISOS,
        {{ std_cast('"_DESCUENTO_ADICIONAL"', 'NUMBER(38,5)') }}               as DESCUENTO_ADICIONAL,
        {{ std_cast('"PESO_DESCUENTO_ADICIONAL"', 'NUMBER(38,5)') }}           as PESO_DESCUENTO_ADICIONAL,
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