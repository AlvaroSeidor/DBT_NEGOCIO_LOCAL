{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_INFO_INDIVIDUO') %}
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
        {{ std_cast('"ID_INDIVIDUO"', 'VARCHAR') }}                            as ID_INDIVIDUO,
        {{ std_cast('"NO_DIB"', 'VARCHAR') }}                                  as NO_DIB,
        {{ std_cast('"NO_CROTAL"', 'VARCHAR') }}                               as NO_CROTAL,
        {{ std_cast('"NO_CROTAL_GUIA"', 'VARCHAR') }}                          as NO_CROTAL_GUIA,
        {{ std_cast('"NO_DIB_MADRE"', 'VARCHAR') }}                            as NO_DIB_MADRE,
        {{ std_cast('"NO_GUIA"', 'VARCHAR') }}                                 as NO_GUIA,
        {{ std_cast('"NO_OT_MATADERO"', 'VARCHAR') }}                          as NO_OT_MATADERO,
        {{ std_cast('"ORDEN_SACRIFICIO"', 'INTEGER') }}                        as ORDEN_SACRIFICIO,
        {{ std_cast('"FECHA_SACRIFICIO"', 'TIMESTAMP_NTZ') }}                  as FECHA_SACRIFICIO,
        {{ std_cast('"PESO_1_2_CAN_1"', 'NUMBER(38,5)') }}                     as PESO_1_2_CAN_1,
        {{ std_cast('"PESO_1_2_CAN_2"', 'NUMBER(38,5)') }}                     as PESO_1_2_CAN_2,
        {{ std_cast('"NO_PARTIDA"', 'VARCHAR') }}                              as NO_PARTIDA,
        {{ std_cast('"NO_LINEA_PARTIDA"', 'INTEGER') }}                        as NO_LINEA_PARTIDA,
        {{ std_cast('"SSCC_1"', 'VARCHAR') }}                                  as SSCC_1,
        {{ std_cast('"SSCC_2"', 'VARCHAR') }}                                  as SSCC_2,
        {{ std_cast('"NO_LOTE"', 'VARCHAR') }}                                 as NO_LOTE,
        {{ std_cast('"HORA_SACRIFICIO"', 'TIMESTAMP_NTZ') }}                   as HORA_SACRIFICIO,
        {{ std_cast('"SERIE_GUIA"', 'VARCHAR') }}                              as SERIE_GUIA,
        {{ std_cast('"PESO_BRUTO"', 'NUMBER(38,5)') }}                         as PESO_BRUTO,
        {{ std_cast('"POBLACION_NACIMIENTO"', 'VARCHAR') }}                    as POBLACION_NACIMIENTO,
        {{ std_cast('"COD_PAIS_REGION_NACIMIENTO"', 'VARCHAR') }}              as COD_PAIS_REGION_NACIMIENTO,
        {{ std_cast('"CODIGO_POSTAL_NACIMIENTO"', 'VARCHAR') }}                as CODIGO_POSTAL_NACIMIENTO,
        {{ std_cast('"PROVINCIA_NACIMIENTO"', 'VARCHAR') }}                    as PROVINCIA_NACIMIENTO,
        {{ std_cast('"FECHA_NACIMIENTO"', 'TIMESTAMP_NTZ') }}                  as FECHA_NACIMIENTO,
        {{ std_cast('"COD_PAIS_ENGORDE"', 'VARCHAR') }}                        as COD_PAIS_ENGORDE,
        {{ std_cast('"COD_PAIS_ENGORDE_2"', 'VARCHAR') }}                      as COD_PAIS_ENGORDE_2,
        {{ std_cast('"COD_PAIS_ENGORDE_3"', 'VARCHAR') }}                      as COD_PAIS_ENGORDE_3,
        {{ std_cast('"COD_PAIS_ENGORDE_4"', 'VARCHAR') }}                      as COD_PAIS_ENGORDE_4,
        {{ std_cast('"POBLACION_SACRIFICIO"', 'VARCHAR') }}                    as POBLACION_SACRIFICIO,
        {{ std_cast('"COD_PAIS_REGION_SACRIFICIO"', 'VARCHAR') }}              as COD_PAIS_REGION_SACRIFICIO,
        {{ std_cast('"CODIGO_POSTAL_SACRIFICIO"', 'VARCHAR') }}                as CODIGO_POSTAL_SACRIFICIO,
        {{ std_cast('"PROVINCIA_SACRIFICIO"', 'VARCHAR') }}                    as PROVINCIA_SACRIFICIO,
        {{ std_cast('"NO_CUADRA"', 'VARCHAR') }}                               as NO_CUADRA,
        {{ std_cast('"FECHA_DESCARGA"', 'TIMESTAMP_NTZ') }}                    as FECHA_DESCARGA,
        {{ std_cast('"HORA_DESCARGA"', 'TIMESTAMP_NTZ') }}                     as HORA_DESCARGA,
        {{ std_cast('"COD_RAZA"', 'VARCHAR') }}                                as COD_RAZA,
        {{ std_cast('"SEXO"', 'INTEGER') }}                                    as SEXO,
        {{ std_cast('"RAZA"', 'VARCHAR') }}                                    as RAZA,
        {{ std_cast('"ITEM_NO_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"VARIAN_CODE"', 'VARCHAR') }}                             as VARIAN_CODE,
        {{ std_cast('"DESCRIPCION"', 'VARCHAR') }}                             as DESCRIPCION,
        {{ std_cast('"DESCRIPCION_2"', 'VARCHAR') }}                           as DESCRIPCION_2,
        {{ std_cast('"ESTADO_INDIVIDUO"', 'VARCHAR') }}                        as ESTADO_INDIVIDUO,
        {{ std_cast('"BLOQUEADO"', 'INTEGER') }}                               as BLOQUEADO,
        {{ std_cast('"FECHA_CREACION"', 'TIMESTAMP_NTZ') }}                    as FECHA_CREACION,
        {{ std_cast('"HORA_CREACION"', 'TIMESTAMP_NTZ') }}                     as HORA_CREACION,
        {{ std_cast('"COD_CONFORMACION"', 'VARCHAR') }}                        as COD_CONFORMACION,
        {{ std_cast('"GRADO_CONFORMACION"', 'VARCHAR') }}                      as GRADO_CONFORMACION,
        {{ std_cast('"COD_ENGRASAMIENTO"', 'VARCHAR') }}                       as COD_ENGRASAMIENTO,
        {{ std_cast('"GRADO_GRASA"', 'VARCHAR') }}                             as GRADO_GRASA,
        {{ std_cast('"CASTRADO"', 'INTEGER') }}                                as CASTRADO,
        {{ std_cast('"HA_PARIDO"', 'INTEGER') }}                               as HA_PARIDO,
        {{ std_cast('"SACRIFICADO"', 'INTEGER') }}                             as SACRIFICADO,
        {{ std_cast('"NUM_ETIQUETAS_IMP_"', 'INTEGER') }}                      as NUM_ETIQUETAS_IMP,
        {{ std_cast('"COD_ESPECIE"', 'VARCHAR') }}                             as COD_ESPECIE,
        {{ std_cast('"ESTADO_DATOS"', 'INTEGER') }}                            as ESTADO_DATOS,
        {{ std_cast('"MENSAJE_ESTADO_DATOS"', 'VARCHAR') }}                    as MENSAJE_ESTADO_DATOS,
        {{ std_cast('"LAST_UPDATE_CARACT_"', 'TIMESTAMP_NTZ') }}               as LAST_UPDATE_CARACT,
        {{ std_cast('"MUERTO_EN_EXPLOTACION"', 'INTEGER') }}                   as MUERTO_EN_EXPLOTACION,
        {{ std_cast('"ES_HALAL"', 'INTEGER') }}                                as ES_HALAL,
        {{ std_cast('"NO_OT_DESPIECE"', 'VARCHAR') }}                          as NO_OT_DESPIECE,
        {{ std_cast('"PH"', 'NUMBER(38,5)') }}                                 as PH,
        {{ std_cast('"STOCK_CONGELADO"', 'INTEGER') }}                         as STOCK_CONGELADO,
        {{ std_cast('"COD_OPERARIO_CLASIFICACION"', 'VARCHAR') }}              as COD_OPERARIO_CLASIFICACION,
        {{ std_cast('"NOMBRE_OPERARIO_CLASIFICACION"', 'VARCHAR') }}           as NOMBRE_OPERARIO_CLASIFICACION,
        {{ std_cast('"NO_ASIGN_CARACT_PRIORITARIA"', 'INTEGER') }}             as NO_ASIGN_CARACT_PRIORITARIA,
        {{ std_cast('"GTR_CODIGO_ESTADO_LLEGADA"', 'INTEGER') }}               as GTR_CODIGO_ESTADO_LLEGADA,
        {{ std_cast('"GTR_ESTADO_COM_MUERTE"', 'INTEGER') }}                   as GTR_ESTADO_COM_MUERTE,
        {{ std_cast('"GTR_IDENTIFICADOR"', 'VARCHAR') }}                       as GTR_IDENTIFICADOR,
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
    PARTITION BY id_individuo
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
