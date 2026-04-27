{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_LIN_PARTIDA') %}
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
        {{ std_cast('"NO_LINEA"', 'INTEGER') }}                                as NO_LINEA,
        {{ std_cast('"ORDEN_SACRIFICIO"', 'INTEGER') }}                        as ORDEN_SACRIFICIO,
        {{ std_cast('"ORDEN_SACRIFICIO_2"', 'INTEGER') }}                      as ORDEN_SACRIFICIO_2,
        {{ std_cast('"ITEM_NO_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"RAZA"', 'VARCHAR') }}                                    as RAZA,
        {{ std_cast('"SEXO"', 'INTEGER') }}                                    as SEXO,
        {{ std_cast('"COD_CATEGORIA_PESO"', 'VARCHAR') }}                      as COD_CATEGORIA_PESO,
        {{ std_cast('"NO_PRODUCTO_RESULTADO"', 'VARCHAR') }}                   as NO_PRODUCTO_RESULTADO,
        {{ std_cast('"GRAS"', 'VARCHAR') }}                                    as GRAS,
        {{ std_cast('"CERTIFICADO"', 'VARCHAR') }}                             as CERTIFICADO,
        {{ std_cast('"PESO_1_2_CAN_1"', 'NUMBER(38,5)') }}                     as PESO_1_2_CAN_1,
        {{ std_cast('"PESO_1_2_CAN_2"', 'NUMBER(38,5)') }}                     as PESO_1_2_CAN_2,
        {{ std_cast('"TARA"', 'NUMBER(38,5)') }}                               as TARA,
        {{ std_cast('"MERMA"', 'NUMBER(38,5)') }}                              as MERMA,
        {{ std_cast('"PESO_NETO"', 'NUMBER(38,5)') }}                          as PESO_NETO,
        {{ std_cast('"PESO_PIEL"', 'NUMBER(38,5)') }}                          as PESO_PIEL,
        {{ std_cast('"NO_CROTALO"', 'VARCHAR') }}                              as NO_CROTALO,
        {{ std_cast('"NO_CROTALO_GUIA"', 'VARCHAR') }}                         as NO_CROTALO_GUIA,
        {{ std_cast('"MACHO"', 'INTEGER') }}                                   as MACHO,
        {{ std_cast('"PRECIO"', 'NUMBER(38,5)') }}                             as PRECIO,
        {{ std_cast('"NO_LOTE"', 'VARCHAR') }}                                 as NO_LOTE,
        {{ std_cast('"PESO_BRUTO"', 'NUMBER(38,5)') }}                         as PESO_BRUTO,
        {{ std_cast('"DECOMISO"', 'INTEGER') }}                                as DECOMISO,
        {{ std_cast('"ASICI_JAMON"', 'VARCHAR') }}                             as ASICI_JAMON,
        {{ std_cast('"ASICI_PALETA"', 'VARCHAR') }}                            as ASICI_PALETA,
        {{ std_cast('"CANAL_DESCLASIFICADA"', 'INTEGER') }}                    as CANAL_DESCLASIFICADA,
        {{ std_cast('"_OREO"', 'NUMBER(38,5)') }}                              as OREO,
        {{ std_cast('"PESO_OREO"', 'NUMBER(38,5)') }}                          as PESO_OREO,
        {{ std_cast('"COD_CONFORMACION"', 'VARCHAR') }}                        as COD_CONFORMACION,
        {{ std_cast('"COD_PENALIZACION"', 'VARCHAR') }}                        as COD_PENALIZACION,
        {{ std_cast('"ERROR_CLASIFICACION"', 'INTEGER') }}                     as ERROR_CLASIFICACION,
        {{ std_cast('"TEXTO_ERROR_CLASIFICACION"', 'VARCHAR') }}               as TEXTO_ERROR_CLASIFICACION,
        {{ std_cast('"HISTORICO"', 'INTEGER') }}                               as HISTORICO,
        {{ std_cast('"NO_PESADA"', 'VARCHAR') }}                               as NO_PESADA,
        {{ std_cast('"PRECIO_PENALIZACION"', 'NUMBER(38,5)') }}                as PRECIO_PENALIZACION,
        {{ std_cast('"UNIDAD_MEDIDA_PENALIZACION"', 'VARCHAR') }}              as UNIDAD_MEDIDA_PENALIZACION,
        {{ std_cast('"IMPORTE_PENALIZACION"', 'NUMBER(38,5)') }}               as IMPORTE_PENALIZACION,
        {{ std_cast('"GRADO_CONFORMACION"', 'VARCHAR') }}                      as GRADO_CONFORMACION,
        {{ std_cast('"COD_ENGRASAMIENTO"', 'VARCHAR') }}                       as COD_ENGRASAMIENTO,
        {{ std_cast('"GRADO_GRASA"', 'VARCHAR') }}                             as GRADO_GRASA,
        {{ std_cast('"CATEGORIA_EDAT"', 'VARCHAR') }}                          as CATEGORIA_EDAT,
        {{ std_cast('"GRADO_EDAT"', 'VARCHAR') }}                              as GRADO_EDAT,
        {{ std_cast('"NO_DIB"', 'VARCHAR') }}                                  as NO_DIB,
        {{ std_cast('"COD_PENALIZACION_2"', 'VARCHAR') }}                      as COD_PENALIZACION_2,
        {{ std_cast('"PRECIO_PENALIZACION_2"', 'NUMBER(38,5)') }}              as PRECIO_PENALIZACION_2,
        {{ std_cast('"IMPORTE_PENALIZACION_2"', 'NUMBER(38,5)') }}             as IMPORTE_PENALIZACION_2,
        {{ std_cast('"VARIAN_CODE"', 'VARCHAR') }}                             as VARIAN_CODE,
        {{ std_cast('"COD_RAZA"', 'VARCHAR') }}                                as COD_RAZA,
        {{ std_cast('"FILTRO_PESO_MINIMO"', 'NUMBER(38,5)') }}                 as FILTRO_PESO_MINIMO,
        {{ std_cast('"FILTRO_PESO_MAXIMO"', 'NUMBER(38,5)') }}                 as FILTRO_PESO_MAXIMO,
        {{ std_cast('"NOMBRE_PROVEEDOR"', 'VARCHAR') }}                        as NOMBRE_PROVEEDOR,
        {{ std_cast('"FECHA_SACRIFICIO"', 'TIMESTAMP_NTZ') }}                  as FECHA_SACRIFICIO,
        {{ std_cast('"FECHA_NACIMIENTO"', 'TIMESTAMP_NTZ') }}                  as FECHA_NACIMIENTO,
        {{ std_cast('"NO_CUADRA"', 'VARCHAR') }}                               as NO_CUADRA,
        {{ std_cast('"FECHA_DESCARGA"', 'TIMESTAMP_NTZ') }}                    as FECHA_DESCARGA,
        {{ std_cast('"HORA_DESCARGA"', 'TIMESTAMP_NTZ') }}                     as HORA_DESCARGA,
        {{ std_cast('"NO_APLICAR_PENALIZACIONES"', 'INTEGER') }}               as NO_APLICAR_PENALIZACIONES,
        {{ std_cast('"NO_OT_MATADERO"', 'VARCHAR') }}                          as NO_OT_MATADERO,
        {{ std_cast('"LOCATION_CODE"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"ID_INDIVIDUO"', 'VARCHAR') }}                            as ID_INDIVIDUO,
        {{ std_cast('"ESTADO_INDIVIDUO"', 'VARCHAR') }}                        as ESTADO_INDIVIDUO,
        {{ std_cast('"BLOQUEADO"', 'INTEGER') }}                               as BLOQUEADO,
        {{ std_cast('"COD_ESPECIE"', 'VARCHAR') }}                             as COD_ESPECIE,
        {{ std_cast('"SSCC_1"', 'VARCHAR') }}                                  as SSCC_1,
        {{ std_cast('"SSCC_2"', 'VARCHAR') }}                                  as SSCC_2,
        {{ std_cast('"TIPO_PULIDO"', 'VARCHAR') }}                             as TIPO_PULIDO,
        {{ std_cast('"TIPO_DECOMISO"', 'INTEGER') }}                           as TIPO_DECOMISO,
        {{ std_cast('"NO_GUIA_SANITARIA"', 'VARCHAR') }}                       as NO_GUIA_SANITARIA,
        {{ std_cast('"NO_GRANJA"', 'VARCHAR') }}                               as NO_GRANJA,
        {{ std_cast('"NO_SERIE_GUIA"', 'VARCHAR') }}                           as NO_SERIE_GUIA,
        {{ std_cast('"MUERTO_EN_EXPLOTACION"', 'INTEGER') }}                   as MUERTO_EN_EXPLOTACION,
        {{ std_cast('"_MAGRO"', 'NUMBER(38,5)') }}                             as MAGRO,
        {{ std_cast('"_DESCUENTO_ADICIONAL"', 'NUMBER(38,5)') }}               as DESCUENTO_ADICIONAL,
        {{ std_cast('"PESO_DESCUENTO_ADICIONAL"', 'NUMBER(38,5)') }}           as PESO_DESCUENTO_ADICIONAL,
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
    PARTITION BY no_partida, no_linea
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
