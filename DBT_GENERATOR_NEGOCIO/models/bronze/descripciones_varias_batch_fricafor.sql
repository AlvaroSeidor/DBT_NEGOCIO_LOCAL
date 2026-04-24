{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_DESCRIPCIONES_VARIAS') %}
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
        {{ std_cast('"IDTABLA"', 'INTEGER') }}                                 as IDTABLA,
        {{ std_cast('"CODIGO"', 'VARCHAR') }}                                  as CODIGO,
        {{ std_cast('"CODIGO_2"', 'VARCHAR') }}                                as CODIGO_2,
        {{ std_cast('"CODIGO_3"', 'VARCHAR') }}                                as CODIGO_3,
        {{ std_cast('"CODIGO_4"', 'VARCHAR') }}                                as CODIGO_4,
        {{ std_cast('"IDIOMA"', 'VARCHAR') }}                                  as IDIOMA,
        {{ std_cast('"FECHA"', 'TIMESTAMP_NTZ') }}                             as FECHA,
        {{ std_cast('"NO_LINEA"', 'INTEGER') }}                                as NO_LINEA,
        {{ std_cast('"DESCRIPCION"', 'VARCHAR') }}                             as DESCRIPCION,
        {{ std_cast('"NOMBRE"', 'VARCHAR') }}                                  as NOMBRE,
        {{ std_cast('"TIPO"', 'INTEGER') }}                                    as TIPO,
        {{ std_cast('"QUANTITY"', 'NUMBER(38,5)') }}                           as QUANTITY,
        {{ std_cast('"MAX_QUANTITY"', 'NUMBER(38,5)') }}                       as MAX_QUANTITY,
        {{ std_cast('"UNIT_OF_MEASURE_CODE"', 'VARCHAR') }}                    as UNIT_OF_MEASURE_CODE,
        {{ std_cast('"CODIGO_5"', 'VARCHAR') }}                                as CODIGO_5,
        {{ std_cast('"COLUMNA_1"', 'VARCHAR') }}                               as COLUMNA_1,
        {{ std_cast('"COLUMNA_2"', 'VARCHAR') }}                               as COLUMNA_2,
        {{ std_cast('"COLUMNA_3"', 'VARCHAR') }}                               as COLUMNA_3,
        {{ std_cast('"DECIMAL_1"', 'NUMBER(38,5)') }}                          as DECIMAL_1,
        {{ std_cast('"DECIMAL_2"', 'NUMBER(38,5)') }}                          as DECIMAL_2,
        {{ std_cast('"DECIMAL_3"', 'NUMBER(38,5)') }}                          as DECIMAL_3,
        {{ std_cast('"DECIMAL_4"', 'NUMBER(38,5)') }}                          as DECIMAL_4,
        {{ std_cast('"FECHA_2"', 'TIMESTAMP_NTZ') }}                           as FECHA_2,
        {{ std_cast('"PVP_ETIQUETA"', 'NUMBER(38,5)') }}                       as PVP_ETIQUETA,
        {{ std_cast('"RELACION_MEDIDA"', 'INTEGER') }}                         as RELACION_MEDIDA,
        {{ std_cast('"BOOLEAN_1"', 'INTEGER') }}                               as BOOLEAN_1,
        {{ std_cast('"TEXTO_1"', 'VARCHAR') }}                                 as TEXTO_1,
        {{ std_cast('"TEXTO_2"', 'VARCHAR') }}                                 as TEXTO_2,
        {{ std_cast('"PRECIO"', 'NUMBER(38,5)') }}                             as PRECIO,
        {{ std_cast('"DESCUENTO"', 'NUMBER(38,5)') }}                          as DESCUENTO,
        {{ std_cast('"BLOQUEADO"', 'INTEGER') }}                               as BLOQUEADO,
        {{ std_cast('"DATEFORMULA_1"', 'VARCHAR') }}                           as DATEFORMULA_1,
        {{ std_cast('"FECHA_3"', 'TIMESTAMP_NTZ') }}                           as FECHA_3,
        {{ std_cast('"PROCESO"', 'INTEGER') }}                                 as PROCESO,
        {{ std_cast('"COD_PRODUCTO"', 'VARCHAR') }}                            as COD_PRODUCTO,
        {{ std_cast('"BOOLEAN_2"', 'INTEGER') }}                               as BOOLEAN_2,
        {{ std_cast('"BOOLEAN_3"', 'INTEGER') }}                               as BOOLEAN_3,
        {{ std_cast('"BOOLEAN_4"', 'INTEGER') }}                               as BOOLEAN_4,
        {{ std_cast('"BOOLEAN_5"', 'INTEGER') }}                               as BOOLEAN_5,
        {{ std_cast('"BOOLEAN_6"', 'INTEGER') }}                               as BOOLEAN_6,
        {{ std_cast('"VARIANT_CODE"', 'VARCHAR') }}                            as VARIANT_CODE,
        {{ std_cast('"FECHA_1"', 'TIMESTAMP_NTZ') }}                           as FECHA_1,
        {{ std_cast('"FECHA_4"', 'TIMESTAMP_NTZ') }}                           as FECHA_4,
        {{ std_cast('"FECHA_5"', 'TIMESTAMP_NTZ') }}                           as FECHA_5,
        {{ std_cast('"FECHA_6"', 'TIMESTAMP_NTZ') }}                           as FECHA_6,
        {{ std_cast('"FECHA_7"', 'TIMESTAMP_NTZ') }}                           as FECHA_7,
        {{ std_cast('"FECHA_8"', 'TIMESTAMP_NTZ') }}                           as FECHA_8,
        {{ std_cast('"FECHA_9"', 'TIMESTAMP_NTZ') }}                           as FECHA_9,
        {{ std_cast('"DECIMAL_5"', 'NUMBER(38,5)') }}                          as DECIMAL_5,
        {{ std_cast('"DECIMAL_6"', 'NUMBER(38,5)') }}                          as DECIMAL_6,
        {{ std_cast('"DECIMAL_7"', 'NUMBER(38,5)') }}                          as DECIMAL_7,
        {{ std_cast('"DECIMAL_8"', 'NUMBER(38,5)') }}                          as DECIMAL_8,
        {{ std_cast('"DECIMAL_9"', 'NUMBER(38,5)') }}                          as DECIMAL_9,
        {{ std_cast('"COLUMNA_4"', 'VARCHAR') }}                               as COLUMNA_4,
        {{ std_cast('"COLUMNA_5"', 'VARCHAR') }}                               as COLUMNA_5,
        {{ std_cast('"COLUMNA_6"', 'VARCHAR') }}                               as COLUMNA_6,
        {{ std_cast('"COLUMNA_7"', 'VARCHAR') }}                               as COLUMNA_7,
        {{ std_cast('"COLUMNA_8"', 'VARCHAR') }}                               as COLUMNA_8,
        {{ std_cast('"COLUMNA_9"', 'VARCHAR') }}                               as COLUMNA_9,
        {{ std_cast('"BOOLEAN_7"', 'INTEGER') }}                               as BOOLEAN_7,
        {{ std_cast('"BOOLEAN_8"', 'INTEGER') }}                               as BOOLEAN_8,
        {{ std_cast('"BOOLEAN_9"', 'INTEGER') }}                               as BOOLEAN_9,
        {{ std_cast('"ENTERO_1"', 'INTEGER') }}                                as ENTERO_1,
        {{ std_cast('"ENTERO_2"', 'INTEGER') }}                                as ENTERO_2,
        {{ std_cast('"ENTERO_3"', 'INTEGER') }}                                as ENTERO_3,
        {{ std_cast('"ENTERO_4"', 'INTEGER') }}                                as ENTERO_4,
        {{ std_cast('"ENTERO_5"', 'INTEGER') }}                                as ENTERO_5,
        {{ std_cast('"ENTERO_6"', 'INTEGER') }}                                as ENTERO_6,
        {{ std_cast('"ENTERO_7"', 'INTEGER') }}                                as ENTERO_7,
        {{ std_cast('"ENTERO_8"', 'INTEGER') }}                                as ENTERO_8,
        {{ std_cast('"ENTERO_9"', 'INTEGER') }}                                as ENTERO_9,
        {{ std_cast('"CODIGO_6"', 'VARCHAR') }}                                as CODIGO_6,
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