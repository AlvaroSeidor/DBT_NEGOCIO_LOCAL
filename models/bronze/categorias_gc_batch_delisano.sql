{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_DL_CATEGORIAS_GC') %}
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
        {{ std_cast('"timestamp"', 'INTEGER') }}                               as TIMESTAMP,
        {{ std_cast('"CodClasif"', 'VARCHAR') }}                               as CODCLASIF,
        {{ std_cast('"Gama"', 'VARCHAR') }}                                    as GAMA,
        {{ std_cast('"NomGama"', 'VARCHAR') }}                                 as NOMGAMA,
        {{ std_cast('"Categoria"', 'VARCHAR') }}                               as CATEGORIA,
        {{ std_cast('"NomCategoria"', 'VARCHAR') }}                            as NOMCATEGORIA,
        {{ std_cast('"Familia"', 'VARCHAR') }}                                 as FAMILIA,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'DLS'                                                                     as tec_des_cod_siglas
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src