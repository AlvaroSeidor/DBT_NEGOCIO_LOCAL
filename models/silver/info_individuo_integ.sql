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
                ID_INDIVIDUO, tec_des_empresa,
                HASH(TIMESTAMP, ID_INDIVIDUO, NO_DIB, NO_CROTAL, NO_CROTAL_GUIA, NO_DIB_MADRE, NO_GUIA, NO_OT_MATADERO, ORDEN_SACRIFICIO, FECHA_SACRIFICIO, PESO_1_2_CAN_1, PESO_1_2_CAN_2, NO_PARTIDA, NO_LINEA_PARTIDA, SSCC_1, SSCC_2, NO_LOTE, HORA_SACRIFICIO, SERIE_GUIA, PESO_BRUTO, POBLACION_NACIMIENTO, COD_PAIS_REGION_NACIMIENTO, CODIGO_POSTAL_NACIMIENTO, PROVINCIA_NACIMIENTO, FECHA_NACIMIENTO, COD_PAIS_ENGORDE, COD_PAIS_ENGORDE_2, COD_PAIS_ENGORDE_3, COD_PAIS_ENGORDE_4, POBLACION_SACRIFICIO, COD_PAIS_REGION_SACRIFICIO, CODIGO_POSTAL_SACRIFICIO, PROVINCIA_SACRIFICIO, NO_CUADRA, FECHA_DESCARGA, HORA_DESCARGA, COD_RAZA, SEXO, RAZA, ITEM_NO, VARIAN_CODE, DESCRIPCION, DESCRIPCION_2, ESTADO_INDIVIDUO, BLOQUEADO, FECHA_CREACION, HORA_CREACION, COD_CONFORMACION, GRADO_CONFORMACION, COD_ENGRASAMIENTO, GRADO_GRASA, CASTRADO, HA_PARIDO, SACRIFICADO, NUM_ETIQUETAS_IMP, COD_ESPECIE, ESTADO_DATOS, MENSAJE_ESTADO_DATOS, LAST_UPDATE_CARACT, MUERTO_EN_EXPLOTACION, ES_HALAL, NO_OT_DESPIECE, PH, STOCK_CONGELADO, COD_OPERARIO_CLASIFICACION, NOMBRE_OPERARIO_CLASIFICACION, NO_ASIGN_CARACT_PRIORITARIA, GTR_CODIGO_ESTADO_LLEGADA, GTR_ESTADO_COM_MUERTE, GTR_IDENTIFICADOR, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('info_individuo_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.ID_INDIVIDUO is not distinct from s.ID_INDIVIDUO and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_INFO_INDIVIDUO') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('info_individuo_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        id_individuo, 
        no_dib, 
        no_crotal, 
        no_crotal_guia, 
        no_dib_madre, 
        no_guia, 
        no_ot_matadero, 
        orden_sacrificio, 
        fecha_sacrificio, 
        peso_1_2_can_1, 
        peso_1_2_can_2, 
        no_partida, 
        no_linea_partida, 
        sscc_1, 
        sscc_2, 
        no_lote, 
        hora_sacrificio, 
        serie_guia, 
        peso_bruto, 
        poblacion_nacimiento, 
        cod_pais_region_nacimiento, 
        codigo_postal_nacimiento, 
        provincia_nacimiento, 
        fecha_nacimiento, 
        cod_pais_engorde, 
        cod_pais_engorde_2, 
        cod_pais_engorde_3, 
        cod_pais_engorde_4, 
        poblacion_sacrificio, 
        cod_pais_region_sacrificio, 
        codigo_postal_sacrificio, 
        provincia_sacrificio, 
        no_cuadra, 
        fecha_descarga, 
        hora_descarga, 
        cod_raza, 
        sexo, 
        raza, 
        item_no, 
        varian_code, 
        descripcion, 
        descripcion_2, 
        estado_individuo, 
        bloqueado, 
        fecha_creacion, 
        hora_creacion, 
        cod_conformacion, 
        grado_conformacion, 
        cod_engrasamiento, 
        grado_grasa, 
        castrado, 
        ha_parido, 
        sacrificado, 
        num_etiquetas_imp, 
        cod_especie, 
        estado_datos, 
        mensaje_estado_datos, 
        last_update_caract, 
        muerto_en_explotacion, 
        es_halal, 
        no_ot_despiece, 
        ph, 
        stock_congelado, 
        cod_operario_clasificacion, 
        nombre_operario_clasificacion, 
        no_asign_caract_prioritaria, 
        gtr_codigo_estado_llegada, 
        gtr_estado_com_muerte, 
        gtr_identificador, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, ID_INDIVIDUO, NO_DIB, NO_CROTAL, NO_CROTAL_GUIA, NO_DIB_MADRE, NO_GUIA, NO_OT_MATADERO, ORDEN_SACRIFICIO, FECHA_SACRIFICIO, PESO_1_2_CAN_1, PESO_1_2_CAN_2, NO_PARTIDA, NO_LINEA_PARTIDA, SSCC_1, SSCC_2, NO_LOTE, HORA_SACRIFICIO, SERIE_GUIA, PESO_BRUTO, POBLACION_NACIMIENTO, COD_PAIS_REGION_NACIMIENTO, CODIGO_POSTAL_NACIMIENTO, PROVINCIA_NACIMIENTO, FECHA_NACIMIENTO, COD_PAIS_ENGORDE, COD_PAIS_ENGORDE_2, COD_PAIS_ENGORDE_3, COD_PAIS_ENGORDE_4, POBLACION_SACRIFICIO, COD_PAIS_REGION_SACRIFICIO, CODIGO_POSTAL_SACRIFICIO, PROVINCIA_SACRIFICIO, NO_CUADRA, FECHA_DESCARGA, HORA_DESCARGA, COD_RAZA, SEXO, RAZA, ITEM_NO, VARIAN_CODE, DESCRIPCION, DESCRIPCION_2, ESTADO_INDIVIDUO, BLOQUEADO, FECHA_CREACION, HORA_CREACION, COD_CONFORMACION, GRADO_CONFORMACION, COD_ENGRASAMIENTO, GRADO_GRASA, CASTRADO, HA_PARIDO, SACRIFICADO, NUM_ETIQUETAS_IMP, COD_ESPECIE, ESTADO_DATOS, MENSAJE_ESTADO_DATOS, LAST_UPDATE_CARACT, MUERTO_EN_EXPLOTACION, ES_HALAL, NO_OT_DESPIECE, PH, STOCK_CONGELADO, COD_OPERARIO_CLASIFICACION, NOMBRE_OPERARIO_CLASIFICACION, NO_ASIGN_CARACT_PRIORITARIA, GTR_CODIGO_ESTADO_LLEGADA, GTR_ESTADO_COM_MUERTE, GTR_IDENTIFICADOR, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('info_individuo_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.ID_INDIVIDUO is not distinct from s.ID_INDIVIDUO and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.ID_INDIVIDUO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
