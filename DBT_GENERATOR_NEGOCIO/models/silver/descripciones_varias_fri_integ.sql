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
                tec_des_empresa,
                HASH(TIMESTAMP, IDTABLA, CODIGO, CODIGO_2, CODIGO_3, IDIOMA, FECHA, NO_LINEA, DESCRIPCION, COBERTURA, CREDITO, GRUPO_RIESGO, FECHA_REVISION, FECHA_FINAL, RIESGO_EMPRESA, MOTIVO_DENEGACION, MOTIVO_SOLVENCIA, PARTICULARIDAD_1, PARTICULARIDAD_2, PARTICULARIDAD_3, PARTICULARIDAD_4, PARTICULARIDAD_5, COLUMNA, COLUMNA_2, COLUMNA_3, DECIMAL, DECIMAL_2, DECIMAL_3, FECHA_2, FECHA_3, BOOLEAN, BOOLEAN_2, BOOLEAN_3, TEXTO, TEXTO_2, TEXTO_3, ENTERO, ENTERO_2, ENTERO_3, NOMBRE, NOMBRE_2, NOMBRE_3, ORIGEN, COLOR, CODIGO_4, DATEFORMULA, DATEFORMULA_2, CODIGO_5, DECIMAL_4, DATETIME, SEXO, BOOLEAN_4, SERIE, NUMERO_INICIAL, NUMERO_FINAL, IDENTIFICADOR_DOCUMENTO, DIGITO_CONTROL_IDEN_DOC, NUMERACION_BLOQUEADA, ULTIMO_NUMERO_USADO, NUMERO_DE_AVISO, IMPORTE, BANCO, COBRO_PAGO, LIQUIDADO, CODIGO_GASTO, FECHA_INICIAL_SII, FECHA_FINAL_SII, RUTA_FICHERO, DATETIME_2, TIPO_CONTROL, TIPO_DOCUMENTO, DURACION_SOLICITUD, DURACION_RESPUESTA, CODIGO_6, CODIGO_7, TEXTO_4, TEXTO_5, TEXTO_6, CODIGO_8, TIPO_TRATAMIENTO, CODIGO_9, RESULTADO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('descripciones_varias_fri_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and 1=1
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
        "{% set log_ref = source('support_param', 'INSERT_LOGS') %}{% set asociados = var('empresas', []) %}{% for a in asociados %}
            {% set src_ref = source('bronze_' ~ a, 'FRICAFOR_DESCRIPCIONES_VARIAS_FRI') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('descripciones_varias_fri_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('descripciones_varias_fri_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        idtabla, 
        codigo, 
        codigo_2, 
        codigo_3, 
        idioma, 
        fecha, 
        no_linea, 
        descripcion, 
        cobertura, 
        credito, 
        grupo_riesgo, 
        fecha_revision, 
        fecha_final, 
        riesgo_empresa, 
        motivo_denegacion, 
        motivo_solvencia, 
        particularidad_1, 
        particularidad_2, 
        particularidad_3, 
        particularidad_4, 
        particularidad_5, 
        columna, 
        columna_2, 
        columna_3, 
        decimal, 
        decimal_2, 
        decimal_3, 
        fecha_2, 
        fecha_3, 
        boolean, 
        boolean_2, 
        boolean_3, 
        texto, 
        texto_2, 
        texto_3, 
        entero, 
        entero_2, 
        entero_3, 
        nombre, 
        nombre_2, 
        nombre_3, 
        origen, 
        color, 
        codigo_4, 
        dateformula, 
        dateformula_2, 
        codigo_5, 
        decimal_4, 
        datetime, 
        sexo, 
        boolean_4, 
        serie, 
        numero_inicial, 
        numero_final, 
        identificador_documento, 
        digito_control_iden_doc, 
        numeracion_bloqueada, 
        ultimo_numero_usado, 
        numero_de_aviso, 
        importe, 
        banco, 
        cobro_pago, 
        liquidado, 
        codigo_gasto, 
        fecha_inicial_sii, 
        fecha_final_sii, 
        ruta_fichero, 
        datetime_2, 
        tipo_control, 
        tipo_documento, 
        duracion_solicitud, 
        duracion_respuesta, 
        codigo_6, 
        codigo_7, 
        texto_4, 
        texto_5, 
        texto_6, 
        codigo_8, 
        tipo_tratamiento, 
        codigo_9, 
        resultado, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, IDTABLA, CODIGO, CODIGO_2, CODIGO_3, IDIOMA, FECHA, NO_LINEA, DESCRIPCION, COBERTURA, CREDITO, GRUPO_RIESGO, FECHA_REVISION, FECHA_FINAL, RIESGO_EMPRESA, MOTIVO_DENEGACION, MOTIVO_SOLVENCIA, PARTICULARIDAD_1, PARTICULARIDAD_2, PARTICULARIDAD_3, PARTICULARIDAD_4, PARTICULARIDAD_5, COLUMNA, COLUMNA_2, COLUMNA_3, DECIMAL, DECIMAL_2, DECIMAL_3, FECHA_2, FECHA_3, BOOLEAN, BOOLEAN_2, BOOLEAN_3, TEXTO, TEXTO_2, TEXTO_3, ENTERO, ENTERO_2, ENTERO_3, NOMBRE, NOMBRE_2, NOMBRE_3, ORIGEN, COLOR, CODIGO_4, DATEFORMULA, DATEFORMULA_2, CODIGO_5, DECIMAL_4, DATETIME, SEXO, BOOLEAN_4, SERIE, NUMERO_INICIAL, NUMERO_FINAL, IDENTIFICADOR_DOCUMENTO, DIGITO_CONTROL_IDEN_DOC, NUMERACION_BLOQUEADA, ULTIMO_NUMERO_USADO, NUMERO_DE_AVISO, IMPORTE, BANCO, COBRO_PAGO, LIQUIDADO, CODIGO_GASTO, FECHA_INICIAL_SII, FECHA_FINAL_SII, RUTA_FICHERO, DATETIME_2, TIPO_CONTROL, TIPO_DOCUMENTO, DURACION_SOLICITUD, DURACION_RESPUESTA, CODIGO_6, CODIGO_7, TEXTO_4, TEXTO_5, TEXTO_6, CODIGO_8, TIPO_TRATAMIENTO, CODIGO_9, RESULTADO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('descripciones_varias_fri_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  1=1
    and t.tec_cod_vigencia    = 1
where 1=1
    or t.tec_hash <> s.tec_hash
{% endif %}
