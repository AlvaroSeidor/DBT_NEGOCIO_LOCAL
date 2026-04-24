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
                HASH(TIMESTAMP, CODE, NAME, COMMISSION, PRIVACY_BLOCKED, GLOBAL_DIMENSION_1_CODE, GLOBAL_DIMENSION_2_CODE, E_MAIL, PHONE_NO, JOB_TITLE, SEARCH_E_MAIL, E_MAIL_2, COMPRADOR, LOCATION_CODE, VENDEDOR, NOMBRE_2, JEFE_DE_VENTAS, ALIAS, JEFE_DE_ZONA, DIRECCION, IVA, IRPF, DIRECCION_2, CODIGO_POSTAL, NO_PROVEEDOR, POBLACION, FECHA_ULT_VOLCADO_TARIFA, HORA_ULT_VOLCADO_TARIFA, PROVINCIA, FECHA_TARIFA_VOLCADA, PAIS, BALANCE_COMISSION_ACCOUNT, TELEFONO_2, EXPENSES_COMISSION_ACCOUNT, COD_IDIOMA, CALC_COMISION_X_FECHA_COBRO, CIF_NIF, TEC_DES_EMPRESA, COMMISSION_GROUP_CODE, PAY_TO_VENDOR_NO, TEC_ID_INGESTA, TEC_TS_INGESTA, COMMISSION_ACCOUNT, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('salesperson_purchaser_batch') }}
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
            {% set src_ref = source('bronze_' ~ a, 'COOPECARN_SALESPERSON_PURCHASER') %}
            -- Sella ÚNICAMENTE el batch (tec_id_ingesta) que está procesando la view {{ ref('salesperson_purchaser_batch_' ~ a) }}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref.database }}')
              and upper(l.schema)     = upper('{{ src_ref.schema }}')
              and upper(l.table_name) = upper('{{ src_ref.identifier }}')
              and exists (
                  select 1
                  from {{ ref('salesperson_purchaser_batch_' ~ a) }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
        {% endfor %}"
    ]
) }}

with source as (
    select
        timestamp, 
        code, 
        name, 
        commission, 
        privacy_blocked, 
        global_dimension_1_code, 
        global_dimension_2_code, 
        e_mail, 
        phone_no, 
        job_title, 
        search_e_mail, 
        e_mail_2, 
        comprador, 
        location_code, 
        vendedor, 
        nombre_2, 
        jefe_de_ventas, 
        alias, 
        jefe_de_zona, 
        direccion, 
        iva, 
        irpf, 
        direccion_2, 
        codigo_postal, 
        no_proveedor, 
        poblacion, 
        fecha_ult_volcado_tarifa, 
        hora_ult_volcado_tarifa, 
        provincia, 
        fecha_tarifa_volcada, 
        pais, 
        balance_comission_account, 
        telefono_2, 
        expenses_comission_account, 
        cod_idioma, 
        calc_comision_x_fecha_cobro, 
        cif_nif, 
        tec_des_empresa, 
        commission_group_code, 
        pay_to_vendor_no, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        commission_account, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, CODE, NAME, COMMISSION, PRIVACY_BLOCKED, GLOBAL_DIMENSION_1_CODE, GLOBAL_DIMENSION_2_CODE, E_MAIL, PHONE_NO, JOB_TITLE, SEARCH_E_MAIL, E_MAIL_2, COMPRADOR, LOCATION_CODE, VENDEDOR, NOMBRE_2, JEFE_DE_VENTAS, ALIAS, JEFE_DE_ZONA, DIRECCION, IVA, IRPF, DIRECCION_2, CODIGO_POSTAL, NO_PROVEEDOR, POBLACION, FECHA_ULT_VOLCADO_TARIFA, HORA_ULT_VOLCADO_TARIFA, PROVINCIA, FECHA_TARIFA_VOLCADA, PAIS, BALANCE_COMISSION_ACCOUNT, TELEFONO_2, EXPENSES_COMISSION_ACCOUNT, COD_IDIOMA, CALC_COMISION_X_FECHA_COBRO, CIF_NIF, TEC_DES_EMPRESA, COMMISSION_GROUP_CODE, PAY_TO_VENDOR_NO, TEC_ID_INGESTA, TEC_TS_INGESTA, COMMISSION_ACCOUNT, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('salesperson_purchaser_batch') }}
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
