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
                CUSTOMER_NO, CODE, tec_des_empresa,
                HASH(TIMESTAMP, CUSTOMER_NO, CODE, NAME, NAME_2, ADDRESS, ADDRESS_2, CITY, CONTACT, PHONE_NO, TELEX_NO, SHIPMENT_METHOD_CODE, SHIPPING_AGENT_CODE, PLACE_OF_EXPORT, COUNTRY_REGION_CODE, LAST_DATE_MODIFIED, LOCATION_CODE, FAX_NO, TELEX_ANSWER_BACK, POST_CODE, COUNTY, EMAIL, E_MAIL, HOME_PAGE, TAX_AREA_CODE, TAX_LIABLE, SHIPPING_AGENT_SERVICE_CODE, SERVICE_ZONE_CODE, GLN, COD_RUTA, CONTACT_NO, IDPGND_EXCLUDE_SIG_RATE, GENERICAL_TRANSPORT_COST, VAT_BUS_POSTING_GROUP, HORARIO_ENTREGA, LANGUAGE_CODE, VAT_REGISTRATION_NO, E_MAIL_ENVIO_ALBARAN, OBSERVACIONES_ENVIO, E_MAIL_ENVIO_RECOGIDA, GRUPO_IRPF, CDIGO_IDIOMA, LOAD_TIME, OBSERVACIONES_PICKING, ZONA_TRANSPORTE, NM_SERIE_FACTURA_VENTA_REGISTRADA, LOAD_HR, NM_SERIE_ABONO_VENTA_REGISTRADA, LOAD_PREVIOUS_DAY, EDI_REF_ALBARAN, TRIP_NO, TASAPUNTOVERDE, TEC_DES_EMPRESA, SHIPMENT_ZONE_CODE, TIPO_DESADV, TEC_ID_INGESTA, DL_CLIENTEFINAL, TEC_TS_INGESTA, RETENER_FACTURACION, DL_NUMTIENDAS, TEC_TS_STAGING, ADDITIONAL_INFO, TEC_TS_INTEGRACION_B, DL_NTIENDAS, SIN_RESTRICCION_IDIOMAS, DL_GAMANEGOCIO, IF_LC_ID, IF_SYS_STATE, IF_PAIS_COD_ISO_3, IF_SYS_PROVINCE) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('ship_to_address_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.CUSTOMER_NO is not distinct from s.CUSTOMER_NO and t.CODE is not distinct from s.CODE and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_coopecarn = source('bronze_coopecarn', 'COOPECARN_SHIP_TO_ADDRESS') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_coopecarn.database }}')
              and upper(l.schema)     = upper('{{ src_ref_coopecarn.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_coopecarn.identifier }}')
              and exists (
                  select 1
                  from {{ ref('ship_to_address_batch_coopecarn') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
{% set src_ref_delisano = source('bronze_delisano', 'V_DL_DIRECCIONES_CLIENTE') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_delisano.database }}')
              and upper(l.schema)     = upper('{{ src_ref_delisano.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_delisano.identifier }}')
              and exists (
                  select 1
                  from {{ ref('ship_to_address_batch_delisano') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_SHIP_TO_ADDRESS') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('ship_to_address_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        customer_no, 
        code, 
        name, 
        name_2, 
        address, 
        address_2, 
        city, 
        contact, 
        phone_no, 
        telex_no, 
        shipment_method_code, 
        shipping_agent_code, 
        place_of_export, 
        country_region_code, 
        last_date_modified, 
        location_code, 
        fax_no, 
        telex_answer_back, 
        post_code, 
        county, 
        email, 
        e_mail, 
        home_page, 
        tax_area_code, 
        tax_liable, 
        shipping_agent_service_code, 
        service_zone_code, 
        gln, 
        cod_ruta, 
        contact_no, 
        idpgnd_exclude_sig_rate, 
        generical_transport_cost, 
        vat_bus_posting_group, 
        horario_entrega, 
        language_code, 
        vat_registration_no, 
        e_mail_envio_albaran, 
        observaciones_envio, 
        e_mail_envio_recogida, 
        grupo_irpf, 
        cdigo_idioma, 
        load_time, 
        observaciones_picking, 
        zona_transporte, 
        nm_serie_factura_venta_registrada, 
        load_hr, 
        nm_serie_abono_venta_registrada, 
        load_previous_day, 
        edi_ref_albaran, 
        trip_no, 
        tasapuntoverde, 
        tec_des_empresa, 
        shipment_zone_code, 
        tipo_desadv, 
        tec_id_ingesta, 
        dl_clientefinal, 
        tec_ts_ingesta, 
        retener_facturacion, 
        dl_numtiendas, 
        tec_ts_staging, 
        additional_info, 
        tec_ts_integracion_b, 
        dl_ntiendas, 
        sin_restriccion_idiomas, 
        dl_gamanegocio, 
        if_lc_id, 
        if_sys_state, 
        if_pais_cod_iso_3, 
        if_sys_province, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, CUSTOMER_NO, CODE, NAME, NAME_2, ADDRESS, ADDRESS_2, CITY, CONTACT, PHONE_NO, TELEX_NO, SHIPMENT_METHOD_CODE, SHIPPING_AGENT_CODE, PLACE_OF_EXPORT, COUNTRY_REGION_CODE, LAST_DATE_MODIFIED, LOCATION_CODE, FAX_NO, TELEX_ANSWER_BACK, POST_CODE, COUNTY, EMAIL, E_MAIL, HOME_PAGE, TAX_AREA_CODE, TAX_LIABLE, SHIPPING_AGENT_SERVICE_CODE, SERVICE_ZONE_CODE, GLN, COD_RUTA, CONTACT_NO, IDPGND_EXCLUDE_SIG_RATE, GENERICAL_TRANSPORT_COST, VAT_BUS_POSTING_GROUP, HORARIO_ENTREGA, LANGUAGE_CODE, VAT_REGISTRATION_NO, E_MAIL_ENVIO_ALBARAN, OBSERVACIONES_ENVIO, E_MAIL_ENVIO_RECOGIDA, GRUPO_IRPF, CDIGO_IDIOMA, LOAD_TIME, OBSERVACIONES_PICKING, ZONA_TRANSPORTE, NM_SERIE_FACTURA_VENTA_REGISTRADA, LOAD_HR, NM_SERIE_ABONO_VENTA_REGISTRADA, LOAD_PREVIOUS_DAY, EDI_REF_ALBARAN, TRIP_NO, TASAPUNTOVERDE, TEC_DES_EMPRESA, SHIPMENT_ZONE_CODE, TIPO_DESADV, TEC_ID_INGESTA, DL_CLIENTEFINAL, TEC_TS_INGESTA, RETENER_FACTURACION, DL_NUMTIENDAS, TEC_TS_STAGING, ADDITIONAL_INFO, TEC_TS_INTEGRACION_B, DL_NTIENDAS, SIN_RESTRICCION_IDIOMAS, DL_GAMANEGOCIO, IF_LC_ID, IF_SYS_STATE, IF_PAIS_COD_ISO_3, IF_SYS_PROVINCE) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('ship_to_address_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.CUSTOMER_NO is not distinct from s.CUSTOMER_NO and t.CODE is not distinct from s.CODE and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.CUSTOMER_NO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
