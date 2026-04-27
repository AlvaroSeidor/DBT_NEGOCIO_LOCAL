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
                NO, tec_des_empresa,
                HASH(TIMESTAMP, NO, INCIDENCE_DATE, DESCRIPTION, DETECTION_RESPONSIBLE, DETECTION_DATE, CLOSING_RESPONSIBLE, CLOSING_DATE, INCIDENCE_TYPE, INCIDENCE_CODE, INCIDENCE_NAME, STATUS, CREATION_DATE, ITEM_NO, VARIANT_CODE, LOT_NO, QUANTITY, DOCUMENT_NO, TYPE, CLASSIFICATION, DESTINO, QUALITY_CATEGORY, DOCUMENT_LINE_NO, CUSTOMER_ITEM_CODE, CENTER_CODE, LOGISTIC_BLOCK, SHOOP_NUMBER, ADRESS, POST_CODE, CITY, COUNTRY, PHONE_NO, COLLECTION_PROCESSING, COLLECTION_DATE, COLLECTION_RESULT, PICTURES_LINK, COLLECTION_RECEPTION_DATE, INCIDENCE_RECEPTION_DATE, COLLECTION_NOTES, USER_ID, INCIDENCE_SUB_TYPE, MURANO_CODE, RESPONSIBILITY_CENTER, EXPEDIENT, INCIDENCE_NUMBER, INCIDENCE_SOURCE, MEAT_SUPLIER, UNIT_OF_MEASURE, COMPLAINTS_NUMBER, LINK_PICTURES, PRODUCT_LINE, SUPLIER_LOT_NO, ITEM_DESCRIPTION, INPUT_CHANEL, R, COMPLAINT_TYPE, INCIDENCE_SUB_SUB_TYPE, CLAIM_CODE, SIN_USO, DESCRRIPTION_CUSTOMER, MEAT_SUPPLIER, WORK_SHIFT, SHOP_NAME, ZONE, ID_STANDARD_TEXT, ID_STD_COMPLAINT_CODE, LOT_QUANTITY, INCIDENCE_SUB_SUB_TYPE_2, REGISTRO, INCIDENCE_SUB_TYPE_COPY, ID_STD_COMPLAINT_CODE_COPY, PRODUCTION_DATE, PRODUCTION_TIME, DETECTOR_DANSENSOR, INTERN_VENDOR, INTERN_VENDOR_NAME, PACKAGING_VENDOR_NO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('incidence_quality_header_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.NO is not distinct from s.NO and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_coopecarn = source('bronze_coopecarn', 'COOPECARN_INCIDENCE_QUALITY_HEADER') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_coopecarn.database }}')
              and upper(l.schema)     = upper('{{ src_ref_coopecarn.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_coopecarn.identifier }}')
              and exists (
                  select 1
                  from {{ ref('incidence_quality_header_batch_coopecarn') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        no, 
        incidence_date, 
        description, 
        detection_responsible, 
        detection_date, 
        closing_responsible, 
        closing_date, 
        incidence_type, 
        incidence_code, 
        incidence_name, 
        status, 
        creation_date, 
        item_no, 
        variant_code, 
        lot_no, 
        quantity, 
        document_no, 
        type, 
        classification, 
        destino, 
        quality_category, 
        document_line_no, 
        customer_item_code, 
        center_code, 
        logistic_block, 
        shoop_number, 
        adress, 
        post_code, 
        city, 
        country, 
        phone_no, 
        collection_processing, 
        collection_date, 
        collection_result, 
        pictures_link, 
        collection_reception_date, 
        incidence_reception_date, 
        collection_notes, 
        user_id, 
        incidence_sub_type, 
        murano_code, 
        responsibility_center, 
        expedient, 
        incidence_number, 
        incidence_source, 
        meat_suplier, 
        unit_of_measure, 
        complaints_number, 
        link_pictures, 
        product_line, 
        suplier_lot_no, 
        item_description, 
        input_chanel, 
        r, 
        complaint_type, 
        incidence_sub_sub_type, 
        claim_code, 
        sin_uso, 
        descrription_customer, 
        meat_supplier, 
        work_shift, 
        shop_name, 
        zone, 
        id_standard_text, 
        id_std_complaint_code, 
        lot_quantity, 
        incidence_sub_sub_type_2, 
        registro, 
        incidence_sub_type_copy, 
        id_std_complaint_code_copy, 
        production_date, 
        production_time, 
        detector_dansensor, 
        intern_vendor, 
        intern_vendor_name, 
        packaging_vendor_no, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, NO, INCIDENCE_DATE, DESCRIPTION, DETECTION_RESPONSIBLE, DETECTION_DATE, CLOSING_RESPONSIBLE, CLOSING_DATE, INCIDENCE_TYPE, INCIDENCE_CODE, INCIDENCE_NAME, STATUS, CREATION_DATE, ITEM_NO, VARIANT_CODE, LOT_NO, QUANTITY, DOCUMENT_NO, TYPE, CLASSIFICATION, DESTINO, QUALITY_CATEGORY, DOCUMENT_LINE_NO, CUSTOMER_ITEM_CODE, CENTER_CODE, LOGISTIC_BLOCK, SHOOP_NUMBER, ADRESS, POST_CODE, CITY, COUNTRY, PHONE_NO, COLLECTION_PROCESSING, COLLECTION_DATE, COLLECTION_RESULT, PICTURES_LINK, COLLECTION_RECEPTION_DATE, INCIDENCE_RECEPTION_DATE, COLLECTION_NOTES, USER_ID, INCIDENCE_SUB_TYPE, MURANO_CODE, RESPONSIBILITY_CENTER, EXPEDIENT, INCIDENCE_NUMBER, INCIDENCE_SOURCE, MEAT_SUPLIER, UNIT_OF_MEASURE, COMPLAINTS_NUMBER, LINK_PICTURES, PRODUCT_LINE, SUPLIER_LOT_NO, ITEM_DESCRIPTION, INPUT_CHANEL, R, COMPLAINT_TYPE, INCIDENCE_SUB_SUB_TYPE, CLAIM_CODE, SIN_USO, DESCRRIPTION_CUSTOMER, MEAT_SUPPLIER, WORK_SHIFT, SHOP_NAME, ZONE, ID_STANDARD_TEXT, ID_STD_COMPLAINT_CODE, LOT_QUANTITY, INCIDENCE_SUB_SUB_TYPE_2, REGISTRO, INCIDENCE_SUB_TYPE_COPY, ID_STD_COMPLAINT_CODE_COPY, PRODUCTION_DATE, PRODUCTION_TIME, DETECTOR_DANSENSOR, INTERN_VENDOR, INTERN_VENDOR_NAME, PACKAGING_VENDOR_NO, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('incidence_quality_header_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.NO is not distinct from s.NO and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.NO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
