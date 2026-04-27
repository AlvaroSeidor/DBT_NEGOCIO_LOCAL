{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_COOPECARN', materialized='view', tags=['bronze','batch','COOPECARN']) }}

{% set src_ref = source('bronze_coopecarn', 'COOPECARN_INCIDENCE_QUALITY_HEADER') %}
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
        {{ std_cast('"NO_"', 'VARCHAR') }}                                     as NO,
        {{ std_cast('"INCIDENCE_DATE"', 'TIMESTAMP_NTZ') }}                    as INCIDENCE_DATE,
        {{ std_cast('"DESCRIPTION"', 'VARCHAR') }}                             as DESCRIPTION,
        {{ std_cast('"DETECTION_RESPONSIBLE"', 'VARCHAR') }}                   as DETECTION_RESPONSIBLE,
        {{ std_cast('"DETECTION_DATE"', 'TIMESTAMP_NTZ') }}                    as DETECTION_DATE,
        {{ std_cast('"CLOSING_RESPONSIBLE"', 'VARCHAR') }}                     as CLOSING_RESPONSIBLE,
        {{ std_cast('"CLOSING_DATE"', 'TIMESTAMP_NTZ') }}                      as CLOSING_DATE,
        {{ std_cast('"INCIDENCE_TYPE"', 'INTEGER') }}                          as INCIDENCE_TYPE,
        {{ std_cast('"INCIDENCE_CODE"', 'VARCHAR') }}                          as INCIDENCE_CODE,
        {{ std_cast('"INCIDENCE_NAME"', 'VARCHAR') }}                          as INCIDENCE_NAME,
        {{ std_cast('"STATUS"', 'INTEGER') }}                                  as STATUS,
        {{ std_cast('"CREATION_DATE"', 'TIMESTAMP_NTZ') }}                     as CREATION_DATE,
        {{ std_cast('"ITEM_NO_"', 'VARCHAR') }}                                as ITEM_NO,
        {{ std_cast('"VARIANT_CODE"', 'VARCHAR') }}                            as VARIANT_CODE,
        {{ std_cast('"LOT_NO_"', 'VARCHAR') }}                                 as LOT_NO,
        {{ std_cast('"QUANTITY"', 'NUMBER(38,5)') }}                           as QUANTITY,
        {{ std_cast('"DOCUMENT_NO_"', 'VARCHAR') }}                            as DOCUMENT_NO,
        {{ std_cast('"TYPE"', 'VARCHAR') }}                                    as TYPE,
        {{ std_cast('"CLASSIFICATION"', 'VARCHAR') }}                          as CLASSIFICATION,
        {{ std_cast('"DESTINO"', 'VARCHAR') }}                                 as DESTINO,
        {{ std_cast('"QUALITY_CATEGORY"', 'VARCHAR') }}                        as QUALITY_CATEGORY,
        {{ std_cast('"DOCUMENT_LINE_NO_"', 'INTEGER') }}                       as DOCUMENT_LINE_NO,
        {{ std_cast('"CUSTOMER_ITEM_CODE"', 'VARCHAR') }}                      as CUSTOMER_ITEM_CODE,
        {{ std_cast('"CENTER_CODE"', 'VARCHAR') }}                             as CENTER_CODE,
        {{ std_cast('"LOGISTIC_BLOCK"', 'VARCHAR') }}                          as LOGISTIC_BLOCK,
        {{ std_cast('"SHOOP_NUMBER"', 'VARCHAR') }}                            as SHOOP_NUMBER,
        {{ std_cast('"ADRESS"', 'VARCHAR') }}                                  as ADRESS,
        {{ std_cast('"POST_CODE"', 'VARCHAR') }}                               as POST_CODE,
        {{ std_cast('"CITY"', 'VARCHAR') }}                                    as CITY,
        {{ std_cast('"COUNTRY"', 'VARCHAR') }}                                 as COUNTRY,
        {{ std_cast('"PHONE_NO_"', 'VARCHAR') }}                               as PHONE_NO,
        {{ std_cast('"COLLECTION_PROCESSING"', 'INTEGER') }}                   as COLLECTION_PROCESSING,
        {{ std_cast('"COLLECTION_DATE"', 'TIMESTAMP_NTZ') }}                   as COLLECTION_DATE,
        {{ std_cast('"COLLECTION_RESULT"', 'VARCHAR') }}                       as COLLECTION_RESULT,
        {{ std_cast('"PICTURES_LINK"', 'VARCHAR') }}                           as PICTURES_LINK,
        {{ std_cast('"COLLECTION_RECEPTION_DATE"', 'TIMESTAMP_NTZ') }}         as COLLECTION_RECEPTION_DATE,
        {{ std_cast('"INCIDENCE_RECEPTION_DATE"', 'TIMESTAMP_NTZ') }}          as INCIDENCE_RECEPTION_DATE,
        {{ std_cast('"COLLECTION_NOTES"', 'VARCHAR') }}                        as COLLECTION_NOTES,
        {{ std_cast('"USER_ID"', 'VARCHAR') }}                                 as USER_ID,
        {{ std_cast('"INCIDENCE_SUB_TYPE"', 'VARCHAR') }}                      as INCIDENCE_SUB_TYPE,
        {{ std_cast('"MURANO_CODE"', 'VARCHAR') }}                             as MURANO_CODE,
        {{ std_cast('"RESPONSIBILITY_CENTER"', 'VARCHAR') }}                   as RESPONSIBILITY_CENTER,
        {{ std_cast('"EXPEDIENT"', 'VARCHAR') }}                               as EXPEDIENT,
        {{ std_cast('"INCIDENCE_NUMBER"', 'VARCHAR') }}                        as INCIDENCE_NUMBER,
        {{ std_cast('"INCIDENCE_SOURCE"', 'INTEGER') }}                        as INCIDENCE_SOURCE,
        {{ std_cast('"MEAT_SUPLIER"', 'VARCHAR') }}                            as MEAT_SUPLIER,
        {{ std_cast('"UNIT_OF_MEASURE"', 'VARCHAR') }}                         as UNIT_OF_MEASURE,
        {{ std_cast('"COMPLAINTS_NUMBER"', 'INTEGER') }}                       as COMPLAINTS_NUMBER,
        {{ std_cast('"LINK_PICTURES"', 'VARCHAR') }}                           as LINK_PICTURES,
        {{ std_cast('"PRODUCT_LINE"', 'VARCHAR') }}                            as PRODUCT_LINE,
        {{ std_cast('"SUPLIER_LOT_NO_"', 'VARCHAR') }}                         as SUPLIER_LOT_NO,
        {{ std_cast('"ITEM_DESCRIPTION"', 'VARCHAR') }}                        as ITEM_DESCRIPTION,
        {{ std_cast('"INPUT_CHANEL"', 'VARCHAR') }}                            as INPUT_CHANEL,
        {{ std_cast('"R"', 'INTEGER') }}                                       as R,
        {{ std_cast('"COMPLAINT_TYPE"', 'VARCHAR') }}                          as COMPLAINT_TYPE,
        {{ std_cast('"INCIDENCE_SUB_SUB_TYPE"', 'VARCHAR') }}                  as INCIDENCE_SUB_SUB_TYPE,
        {{ std_cast('"CLAIM_CODE"', 'VARCHAR') }}                              as CLAIM_CODE,
        {{ std_cast('"SIN_USO"', 'INTEGER') }}                                 as SIN_USO,
        {{ std_cast('"DESCRRIPTION_CUSTOMER"', 'INTEGER') }}                   as DESCRRIPTION_CUSTOMER,
        {{ std_cast('"MEAT_SUPPLIER"', 'VARCHAR') }}                           as MEAT_SUPPLIER,
        {{ std_cast('"WORK_SHIFT"', 'INTEGER') }}                              as WORK_SHIFT,
        {{ std_cast('"SHOP_NAME"', 'VARCHAR') }}                               as SHOP_NAME,
        {{ std_cast('"ZONE"', 'VARCHAR') }}                                    as ZONE,
        {{ std_cast('"ID_STANDARD_TEXT"', 'INTEGER') }}                        as ID_STANDARD_TEXT,
        {{ std_cast('"ID_STD_COMPLAINT_CODE"', 'INTEGER') }}                   as ID_STD_COMPLAINT_CODE,
        {{ std_cast('"LOT_QUANTITY"', 'NUMBER(38,5)') }}                       as LOT_QUANTITY,
        {{ std_cast('"INCIDENCE_SUB_SUB_TYPE_2"', 'VARCHAR') }}                as INCIDENCE_SUB_SUB_TYPE_2,
        {{ std_cast('"REGISTRO"', 'INTEGER') }}                                as REGISTRO,
        {{ std_cast('"INCIDENCE_SUB_TYPE_COPY"', 'VARCHAR') }}                 as INCIDENCE_SUB_TYPE_COPY,
        {{ std_cast('"ID_STD_COMPLAINT_CODE_COPY"', 'INTEGER') }}              as ID_STD_COMPLAINT_CODE_COPY,
        {{ std_cast('"PRODUCTION_DATE"', 'TIMESTAMP_NTZ') }}                   as PRODUCTION_DATE,
        {{ std_cast('"PRODUCTION_TIME"', 'TIMESTAMP_NTZ') }}                   as PRODUCTION_TIME,
        {{ std_cast('"DETECTOR_DANSENSOR"', 'INTEGER') }}                      as DETECTOR_DANSENSOR,
        {{ std_cast('"INTERN_VENDOR"', 'VARCHAR') }}                           as INTERN_VENDOR,
        {{ std_cast('"INTERN_VENDOR_NAME"', 'VARCHAR') }}                      as INTERN_VENDOR_NAME,
        {{ std_cast('"PACKAGING_VENDOR_NO_"', 'VARCHAR') }}                    as PACKAGING_VENDOR_NO,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'COO'                                                                     as tec_des_cod_siglas
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY no
    ORDER BY tec_ts_ingesta DESC, TIMESTAMP DESC
) = 1
