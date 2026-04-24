{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_FRICAFOR', materialized='view', tags=['bronze','batch','FRICAFOR']) }}

{% set src_ref = source('bronze_fricafor', 'FRICAFOR_SHIP_TO_ADDRESS') %}
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
        {{ std_cast('"CUSTOMER_NO_"', 'VARCHAR') }}                            as CUSTOMER_NO,
        {{ std_cast('"CODE"', 'VARCHAR') }}                                    as CODE,
        {{ std_cast('"NAME"', 'VARCHAR') }}                                    as NAME,
        {{ std_cast('"NAME_2"', 'VARCHAR') }}                                  as NAME_2,
        {{ std_cast('"ADDRESS"', 'VARCHAR') }}                                 as ADDRESS,
        {{ std_cast('"ADDRESS_2"', 'VARCHAR') }}                               as ADDRESS_2,
        {{ std_cast('"CITY"', 'VARCHAR') }}                                    as CITY,
        {{ std_cast('"CONTACT"', 'VARCHAR') }}                                 as CONTACT,
        {{ std_cast('"PHONE_NO_"', 'VARCHAR') }}                               as PHONE_NO,
        {{ std_cast('"TELEX_NO_"', 'VARCHAR') }}                               as TELEX_NO,
        {{ std_cast('"SHIPMENT_METHOD_CODE"', 'VARCHAR') }}                    as SHIPMENT_METHOD_CODE,
        {{ std_cast('"SHIPPING_AGENT_CODE"', 'VARCHAR') }}                     as SHIPPING_AGENT_CODE,
        {{ std_cast('"PLACE_OF_EXPORT"', 'VARCHAR') }}                         as PLACE_OF_EXPORT,
        {{ std_cast('"COUNTRY_REGION_CODE"', 'VARCHAR') }}                     as COUNTRY_REGION_CODE,
        {{ std_cast('"LAST_DATE_MODIFIED"', 'TIMESTAMP_NTZ') }}                as LAST_DATE_MODIFIED,
        {{ std_cast('"LOCATION_CODE"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"FAX_NO_"', 'VARCHAR') }}                                 as FAX_NO,
        {{ std_cast('"TELEX_ANSWER_BACK"', 'VARCHAR') }}                       as TELEX_ANSWER_BACK,
        {{ std_cast('"POST_CODE"', 'VARCHAR') }}                               as POST_CODE,
        {{ std_cast('"COUNTY"', 'VARCHAR') }}                                  as COUNTY,
        {{ std_cast('"E_MAIL"', 'VARCHAR') }}                                  as E_MAIL,
        {{ std_cast('"HOME_PAGE"', 'VARCHAR') }}                               as HOME_PAGE,
        {{ std_cast('"TAX_AREA_CODE"', 'VARCHAR') }}                           as TAX_AREA_CODE,
        {{ std_cast('"TAX_LIABLE"', 'INTEGER') }}                              as TAX_LIABLE,
        {{ std_cast('"SHIPPING_AGENT_SERVICE_CODE"', 'VARCHAR') }}             as SHIPPING_AGENT_SERVICE_CODE,
        {{ std_cast('"SERVICE_ZONE_CODE"', 'VARCHAR') }}                       as SERVICE_ZONE_CODE,
        {{ std_cast('"COD_RUTA"', 'VARCHAR') }}                                as COD_RUTA,
        {{ std_cast('"HORARIO_ENTREGA"', 'VARCHAR') }}                         as HORARIO_ENTREGA,
        {{ std_cast('"E_MAIL_ENVIO_ALBARAN"', 'VARCHAR') }}                    as E_MAIL_ENVIO_ALBARAN,
        {{ std_cast('"E_MAIL_ENVIO_RECOGIDA"', 'VARCHAR') }}                   as E_MAIL_ENVIO_RECOGIDA,
        {{ std_cast('"OBSERVACIONES_PICKING"', 'VARCHAR') }}                   as OBSERVACIONES_PICKING,
        {{ std_cast('"ZONA_TRANSPORTE"', 'VARCHAR') }}                         as ZONA_TRANSPORTE,
        {{ std_cast('"EDI_REF_ALBARAN"', 'VARCHAR') }}                         as EDI_REF_ALBARAN,
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