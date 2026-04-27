{{ config(database='PROD_BRONZE' if target.name == 'prod' else 'DEV_BRONZE', schema='BRONZE_DELISANO', materialized='view', tags=['bronze','batch','DELISANO']) }}

{% set src_ref = source('bronze_delisano', 'V_DL_DIRECCIONES_CLIENTE') %}
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
        {{ std_cast('"Customer No_"', 'VARCHAR') }}                            as CUSTOMER_NO,
        {{ std_cast('"Code"', 'VARCHAR') }}                                    as CODE,
        {{ std_cast('"Name"', 'VARCHAR') }}                                    as NAME,
        {{ std_cast('"Name 2"', 'VARCHAR') }}                                  as NAME_2,
        {{ std_cast('"Address"', 'VARCHAR') }}                                 as ADDRESS,
        {{ std_cast('"Address 2"', 'VARCHAR') }}                               as ADDRESS_2,
        {{ std_cast('"City"', 'VARCHAR') }}                                    as CITY,
        {{ std_cast('"Contact"', 'VARCHAR') }}                                 as CONTACT,
        {{ std_cast('"Phone No_"', 'VARCHAR') }}                               as PHONE_NO,
        {{ std_cast('"Telex No_"', 'VARCHAR') }}                               as TELEX_NO,
        {{ std_cast('"Shipment Method Code"', 'VARCHAR') }}                    as SHIPMENT_METHOD_CODE,
        {{ std_cast('"Shipping Agent Code"', 'VARCHAR') }}                     as SHIPPING_AGENT_CODE,
        {{ std_cast('"Place of Export"', 'VARCHAR') }}                         as PLACE_OF_EXPORT,
        {{ std_cast('"Country_Region Code"', 'VARCHAR') }}                     as COUNTRY_REGION_CODE,
        {{ std_cast('"Last Date Modified"', 'TIMESTAMP_NTZ') }}                as LAST_DATE_MODIFIED,
        {{ std_cast('"Location Code"', 'VARCHAR') }}                           as LOCATION_CODE,
        {{ std_cast('"Fax No_"', 'VARCHAR') }}                                 as FAX_NO,
        {{ std_cast('"Telex Answer Back"', 'VARCHAR') }}                       as TELEX_ANSWER_BACK,
        {{ std_cast('"Post Code"', 'VARCHAR') }}                               as POST_CODE,
        {{ std_cast('"County"', 'VARCHAR') }}                                  as COUNTY,
        {{ std_cast('"E-Mail"', 'VARCHAR') }}                                  as EMAIL,
        {{ std_cast('"Home Page"', 'VARCHAR') }}                               as HOME_PAGE,
        {{ std_cast('"Tax Area Code"', 'VARCHAR') }}                           as TAX_AREA_CODE,
        {{ std_cast('"Tax Liable"', 'INTEGER') }}                              as TAX_LIABLE,
        {{ std_cast('"Shipping Agent Service Code"', 'VARCHAR') }}             as SHIPPING_AGENT_SERVICE_CODE,
        {{ std_cast('"Service Zone Code"', 'VARCHAR') }}                       as SERVICE_ZONE_CODE,
        {{ std_cast('"GLN"', 'VARCHAR') }}                                     as GLN,
        {{ std_cast('"IDPGND Exclude SIG Rate"', 'INTEGER') }}                 as IDPGND_EXCLUDE_SIG_RATE,
        {{ std_cast('"VAT Bus_ Posting Group"', 'VARCHAR') }}                  as VAT_BUS_POSTING_GROUP,
        {{ std_cast('"VAT Registration No_"', 'VARCHAR') }}                    as VAT_REGISTRATION_NO,
        {{ std_cast('"Grupo IRPF"', 'VARCHAR') }}                              as GRUPO_IRPF,
        {{ std_cast('"Código idioma"', 'VARCHAR') }}                           as CDIGO_IDIOMA,
        {{ std_cast('"Núm_ Serie factura venta registrada"', 'VARCHAR') }}     as NM_SERIE_FACTURA_VENTA_REGISTRADA,
        {{ std_cast('"Núm_ Serie abono venta registrada"', 'VARCHAR') }}       as NM_SERIE_ABONO_VENTA_REGISTRADA,
        {{ std_cast('"TasaPuntoVerde"', 'INTEGER') }}                          as TASAPUNTOVERDE,
        {{ std_cast('"Tipo DESADV"', 'VARCHAR') }}                             as TIPO_DESADV,
        {{ std_cast('"DL_ClienteFinal"', 'VARCHAR') }}                         as DL_CLIENTEFINAL,
        {{ std_cast('"DL_NumTiendas"', 'VARCHAR') }}                           as DL_NUMTIENDAS,
        {{ std_cast('"DL_NTiendas"', 'INTEGER') }}                             as DL_NTIENDAS,
        {{ std_cast('"DL_GamaNegocio"', 'VARCHAR') }}                          as DL_GAMANEGOCIO,
        {{ std_cast('"IF_LC_ID"', 'VARCHAR') }}                                as IF_LC_ID,
        {{ std_cast('"IF_SYS_STATE"', 'VARCHAR') }}                            as IF_SYS_STATE,
        {{ std_cast('"IF_PAIS_COD_ISO_3"', 'VARCHAR') }}                       as IF_PAIS_COD_ISO_3,
        {{ std_cast('"IF_SYS_PROVINCE"', 'VARCHAR') }}                         as IF_SYS_PROVINCE,
        {{ std_cast('"TEC_DES_EMPRESA"', 'VARCHAR') }}                         as TEC_DES_EMPRESA,
        {{ std_cast('"TEC_ID_INGESTA"', 'VARCHAR') }}                          as TEC_ID_INGESTA,
        {{ std_cast('"TEC_TS_INGESTA"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_INGESTA,
        {{ std_cast('"TEC_TS_STAGING"', 'TIMESTAMP_NTZ') }}                    as TEC_TS_STAGING,
        {{ std_cast('"TEC_TS_INTEGRACION_B"', 'TIMESTAMP_NTZ') }}              as TEC_TS_INTEGRACION_B,
        'DLS'                                                                     as tec_des_cod_siglas,
        'DELISANO'                                                             as tec_des_empresa,
        tec_id_ingesta,
        tec_ts_ingesta,
        tec_ts_staging,
        tec_ts_integracion_b
    from {{ src_ref }}
    where tec_id_ingesta = (select tec_id_ingesta from next_log)
)
select *
from src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_no, code
    ORDER BY tec_ts_ingesta DESC, tec_ts_staging DESC
) = 1
