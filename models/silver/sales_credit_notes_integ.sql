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
                HASH(NO, SELLTO_CUSTOMER_NO, BILLTO_CUSTOMER_NO, BILLTO_NAME, BILLTO_NAME_2, BILLTO_ADDRESS, BILLTO_ADDRESS_2, BILLTO_CITY, BILLTO_CONTACT, YOUR_REFERENCE, SHIPTO_CODE, SHIPTO_NAME, SHIPTO_NAME_2, SHIPTO_ADDRESS, SHIPTO_ADDRESS_2, SHIPTO_CITY, SHIPTO_CONTACT, POSTING_DATE, SHIPMENT_DATE, POSTING_DESCRIPTION, PAYMENT_TERMS_CODE, DUE_DATE, PAYMENT_DISCOUNT, PMT_DISCOUNT_DATE, SHIPMENT_METHOD_CODE, LOCATION_CODE, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_POSTING_GROUP, CURRENCY_CODE, CURRENCY_FACTOR, CUSTOMER_PRICE_GROUP, PRICES_INCLUDING_VAT, INVOICE_DISC_CODE, CUSTOMER_DISC_GROUP, LANGUAGE_CODE, SALESPERSON_CODE, NO_PRINTED, ON_HOLD, APPLIESTO_DOC_TYPE, APPLIESTO_DOC_NO, BAL_ACCOUNT_NO, VAT_REGISTRATION_NO, REASON_CODE, GEN_BUS_POSTING_GROUP, EU_3PARTY_TRADE, TRANSACTION_TYPE, TRANSPORT_METHOD, VAT_COUNTRY_REGION_CODE, SELLTO_CUSTOMER_NAME, SELLTO_CUSTOMER_NAME_2, SELLTO_ADDRESS, SELLTO_ADDRESS_2, SELLTO_CITY, SELLTO_CONTACT, BILLTO_POST_CODE, BILLTO_COUNTY, BILLTO_COUNTRY_REGION_CODE, SELLTO_POST_CODE, SELLTO_COUNTY, SELLTO_COUNTRY_REGION_CODE, SHIPTO_POST_CODE, SHIPTO_COUNTY, SHIPTO_COUNTRY_REGION_CODE, BAL_ACCOUNT_TYPE, EXIT_POINT, CORRECTION, DOCUMENT_DATE, EXTERNAL_DOCUMENT_NO, AREA, TRANSACTION_SPECIFICATION, PAYMENT_METHOD_CODE, PREASSIGNED_NO_SERIES, NO_SERIES, PREASSIGNED_NO, USER_ID, SOURCE_CODE, TAX_AREA_CODE, TAX_LIABLE, VAT_BUS_POSTING_GROUP, VAT_BASE_DISCOUNT, PREPMT_CR_MEMO_NO_SERIES, PREPAYMENT_CREDIT_MEMO, PREPAYMENT_ORDER_NO, DIMENSION_SET_ID, DOCUMENT_EXCHANGE_IDENTIFIER, DOCUMENT_EXCHANGE_STATUS, DOC_EXCH_ORIGINAL_IDENTIFIER, CUST_LEDGER_ENTRY_NO, CAMPAIGN_NO, SELLTO_CONTACT_NO, BILLTO_CONTACT_NO, OPPORTUNITY_NO, RESPONSIBILITY_CENTER, RETURN_ORDER_NO, RETURN_ORDER_NO_SERIES, ALLOW_LINE_DISC, GET_RETURN_RECEIPT_USED, ID, CORRECTED_INVOICE_NO, INVOICE_TYPE, CR_MEMO_TYPE, SPECIAL_SCHEME_CODE, OPERATION_DESCRIPTION, CORRECTION_TYPE, OPERATION_DESCRIPTION_2, SUCCEEDED_COMPANY_NAME, SUCCEEDED_VAT_REGISTRATION_NO, APPLIESTO_BILL_NO, CUST_BANK_ACC_CODE, PAYAT_CODE, SELLTO_PHONE_NO, SELLTO_EMAIL, ID_TYPE, IDPIRPF_IRPF_GROUP, IDPGND_SHIPTO_EXCL_SIG_RATE, IF_OR_TYPE, IF_DATA_1, IF_DATA_2, IF_DATA_3, IF_DATA_4, IF_DATA_5, IF_ML_TIME, IF_SAE_CERTIFICATION, CO_ID_CONT, CO_ID_PREC, TOTALNETWEIGHT, TOTALGROSSWEIGHT, PACKAGESPALLETNUMBER, AIT_BILLING_ADDRESS_CODE, IDPEDI_INVOIC, IDPEDI_INVOIC_SENT, AIT_CREATION_USER, AIT_CREATION_DATETIME, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('sales_credit_notes_batch') }}
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
{% set src_ref_delisano = source('bronze_delisano', 'V_DL_SALES_CREDIT_NOTES') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_delisano.database }}')
              and upper(l.schema)     = upper('{{ src_ref_delisano.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_delisano.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_credit_notes_batch_delisano') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        no, 
        sellto_customer_no, 
        billto_customer_no, 
        billto_name, 
        billto_name_2, 
        billto_address, 
        billto_address_2, 
        billto_city, 
        billto_contact, 
        your_reference, 
        shipto_code, 
        shipto_name, 
        shipto_name_2, 
        shipto_address, 
        shipto_address_2, 
        shipto_city, 
        shipto_contact, 
        posting_date, 
        shipment_date, 
        posting_description, 
        payment_terms_code, 
        due_date, 
        payment_discount, 
        pmt_discount_date, 
        shipment_method_code, 
        location_code, 
        shortcut_dimension_1_code, 
        shortcut_dimension_2_code, 
        customer_posting_group, 
        currency_code, 
        currency_factor, 
        customer_price_group, 
        prices_including_vat, 
        invoice_disc_code, 
        customer_disc_group, 
        language_code, 
        salesperson_code, 
        no_printed, 
        on_hold, 
        appliesto_doc_type, 
        appliesto_doc_no, 
        bal_account_no, 
        vat_registration_no, 
        reason_code, 
        gen_bus_posting_group, 
        eu_3party_trade, 
        transaction_type, 
        transport_method, 
        vat_country_region_code, 
        sellto_customer_name, 
        sellto_customer_name_2, 
        sellto_address, 
        sellto_address_2, 
        sellto_city, 
        sellto_contact, 
        billto_post_code, 
        billto_county, 
        billto_country_region_code, 
        sellto_post_code, 
        sellto_county, 
        sellto_country_region_code, 
        shipto_post_code, 
        shipto_county, 
        shipto_country_region_code, 
        bal_account_type, 
        exit_point, 
        correction, 
        document_date, 
        external_document_no, 
        area, 
        transaction_specification, 
        payment_method_code, 
        preassigned_no_series, 
        no_series, 
        preassigned_no, 
        user_id, 
        source_code, 
        tax_area_code, 
        tax_liable, 
        vat_bus_posting_group, 
        vat_base_discount, 
        prepmt_cr_memo_no_series, 
        prepayment_credit_memo, 
        prepayment_order_no, 
        dimension_set_id, 
        document_exchange_identifier, 
        document_exchange_status, 
        doc_exch_original_identifier, 
        cust_ledger_entry_no, 
        campaign_no, 
        sellto_contact_no, 
        billto_contact_no, 
        opportunity_no, 
        responsibility_center, 
        return_order_no, 
        return_order_no_series, 
        allow_line_disc, 
        get_return_receipt_used, 
        id, 
        corrected_invoice_no, 
        invoice_type, 
        cr_memo_type, 
        special_scheme_code, 
        operation_description, 
        correction_type, 
        operation_description_2, 
        succeeded_company_name, 
        succeeded_vat_registration_no, 
        appliesto_bill_no, 
        cust_bank_acc_code, 
        payat_code, 
        sellto_phone_no, 
        sellto_email, 
        id_type, 
        idpirpf_irpf_group, 
        idpgnd_shipto_excl_sig_rate, 
        if_or_type, 
        if_data_1, 
        if_data_2, 
        if_data_3, 
        if_data_4, 
        if_data_5, 
        if_ml_time, 
        if_sae_certification, 
        co_id_cont, 
        co_id_prec, 
        totalnetweight, 
        totalgrossweight, 
        packagespalletnumber, 
        ait_billing_address_code, 
        idpedi_invoic, 
        idpedi_invoic_sent, 
        ait_creation_user, 
        ait_creation_datetime, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(NO, SELLTO_CUSTOMER_NO, BILLTO_CUSTOMER_NO, BILLTO_NAME, BILLTO_NAME_2, BILLTO_ADDRESS, BILLTO_ADDRESS_2, BILLTO_CITY, BILLTO_CONTACT, YOUR_REFERENCE, SHIPTO_CODE, SHIPTO_NAME, SHIPTO_NAME_2, SHIPTO_ADDRESS, SHIPTO_ADDRESS_2, SHIPTO_CITY, SHIPTO_CONTACT, POSTING_DATE, SHIPMENT_DATE, POSTING_DESCRIPTION, PAYMENT_TERMS_CODE, DUE_DATE, PAYMENT_DISCOUNT, PMT_DISCOUNT_DATE, SHIPMENT_METHOD_CODE, LOCATION_CODE, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_POSTING_GROUP, CURRENCY_CODE, CURRENCY_FACTOR, CUSTOMER_PRICE_GROUP, PRICES_INCLUDING_VAT, INVOICE_DISC_CODE, CUSTOMER_DISC_GROUP, LANGUAGE_CODE, SALESPERSON_CODE, NO_PRINTED, ON_HOLD, APPLIESTO_DOC_TYPE, APPLIESTO_DOC_NO, BAL_ACCOUNT_NO, VAT_REGISTRATION_NO, REASON_CODE, GEN_BUS_POSTING_GROUP, EU_3PARTY_TRADE, TRANSACTION_TYPE, TRANSPORT_METHOD, VAT_COUNTRY_REGION_CODE, SELLTO_CUSTOMER_NAME, SELLTO_CUSTOMER_NAME_2, SELLTO_ADDRESS, SELLTO_ADDRESS_2, SELLTO_CITY, SELLTO_CONTACT, BILLTO_POST_CODE, BILLTO_COUNTY, BILLTO_COUNTRY_REGION_CODE, SELLTO_POST_CODE, SELLTO_COUNTY, SELLTO_COUNTRY_REGION_CODE, SHIPTO_POST_CODE, SHIPTO_COUNTY, SHIPTO_COUNTRY_REGION_CODE, BAL_ACCOUNT_TYPE, EXIT_POINT, CORRECTION, DOCUMENT_DATE, EXTERNAL_DOCUMENT_NO, AREA, TRANSACTION_SPECIFICATION, PAYMENT_METHOD_CODE, PREASSIGNED_NO_SERIES, NO_SERIES, PREASSIGNED_NO, USER_ID, SOURCE_CODE, TAX_AREA_CODE, TAX_LIABLE, VAT_BUS_POSTING_GROUP, VAT_BASE_DISCOUNT, PREPMT_CR_MEMO_NO_SERIES, PREPAYMENT_CREDIT_MEMO, PREPAYMENT_ORDER_NO, DIMENSION_SET_ID, DOCUMENT_EXCHANGE_IDENTIFIER, DOCUMENT_EXCHANGE_STATUS, DOC_EXCH_ORIGINAL_IDENTIFIER, CUST_LEDGER_ENTRY_NO, CAMPAIGN_NO, SELLTO_CONTACT_NO, BILLTO_CONTACT_NO, OPPORTUNITY_NO, RESPONSIBILITY_CENTER, RETURN_ORDER_NO, RETURN_ORDER_NO_SERIES, ALLOW_LINE_DISC, GET_RETURN_RECEIPT_USED, ID, CORRECTED_INVOICE_NO, INVOICE_TYPE, CR_MEMO_TYPE, SPECIAL_SCHEME_CODE, OPERATION_DESCRIPTION, CORRECTION_TYPE, OPERATION_DESCRIPTION_2, SUCCEEDED_COMPANY_NAME, SUCCEEDED_VAT_REGISTRATION_NO, APPLIESTO_BILL_NO, CUST_BANK_ACC_CODE, PAYAT_CODE, SELLTO_PHONE_NO, SELLTO_EMAIL, ID_TYPE, IDPIRPF_IRPF_GROUP, IDPGND_SHIPTO_EXCL_SIG_RATE, IF_OR_TYPE, IF_DATA_1, IF_DATA_2, IF_DATA_3, IF_DATA_4, IF_DATA_5, IF_ML_TIME, IF_SAE_CERTIFICATION, CO_ID_CONT, CO_ID_PREC, TOTALNETWEIGHT, TOTALGROSSWEIGHT, PACKAGESPALLETNUMBER, AIT_BILLING_ADDRESS_CODE, IDPEDI_INVOIC, IDPEDI_INVOIC_SENT, AIT_CREATION_USER, AIT_CREATION_DATETIME, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('sales_credit_notes_batch') }}
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
