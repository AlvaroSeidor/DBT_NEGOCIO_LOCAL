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
                HASH(TIMESTAMP, NO, SELLTO_CUSTOMER_NO, SELL_TO_CUSTOMER_NO, BILLTO_CUSTOMER_NO, BILL_TO_CUSTOMER_NO, BILLTO_NAME, BILL_TO_NAME, BILLTO_NAME_2, BILL_TO_NAME_2, BILLTO_ADDRESS, BILLTO_ADDRESS_2, BILL_TO_ADDRESS, BILLTO_CITY, BILL_TO_ADDRESS_2, BILLTO_CONTACT, BILL_TO_CITY, YOUR_REFERENCE, BILL_TO_CONTACT, SHIPTO_CODE, SHIP_TO_CODE, SHIPTO_NAME, SHIP_TO_NAME, SHIPTO_NAME_2, SHIP_TO_NAME_2, SHIPTO_ADDRESS, SHIP_TO_ADDRESS, SHIPTO_ADDRESS_2, SHIPTO_CITY, SHIP_TO_ADDRESS_2, SHIP_TO_CITY, SHIPTO_CONTACT, SHIP_TO_CONTACT, ORDER_DATE, POSTING_DATE, SHIPMENT_DATE, POSTING_DESCRIPTION, PAYMENT_TERMS_CODE, DUE_DATE, PAYMENT_DISCOUNT, PMT_DISCOUNT_DATE, SHIPMENT_METHOD_CODE, LOCATION_CODE, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_POSTING_GROUP, CURRENCY_CODE, CURRENCY_FACTOR, CUSTOMER_PRICE_GROUP, PRICES_INCLUDING_VAT, INVOICE_DISC_CODE, CUSTOMER_DISC_GROUP, LANGUAGE_CODE, SALESPERSON_CODE, ORDER_NO, NO_PRINTED, ON_HOLD, APPLIESTO_DOC_TYPE, APPLIES_TO_DOC_TYPE, APPLIESTO_DOC_NO, APPLIES_TO_DOC_NO, BAL_ACCOUNT_NO, VAT_REGISTRATION_NO, REASON_CODE, GEN_BUS_POSTING_GROUP, EU_3PARTY_TRADE, TRANSACTION_TYPE, EU_3_PARTY_TRADE, TRANSPORT_METHOD, VAT_COUNTRY_REGION_CODE, SELLTO_CUSTOMER_NAME, SELL_TO_CUSTOMER_NAME, SELLTO_CUSTOMER_NAME_2, SELLTO_ADDRESS, SELL_TO_CUSTOMER_NAME_2, SELLTO_ADDRESS_2, SELL_TO_ADDRESS, SELLTO_CITY, SELL_TO_ADDRESS_2, SELL_TO_CITY, SELLTO_CONTACT, SELL_TO_CONTACT, BILLTO_POST_CODE, BILL_TO_POST_CODE, BILLTO_COUNTY, BILL_TO_COUNTY, BILLTO_COUNTRY_REGION_CODE, BILL_TO_COUNTRY_REGION_CODE, SELLTO_POST_CODE, SELL_TO_POST_CODE, SELLTO_COUNTY, SELL_TO_COUNTY, SELLTO_COUNTRY_REGION_CODE, SHIPTO_POST_CODE, SELL_TO_COUNTRY_REGION_CODE, SHIPTO_COUNTY, SHIP_TO_POST_CODE, SHIP_TO_COUNTY, SHIPTO_COUNTRY_REGION_CODE, SHIP_TO_COUNTRY_REGION_CODE, BAL_ACCOUNT_TYPE, EXIT_POINT, CORRECTION, DOCUMENT_DATE, EXTERNAL_DOCUMENT_NO, AREA, TRANSACTION_SPECIFICATION, PAYMENT_METHOD_CODE, SHIPPING_AGENT_CODE, PACKAGE_TRACKING_NO, PREASSIGNED_NO_SERIES, NO_SERIES, PRE_ASSIGNED_NO_SERIES, ORDER_NO_SERIES, PREASSIGNED_NO, USER_ID, PRE_ASSIGNED_NO, SOURCE_CODE, TAX_AREA_CODE, TAX_LIABLE, VAT_BUS_POSTING_GROUP, VAT_BASE_DISCOUNT, INVOICE_DISCOUNT_CALCULATION, PREPAYMENT_NO_SERIES, INVOICE_DISCOUNT_VALUE, PREPAYMENT_INVOICE, PREPAYMENT_ORDER_NO, QUOTE_NO, DIMENSION_SET_ID, PAYMENT_INSTRUCTIONS_NAME, DOCUMENT_EXCHANGE_IDENTIFIER, PAYMENT_SERVICE_SET_ID, DOCUMENT_EXCHANGE_STATUS, DOC_EXCH_ORIGINAL_IDENTIFIER, COUPLED_TO_CRM, CREDIT_CARD_NO, DIRECT_DEBIT_MANDATE_ID, CANCELED_BY, CUST_LEDGER_ENTRY_NO, CAMPAIGN_NO, SELL_TO_CONTACT_NO, BILL_TO_CONTACT_NO, OPPORTUNITY_NO, SELLTO_CONTACT_NO, RESPONSIBILITY_CENTER, BILLTO_CONTACT_NO, ALLOW_LINE_DISC, GET_SHIPMENT_USED, INVOICE_TYPE, CONTAINER, CR_MEMO_TYPE, SEAL, TRUCK_LICENSE_PLATE, SPECIAL_SCHEME_CODE, ID, DRIVER, OPERATION_DESCRIPTION, BANK_ACCOUNT_TO_TRANSFER, OPERATION_DESCRIPTION_2, MOTIVO_DEVOLUCION_EDI, SUCCEEDED_COMPANY_NAME, SUCCEEDED_VAT_REGISTRATION_NO, DEPARTAMENTO_EDI, NO_MENSAJE_EDI, HEADER_DISCOUNT, RAPPEL, COD_CADENA, PROMOCIONES_POR, INVOICE_CANCELATION, APPLIESTO_BILL_NO, CORRECTED_INVOICE_TYPE, COD_DTO_LINEA, TIPO_PEDIDO, CUST_BANK_ACC_CODE, SIMPLIFIED_TYPE, TIPO_PUNTO_VERDE, FROM_TICKET_NO, PAYAT_CODE, TIPO_RESIDUOS, SELLTO_PHONE_NO, TO_TICKET_NO, SELLTO_EMAIL, APPLIES_TO_BILL_NO, EDI_NO_EXPORTACIONES, ID_TYPE, PAY_AT_CODE, IDPIRPF_IRPF_GROUP, CERRADO, PESO_NETO, DTO_FACTURA, IDPGND_SHIPTO_EXCL_SIG_RATE, PESO_BRUTO, FORMA_PAGO_FACTURAPLUS, IF_OR_TYPE, COD_BANCO, CASH_ENTRY, COMMISSION_SALESPERSON, IF_DATA_1, 2ND_SALESPERSON_CODE, SPETIAL_OPERATION_TYPE, IF_DATA_2, COMMISSION_2ND_SALESPERSON, SPETIAL_OPERATION_CODE, IF_DATA_3, IF_DATA_4, GROUP_SHIPMENT_TYPE, INCOME_TAX_RETENTION_CODE, SHIPMENT_FREQUENCY, INCOME_TAX_RETENTION, IF_DATA_5, INCOME_TAX_RETENTION_KEY, INVOICE_DETAIL, IF_ML_TIME, IF_SAE_CERTIFICATION, REVERTIDO_EN_NO_ABONO, INCOME_TAX_RETENTION_SUBKEY, NO_FACTURA_ANULADA, TEXTO_SUBVENCION, CO_ID_CONT, CO_ID_PREC, ANULACION_DE_FACTURA, REVERTIDA, TOTALNETWEIGHT, TOTALGROSSWEIGHT, PRINT_BY_CONCEPT, ODS_JOB_TEMPLATE_CODE, SIMPLIFIED_INVOICE, PACKAGESPALLETNUMBER, OPERATION_DATE, PACKAGESNUMBER, AIT_BILLING_ADDRESS_CODE, TO_DOCUMENT_NO, FROM_DOCUMENT_NO, IDPEDI_INVOIC, REGIME, IDPEDI_INVOIC_SENT, TAX_FREE, AIT_CREATION_USER, SII_CORRECTION_TYPE, AIT_CREATION_DATETIME, SII_OPERATION_DESCRIPTION, TAX_FREE_VENDOR_NO, TEC_DES_EMPRESA, CUSTOMER_SALES_DEPARTMENT, SII_INVOICE_TYPE, EDI_BLOCKED, TEC_ID_INGESTA, TEC_TS_INGESTA, SIMPLIFIED_ART_6_1_D, DESADV_NO, PLANNING_DELIVERY_DATE, REFEXTERNA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B, NUM_REGISTRO_ACUERDO_FACT, EMITIDA_POR_TERCERO, INVOICE_BLOCKED, EMITIDA_POR_NORMATIVA_DEL_GAS, INTERCOMPANY_INVOICE, CUSTOMER_RECEPTION_NO, PAYMENT_DATE, PAYMENT_NO_DAYS, EDI_ORDER_NO, SALES_SHIPMENT_NO, IPAD_ORDER, WITHOUT_PLASTIC_DECLARATION, FIX_SALES_PRICE, SCRAP_AMOUNT) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('sales_invoice_header_batch') }}
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
{% set src_ref_coopecarn = source('bronze_coopecarn', 'COOPECARN_SALES_INVOICE_HEADER') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_coopecarn.database }}')
              and upper(l.schema)     = upper('{{ src_ref_coopecarn.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_coopecarn.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_invoice_header_batch_coopecarn') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
{% set src_ref_delisano = source('bronze_delisano', 'V_DL_SALES_INVOICE_HEADERS') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_delisano.database }}')
              and upper(l.schema)     = upper('{{ src_ref_delisano.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_delisano.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_invoice_header_batch_delisano') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_SALES_INVOICE_HEADER') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_invoice_header_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        no, 
        sellto_customer_no, 
        sell_to_customer_no, 
        billto_customer_no, 
        bill_to_customer_no, 
        billto_name, 
        bill_to_name, 
        billto_name_2, 
        bill_to_name_2, 
        billto_address, 
        billto_address_2, 
        bill_to_address, 
        billto_city, 
        bill_to_address_2, 
        billto_contact, 
        bill_to_city, 
        your_reference, 
        bill_to_contact, 
        shipto_code, 
        ship_to_code, 
        shipto_name, 
        ship_to_name, 
        shipto_name_2, 
        ship_to_name_2, 
        shipto_address, 
        ship_to_address, 
        shipto_address_2, 
        shipto_city, 
        ship_to_address_2, 
        ship_to_city, 
        shipto_contact, 
        ship_to_contact, 
        order_date, 
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
        order_no, 
        no_printed, 
        on_hold, 
        appliesto_doc_type, 
        applies_to_doc_type, 
        appliesto_doc_no, 
        applies_to_doc_no, 
        bal_account_no, 
        vat_registration_no, 
        reason_code, 
        gen_bus_posting_group, 
        eu_3party_trade, 
        transaction_type, 
        eu_3_party_trade, 
        transport_method, 
        vat_country_region_code, 
        sellto_customer_name, 
        sell_to_customer_name, 
        sellto_customer_name_2, 
        sellto_address, 
        sell_to_customer_name_2, 
        sellto_address_2, 
        sell_to_address, 
        sellto_city, 
        sell_to_address_2, 
        sell_to_city, 
        sellto_contact, 
        sell_to_contact, 
        billto_post_code, 
        bill_to_post_code, 
        billto_county, 
        bill_to_county, 
        billto_country_region_code, 
        bill_to_country_region_code, 
        sellto_post_code, 
        sell_to_post_code, 
        sellto_county, 
        sell_to_county, 
        sellto_country_region_code, 
        shipto_post_code, 
        sell_to_country_region_code, 
        shipto_county, 
        ship_to_post_code, 
        ship_to_county, 
        shipto_country_region_code, 
        ship_to_country_region_code, 
        bal_account_type, 
        exit_point, 
        correction, 
        document_date, 
        external_document_no, 
        area, 
        transaction_specification, 
        payment_method_code, 
        shipping_agent_code, 
        package_tracking_no, 
        preassigned_no_series, 
        no_series, 
        pre_assigned_no_series, 
        order_no_series, 
        preassigned_no, 
        user_id, 
        pre_assigned_no, 
        source_code, 
        tax_area_code, 
        tax_liable, 
        vat_bus_posting_group, 
        vat_base_discount, 
        invoice_discount_calculation, 
        prepayment_no_series, 
        invoice_discount_value, 
        prepayment_invoice, 
        prepayment_order_no, 
        quote_no, 
        dimension_set_id, 
        payment_instructions_name, 
        document_exchange_identifier, 
        payment_service_set_id, 
        document_exchange_status, 
        doc_exch_original_identifier, 
        coupled_to_crm, 
        credit_card_no, 
        direct_debit_mandate_id, 
        canceled_by, 
        cust_ledger_entry_no, 
        campaign_no, 
        sell_to_contact_no, 
        bill_to_contact_no, 
        opportunity_no, 
        sellto_contact_no, 
        responsibility_center, 
        billto_contact_no, 
        allow_line_disc, 
        get_shipment_used, 
        invoice_type, 
        container, 
        cr_memo_type, 
        seal, 
        truck_license_plate, 
        special_scheme_code, 
        id, 
        driver, 
        operation_description, 
        bank_account_to_transfer, 
        operation_description_2, 
        motivo_devolucion_edi, 
        succeeded_company_name, 
        succeeded_vat_registration_no, 
        departamento_edi, 
        no_mensaje_edi, 
        header_discount, 
        rappel, 
        cod_cadena, 
        promociones_por, 
        invoice_cancelation, 
        appliesto_bill_no, 
        corrected_invoice_type, 
        cod_dto_linea, 
        tipo_pedido, 
        cust_bank_acc_code, 
        simplified_type, 
        tipo_punto_verde, 
        from_ticket_no, 
        payat_code, 
        tipo_residuos, 
        sellto_phone_no, 
        to_ticket_no, 
        sellto_email, 
        applies_to_bill_no, 
        edi_no_exportaciones, 
        id_type, 
        pay_at_code, 
        idpirpf_irpf_group, 
        cerrado, 
        peso_neto, 
        dto_factura, 
        idpgnd_shipto_excl_sig_rate, 
        peso_bruto, 
        forma_pago_facturaplus, 
        if_or_type, 
        cod_banco, 
        cash_entry, 
        commission_salesperson, 
        if_data_1, 
        2nd_salesperson_code, 
        spetial_operation_type, 
        if_data_2, 
        commission_2nd_salesperson, 
        spetial_operation_code, 
        if_data_3, 
        if_data_4, 
        group_shipment_type, 
        income_tax_retention_code, 
        shipment_frequency, 
        income_tax_retention, 
        if_data_5, 
        income_tax_retention_key, 
        invoice_detail, 
        if_ml_time, 
        if_sae_certification, 
        revertido_en_no_abono, 
        income_tax_retention_subkey, 
        no_factura_anulada, 
        texto_subvencion, 
        co_id_cont, 
        co_id_prec, 
        anulacion_de_factura, 
        revertida, 
        totalnetweight, 
        totalgrossweight, 
        print_by_concept, 
        ods_job_template_code, 
        simplified_invoice, 
        packagespalletnumber, 
        operation_date, 
        packagesnumber, 
        ait_billing_address_code, 
        to_document_no, 
        from_document_no, 
        idpedi_invoic, 
        regime, 
        idpedi_invoic_sent, 
        tax_free, 
        ait_creation_user, 
        sii_correction_type, 
        ait_creation_datetime, 
        sii_operation_description, 
        tax_free_vendor_no, 
        tec_des_empresa, 
        customer_sales_department, 
        sii_invoice_type, 
        edi_blocked, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        simplified_art_6_1_d, 
        desadv_no, 
        planning_delivery_date, 
        refexterna, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        num_registro_acuerdo_fact, 
        emitida_por_tercero, 
        invoice_blocked, 
        emitida_por_normativa_del_gas, 
        intercompany_invoice, 
        customer_reception_no, 
        payment_date, 
        payment_no_days, 
        edi_order_no, 
        sales_shipment_no, 
        ipad_order, 
        without_plastic_declaration, 
        fix_sales_price, 
        scrap_amount, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, NO, SELLTO_CUSTOMER_NO, SELL_TO_CUSTOMER_NO, BILLTO_CUSTOMER_NO, BILL_TO_CUSTOMER_NO, BILLTO_NAME, BILL_TO_NAME, BILLTO_NAME_2, BILL_TO_NAME_2, BILLTO_ADDRESS, BILLTO_ADDRESS_2, BILL_TO_ADDRESS, BILLTO_CITY, BILL_TO_ADDRESS_2, BILLTO_CONTACT, BILL_TO_CITY, YOUR_REFERENCE, BILL_TO_CONTACT, SHIPTO_CODE, SHIP_TO_CODE, SHIPTO_NAME, SHIP_TO_NAME, SHIPTO_NAME_2, SHIP_TO_NAME_2, SHIPTO_ADDRESS, SHIP_TO_ADDRESS, SHIPTO_ADDRESS_2, SHIPTO_CITY, SHIP_TO_ADDRESS_2, SHIP_TO_CITY, SHIPTO_CONTACT, SHIP_TO_CONTACT, ORDER_DATE, POSTING_DATE, SHIPMENT_DATE, POSTING_DESCRIPTION, PAYMENT_TERMS_CODE, DUE_DATE, PAYMENT_DISCOUNT, PMT_DISCOUNT_DATE, SHIPMENT_METHOD_CODE, LOCATION_CODE, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_POSTING_GROUP, CURRENCY_CODE, CURRENCY_FACTOR, CUSTOMER_PRICE_GROUP, PRICES_INCLUDING_VAT, INVOICE_DISC_CODE, CUSTOMER_DISC_GROUP, LANGUAGE_CODE, SALESPERSON_CODE, ORDER_NO, NO_PRINTED, ON_HOLD, APPLIESTO_DOC_TYPE, APPLIES_TO_DOC_TYPE, APPLIESTO_DOC_NO, APPLIES_TO_DOC_NO, BAL_ACCOUNT_NO, VAT_REGISTRATION_NO, REASON_CODE, GEN_BUS_POSTING_GROUP, EU_3PARTY_TRADE, TRANSACTION_TYPE, EU_3_PARTY_TRADE, TRANSPORT_METHOD, VAT_COUNTRY_REGION_CODE, SELLTO_CUSTOMER_NAME, SELL_TO_CUSTOMER_NAME, SELLTO_CUSTOMER_NAME_2, SELLTO_ADDRESS, SELL_TO_CUSTOMER_NAME_2, SELLTO_ADDRESS_2, SELL_TO_ADDRESS, SELLTO_CITY, SELL_TO_ADDRESS_2, SELL_TO_CITY, SELLTO_CONTACT, SELL_TO_CONTACT, BILLTO_POST_CODE, BILL_TO_POST_CODE, BILLTO_COUNTY, BILL_TO_COUNTY, BILLTO_COUNTRY_REGION_CODE, BILL_TO_COUNTRY_REGION_CODE, SELLTO_POST_CODE, SELL_TO_POST_CODE, SELLTO_COUNTY, SELL_TO_COUNTY, SELLTO_COUNTRY_REGION_CODE, SHIPTO_POST_CODE, SELL_TO_COUNTRY_REGION_CODE, SHIPTO_COUNTY, SHIP_TO_POST_CODE, SHIP_TO_COUNTY, SHIPTO_COUNTRY_REGION_CODE, SHIP_TO_COUNTRY_REGION_CODE, BAL_ACCOUNT_TYPE, EXIT_POINT, CORRECTION, DOCUMENT_DATE, EXTERNAL_DOCUMENT_NO, AREA, TRANSACTION_SPECIFICATION, PAYMENT_METHOD_CODE, SHIPPING_AGENT_CODE, PACKAGE_TRACKING_NO, PREASSIGNED_NO_SERIES, NO_SERIES, PRE_ASSIGNED_NO_SERIES, ORDER_NO_SERIES, PREASSIGNED_NO, USER_ID, PRE_ASSIGNED_NO, SOURCE_CODE, TAX_AREA_CODE, TAX_LIABLE, VAT_BUS_POSTING_GROUP, VAT_BASE_DISCOUNT, INVOICE_DISCOUNT_CALCULATION, PREPAYMENT_NO_SERIES, INVOICE_DISCOUNT_VALUE, PREPAYMENT_INVOICE, PREPAYMENT_ORDER_NO, QUOTE_NO, DIMENSION_SET_ID, PAYMENT_INSTRUCTIONS_NAME, DOCUMENT_EXCHANGE_IDENTIFIER, PAYMENT_SERVICE_SET_ID, DOCUMENT_EXCHANGE_STATUS, DOC_EXCH_ORIGINAL_IDENTIFIER, COUPLED_TO_CRM, CREDIT_CARD_NO, DIRECT_DEBIT_MANDATE_ID, CANCELED_BY, CUST_LEDGER_ENTRY_NO, CAMPAIGN_NO, SELL_TO_CONTACT_NO, BILL_TO_CONTACT_NO, OPPORTUNITY_NO, SELLTO_CONTACT_NO, RESPONSIBILITY_CENTER, BILLTO_CONTACT_NO, ALLOW_LINE_DISC, GET_SHIPMENT_USED, INVOICE_TYPE, CONTAINER, CR_MEMO_TYPE, SEAL, TRUCK_LICENSE_PLATE, SPECIAL_SCHEME_CODE, ID, DRIVER, OPERATION_DESCRIPTION, BANK_ACCOUNT_TO_TRANSFER, OPERATION_DESCRIPTION_2, MOTIVO_DEVOLUCION_EDI, SUCCEEDED_COMPANY_NAME, SUCCEEDED_VAT_REGISTRATION_NO, DEPARTAMENTO_EDI, NO_MENSAJE_EDI, HEADER_DISCOUNT, RAPPEL, COD_CADENA, PROMOCIONES_POR, INVOICE_CANCELATION, APPLIESTO_BILL_NO, CORRECTED_INVOICE_TYPE, COD_DTO_LINEA, TIPO_PEDIDO, CUST_BANK_ACC_CODE, SIMPLIFIED_TYPE, TIPO_PUNTO_VERDE, FROM_TICKET_NO, PAYAT_CODE, TIPO_RESIDUOS, SELLTO_PHONE_NO, TO_TICKET_NO, SELLTO_EMAIL, APPLIES_TO_BILL_NO, EDI_NO_EXPORTACIONES, ID_TYPE, PAY_AT_CODE, IDPIRPF_IRPF_GROUP, CERRADO, PESO_NETO, DTO_FACTURA, IDPGND_SHIPTO_EXCL_SIG_RATE, PESO_BRUTO, FORMA_PAGO_FACTURAPLUS, IF_OR_TYPE, COD_BANCO, CASH_ENTRY, COMMISSION_SALESPERSON, IF_DATA_1, 2ND_SALESPERSON_CODE, SPETIAL_OPERATION_TYPE, IF_DATA_2, COMMISSION_2ND_SALESPERSON, SPETIAL_OPERATION_CODE, IF_DATA_3, IF_DATA_4, GROUP_SHIPMENT_TYPE, INCOME_TAX_RETENTION_CODE, SHIPMENT_FREQUENCY, INCOME_TAX_RETENTION, IF_DATA_5, INCOME_TAX_RETENTION_KEY, INVOICE_DETAIL, IF_ML_TIME, IF_SAE_CERTIFICATION, REVERTIDO_EN_NO_ABONO, INCOME_TAX_RETENTION_SUBKEY, NO_FACTURA_ANULADA, TEXTO_SUBVENCION, CO_ID_CONT, CO_ID_PREC, ANULACION_DE_FACTURA, REVERTIDA, TOTALNETWEIGHT, TOTALGROSSWEIGHT, PRINT_BY_CONCEPT, ODS_JOB_TEMPLATE_CODE, SIMPLIFIED_INVOICE, PACKAGESPALLETNUMBER, OPERATION_DATE, PACKAGESNUMBER, AIT_BILLING_ADDRESS_CODE, TO_DOCUMENT_NO, FROM_DOCUMENT_NO, IDPEDI_INVOIC, REGIME, IDPEDI_INVOIC_SENT, TAX_FREE, AIT_CREATION_USER, SII_CORRECTION_TYPE, AIT_CREATION_DATETIME, SII_OPERATION_DESCRIPTION, TAX_FREE_VENDOR_NO, TEC_DES_EMPRESA, CUSTOMER_SALES_DEPARTMENT, SII_INVOICE_TYPE, EDI_BLOCKED, TEC_ID_INGESTA, TEC_TS_INGESTA, SIMPLIFIED_ART_6_1_D, DESADV_NO, PLANNING_DELIVERY_DATE, REFEXTERNA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B, NUM_REGISTRO_ACUERDO_FACT, EMITIDA_POR_TERCERO, INVOICE_BLOCKED, EMITIDA_POR_NORMATIVA_DEL_GAS, INTERCOMPANY_INVOICE, CUSTOMER_RECEPTION_NO, PAYMENT_DATE, PAYMENT_NO_DAYS, EDI_ORDER_NO, SALES_SHIPMENT_NO, IPAD_ORDER, WITHOUT_PLASTIC_DECLARATION, FIX_SALES_PRICE, SCRAP_AMOUNT) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('sales_invoice_header_batch') }}
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
