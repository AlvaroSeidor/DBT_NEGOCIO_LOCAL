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
                DOCUMENT_NO, LINE_NO, tec_des_empresa,
                HASH(DOCUMENT_NO, LINE_NO, SELLTO_CUSTOMER_NO, TYPE, NO, LOCATION_CODE, POSTING_GROUP, SHIPMENT_DATE, DESCRIPTION, DESCRIPTION_2, UNIT_OF_MEASURE, QUANTITY, UNIT_PRICE, UNIT_COST_LCY, VAT, LINE_DISCOUNT, ALLOW_INVOICE_DISC, GROSS_WEIGHT, NET_WEIGHT, UNITS_PER_PARCEL, UNIT_VOLUME, APPL_TO_ITEM_ENTRY, ITEM_RCPT_ENTRY_NO, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_PRICE_GROUP, JOB_NO, WORK_TYPE_CODE, QUANTITY_INVOICED, BILLTO_CUSTOMER_NO, GEN_BUS_POSTING_GROUP, GEN_PROD_POSTING_GROUP, VAT_CALCULATION_TYPE, TRANSACTION_TYPE, TRANSPORT_METHOD, ATTACHED_TO_LINE_NO, EXIT_POINT, AREA, TRANSACTION_SPECIFICATION, TAX_AREA_CODE, TAX_LIABLE, TAX_GROUP_CODE, VAT_BUS_POSTING_GROUP, VAT_PROD_POSTING_GROUP, BLANKET_ORDER_NO, BLANKET_ORDER_LINE_NO, VAT_BASE_AMOUNT, UNIT_COST, POSTING_DATE, DIMENSION_SET_ID, VARIANT_CODE, BIN_CODE, QTY_PER_UNIT_OF_MEASURE, UNIT_OF_MEASURE_CODE, QUANTITY_BASE, QTY_INVOICED_BASE, FA_POSTING_DATE, DEPRECIATION_BOOK_CODE, DEPR_UNTIL_FA_POSTING_DATE, DUPLICATE_IN_DEPRECIATION_BOOK, USE_DUPLICATION_LIST, RESPONSIBILITY_CENTER, CROSSREFERENCE_NO, UNIT_OF_MEASURE_CROSS_REF, CROSSREFERENCE_TYPE, CROSSREFERENCE_TYPE_NO, ITEM_CATEGORY_CODE, NONSTOCK, PURCHASING_CODE, PRODUCT_GROUP_CODE, RETURN_QTY_RCD_NOT_INVD, APPL_FROM_ITEM_ENTRY, ITEM_CHARGE_BASE_AMOUNT, CORRECTION, RETURN_ORDER_NO, RETURN_ORDER_LINE_NO, RETURN_REASON_CODE, ALLOW_LINE_DISC, CUSTOMER_DISC_GROUP, IDPVUM_QUANTITY, IDPVUM_UNIT_OF_MEASURE_CODE, IDPVUM_UNIT_PRICE, IDPVUM_PRICE_IN_VUM, IDPVUM_VUM_PER_UNIT, IDPVUM_QUANTITY_INVOICED, IDPVUM_RETN_QTY_RD_NOT_INV, IDPGND_SIG_AMOUNT, IDPGND_SIG_RATE, IDPGND_SIG, IDPGND_PACKAGING_FORMAT_NO, IDPVSH_LINE_DISCOUNT_AMOUNT, IDPVSH_AMOUNT, IDPVSH_AMOUNT_INCLUDING_VAT, IDPVSH_INV_DISCOUNT_AMOUNT, IDPVSH_LINE_AMOUNT, IDPVSH_VAT_DIFFERENCE, IDPVSH_VAT_IDENTIFIER, IDPVSH_PMT_DISC_RCD_AMOUNT, IF_OR_TYPE, IF_UNIT_PRICE_CODE, IF_NUM_BOXES_NEEDED, IF_NUM_CONT_NEEDED, IF_DUN14, IDPVSES_EC, IDPVSES_EC_DIFFERENCE, CERRADO, TEMPQTY, ORIGINAL_QUANTITY, ORIGINAL_QUANTITY_UMV, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('return_rcpt_line_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.DOCUMENT_NO is not distinct from s.DOCUMENT_NO and t.LINE_NO is not distinct from s.LINE_NO and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_delisano = source('bronze_delisano', 'V_DL_RETURN_RCPT_LINE') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_delisano.database }}')
              and upper(l.schema)     = upper('{{ src_ref_delisano.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_delisano.identifier }}')
              and exists (
                  select 1
                  from {{ ref('return_rcpt_line_batch_delisano') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        document_no, 
        line_no, 
        sellto_customer_no, 
        type, 
        no, 
        location_code, 
        posting_group, 
        shipment_date, 
        description, 
        description_2, 
        unit_of_measure, 
        quantity, 
        unit_price, 
        unit_cost_lcy, 
        vat, 
        line_discount, 
        allow_invoice_disc, 
        gross_weight, 
        net_weight, 
        units_per_parcel, 
        unit_volume, 
        appl_to_item_entry, 
        item_rcpt_entry_no, 
        shortcut_dimension_1_code, 
        shortcut_dimension_2_code, 
        customer_price_group, 
        job_no, 
        work_type_code, 
        quantity_invoiced, 
        billto_customer_no, 
        gen_bus_posting_group, 
        gen_prod_posting_group, 
        vat_calculation_type, 
        transaction_type, 
        transport_method, 
        attached_to_line_no, 
        exit_point, 
        area, 
        transaction_specification, 
        tax_area_code, 
        tax_liable, 
        tax_group_code, 
        vat_bus_posting_group, 
        vat_prod_posting_group, 
        blanket_order_no, 
        blanket_order_line_no, 
        vat_base_amount, 
        unit_cost, 
        posting_date, 
        dimension_set_id, 
        variant_code, 
        bin_code, 
        qty_per_unit_of_measure, 
        unit_of_measure_code, 
        quantity_base, 
        qty_invoiced_base, 
        fa_posting_date, 
        depreciation_book_code, 
        depr_until_fa_posting_date, 
        duplicate_in_depreciation_book, 
        use_duplication_list, 
        responsibility_center, 
        crossreference_no, 
        unit_of_measure_cross_ref, 
        crossreference_type, 
        crossreference_type_no, 
        item_category_code, 
        nonstock, 
        purchasing_code, 
        product_group_code, 
        return_qty_rcd_not_invd, 
        appl_from_item_entry, 
        item_charge_base_amount, 
        correction, 
        return_order_no, 
        return_order_line_no, 
        return_reason_code, 
        allow_line_disc, 
        customer_disc_group, 
        idpvum_quantity, 
        idpvum_unit_of_measure_code, 
        idpvum_unit_price, 
        idpvum_price_in_vum, 
        idpvum_vum_per_unit, 
        idpvum_quantity_invoiced, 
        idpvum_retn_qty_rd_not_inv, 
        idpgnd_sig_amount, 
        idpgnd_sig_rate, 
        idpgnd_sig, 
        idpgnd_packaging_format_no, 
        idpvsh_line_discount_amount, 
        idpvsh_amount, 
        idpvsh_amount_including_vat, 
        idpvsh_inv_discount_amount, 
        idpvsh_line_amount, 
        idpvsh_vat_difference, 
        idpvsh_vat_identifier, 
        idpvsh_pmt_disc_rcd_amount, 
        if_or_type, 
        if_unit_price_code, 
        if_num_boxes_needed, 
        if_num_cont_needed, 
        if_dun14, 
        idpvses_ec, 
        idpvses_ec_difference, 
        cerrado, 
        tempqty, 
        original_quantity, 
        original_quantity_umv, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(DOCUMENT_NO, LINE_NO, SELLTO_CUSTOMER_NO, TYPE, NO, LOCATION_CODE, POSTING_GROUP, SHIPMENT_DATE, DESCRIPTION, DESCRIPTION_2, UNIT_OF_MEASURE, QUANTITY, UNIT_PRICE, UNIT_COST_LCY, VAT, LINE_DISCOUNT, ALLOW_INVOICE_DISC, GROSS_WEIGHT, NET_WEIGHT, UNITS_PER_PARCEL, UNIT_VOLUME, APPL_TO_ITEM_ENTRY, ITEM_RCPT_ENTRY_NO, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_PRICE_GROUP, JOB_NO, WORK_TYPE_CODE, QUANTITY_INVOICED, BILLTO_CUSTOMER_NO, GEN_BUS_POSTING_GROUP, GEN_PROD_POSTING_GROUP, VAT_CALCULATION_TYPE, TRANSACTION_TYPE, TRANSPORT_METHOD, ATTACHED_TO_LINE_NO, EXIT_POINT, AREA, TRANSACTION_SPECIFICATION, TAX_AREA_CODE, TAX_LIABLE, TAX_GROUP_CODE, VAT_BUS_POSTING_GROUP, VAT_PROD_POSTING_GROUP, BLANKET_ORDER_NO, BLANKET_ORDER_LINE_NO, VAT_BASE_AMOUNT, UNIT_COST, POSTING_DATE, DIMENSION_SET_ID, VARIANT_CODE, BIN_CODE, QTY_PER_UNIT_OF_MEASURE, UNIT_OF_MEASURE_CODE, QUANTITY_BASE, QTY_INVOICED_BASE, FA_POSTING_DATE, DEPRECIATION_BOOK_CODE, DEPR_UNTIL_FA_POSTING_DATE, DUPLICATE_IN_DEPRECIATION_BOOK, USE_DUPLICATION_LIST, RESPONSIBILITY_CENTER, CROSSREFERENCE_NO, UNIT_OF_MEASURE_CROSS_REF, CROSSREFERENCE_TYPE, CROSSREFERENCE_TYPE_NO, ITEM_CATEGORY_CODE, NONSTOCK, PURCHASING_CODE, PRODUCT_GROUP_CODE, RETURN_QTY_RCD_NOT_INVD, APPL_FROM_ITEM_ENTRY, ITEM_CHARGE_BASE_AMOUNT, CORRECTION, RETURN_ORDER_NO, RETURN_ORDER_LINE_NO, RETURN_REASON_CODE, ALLOW_LINE_DISC, CUSTOMER_DISC_GROUP, IDPVUM_QUANTITY, IDPVUM_UNIT_OF_MEASURE_CODE, IDPVUM_UNIT_PRICE, IDPVUM_PRICE_IN_VUM, IDPVUM_VUM_PER_UNIT, IDPVUM_QUANTITY_INVOICED, IDPVUM_RETN_QTY_RD_NOT_INV, IDPGND_SIG_AMOUNT, IDPGND_SIG_RATE, IDPGND_SIG, IDPGND_PACKAGING_FORMAT_NO, IDPVSH_LINE_DISCOUNT_AMOUNT, IDPVSH_AMOUNT, IDPVSH_AMOUNT_INCLUDING_VAT, IDPVSH_INV_DISCOUNT_AMOUNT, IDPVSH_LINE_AMOUNT, IDPVSH_VAT_DIFFERENCE, IDPVSH_VAT_IDENTIFIER, IDPVSH_PMT_DISC_RCD_AMOUNT, IF_OR_TYPE, IF_UNIT_PRICE_CODE, IF_NUM_BOXES_NEEDED, IF_NUM_CONT_NEEDED, IF_DUN14, IDPVSES_EC, IDPVSES_EC_DIFFERENCE, CERRADO, TEMPQTY, ORIGINAL_QUANTITY, ORIGINAL_QUANTITY_UMV, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('return_rcpt_line_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.DOCUMENT_NO is not distinct from s.DOCUMENT_NO and t.LINE_NO is not distinct from s.LINE_NO and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.DOCUMENT_NO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
