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
                HASH(TIMESTAMP, DOCUMENT_NO, LINE_NO, SELL_TO_CUSTOMER_NO, TYPE, NO, LOCATION_CODE, POSTING_GROUP, SHIPMENT_DATE, DESCRIPTION, DESCRIPTION_2, UNIT_OF_MEASURE, QUANTITY, UNIT_PRICE, UNIT_COST_LCY, VAT, LINE_DISCOUNT, ALLOW_INVOICE_DISC, GROSS_WEIGHT, NET_WEIGHT, UNITS_PER_PARCEL, UNIT_VOLUME, APPL_TO_ITEM_ENTRY, ITEM_SHPT_ENTRY_NO, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_PRICE_GROUP, JOB_NO, WORK_TYPE_CODE, QTY_SHIPPED_NOT_INVOICED, QUANTITY_INVOICED, ORDER_NO, ORDER_LINE_NO, BILL_TO_CUSTOMER_NO, PURCHASE_ORDER_NO, PURCH_ORDER_LINE_NO, DROP_SHIPMENT, GEN_BUS_POSTING_GROUP, GEN_PROD_POSTING_GROUP, VAT_CALCULATION_TYPE, TRANSACTION_TYPE, TRANSPORT_METHOD, ATTACHED_TO_LINE_NO, EXIT_POINT, AREA, TRANSACTION_SPECIFICATION, TAX_AREA_CODE, TAX_LIABLE, TAX_GROUP_CODE, VAT_BUS_POSTING_GROUP, VAT_PROD_POSTING_GROUP, BLANKET_ORDER_NO, BLANKET_ORDER_LINE_NO, VAT_BASE_AMOUNT, UNIT_COST, POSTING_DATE, DIMENSION_SET_ID, AUTHORIZED_FOR_CREDIT_CARD, JOB_TASK_NO, JOB_CONTRACT_ENTRY_NO, VARIANT_CODE, BIN_CODE, QTY_PER_UNIT_OF_MEASURE, UNIT_OF_MEASURE_CODE, QUANTITY_BASE, QTY_INVOICED_BASE, FA_POSTING_DATE, DEPRECIATION_BOOK_CODE, DEPR_UNTIL_FA_POSTING_DATE, DUPLICATE_IN_DEPRECIATION_BOOK, USE_DUPLICATION_LIST, RESPONSIBILITY_CENTER, CROSS_REFERENCE_NO, UNIT_OF_MEASURE_CROSS_REF, CROSS_REFERENCE_TYPE, CROSS_REFERENCE_TYPE_NO, ITEM_CATEGORY_CODE, NONSTOCK, PURCHASING_CODE, PRODUCT_GROUP_CODE, REQUESTED_DELIVERY_DATE, PROMISED_DELIVERY_DATE, SHIPPING_TIME, OUTBOUND_WHSE_HANDLING_TIME, PLANNED_DELIVERY_DATE, PLANNED_SHIPMENT_DATE, APPL_FROM_ITEM_ENTRY, ITEM_CHARGE_BASE_AMOUNT, CORRECTION, RETURN_REASON_CODE, ALLOW_LINE_DISC, CUSTOMER_DISC_GROUP, MURANO_CODE, COD_CADENA, KG, PROMOCIONES_POR, BOXES, CODIGO_EMBALAJE, PALLETS, CANTIDAD_EMBALAJE, ROWS_PALET, CANTIDAD_UNIDADES, CANTIDAD_KILOS, SELECTED_UNIT_TYPE, COM_1, CANTIDAD_EMBALAJE_FACTURADA, COM_2, CANTIDAD_UNIDADES_FACTURADA, CANTIDAD_KILOS_FACTURADO, COM_3, AMOUNT, NO_PRESENTACION, AMOUNT_INCLUDING_VAT, DESCRIPCION_PRESENTACION, LINE_DISC_AMOUNT, OBSERVACIONES, MOTIVO_DEVOLUCION, PMT_DISC_GIVEN_AMOUNT, CANTIDAD_EMBALAJE_PDTE, INV_DISCOUNT_AMOUNT, CANTIDAD_UNIDADES_PDTE, LINE_AMOUNT, DTO_1, CANTIDAD_KILOS_PDTE, SERVICIO, TIPO_DTO_2, DTO_2, PRECIO_PUNTO_VERDE_CDAD, TIPO_DTO_3, UNIDAD_MEDIDA_PUNTO_VERDE, DTO_3, GRUPO_RESIDUOS, PRECIO_RESIDUOS_CDAD, TIPO_DTO_4, DTO_4, UNIDAD_MEDIDA_RESIDUOS, IMPORTE_RESIDUOS, TIPO_DTO_5, LINEA_EMBALAJE, DTO_5, FACTURADO_COMPLETAMENTE, PDTE_ENVIO_EMBALAJE, PRECIO_ORIGEN, CODIGO_PALET, DTO_ORIGEN, CANTIDAD_PALETS, NO_ENVIO, IMPORTE_PROVISIONADO, IMPORTE_DESPROVISIONADO, NO_LINEA_ENVIO, NO_ENVIO_REGISTRADO, NO_CUENTA_PROVISION_VENTA, NO_LINEA_ENVIO_REGISTRADO, NO_CUENTA_PROVISION_CLIENTE, COMENTARIO_GENERAL, CLAIM_CODE, COMENTARIO_ASOCIADO, EDI_POSITION, FECHA_DE_PEDIDO, CREATION_DATE, PLATE_SHIPPED, INCOME_TAX_RETENTION_CODE, INCOME_TAX_RETENCION, PLATE_CONFIRMED, INCOME_TAX_BASE, FCP, PROD_ORDER_NO, SALESPERSON_CODE, COMMISSION_SALESPERSON, PROD_ORDER_LINE_NO, 2ND_SALESPERSON_CODE, MANUFACTURING_DATE, Q_CONFIRMADA_RECADV, COMMISSION_2ND_SALESPERSON, EC_AMOUNT, LINEA_CONFIRMADA, EC, TEC_DES_EMPRESA, TEC_ID_INGESTA, VAT_AMOUNT, LINE_DISCOUNT_AMOUNT, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B, SPECIAL_LINE_TYPE, TARIFF_NO, RECADCV_QUANTITY, EDI_POSITION_SUBLINE, GLN, EDI_PRICE, EDI_DISCOUNT) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('sales_shipment_line_batch') }}
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
{% set src_ref_coopecarn = source('bronze_coopecarn', 'COOPECARN_SALES_SHIPMENT_LINE') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_coopecarn.database }}')
              and upper(l.schema)     = upper('{{ src_ref_coopecarn.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_coopecarn.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_shipment_line_batch_coopecarn') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );
{% set src_ref_fricafor = source('bronze_fricafor', 'FRICAFOR_SALES_SHIPMENT_LINE') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_fricafor.database }}')
              and upper(l.schema)     = upper('{{ src_ref_fricafor.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_fricafor.identifier }}')
              and exists (
                  select 1
                  from {{ ref('sales_shipment_line_batch_fricafor') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        document_no, 
        line_no, 
        sell_to_customer_no, 
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
        item_shpt_entry_no, 
        shortcut_dimension_1_code, 
        shortcut_dimension_2_code, 
        customer_price_group, 
        job_no, 
        work_type_code, 
        qty_shipped_not_invoiced, 
        quantity_invoiced, 
        order_no, 
        order_line_no, 
        bill_to_customer_no, 
        purchase_order_no, 
        purch_order_line_no, 
        drop_shipment, 
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
        authorized_for_credit_card, 
        job_task_no, 
        job_contract_entry_no, 
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
        cross_reference_no, 
        unit_of_measure_cross_ref, 
        cross_reference_type, 
        cross_reference_type_no, 
        item_category_code, 
        nonstock, 
        purchasing_code, 
        product_group_code, 
        requested_delivery_date, 
        promised_delivery_date, 
        shipping_time, 
        outbound_whse_handling_time, 
        planned_delivery_date, 
        planned_shipment_date, 
        appl_from_item_entry, 
        item_charge_base_amount, 
        correction, 
        return_reason_code, 
        allow_line_disc, 
        customer_disc_group, 
        murano_code, 
        cod_cadena, 
        kg, 
        promociones_por, 
        boxes, 
        codigo_embalaje, 
        pallets, 
        cantidad_embalaje, 
        rows_palet, 
        cantidad_unidades, 
        cantidad_kilos, 
        selected_unit_type, 
        com_1, 
        cantidad_embalaje_facturada, 
        com_2, 
        cantidad_unidades_facturada, 
        cantidad_kilos_facturado, 
        com_3, 
        amount, 
        no_presentacion, 
        amount_including_vat, 
        descripcion_presentacion, 
        line_disc_amount, 
        observaciones, 
        motivo_devolucion, 
        pmt_disc_given_amount, 
        cantidad_embalaje_pdte, 
        inv_discount_amount, 
        cantidad_unidades_pdte, 
        line_amount, 
        dto_1, 
        cantidad_kilos_pdte, 
        servicio, 
        tipo_dto_2, 
        dto_2, 
        precio_punto_verde_cdad, 
        tipo_dto_3, 
        unidad_medida_punto_verde, 
        dto_3, 
        grupo_residuos, 
        precio_residuos_cdad, 
        tipo_dto_4, 
        dto_4, 
        unidad_medida_residuos, 
        importe_residuos, 
        tipo_dto_5, 
        linea_embalaje, 
        dto_5, 
        facturado_completamente, 
        pdte_envio_embalaje, 
        precio_origen, 
        codigo_palet, 
        dto_origen, 
        cantidad_palets, 
        no_envio, 
        importe_provisionado, 
        importe_desprovisionado, 
        no_linea_envio, 
        no_envio_registrado, 
        no_cuenta_provision_venta, 
        no_linea_envio_registrado, 
        no_cuenta_provision_cliente, 
        comentario_general, 
        claim_code, 
        comentario_asociado, 
        edi_position, 
        fecha_de_pedido, 
        creation_date, 
        plate_shipped, 
        income_tax_retention_code, 
        income_tax_retencion, 
        plate_confirmed, 
        income_tax_base, 
        fcp, 
        prod_order_no, 
        salesperson_code, 
        commission_salesperson, 
        prod_order_line_no, 
        2nd_salesperson_code, 
        manufacturing_date, 
        q_confirmada_recadv, 
        commission_2nd_salesperson, 
        ec_amount, 
        linea_confirmada, 
        ec, 
        tec_des_empresa, 
        tec_id_ingesta, 
        vat_amount, 
        line_discount_amount, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        special_line_type, 
        tariff_no, 
        recadcv_quantity, 
        edi_position_subline, 
        gln, 
        edi_price, 
        edi_discount, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, DOCUMENT_NO, LINE_NO, SELL_TO_CUSTOMER_NO, TYPE, NO, LOCATION_CODE, POSTING_GROUP, SHIPMENT_DATE, DESCRIPTION, DESCRIPTION_2, UNIT_OF_MEASURE, QUANTITY, UNIT_PRICE, UNIT_COST_LCY, VAT, LINE_DISCOUNT, ALLOW_INVOICE_DISC, GROSS_WEIGHT, NET_WEIGHT, UNITS_PER_PARCEL, UNIT_VOLUME, APPL_TO_ITEM_ENTRY, ITEM_SHPT_ENTRY_NO, SHORTCUT_DIMENSION_1_CODE, SHORTCUT_DIMENSION_2_CODE, CUSTOMER_PRICE_GROUP, JOB_NO, WORK_TYPE_CODE, QTY_SHIPPED_NOT_INVOICED, QUANTITY_INVOICED, ORDER_NO, ORDER_LINE_NO, BILL_TO_CUSTOMER_NO, PURCHASE_ORDER_NO, PURCH_ORDER_LINE_NO, DROP_SHIPMENT, GEN_BUS_POSTING_GROUP, GEN_PROD_POSTING_GROUP, VAT_CALCULATION_TYPE, TRANSACTION_TYPE, TRANSPORT_METHOD, ATTACHED_TO_LINE_NO, EXIT_POINT, AREA, TRANSACTION_SPECIFICATION, TAX_AREA_CODE, TAX_LIABLE, TAX_GROUP_CODE, VAT_BUS_POSTING_GROUP, VAT_PROD_POSTING_GROUP, BLANKET_ORDER_NO, BLANKET_ORDER_LINE_NO, VAT_BASE_AMOUNT, UNIT_COST, POSTING_DATE, DIMENSION_SET_ID, AUTHORIZED_FOR_CREDIT_CARD, JOB_TASK_NO, JOB_CONTRACT_ENTRY_NO, VARIANT_CODE, BIN_CODE, QTY_PER_UNIT_OF_MEASURE, UNIT_OF_MEASURE_CODE, QUANTITY_BASE, QTY_INVOICED_BASE, FA_POSTING_DATE, DEPRECIATION_BOOK_CODE, DEPR_UNTIL_FA_POSTING_DATE, DUPLICATE_IN_DEPRECIATION_BOOK, USE_DUPLICATION_LIST, RESPONSIBILITY_CENTER, CROSS_REFERENCE_NO, UNIT_OF_MEASURE_CROSS_REF, CROSS_REFERENCE_TYPE, CROSS_REFERENCE_TYPE_NO, ITEM_CATEGORY_CODE, NONSTOCK, PURCHASING_CODE, PRODUCT_GROUP_CODE, REQUESTED_DELIVERY_DATE, PROMISED_DELIVERY_DATE, SHIPPING_TIME, OUTBOUND_WHSE_HANDLING_TIME, PLANNED_DELIVERY_DATE, PLANNED_SHIPMENT_DATE, APPL_FROM_ITEM_ENTRY, ITEM_CHARGE_BASE_AMOUNT, CORRECTION, RETURN_REASON_CODE, ALLOW_LINE_DISC, CUSTOMER_DISC_GROUP, MURANO_CODE, COD_CADENA, KG, PROMOCIONES_POR, BOXES, CODIGO_EMBALAJE, PALLETS, CANTIDAD_EMBALAJE, ROWS_PALET, CANTIDAD_UNIDADES, CANTIDAD_KILOS, SELECTED_UNIT_TYPE, COM_1, CANTIDAD_EMBALAJE_FACTURADA, COM_2, CANTIDAD_UNIDADES_FACTURADA, CANTIDAD_KILOS_FACTURADO, COM_3, AMOUNT, NO_PRESENTACION, AMOUNT_INCLUDING_VAT, DESCRIPCION_PRESENTACION, LINE_DISC_AMOUNT, OBSERVACIONES, MOTIVO_DEVOLUCION, PMT_DISC_GIVEN_AMOUNT, CANTIDAD_EMBALAJE_PDTE, INV_DISCOUNT_AMOUNT, CANTIDAD_UNIDADES_PDTE, LINE_AMOUNT, DTO_1, CANTIDAD_KILOS_PDTE, SERVICIO, TIPO_DTO_2, DTO_2, PRECIO_PUNTO_VERDE_CDAD, TIPO_DTO_3, UNIDAD_MEDIDA_PUNTO_VERDE, DTO_3, GRUPO_RESIDUOS, PRECIO_RESIDUOS_CDAD, TIPO_DTO_4, DTO_4, UNIDAD_MEDIDA_RESIDUOS, IMPORTE_RESIDUOS, TIPO_DTO_5, LINEA_EMBALAJE, DTO_5, FACTURADO_COMPLETAMENTE, PDTE_ENVIO_EMBALAJE, PRECIO_ORIGEN, CODIGO_PALET, DTO_ORIGEN, CANTIDAD_PALETS, NO_ENVIO, IMPORTE_PROVISIONADO, IMPORTE_DESPROVISIONADO, NO_LINEA_ENVIO, NO_ENVIO_REGISTRADO, NO_CUENTA_PROVISION_VENTA, NO_LINEA_ENVIO_REGISTRADO, NO_CUENTA_PROVISION_CLIENTE, COMENTARIO_GENERAL, CLAIM_CODE, COMENTARIO_ASOCIADO, EDI_POSITION, FECHA_DE_PEDIDO, CREATION_DATE, PLATE_SHIPPED, INCOME_TAX_RETENTION_CODE, INCOME_TAX_RETENCION, PLATE_CONFIRMED, INCOME_TAX_BASE, FCP, PROD_ORDER_NO, SALESPERSON_CODE, COMMISSION_SALESPERSON, PROD_ORDER_LINE_NO, 2ND_SALESPERSON_CODE, MANUFACTURING_DATE, Q_CONFIRMADA_RECADV, COMMISSION_2ND_SALESPERSON, EC_AMOUNT, LINEA_CONFIRMADA, EC, TEC_DES_EMPRESA, TEC_ID_INGESTA, VAT_AMOUNT, LINE_DISCOUNT_AMOUNT, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B, SPECIAL_LINE_TYPE, TARIFF_NO, RECADCV_QUANTITY, EDI_POSITION_SUBLINE, GLN, EDI_PRICE, EDI_DISCOUNT) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('sales_shipment_line_batch') }}
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
