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
                ITEM_NO, CODE, tec_des_empresa,
                HASH(TIMESTAMP, ITEM_NO, CODE, DESCRIPTION, DESCRIPTION_2, SALES_UNIT_OF_MEASURE, CHECKED, INTERNAL_REVIEW_DATE, PRODUCTION_LABEL_REPORT_NO, INJECTION_MANAGEMENT, GL19, GL1A_BOX, GL1A_PIECE, COMMERTIAL_DESCRIPTION, LANGUAGE_1, LANGUAGE_2, LANGUAGE_3, LANGUAGE_4, LANGUAGE_5, EXCEL_LANGUAGE, TRADERMARK_CODE, TECNICAL_CARD, PIECE_LABEL_CODE, BOX_LABEL_CODE, PALET_LABEL_CODE, PRINT_LABEL_DATE, PRINT_LABEL_USER, SHORT_DESC_LANGUAGE_1, SHORT_DESC_LANGUAGE_2, SHORT_DESC_LANGUAGE_3, SHORT_DESC_LANGUAGE_4, SHORT_DESC_LANGUAGE_5, DESC_LANGUAGE_1, DESC_LANGUAGE_2, DESC_LANGUAGE_3, DESC_LANGUAGE_4, DESC_LANGUAGE_5, INGR_LANGUAGE_1, INGR_LANGUAGE_2, INGR_LANGUAGE_3, INGR_LANGUAGE_4, INGR_LANGUAGE_5, ROUTING_NO, PACKING_BOM, MAQUILA, OLD_ITEM_NO, DECLARED_INJECTION, FIXED_WEIGHT, BOX_CODE, KG_BOX, BOX_ROW, ROWS_PALET, BOX_PALET, KG_PALET, UNITS_BOX, UNITS_PACKAGE, PACKAGES_BOX, UNITS_BOX_TXT, UNITS_PACKAGE_TXT, PACKAGES_BOX_TXT, UNITS_CONTAINER_TXT, LABEL_CODE, PRODUCT_MANUF_INSTRUCTION, SHIPMENT_REMARKS, TARIFF_NO, SALES_BLOCKED, GTIN, PALLET_CODE, JAIL_CODE, BUCKET_CODE, PLATE_MANDATORY, PLATE_CODE, PLATES_BOX, PLATES_PALLET, PLATE_WEIGHT, PLATES_TO_CREATE_ORDER, MANUFACTUING_DATE, QUANTITY_TO_CREATE_ORDER, QUANTITY_TO_CONSUME, EAN_TEMPLATE_BOX_1, EAN_TEMPLATE_BOX_2, EAN_TEMPLATE_PIECE_1, EAN_TEMPLATE_PIECE_2, EAN_CODE_BOX_1, EAN_CODE_BOX_2, EAN_CODE_PIECE_1, EAN_CODE_PIECE_2, EAN_TYPE_BOX_1, EAN_TYPE_BOX_2, EAN_TYPE_PIECE_1, EAN_TYPE_PIECE_2, PRINT_WEIGHT_PIECE, UNIT_PRICE, MANUFACTURED_BY, MEAT_PREPARATION, WATER_ADDED, MEAT_ORIGIN, CUSTOMER_NO, CUSTOMER_ITEM_NO, PRINT_CUSTOMER_ITEM_NO, PRINT_CUST_LOT_NO, PRINT_CUSTOMER_INFORMATION, MANUFACTURED_FOR, RM_CODE, KG_MP_TN, SALT_CODE, KG_SALT_TN, PACKAGING_WEIGHT, PLASTIC_WEIGHT_GR, PRESENTATION, TECNICAL_CARD_VERSION, TECNICAL_CARD_VERSION_DATE, WEIGHT_PIECE, THICK_PIECE, EXPIRATION_CALCULATION, ENFORCE_PLANNING, BOLD, BLOCK_VARIANT, BLOCK_PURCHASE_PRODUCTION, CONTROL_CAJAS_EXPEDICION, SUBSET_1, SUBSET_2, SUBSET_3, SUBSET_4, SUBSET_5, RM_FIELD_1, RM_FIELD_2, RM_FIELD_3, RM_COST_DISTR, PVP, R, FCP, TOLERANCIA_EXCESO_PESO, FCP_LOCATION, TOLERANCIA_CARGA_EXPEDICION, CONTROL_FABRICACION_MERCADONA, FILM_TOP, FILM_BOTTOM, BAG, LABEL, BOX_PALET_MERCADONA, PRODUCTION_GROUPED, GL19_PIECE, SCRAP_CATEGORY, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash,
                date(tec_ts_ingesta) as tec_fec_inicio
            from {{ ref('item_variant_batch') }}
        ) s
        on t.tec_cod_vigencia = 1
            and t.ITEM_NO is not distinct from s.ITEM_NO and t.CODE is not distinct from s.CODE and t.tec_des_empresa is not distinct from s.tec_des_empresa
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
{% set src_ref_coopecarn = source('bronze_coopecarn', 'COOPECARN_ITEM_VARIANT') %}
            update {{ log_ref }} l
            set tec_ts_integracion_b = current_timestamp()
            where l.tec_ts_integracion_b is null
              and l.start_watermark <> l.end_watermark
              and upper(l.database)   = upper('{{ src_ref_coopecarn.database }}')
              and upper(l.schema)     = upper('{{ src_ref_coopecarn.schema }}')
              and upper(l.table_name) = upper('{{ src_ref_coopecarn.identifier }}')
              and exists (
                  select 1
                  from {{ ref('item_variant_batch_coopecarn') }} v
                  where v.tec_id_ingesta = l.tec_id_ingesta
              );"
    ]
) }}

with source as (
    select
        timestamp, 
        item_no, 
        code, 
        description, 
        description_2, 
        sales_unit_of_measure, 
        checked, 
        internal_review_date, 
        production_label_report_no, 
        injection_management, 
        gl19, 
        gl1a_box, 
        gl1a_piece, 
        commertial_description, 
        language_1, 
        language_2, 
        language_3, 
        language_4, 
        language_5, 
        excel_language, 
        tradermark_code, 
        tecnical_card, 
        piece_label_code, 
        box_label_code, 
        palet_label_code, 
        print_label_date, 
        print_label_user, 
        short_desc_language_1, 
        short_desc_language_2, 
        short_desc_language_3, 
        short_desc_language_4, 
        short_desc_language_5, 
        desc_language_1, 
        desc_language_2, 
        desc_language_3, 
        desc_language_4, 
        desc_language_5, 
        ingr_language_1, 
        ingr_language_2, 
        ingr_language_3, 
        ingr_language_4, 
        ingr_language_5, 
        routing_no, 
        packing_bom, 
        maquila, 
        old_item_no, 
        declared_injection, 
        fixed_weight, 
        box_code, 
        kg_box, 
        box_row, 
        rows_palet, 
        box_palet, 
        kg_palet, 
        units_box, 
        units_package, 
        packages_box, 
        units_box_txt, 
        units_package_txt, 
        packages_box_txt, 
        units_container_txt, 
        label_code, 
        product_manuf_instruction, 
        shipment_remarks, 
        tariff_no, 
        sales_blocked, 
        gtin, 
        pallet_code, 
        jail_code, 
        bucket_code, 
        plate_mandatory, 
        plate_code, 
        plates_box, 
        plates_pallet, 
        plate_weight, 
        plates_to_create_order, 
        manufactuing_date, 
        quantity_to_create_order, 
        quantity_to_consume, 
        ean_template_box_1, 
        ean_template_box_2, 
        ean_template_piece_1, 
        ean_template_piece_2, 
        ean_code_box_1, 
        ean_code_box_2, 
        ean_code_piece_1, 
        ean_code_piece_2, 
        ean_type_box_1, 
        ean_type_box_2, 
        ean_type_piece_1, 
        ean_type_piece_2, 
        print_weight_piece, 
        unit_price, 
        manufactured_by, 
        meat_preparation, 
        water_added, 
        meat_origin, 
        customer_no, 
        customer_item_no, 
        print_customer_item_no, 
        print_cust_lot_no, 
        print_customer_information, 
        manufactured_for, 
        rm_code, 
        kg_mp_tn, 
        salt_code, 
        kg_salt_tn, 
        packaging_weight, 
        plastic_weight_gr, 
        presentation, 
        tecnical_card_version, 
        tecnical_card_version_date, 
        weight_piece, 
        thick_piece, 
        expiration_calculation, 
        enforce_planning, 
        bold, 
        block_variant, 
        block_purchase_production, 
        control_cajas_expedicion, 
        subset_1, 
        subset_2, 
        subset_3, 
        subset_4, 
        subset_5, 
        rm_field_1, 
        rm_field_2, 
        rm_field_3, 
        rm_cost_distr, 
        pvp, 
        r, 
        fcp, 
        tolerancia_exceso_peso, 
        fcp_location, 
        tolerancia_carga_expedicion, 
        control_fabricacion_mercadona, 
        film_top, 
        film_bottom, 
        bag, 
        label, 
        box_palet_mercadona, 
        production_grouped, 
        gl19_piece, 
        scrap_category, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        tec_ts_staging, 
        tec_ts_integracion_b, 
        tec_des_cod_siglas, 
        tec_des_empresa, 
        tec_id_ingesta, 
        tec_ts_ingesta, 
        HASH(TIMESTAMP, ITEM_NO, CODE, DESCRIPTION, DESCRIPTION_2, SALES_UNIT_OF_MEASURE, CHECKED, INTERNAL_REVIEW_DATE, PRODUCTION_LABEL_REPORT_NO, INJECTION_MANAGEMENT, GL19, GL1A_BOX, GL1A_PIECE, COMMERTIAL_DESCRIPTION, LANGUAGE_1, LANGUAGE_2, LANGUAGE_3, LANGUAGE_4, LANGUAGE_5, EXCEL_LANGUAGE, TRADERMARK_CODE, TECNICAL_CARD, PIECE_LABEL_CODE, BOX_LABEL_CODE, PALET_LABEL_CODE, PRINT_LABEL_DATE, PRINT_LABEL_USER, SHORT_DESC_LANGUAGE_1, SHORT_DESC_LANGUAGE_2, SHORT_DESC_LANGUAGE_3, SHORT_DESC_LANGUAGE_4, SHORT_DESC_LANGUAGE_5, DESC_LANGUAGE_1, DESC_LANGUAGE_2, DESC_LANGUAGE_3, DESC_LANGUAGE_4, DESC_LANGUAGE_5, INGR_LANGUAGE_1, INGR_LANGUAGE_2, INGR_LANGUAGE_3, INGR_LANGUAGE_4, INGR_LANGUAGE_5, ROUTING_NO, PACKING_BOM, MAQUILA, OLD_ITEM_NO, DECLARED_INJECTION, FIXED_WEIGHT, BOX_CODE, KG_BOX, BOX_ROW, ROWS_PALET, BOX_PALET, KG_PALET, UNITS_BOX, UNITS_PACKAGE, PACKAGES_BOX, UNITS_BOX_TXT, UNITS_PACKAGE_TXT, PACKAGES_BOX_TXT, UNITS_CONTAINER_TXT, LABEL_CODE, PRODUCT_MANUF_INSTRUCTION, SHIPMENT_REMARKS, TARIFF_NO, SALES_BLOCKED, GTIN, PALLET_CODE, JAIL_CODE, BUCKET_CODE, PLATE_MANDATORY, PLATE_CODE, PLATES_BOX, PLATES_PALLET, PLATE_WEIGHT, PLATES_TO_CREATE_ORDER, MANUFACTUING_DATE, QUANTITY_TO_CREATE_ORDER, QUANTITY_TO_CONSUME, EAN_TEMPLATE_BOX_1, EAN_TEMPLATE_BOX_2, EAN_TEMPLATE_PIECE_1, EAN_TEMPLATE_PIECE_2, EAN_CODE_BOX_1, EAN_CODE_BOX_2, EAN_CODE_PIECE_1, EAN_CODE_PIECE_2, EAN_TYPE_BOX_1, EAN_TYPE_BOX_2, EAN_TYPE_PIECE_1, EAN_TYPE_PIECE_2, PRINT_WEIGHT_PIECE, UNIT_PRICE, MANUFACTURED_BY, MEAT_PREPARATION, WATER_ADDED, MEAT_ORIGIN, CUSTOMER_NO, CUSTOMER_ITEM_NO, PRINT_CUSTOMER_ITEM_NO, PRINT_CUST_LOT_NO, PRINT_CUSTOMER_INFORMATION, MANUFACTURED_FOR, RM_CODE, KG_MP_TN, SALT_CODE, KG_SALT_TN, PACKAGING_WEIGHT, PLASTIC_WEIGHT_GR, PRESENTATION, TECNICAL_CARD_VERSION, TECNICAL_CARD_VERSION_DATE, WEIGHT_PIECE, THICK_PIECE, EXPIRATION_CALCULATION, ENFORCE_PLANNING, BOLD, BLOCK_VARIANT, BLOCK_PURCHASE_PRODUCTION, CONTROL_CAJAS_EXPEDICION, SUBSET_1, SUBSET_2, SUBSET_3, SUBSET_4, SUBSET_5, RM_FIELD_1, RM_FIELD_2, RM_FIELD_3, RM_COST_DISTR, PVP, R, FCP, TOLERANCIA_EXCESO_PESO, FCP_LOCATION, TOLERANCIA_CARGA_EXPEDICION, CONTROL_FABRICACION_MERCADONA, FILM_TOP, FILM_BOTTOM, BAG, LABEL, BOX_PALET_MERCADONA, PRODUCTION_GROUPED, GL19_PIECE, SCRAP_CATEGORY, TEC_DES_EMPRESA, TEC_ID_INGESTA, TEC_TS_INGESTA, TEC_TS_STAGING, TEC_TS_INTEGRACION_B) AS tec_hash, 
        date(tec_ts_ingesta) as tec_fec_inicio
    from {{ ref('item_variant_batch') }}
)
select
    s.*,
    to_date('2999-12-31')             as tec_fec_fin,
    1                                 as tec_cod_vigencia,
    current_timestamp()               as tec_ts_integracion_s
from source s
{% if is_incremental() %}
left join {{ this }} t
    on  t.ITEM_NO is not distinct from s.ITEM_NO and t.CODE is not distinct from s.CODE and t.tec_des_empresa is not distinct from s.tec_des_empresa
    and t.tec_cod_vigencia    = 1
where t.ITEM_NO is null
    or t.tec_hash <> s.tec_hash
{% endif %}
