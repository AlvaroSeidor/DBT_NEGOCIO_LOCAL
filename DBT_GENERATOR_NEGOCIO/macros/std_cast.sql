{% macro std_cast(expr, dtype, fmt=None, scale=None) %}
    {# 1) Prepara la expresión base, limpia espacios y convierte '' a NULL #}
    {%- set e = "NULLIF(TRIM(" ~ expr ~ "), '')" -%}

    {# 2) Normaliza el tipo a mayúsculas #}
    {%- set t = (dtype or 'VARCHAR') | upper -%}

    {# 3) Ramas por tipo destino: #}

    {%- if t in ["TEXT","STRING","VARCHAR"] %}
        {{ e }}  {# devuelve texto limpio o NULL si '' #}

    {%- elif t in ["INT","INTEGER"] %}
        (TRY_TO_NUMBER({{ e }}))::NUMBER(38, 0)

    {%- elif t in ["NUMBER","DECIMAL","NUMERIC"] %}
        {%- if scale is not none %}
            (TRY_TO_NUMBER({{ e }}, 38, {{ scale }}))::NUMBER(38,{{ scale }})
        {%- else %}
            (TRY_TO_NUMBER({{ e }}))::NUMBER(38, 0)
        {%- endif %}

    {%- elif t in ["DATE"] %}
        {%- if fmt %}
            TRY_TO_DATE({{ e }}, '{{ fmt }}')
        {%- else %}
            TRY_TO_DATE({{ e }})
        {%- endif %}

    {%- elif t in ["TIMESTAMP_NTZ","TIMESTAMP"] %}
        COALESCE(TRY_TO_TIMESTAMP_NTZ({{ e }}),TO_TIMESTAMP_NTZ('1900-01-01 00:00:00'))

    {%- elif t in ["BOOLEAN","BOOL"] %}
        {# Para booleanos usamos el expr “en crudo” con TRIM/UPPER y mapeamos a TRUE/FALSE/NULL #}
        IFF(UPPER(TRIM({{ expr }})) IN ('TRUE','T','1','S','SI','Y','YES'), TRUE,
            IFF(UPPER(TRIM({{ expr }})) IN ('FALSE','F','0','N','NO'), FALSE, NULL))::BOOLEAN

    {%- else %}
        {{ expr }}  {# fallback si aparece un tipo no contemplado #}
    {%- endif %}
{% endmacro %}