WITH source AS (

    SELECT *
    FROM {{ ref('stg_weather_data') }}

)

SELECT
    DATE(datetime_br) AS data_id,
    EXTRACT(HOUR FROM datetime_br) AS hora_id,
    station AS localizacao_id,

    precipitacao_total_mm,
    pressao_atmosferica_estacao_mb,
    pressao_atmosferica_max_mb,
    pressao_atmosferica_min_mb,
    radiacao_global_kj_m2,
    temperatura_ar_c,
    temperatura_orvalho_c,
    temperatura_max_c,
    temperatura_min_c,
    temperatura_orvalho_max_c,
    temperatura_orvalho_min_c,
    umidade_relativa_max,
    umidade_relativa_min,
    umidade_relativa,
    vento_direcao_gr,
    vento_rajada_max_ms,
    vento_velocidade_ms

FROM source
WHERE datetime_br IS NOT NULL