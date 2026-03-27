WITH source AS (

    SELECT *
    FROM {{ source('weather_bronze', 'bronze_weather_data') }}

),

clean_strings AS (

    SELECT
        station,
        year,
        data,
        hora_utc,
        source_file,

        REPLACE(precipitacao_total_mm, ',', '.') AS precipitacao_total_mm,
        REPLACE(pressao_atmosferica_estacao_mb, ',', '.') AS pressao_atmosferica_estacao_mb,
        REPLACE(pressao_atmosferica_max_mb, ',', '.') AS pressao_atmosferica_max_mb,
        REPLACE(pressao_atmosferica_min_mb, ',', '.') AS pressao_atmosferica_min_mb,
        REPLACE(radiacao_global_kj_m2, ',', '.') AS radiacao_global_kj_m2,
        REPLACE(temperatura_ar_c, ',', '.') AS temperatura_ar_c,
        REPLACE(temperatura_orvalho_c, ',', '.') AS temperatura_orvalho_c,
        REPLACE(temperatura_max_c, ',', '.') AS temperatura_max_c,
        REPLACE(temperatura_min_c, ',', '.') AS temperatura_min_c,
        REPLACE(temperatura_orvalho_max_c, ',', '.') AS temperatura_orvalho_max_c,
        REPLACE(temperatura_orvalho_min_c, ',', '.') AS temperatura_orvalho_min_c,
        REPLACE(umidade_relativa_max, ',', '.') AS umidade_relativa_max,
        REPLACE(umidade_relativa_min, ',', '.') AS umidade_relativa_min,
        REPLACE(umidade_relativa, ',', '.') AS umidade_relativa,
        REPLACE(vento_direcao_gr, ',', '.') AS vento_direcao_gr,
        REPLACE(vento_rajada_max_ms, ',', '.') AS vento_rajada_max_ms,
        REPLACE(vento_velocidade_ms, ',', '.') AS vento_velocidade_ms

    FROM source

),

parsed_datetime AS (

    SELECT
        *,
        PARSE_TIMESTAMP(
            '%Y/%m/%d %H%M',
            CONCAT(data, ' ', TRIM(REPLACE(hora_utc, ' UTC', '')))
        ) AS datetime_utc
    FROM clean_strings

),

typed AS (

    SELECT
        station,
        year,
        source_file,

        datetime_utc,
        DATETIME(datetime_utc, 'America/Sao_Paulo') AS datetime_br,

        SAFE_CAST(precipitacao_total_mm AS FLOAT64) AS precipitacao_total_mm,
        SAFE_CAST(pressao_atmosferica_estacao_mb AS FLOAT64) AS pressao_atmosferica_estacao_mb,
        SAFE_CAST(pressao_atmosferica_max_mb AS FLOAT64) AS pressao_atmosferica_max_mb,
        SAFE_CAST(pressao_atmosferica_min_mb AS FLOAT64) AS pressao_atmosferica_min_mb,
        SAFE_CAST(radiacao_global_kj_m2 AS FLOAT64) AS radiacao_global_kj_m2,
        SAFE_CAST(temperatura_ar_c AS FLOAT64) AS temperatura_ar_c,
        SAFE_CAST(temperatura_orvalho_c AS FLOAT64) AS temperatura_orvalho_c,
        SAFE_CAST(temperatura_max_c AS FLOAT64) AS temperatura_max_c,
        SAFE_CAST(temperatura_min_c AS FLOAT64) AS temperatura_min_c,
        SAFE_CAST(temperatura_orvalho_max_c AS FLOAT64) AS temperatura_orvalho_max_c,
        SAFE_CAST(temperatura_orvalho_min_c AS FLOAT64) AS temperatura_orvalho_min_c,
        SAFE_CAST(umidade_relativa_max AS FLOAT64) AS umidade_relativa_max,
        SAFE_CAST(umidade_relativa_min AS FLOAT64) AS umidade_relativa_min,
        SAFE_CAST(umidade_relativa AS FLOAT64) AS umidade_relativa,
        SAFE_CAST(vento_direcao_gr AS FLOAT64) AS vento_direcao_gr,
        SAFE_CAST(vento_rajada_max_ms AS FLOAT64) AS vento_rajada_max_ms,
        SAFE_CAST(vento_velocidade_ms AS FLOAT64) AS vento_velocidade_ms

    FROM parsed_datetime

)

SELECT *
FROM typed
WHERE
  NOT (precipitacao_total_mm IS NULL AND
  pressao_atmosferica_estacao_mb IS NULL AND
  pressao_atmosferica_max_mb IS NULL AND
  pressao_atmosferica_min_mb IS NULL AND
  radiacao_global_kj_m2 IS NULL AND
  temperatura_ar_c IS NULL AND
  temperatura_orvalho_c IS NULL AND
  temperatura_max_c IS NULL AND
  temperatura_min_c IS NULL AND
  temperatura_orvalho_max_c IS NULL AND
  temperatura_orvalho_min_c IS NULL AND
  umidade_relativa_max IS NULL AND
  umidade_relativa_min IS NULL AND
  umidade_relativa IS NULL AND
  vento_direcao_gr IS NULL AND
  vento_rajada_max_ms IS NULL AND
  vento_velocidade_ms IS NULL)