CREATE OR REPLACE TABLE weather_silver.silver_weather_data AS

SELECT 
  station,
  year,

  -- Data e Hora UTC
  PARSE_TIMESTAMP(
    '%Y/%m/%d %H%M',
    CONCAT(data, ' ', TRIM(REPLACE(hora_utc, ' UTC', '')))
  ) AS datetime_utc,

  -- Data e Hora Timezone BR
  DATETIME(
    PARSE_TIMESTAMP(
      '%Y/%m/%d %H%M',
      CONCAT(data, ' ', TRIM(REPLACE(hora_utc, ' UTC', '')))
    ),
    'America/Sao_Paulo'
  ) AS datetime_br,

  SAFE_CAST(
      REPLACE(precipitacao_total_mm, ',', '.')
      AS FLOAT64
    ) AS precipitacao_total_mm,

  SAFE_CAST(
      REPLACE(pressao_atmosferica_estacao_mb, ',', '.')
      AS FLOAT64
    ) AS pressao_atmosferica_estacao_mb,

  SAFE_CAST(
      REPLACE(pressao_atmosferica_max_mb, ',', '.')
      AS FLOAT64
    ) AS pressao_atmosferica_max_mb,

  SAFE_CAST(
      REPLACE(pressao_atmosferica_min_mb, ',', '.')
      AS FLOAT64
    ) AS pressao_atmosferica_min_mb,

  SAFE_CAST(
      REPLACE(radiacao_global_kj_m2, ',', '.')
      AS FLOAT64
    ) AS radiacao_global_kj_m2,

  SAFE_CAST(
      REPLACE(temperatura_ar_c, ',', '.')
      AS FLOAT64
    ) AS temperatura_ar_c,

  SAFE_CAST(
      REPLACE(temperatura_orvalho_c, ',', '.')
      AS FLOAT64
    ) AS temperatura_orvalho_c,

  SAFE_CAST(
      REPLACE(temperatura_max_c, ',', '.')
      AS FLOAT64
    ) AS temperatura_max_c,
  
  SAFE_CAST(
      REPLACE(temperatura_min_c, ',', '.')
      AS FLOAT64
    ) AS temperatura_min_c,
  
  SAFE_CAST(
      REPLACE(temperatura_orvalho_max_c, ',', '.')
      AS FLOAT64
    ) AS temperatura_orvalho_max_c,
  
  SAFE_CAST(
      REPLACE(temperatura_orvalho_min_c, ',', '.')
      AS FLOAT64
    ) AS temperatura_orvalho_min_c,
  
  SAFE_CAST(
      REPLACE(umidade_relativa_max, ',', '.')
      AS FLOAT64
    ) AS umidade_relativa_max,
  
  SAFE_CAST(
      REPLACE(umidade_relativa_min, ',', '.')
      AS FLOAT64
    ) AS umidade_relativa_min,
  
  SAFE_CAST(
      REPLACE(umidade_relativa, ',', '.')
      AS FLOAT64
    ) AS umidade_relativa,
  
  SAFE_CAST(
      REPLACE(vento_direcao_gr, ',', '.')
      AS FLOAT64
    ) AS vento_direcao_gr,
  
  SAFE_CAST(
      REPLACE(vento_rajada_max_ms, ',', '.')
      AS FLOAT64
    ) AS vento_rajada_max_ms,
  
  SAFE_CAST(
      REPLACE(vento_velocidade_ms, ',', '.')
      AS FLOAT64
    ) AS vento_velocidade_ms,

  source_file
FROM 
  `ornate-shine-402117.weather_bronze.bronze_weather_data` 
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
  