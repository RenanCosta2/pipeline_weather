WITH source AS (

  SELECT
    UPPER(TRIM(uf)) AS uf
  FROM 
    {{ ref('stg_weather_metadata') }}

),

deduplicated AS (

  SELECT DISTINCT 
    uf
  FROM 
    source
  WHERE 
    uf IS NOT NULL

)

SELECT
  ABS(FARM_FINGERPRINT(uf)) AS uf_id,
  uf
FROM 
  deduplicated