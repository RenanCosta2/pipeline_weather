WITH source AS (

  SELECT
    UPPER(TRIM(regiao)) AS regiao
  FROM 
    {{ ref('stg_weather_metadata') }}

),

deduplicated AS (

  SELECT DISTINCT 
    regiao
  FROM
    source
  WHERE
    regiao IS NOT NULL

)

SELECT
  ABS(FARM_FINGERPRINT(regiao)) AS regiao_id,
  regiao

FROM deduplicated