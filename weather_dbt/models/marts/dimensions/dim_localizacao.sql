WITH source AS (

  SELECT
    station,
    year,
    cidade,
    codigo_wmo,
    data_fundacao,
    latitude,
    longitude,
    altitude,
    UPPER(TRIM(uf)) AS uf,
    UPPER(TRIM(regiao)) AS regiao
  FROM 
    {{ ref('stg_weather_metadata') }}

),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY codigo_wmo
      ORDER BY year DESC
    ) AS rn
  FROM
    source
),

filtered AS (
  SELECT
    *
  FROM
    ranked
  WHERE ranked.rn = 1
)

SELECT
  filtered.codigo_wmo,
  uf.uf_id,
  regiao.regiao_id,
  filtered.cidade,
  filtered.longitude,
  filtered.latitude,
  filtered.altitude,
  filtered.data_fundacao

FROM filtered

LEFT JOIN
  {{ ref('dim_uf') }} AS uf
  ON UPPER(TRIM(filtered.uf)) = uf.uf

LEFT JOIN
  {{ ref('dim_regiao') }} AS regiao
  ON UPPER(TRIM(filtered.regiao)) = regiao.regiao