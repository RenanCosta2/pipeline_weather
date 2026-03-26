CREATE OR REPLACE TABLE weather_silver.silver_weather_metadata AS 

SELECT 
  station,
  year,
  regiao,
  uf,
  estacao AS cidade,
  codigo__wmo_ AS codigo_wmo,
  PARSE_DATE('%d/%m/%y', data_de_fundacao) AS data_fundacao,

  CAST(
      REPLACE(latitude, ',', '.')
      AS FLOAT64
    ) AS latitude,

  CAST(
      REPLACE(longitude, ',', '.')
      AS FLOAT64
    ) AS longitude,

  CAST(
      REPLACE(altitude, ',', '.')
      AS FLOAT64
    ) AS altitude
FROM 
  `ornate-shine-402117.weather_bronze.bronze_weather_metadata`