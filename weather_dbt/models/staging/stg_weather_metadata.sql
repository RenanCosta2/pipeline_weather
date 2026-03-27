WITH source AS (

    SELECT *
    FROM {{ source('weather_bronze', 'bronze_weather_metadata') }}

),

clean_strings AS (
    SELECT
        station,
        year,
        regiao,
        uf,
        estacao AS cidade,
        codigo__wmo_ AS codigo_wmo,
        data_de_fundacao,

        REPLACE(latitude, ',', '.') AS latitude,
        REPLACE(longitude, ',', '.') AS longitude,
        REPLACE(altitude, ',', '.') AS altitude
    FROM
        source
),

parsed_date AS (
    SELECT
        *,
        PARSE_DATE('%d/%m/%y', data_de_fundacao) AS data_fundacao,
    FROM
        clean_strings
),

typed AS (
    SELECT 
        station,
        year,
        regiao,
        uf,
        cidade,
        codigo_wmo,
        data_fundacao,

        SAFE_CAST(latitude AS FLOAT64) AS latitude,
        SAFE_CAST(longitude AS FLOAT64) AS longitude,
        SAFE_CAST(altitude AS FLOAT64) AS altitude,

    FROM 
    parsed_date
)

SELECT *
FROM typed