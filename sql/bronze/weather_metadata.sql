CREATE OR REPLACE EXTERNAL TABLE `ornate-shine-402117.weather_bronze.bronze_weather_metadata`
(
  regiao STRING,
  uf STRING,
  estacao STRING,
  codigo__wmo_ STRING,
  latitude STRING,
  longitude STRING, 
  altitude STRING,
  data_de_fundacao STRING
)
WITH PARTITION COLUMNS (
  year INT64,
  station STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://pipeline_weather_data/bronze/metadata/*.parquet'],
  hive_partition_uri_prefix = 'gs://pipeline_weather_data/bronze/metadata/'
);