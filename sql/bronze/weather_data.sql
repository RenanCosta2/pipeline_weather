CREATE OR REPLACE EXTERNAL TABLE `ornate-shine-402117.weather_bronze.bronze_weather_data`
(
  data STRING,
  hora_utc STRING,
  precipitacao_total_mm STRING,
  pressao_atmosferica_estacao_mb STRING,
  pressao_atmosferica_max_mb STRING,
  pressao_atmosferica_min_mb STRING,
  radiacao_global_kj_m2 STRING,
  temperatura_ar_c STRING,
  temperatura_orvalho_c STRING,
  temperatura_max_c STRING,
  temperatura_min_c STRING,
  temperatura_orvalho_max_c STRING,
  temperatura_orvalho_min_c STRING,
  umidade_relativa_max STRING,
  umidade_relativa_min STRING,
  umidade_relativa STRING,
  vento_direcao_gr STRING,
  vento_rajada_max_ms STRING,
  vento_velocidade_ms STRING,
  source_file STRING
)
WITH PARTITION COLUMNS (
  year INT64,
  station STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://pipeline_weather_data/bronze/data/*.parquet'],
  hive_partition_uri_prefix = 'gs://pipeline_weather_data/bronze/data/'
);