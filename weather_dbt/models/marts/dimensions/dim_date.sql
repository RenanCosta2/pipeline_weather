SELECT DISTINCT
  date AS data_id,
  EXTRACT(YEAR FROM date) AS ano,
  EXTRACT(QUARTER FROM date) AS trimestre,
  EXTRACT(MONTH FROM date) AS mes,
  CASE EXTRACT(MONTH FROM date)
    WHEN 1 THEN 'Janeiro'
    WHEN 2 THEN 'Fevereiro'
    WHEN 3 THEN 'Março'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Maio'
    WHEN 6 THEN 'Junho'
    WHEN 7 THEN 'Julho'
    WHEN 8 THEN 'Agosto'
    WHEN 9 THEN 'Setembro'
    WHEN 10 THEN 'Outubro'
    WHEN 11 THEN 'Novembro'
    WHEN 12 THEN 'Dezembro'
  END AS mes_extenso,
  EXTRACT(DAY FROM date) AS dia,
  EXTRACT(DAYOFWEEK FROM date) AS dia_semana,
  CASE EXTRACT(DAYOFWEEK FROM date)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Segunda-feira'
    WHEN 3 THEN 'Terça-feira'
    WHEN 4 THEN 'Quarta-feira'
    WHEN 5 THEN 'Quinta-feira'
    WHEN 6 THEN 'Sexta-feira'
    WHEN 7 THEN 'Sábado'
  END AS dia_semana_extenso,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM date) IN (1,7) THEN TRUE
    ELSE FALSE
  END AS fim_de_semana
FROM 
  UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2030-12-31')) AS date