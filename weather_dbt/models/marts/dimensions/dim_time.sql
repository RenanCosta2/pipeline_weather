SELECT
  hour AS hora_id,

  CASE 
    WHEN hour BETWEEN 0 AND 5 THEN 'Madrugada'
    WHEN hour BETWEEN 6 AND 11 THEN 'Manhã'
    WHEN hour BETWEEN 12 AND 17 THEN 'Tarde'
    ELSE 'Noite'
  END AS periodo_dia

FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour