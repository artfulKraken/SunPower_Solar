

SELECT
  prod.p_3phsum_kw AS `Production Power`,
  ABS( cons.p_3phsum_kw ) AS `Consumption Power`,
  cons.p_3phsum_kw AS `Consumption Power Color`,
  ( prod.p_3phsum_kw + cons.p_3phsum_kw ) AS `Home Power Usage`
FROM ( 
  SELECT 
    p_3phsum_kw,
    data_time
  FROM 
    solar.production_meters_data 
  WHERE 
    data_time >= DATE_ADD( NOW(), INTERVAL -30 MINUTE )
) AS prod
JOIN (
  SELECT
    p_3phsum_kw,
    data_time
  FROM
    solar.consumption_meters_data
) AS cons
ON prod.data_time = cons.data_time
ORDER BY prod.data_time DESC
LIMIT 1;
