SELECT
  prod.p_3phsum_kw AS `Production Power`,
  CASE
    WHEN prod.p_3phsum_kw > 0 Then 1
    WHEN prod.p_3phsum_kw > 0.0625 Then 2
    WHEN prod.p_3phsum_kw > 0.125 Then 3
    WHEN prod.p_3phsum_kw > 0.25 Then 4
    WHEN prod.p_3phsum_kw > 0.5 Then 5
    WHEN prod.p_3phsum_kw > 1.0 Then 6
    WHEN prod.p_3phsum_kw > 2.0 Then 7
    WHEN prod.p_3phsum_kw > 4.0 Then 8
    WHEN prod.p_3phsum_kw > 8.0 Then 9
    WHEN prod.p_3phsum_kw > 16.0 Then 10
  END AS `Production Power Flow`,
  ABS( cons.p_3phsum_kw ) AS `Consumption Power`,
  CASE
    WHEN ABS( cons.p_3phsum_kw ) > 0 Then 1
    WHEN ABS( cons.p_3phsum_kw ) > 0.0625 Then 2
    WHEN ABS( cons.p_3phsum_kw ) > 0.125 Then 3
    WHEN ABS( cons.p_3phsum_kw ) > 0.25 Then 4
    WHEN ABS( cons.p_3phsum_kw ) > 0.5 Then 5
    WHEN ABS( cons.p_3phsum_kw ) > 1.0 Then 6
    WHEN ABS( cons.p_3phsum_kw ) > 2.0 Then 7
    WHEN ABS( cons.p_3phsum_kw ) > 4.0 Then 8
    WHEN ABS( cons.p_3phsum_kw ) > 8.0 Then 9
    WHEN ABS( cons.p_3phsum_kw ) > 16.0 Then 10
  END AS `Consumption Power Flow`,
  cons.p_3phsum_kw AS `Consumption Power Color`,
  cons.p_3phsum_kw AS `Grid Out Power Color`,
  cons.p_3phsum_kw AS `Grid In Power Color`,
  ( prod.p_3phsum_kw + cons.p_3phsum_kw ) AS `Home Power Usage`,
  CASE
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 0 Then 1
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 0.0625 Then 2
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 0.125 Then 3
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 0.25 Then 4
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 0.5 Then 5
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 1.0 Then 6
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 2.0 Then 7
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 4.0 Then 8
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 8.0 Then 9
    WHEN ( prod.p_3phsum_kw + cons.p_3phsum_kw ) > 16.0 Then 10
  END AS `Home Usage Power Flow`
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
