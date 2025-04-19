SELECT
  p.`Panel Group 1`,
  CASE
    WHEN p.`Panel Group 1` > 0 Then 1
    WHEN p.`Panel Group 1` > 0.0625 Then 2
    WHEN p.`Panel Group 1` > 0.125 Then 3
    WHEN p.`Panel Group 1` > 0.25 Then 4
    WHEN p.`Panel Group 1` > 0.5 Then 5
    WHEN p.`Panel Group 1` > 1.0 Then 6
    WHEN p.`Panel Group 1` > 2.0 Then 7
    WHEN p.`Panel Group 1` > 4.0 Then 8
    WHEN p.`Panel Group 1` > 8.0 Then 9
    WHEN p.`Panel Group 1` > 16.0 Then 10
  END as `Panel Group 1 Flow`,
  p.`E00122250068774 Power`,
  p.`E00122250068886 Power`,
  p.`E00122250068917 Power`,
  p.`E00122250071017 Power`,
  p.`E00122250077989 Power`,
  p.`E00122250084153 Power`,
  p.`E00122250084256 Power`,
  p.`E00122251005754 Power`,
  p.`E00122250068516 Power`,
  p.`Panel Group 2`,
  CASE
    WHEN p.`Panel Group 2` > 0 Then 1
    WHEN p.`Panel Group 2` > 0.0625 Then 2
    WHEN p.`Panel Group 2` > 0.125 Then 3
    WHEN p.`Panel Group 2` > 0.25 Then 4
    WHEN p.`Panel Group 2` > 0.5 Then 5
    WHEN p.`Panel Group 2` > 1.0 Then 6
    WHEN p.`Panel Group 2` > 2.0 Then 7
    WHEN p.`Panel Group 2` > 4.0 Then 8
    WHEN p.`Panel Group 2` > 8.0 Then 9
    WHEN p.`Panel Group 2` > 16.0 Then 10
  END as `Panel Group 2 Flow`,
  p.`E00122251010882 Power`,
  p.`E00122250081238 Power`,
  p.`E00122250068780 Power`,
  p.`E00122250068776 Power`,
  p.`E00122251011493 Power`,
  p.`E00122250068916 Power`,
  p.`Panel Group 3`,
  CASE
    WHEN p.`Panel Group 3` > 0 Then 1
    WHEN p.`Panel Group 3` > 0.0625 Then 2
    WHEN p.`Panel Group 3` > 0.125 Then 3
    WHEN p.`Panel Group 3` > 0.25 Then 4
    WHEN p.`Panel Group 3` > 0.5 Then 5
    WHEN p.`Panel Group 3` > 1.0 Then 6
    WHEN p.`Panel Group 3` > 2.0 Then 7
    WHEN p.`Panel Group 3` > 4.0 Then 8
    WHEN p.`Panel Group 3` > 8.0 Then 9
    WHEN p.`Panel Group 3` > 16.0 Then 10
  END as `Panel Group 3 Flow`,
  p.`E00122250068994 Power`,
  p.`E00122250068485 Power`,
  p.`E00122250068512 Power`,
  p.`E00122250068758 Power`,
  p.`E00122250068698 Power`,
  p.`E00122250071869 Power`,
  p.`Panel Group 4`,
  CASE
    WHEN p.`Panel Group 4` > 0 Then 1
    WHEN p.`Panel Group 4` > 0.0625 Then 2
    WHEN p.`Panel Group 4` > 0.125 Then 3
    WHEN p.`Panel Group 4` > 0.25 Then 4
    WHEN p.`Panel Group 4` > 0.5 Then 5
    WHEN p.`Panel Group 4` > 1.0 Then 6
    WHEN p.`Panel Group 4` > 2.0 Then 7
    WHEN p.`Panel Group 4` > 4.0 Then 8
    WHEN p.`Panel Group 4` > 8.0 Then 9
    WHEN p.`Panel Group 4` > 16.0 Then 10
  END as `Panel Group 4 Flow`,
  p.`E00122251011515 Power`,
  p.`E00122251015947 Power`,
  p.`E00122251016232 Power`,
  p.`E00122251002917 Power`
FROM (
  SELECT
    -- Panel Group 1
    (
      IFNULL( AVG( CASE WHEN serial = 'E00122250068774' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068886' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068917' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250071017' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250077989' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250084153' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250084256' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122251005754' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068516' THEN p_3phsum_kw ELSE NULL END ), 0 )
    ) AS `Panel Group 1`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068774' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068774 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068886' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068886 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068917' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068917 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250071017' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250071017 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250077989' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250077989 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250084153' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250084153 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250084256' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250084256 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251005754' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251005754 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068516' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068516 Power`,
    -- Panel Group 2
    (
      IFNULL( AVG( CASE WHEN serial = 'E00122251010882' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250081238' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068780' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068776' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122251011493' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068916' THEN p_3phsum_kw ELSE NULL END ), 0 )
    ) AS `Panel Group 2`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251010882' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251010882 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250081238' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250081238 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068780' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068780 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068776' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068776 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251011493' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251011493 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068916' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068916 Power`,
    -- Panel Group 3
    (
      IFNULL( AVG( CASE WHEN serial = 'E00122250068994' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068485' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068512' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068758' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250068698' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122250071869' THEN p_3phsum_kw ELSE NULL END ), 0 )
    ) AS `Panel Group 3`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068994' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068994 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068485' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068485 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068512' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068512 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068758' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068758 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250068698' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250068698 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122250071869' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122250071869 Power`,
    -- Panel Group 4
    (
      IFNULL( AVG( CASE WHEN serial = 'E00122251011515' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122251015947' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122251016232' THEN p_3phsum_kw ELSE NULL END ), 0 ) +
      IFNULL( AVG( CASE WHEN serial = 'E00122251002917' THEN p_3phsum_kw ELSE NULL END ), 0 )
    ) AS `Panel Group 4`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251011515' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251011515 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251015947' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251015947 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251016232' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251016232 Power`,
    IFNULL( AVG( CASE WHEN serial = 'E00122251002917' THEN p_3phsum_kw ELSE NULL END ), 0 ) AS `E00122251002917 Power`
  FROM
    solar.inverters_data
  WHERE
    data_time >= DATE_ADD( now(), INTERVAL -30 MINUTE )
  ORDER BY data_time DESC
  LIMIT 1
) AS p;
