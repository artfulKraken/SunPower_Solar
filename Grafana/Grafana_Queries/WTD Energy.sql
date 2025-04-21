SELECT
  SUM( w.`Net Generation`) OVER ( ORDER BY w.End_Time) AS `Cumm. Net Generation`,
  w.End_Time AS `End Time`,
  w.`Production`,
  w.`Useage`,
  w.`Net Generation`
FROM (
  SELECT 
    DATE_ADD(
      DATE_ADD(
        DATE_ADD(
          MAKEDATE(YEAR( CONVERT_TZ( data_time, 'UTC', 'America/Los_Angeles' ) ), 1),
          INTERVAL MONTH( CONVERT_TZ( data_time, 'UTC', 'America/Los_Angeles' ) ) -1 MONTH
        ),
        INTERVAL DAY( CONVERT_TZ( data_time, 'UTC', 'America/Los_Angeles' ) ) -1 DAY
      ), 
      INTERVAL IF(
        HOUR( CONVERT_TZ( data_time, 'UTC', 'America/Los_Angeles' ) ) = 0 AND
          MINUTE( CONVERT_TZ( data_time, 'UTC', 'America/Los_Angeles' ) ) = 0,
        -1,
        0
      ) DAY
    ) AS End_Time,
    SUM( Nrg_Production ) AS `Production`,
    ( -1 * (SUM( Nrg_Production ) + SUM( Nrg_Consumption ) ) ) AS `Useage`,
    ( -1 * SUM( Nrg_Consumption ) ) AS `Net Generation`
  FROM (
    SELECT 
      prod.timestamp AS data_time,
      COALESCE( prod.net_ltea_3phsum_kwh - LAG( prod.net_ltea_3phsum_kwh ) OVER ( ORDER BY prod.timestamp ASC ), 0 ) AS Nrg_Production,
      COALESCE( cons.net_ltea_3phsum_kwh - LAG( cons.net_ltea_3phsum_kwh ) OVER ( ORDER BY cons.timestamp ASC ), 0 ) AS Nrg_Consumption
    FROM (
      SELECT 
        FROM_UNIXTIME( ( ROUND( UNIX_TIMESTAMP( data_time ) / 60, 0 ) * 60 ) ) AS 'timestamp',
        net_ltea_3phsum_kwh
      FROM solar.production_meters_data
      WHERE data_time >= 
        CONVERT_TZ(
          DATE_SUB(
            DATE_ADD(
              DATE_ADD(
                MAKEDATE(YEAR ( CONVERT_TZ( NOW(), 'UTC', 'America/Los_Angeles' ) ), 1),
                INTERVAL MONTH( CONVERT_TZ( NOW(), 'UTC', 'America/Los_Angeles' ) ) -1 MONTH 
              ),
              INTERVAL DAY( CONVERT_TZ( NOW(), 'UTC', 'America/Los_Angeles' ) ) - 1 DAY
            ),
            INTERVAL 
            ( CASE
              WHEN DAYOFWEEK( CONVERT_TZ( NOW(), 'UTC', 'America/Los_Angeles' ) ) = 1 THEN 8
              WHEN DAYOFWEEK( CONVERT_TZ( NOW(), 'UTC', 'America/Los_Angeles' ) ) > 1 THEN DAYOFWEEK( CONVERT_TZ( NOW(), 'UTC', 'America/Los_Angeles' ) )
            END -2 ) DAY
          ),
          'America/Los_Angeles', 'UTC' 
        )
    ) AS prod
    JOIN (
      SELECT 
        FROM_UNIXTIME( ( ROUND( UNIX_TIMESTAMP( data_time ) / 60, 0 ) * 60 ) ) AS 'timestamp',
        net_ltea_3phsum_kwh
      FROM solar.consumption_meters_data
    ) AS cons
    ON prod.timestamp = cons.timestamp
  ) AS prod_cons
  GROUP BY End_Time
  ORDER BY End_Time
) AS w;
