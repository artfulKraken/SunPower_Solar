-- Variable statements do not seem to work in Grafana sql queries (Or I haven't figured it out yet).
-- Replace variables with their values before adding to grafana.
SET @start_dt = 
DATE_ADD( 
  DATE_ADD( 
    MAKEDATE( YEAR ( NOW() ),1 ),
    INTERVAL MONTH( NOW() ) -1 MONTH 
  ), 
  INTERVAL DAY( NOW() ) -1 DAY 
)
;

SELECT 
	DATE_ADD(
    @start_dt, 
    -- Interval is the datapoint that it will be grouped to.  
    INTERVAL HOUR( data_time ) + IF( MINUTE( data_time ) > 0, 1, 0) HOUR
  ) AS End_Time,
	SUM( Nrg_Production ),
	(SUM( Nrg_Production ) + SUM( Nrg_Consumption ) ) AS Nrg_Usage,
	( -1 * SUM( Nrg_Consumption ) ) AS Net_Nrg_Production
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
		WHERE data_time >= ( 
      SELECT 
      SUBDATE( 
        DATE_ADD( 
          @start_dt,
          Interval TIMESTAMPDIFF( 
            HOUR,  
            CONVERT_TZ(
              @start_dt, 
              'UTC', 'America/Los_Angeles' 
            ), 
            @start_dt
          ) HOUR 
        ), 
        INTERVAL IF ( TIMESTAMPDIFF( 
          HOUR,  
          CONVERT_TZ(
            @start_dt, 'UTC', 'America/Los_Angeles' 
          ), 
          @start_dt
        ) > HOUR( NOW() ), 1, 0)  DAY 
      ) 
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
GROUP BY End_Time;
