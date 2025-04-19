SELECT 
  c.time, c.summary, CONCAT( "https://wsgrafana.artfulkraken.com/icons/wx-", c.icon, ".svg" ) AS icon, c.nearestStormDistance, c.nearestStormBearing, c.precipIntensity, c.precipProbability, 
  c.precipIntensityError, c.precipType, c.temperature, c.apparentTemperature, c.dewPoint, c.humidity, c.pressure, 
  c.windSpeed, c.windGust, c.windBearing, c.cloudCover, c.uvIndex, c.visibility, c.ozone, c.smoke, c.fireIndex, 
  c.feelsLike, c.currentDayIce, c.currentDayLiquid, c.currentDaySnow,
  d.sunriseTime, d.dawnTime, d.sunsetTime, d.duskTime, d.moonPhase, d.precipAccumulation, d.temperatureMin, 
  d.temperatureMinTime, d.temperatureMax, d.temperatureMaxTime
FROM ( 
  SELECT 
    solar.current_wx.*,
    DATE(CONVERT_TZ( solar.current_wx.time, 'UTC', 'America/Los_Angeles' )) AS cur_date
  FROM solar.current_wx
  WHERE 
    time = ( SELECT MAX(time) FROM solar.current_wx )
) AS c
JOIN (
  SELECT 
    solar.daily_wx.*,
    DATE(solar.daily_wx.time) AS daily_date
  FROM
    solar.daily_wx
) AS d
ON cur_date = daily_date;
