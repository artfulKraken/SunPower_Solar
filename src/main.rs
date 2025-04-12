/*
Program completes the following actions:
1)  At a regular interval connects to Sunpower PVS6 Supervisor via http api call and collects pvs6 data.
    Processes data into a usable format and then uploads data to mysql database (solar).

2)  Future: Processes energy produced and energy consumed data (in solar db) to tabular format for data analysis  
*/

// USE STATEMENTS
    use reqwest::get;
    use regex::Regex;
    use once_cell::sync::{Lazy, OnceCell};
    use myloginrs::parse as myloginrs_parse;
    use tokio::{ spawn, time::{ Interval, interval_at, Duration as TokioDuration } };
    use std::{ str, fs, path::PathBuf, env, cmp::Ordering, error, fmt, sync::Mutex };
    use log::{ debug, error, info, warn };
    use log4rs;
    use chrono::{ TimeDelta, DateTime, Utc, NaiveDateTime, Duration, DurationRound };
    use sqlx::mysql::MySqlPoolOptions;
    use serde::Deserialize;
    use serde_json::Result;
    use config::Config;

    // CONSTANTS
    const URL_DEVICES_API: &str = "/cgi-bin/dl_cgi?Command=DeviceList";
    const SUPERVISOR: &str = "PVS";
    const METER: &str = "Power Meter";
    const INVERTER: &str = "Inverter";
    const PRODUCTION_METER: &str = "PVS5-METER-P";
    const CONSUMPTION_METER: &str = "PVS5-METER-C";
    // SQL QUERY CONSTANTS
    const SUP_INSERT_QUERY: &str = 
    r#"
        INSERT INTO supervisors_data 
            ( serial, data_time, dl_comm_err, dl_cpu_load, dl_err_count, dl_flash_avail, dl_mem_used, 
                dl_scan_time, dl_skipped_scans, dl_untransmitted, dl_uptime ) 
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#;
    const PM_INSERT_QUERY: &str = 
    r#"
        INSERT INTO production_meters_data
            ( serial, data_time, freq_hz, i_a, net_ltea_3phsum_kwh, 
                p_3phsum_kw, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#;
    const CM_INSERT_QUERY: &str = 
    r#"
        INSERT INTO consumption_meters_data
            ( serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw,
                p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v )
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#;
    const INV_INSERT_QUERY: &str = 
    r#"
        INSERT INTO inverters_data
            ( serial, data_time, freq_hz, i_3phsum_a, i_mppt1_a, ltea_3phsum_kwh, p_3phsum_kw, 
                p_mppt1_kw, stat_ind, t_htsnk_degc, v_mppt1_v, vln_3phavg_v )
            VALUE ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#; 
    // sql query to get latest data for each supervisor (serial #) in supervisors_data table
    const QUERY_GET_LATEST_SUP_DATA: &str =
    r#"
        SELECT * FROM supervisors_data AS sup
        INNER JOIN (
            SELECT serial, MAX(data_time) AS dt_max
            FROM supervisors_data
            GROUP BY serial
        ) AS sup_max
        ON sup.serial = sup_max.serial AND sup.data_time = sup_max.dt_max
    "#;
    // sql query to get latest data for each consumption meter(serial #) in consumption_meters_data table
    const QUERY_GET_LATEST_CM_DATA: &str = 
    r#"
        SELECT * FROM consumption_meters_data AS cm
        INNER JOIN (
            SELECT serial, MAX(data_time) AS dt_max
            FROM consumption_meters_data
            GROUP BY serial
        ) as cm_max
        ON cm.serial = cm_max.serial AND cm.data_time = cm_max.dt_max
    "#;
    // sql query to get latest data for each production meter(serial #) in production_meters_data table
    const QUERY_GET_LATEST_PM_DATA: &str = 
    r#"
        SELECT * FROM production_meters_data AS pm
        INNER JOIN (
            SELECT serial, MAX(data_time) AS dt_max
            FROM production_meters_data
            GROUP BY serial
        ) as pm_max
        ON pm.serial = pm_max.serial AND pm.data_time = pm_max.dt_max
    "#;
    // sql query to get latest data for each inverter (serial #) in inverters_data table
    const QUERY_GET_LATEST_INV_DATA: &str = 
    r#"
        SELECT * FROM inverters_data AS inv
        INNER JOIN (
            SELECT serial, MAX(data_time) AS dt_max
            FROM inverters_data
            GROUP BY serial
        ) AS inv_max
        ON inv.serial = inv_max.serial AND inv.data_time = inv_max.dt_max
    "#;
    //sql query insert (if new) or replace daily weather.
    const REPLACE_DAILY_WX_QUERY: &str = 
        r#"
            REPLACE INTO daily_wx 
                ( latitude, longitude, time, sunriseTime, dawnTime, sunsetTime, duskTime, moonPhase, 
                precipAccumulation, temperatureMin, temperatureMinTime, temperatureMax, temperatureMaxTime ) 
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
        "#;
    //sql query insert current Wx
    const INSERT_CURRENT_WX_QUERY: &str = 
        r#"
            INSERT INTO current_wx 
                ( latitude, longitude, time, summary, icon, nearestStormDistance, nearestStormBearing, precipIntensity,
                    precipProbability, precipIntensityError, precipType, temperature, apparentTemperature, dewPoint, humidity, pressure, 
                    windSpeed, windGust, windBearing, cloudCover, uvIndex , visibility, ozone, smoke, fireIndex, feelsLike, currentDayIce,
                    currentDayLiquid, currentDaySnow ) 
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
        "#;

// ENUMS, STRUCTURES AND IMPLEMENTATIONS

#[derive(Clone, Debug)] 
struct Pvs6DevicesResponse {
    supervisor: Supervisor,
    cons_meter: ConsumptionMeter,
    prod_meter: ProductionMeter,
    inverters: Vec<Inverter>,
}

impl Pvs6DevicesResponse {
    fn new() -> Self {
        Self {
            supervisor: Supervisor::new(),
            cons_meter: ConsumptionMeter::new(),
            prod_meter: ProductionMeter::new(),
            inverters: Vec::new(),
        }
    }
    fn set_values(supervisor: Supervisor, cons_meter: ConsumptionMeter, prod_meter: ProductionMeter, inverters: Vec<Inverter>) -> Self {
        Self {
            supervisor,
            cons_meter,
            prod_meter,
            inverters,
        }
    }
    
}

#[derive(Clone, Deserialize, Debug, sqlx::FromRow)]
struct Supervisor {
    #[serde(alias = "SERIAL")]
    serial: String,
    #[serde(with = "pvs6_date_format", alias = "DATATIME")]
    data_time: Option<DateTime<Utc>>,
    #[serde(with = "string_to_i32")]
    dl_comm_err: Option<i32>,
    #[serde(with = "string_to_f64")]
    dl_cpu_load: Option<f64>,
    #[serde(with = "string_to_i32")]
    dl_err_count: Option<i32>,
    #[serde(with = "string_to_u32")]
    dl_flash_avail: Option<u32>,
    #[serde(with = "string_to_u32")]
    dl_mem_used: Option<u32>,
    #[serde(with = "string_to_i32")]
    dl_scan_time: Option<i32>,
    #[serde(with = "string_to_i32")]
    dl_skipped_scans: Option<i32>,
    #[serde(with = "string_to_u32")]
    dl_untransmitted: Option<u32>,
    #[serde(with = "string_to_i64")]
    dl_uptime: Option<i64>,
}

impl Supervisor {
    fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: None,
            dl_comm_err:  None,
            dl_cpu_load:  None,
            dl_err_count:  None,
            dl_flash_avail:  None,
            dl_mem_used:  None,
            dl_scan_time:  None,
            dl_skipped_scans:  None,
            dl_untransmitted:  None,
            dl_uptime:  None,
        }
    }
    
    fn set_values(
        serial: String,  data_time: Option<DateTime<Utc>>,  dl_comm_err: Option<i32>,  dl_cpu_load: Option<f64>,  dl_err_count: Option<i32>,
        dl_flash_avail: Option<u32>,  dl_mem_used: Option<u32>,  dl_scan_time: Option<i32>,  dl_skipped_scans: Option<i32>,
        dl_untransmitted: Option<u32>,  dl_uptime: Option<i64>,
    ) -> Self {  
        Self {
            serial: serial.to_owned(),
            data_time,
            dl_comm_err,//.map( |opt_float| opt_float ),
            dl_cpu_load,
            dl_err_count,
            dl_flash_avail,
            dl_mem_used,
            dl_scan_time,
            dl_skipped_scans,
            dl_untransmitted,
            dl_uptime,
        }
    }
}

#[derive(Clone, Deserialize, Debug, sqlx::FromRow)]
struct ProductionMeter {
    #[serde(alias = "SERIAL")]
    serial: String,

    #[serde(with = "pvs6_date_format", alias = "DATATIME")]
    data_time: Option<DateTime<Utc>>,

    #[serde(with = "string_to_f64")]
    freq_hz: Option<f64>,

    #[serde(with = "string_to_f64")]
    i_a: Option<f64>,

    #[serde(with = "string_to_f64")]
    net_ltea_3phsum_kwh: Option<f64>,

    #[serde(with = "string_to_f64")]
    p_3phsum_kw: Option<f64>,

    #[serde(with = "string_to_f64")]
    q_3phsum_kvar: Option<f64>,

    #[serde(with = "string_to_f64")]
    s_3phsum_kva: Option<f64>,

    #[serde(with = "string_to_f64")]
    tot_pf_rto: Option<f64>,

    #[serde(with = "string_to_f64")]
    v12_v: Option<f64>,
}

impl ProductionMeter {
    fn new() -> Self {
        Self { 
            serial: String::new(),
            data_time: None,
            freq_hz: None,
            i_a: None,
            net_ltea_3phsum_kwh: None,
            p_3phsum_kw: None,
            q_3phsum_kvar: None,
            s_3phsum_kva: None,
            tot_pf_rto: None,
            v12_v: None,
        }
    }
    fn set_values(
        serial: &str, data_time: Option<DateTime<Utc>>, freq_hz: Option<f64>, i_a: Option<f64>, 
        net_ltea_3phsum_kwh: Option<f64>, p_3phsum_kw: Option<f64>, q_3phsum_kvar: Option<f64>, 
        s_3phsum_kva: Option<f64>, tot_pf_rto: Option<f64>, v12_v: Option<f64>) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time,
            freq_hz,
            i_a,
            net_ltea_3phsum_kwh,
            p_3phsum_kw,
            q_3phsum_kvar,
            s_3phsum_kva,
            tot_pf_rto,
            v12_v,
        }
    }
}

#[derive( Clone, Deserialize, Debug, sqlx::FromRow )]
struct ConsumptionMeter {
    #[serde(alias = "SERIAL")]
    serial: String,
    
    #[serde(with = "pvs6_date_format", alias = "DATATIME")]
    data_time: Option<DateTime<Utc>>,
    
    #[serde(with = "string_to_f64")]
    freq_hz: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    i1_a: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    i2_a: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    neg_ltea_3phsum_kwh: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    net_ltea_3phsum_kwh: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    p_3phsum_kw: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    p1_kw: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    p2_kw: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    pos_ltea_3phsum_kwh: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    q_3phsum_kvar: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    s_3phsum_kva: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    tot_pf_rto: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    v12_v: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    v1n_v: Option<f64>,
    
    #[serde(with = "string_to_f64")]
    v2n_v: Option<f64>,
}

impl ConsumptionMeter {
    fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: None,
            freq_hz: None,
            i1_a: None,
            i2_a: None,
            neg_ltea_3phsum_kwh: None,
            net_ltea_3phsum_kwh: None,
            p_3phsum_kw: None,
            p1_kw: None,
            p2_kw: None,
            pos_ltea_3phsum_kwh: None,
            q_3phsum_kvar: None,
            s_3phsum_kva: None,
            tot_pf_rto: None,
            v12_v: None,
            v1n_v: None,
            v2n_v: None,
        }
    }
    fn set_values( serial: String, data_time: Option<DateTime<Utc>>, freq_hz: Option<f64>, i1_a: Option<f64>, i2_a: Option<f64>,
        neg_ltea_3phsum_kwh: Option<f64>, net_ltea_3phsum_kwh: Option<f64>, p_3phsum_kw: Option<f64>, p1_kw: Option<f64>,
        p2_kw: Option<f64>, pos_ltea_3phsum_kwh: Option<f64>, q_3phsum_kvar: Option<f64>, s_3phsum_kva: Option<f64>,
        tot_pf_rto: Option<f64>, v12_v: Option<f64>, v1n_v: Option<f64>, v2n_v: Option<f64>, ) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time,
            freq_hz,
            i1_a,
            i2_a,
            neg_ltea_3phsum_kwh,
            net_ltea_3phsum_kwh,
            p_3phsum_kw,
            p1_kw: p1_kw,
            p2_kw: p2_kw,
            pos_ltea_3phsum_kwh,
            q_3phsum_kvar,
            s_3phsum_kva,
            tot_pf_rto,
            v12_v,
            v1n_v,
            v2n_v,
        }
    }
}

#[derive(Clone, Deserialize, Debug, sqlx::FromRow)]
struct Inverter {
    #[serde(alias = "SERIAL")]
    serial: String,
    
    #[serde(with = "pvs6_date_format", alias = "DATATIME")]
    data_time: Option<DateTime<Utc>>,

    #[serde(with = "string_to_f64")]
    freq_hz: Option<f64>,

    #[serde(with = "string_to_f64")]    
    i_3phsum_a: Option<f64>,

    #[serde(with = "string_to_f64")]    
    i_mppt1_a: Option<f64>,

    #[serde(with = "string_to_f64")]
    ltea_3phsum_kwh: Option<f64>,

    #[serde(with = "string_to_f64")]
    p_3phsum_kw: Option<f64>,

    #[serde(with = "string_to_f64")]
    p_mppt1_kw: Option<f64>,

    #[serde(with = "string_to_f64")]
    stat_ind: Option<f64>,

    #[serde(with = "string_to_f64")]    
    t_htsnk_degc: Option<f64>,

    #[serde(with = "string_to_f64")]    
    v_mppt1_v: Option<f64>,

    #[serde(with = "string_to_f64")]
    vln_3phavg_v: Option<f64>,
}

impl Inverter {
    /*fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: None,
            freq_hz: None,
            i_3phsum_a: None,
            i_mppt1_a: None,
            ltea_3phsum_kwh: None,
            p_3phsum_kw: None,
            p_mppt1_kw: None,
            stat_ind: None,
            t_htsnk_degc: None,
            v_mppt1_v: None,
            vln_3phavg_v: None,
        }
    }*/

    fn set_values( 
        serial: &str, data_time: Option<DateTime<Utc>>, freq_hz: Option<f64>, i_3phsum_a: Option<f64>, i_mppt1_a: Option<f64>,
        ltea_3phsum_kwh: Option<f64>, p_3phsum_kw: Option<f64>, p_mppt1_kw: Option<f64>, stat_ind: Option<f64>,
        t_htsnk_degc: Option<f64>, v_mppt1_v: Option<f64>, vln_3phavg_v: Option<f64>,
    ) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time: data_time,
            freq_hz,
            i_3phsum_a,
            i_mppt1_a,
            ltea_3phsum_kwh,
            p_3phsum_kw,
            p_mppt1_kw,
            stat_ind,
            t_htsnk_degc,
            v_mppt1_v,
            vln_3phavg_v,
        }
    }
} 


#[derive(Debug)]
enum ErrDeSerEpoch {
    InvalidEpochTimestamp,
    // ...
}
impl error::Error for ErrDeSerEpoch {}
impl fmt::Display for ErrDeSerEpoch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ErrDeSerEpoch::*;
        match self {
            InvalidEpochTimestamp => write!(f, "Expected a valid Epoch Timestamp in integer form"),
            // ...
        }
    }
}

//Config structures
#[derive(Debug, Deserialize, Clone)]
struct Conf {
    pirate_wx: PirateWxConf,
    pvs6: Pvs6Conf,
    mysql: MySqlConf
}
impl Conf {
    fn new() -> Self {
        Self {
            pirate_wx: PirateWxConf::new(),
            pvs6: Pvs6Conf::new(),
            mysql: MySqlConf::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone )]
struct PirateWxConf {
    #[serde( default = "default_string" )]
    lat: String,
    #[serde( default = "default_string" )]
    long: String,
    #[serde( default = "default_string" )]
    units: String,
    #[serde( default = "default_string" )]
    api_key_path: String,
    #[serde( default = "default_string" )]
    api_key: String,
    #[serde( default = "default_pirate_wx_interval" )]
    interval: u64,
    #[serde( default = "default_pirate_wx_interval_unit" )]
    interval_unit: char,
    #[serde( with = "integer_to_chrono_time_delta_ms", default = "default_pirate_wx_offset" )]
    offset: TimeDelta,
}
impl PirateWxConf {
    fn new() -> Self {
        Self {
            lat: String::new(),
            long: String::new(),
            units: String::new(),
            api_key_path: String::new(),
            api_key: String::new(),
            interval: 5,
            interval_unit: 'm',
            offset: TimeDelta::milliseconds(0)
        }
    }
}
fn default_pirate_wx_interval() -> u64 {
    5
}
fn default_pirate_wx_interval_unit() -> char {
    'm'
}
fn default_pirate_wx_offset() -> TimeDelta{
    TimeDelta::milliseconds(0)
}

#[derive(Debug, Deserialize, Clone )]
struct Pvs6Conf {
    #[serde( default = "default_string" )]
    host: String,
    #[serde( default = "default_pvs6_interval" )]
    get_device_interval: u64,
    #[serde( default = "default_pvs6_interval_unit" )]
    get_device_interval_unit: char,
    #[serde( with = "integer_to_chrono_time_delta_ms", default = "default_pvs6_offset" )]
    get_device_offset: TimeDelta
}
impl Pvs6Conf {
    fn new() -> Self {
        Self {
            host: String::new(),
            get_device_interval: 5,
            get_device_interval_unit: 'm',
            get_device_offset: TimeDelta::milliseconds(0),
        }
    }
}
fn default_pvs6_interval() -> u64 {
    5
}
fn default_pvs6_interval_unit() -> char {
    'm'
}
fn default_pvs6_offset() -> TimeDelta{
    TimeDelta::milliseconds(-200)
}

#[derive(Debug, Deserialize, Clone )]
struct MySqlConf {
    #[serde( default = "default_string" )]
    login_info_loc: String,
    #[serde( default = "default_string" )]
    login_path: String,
    #[serde( default = "default_string" )]
    host: String,
    #[serde( default = "default_string" )]
    port: String,
    #[serde( default = "default_string" )]
    database: String,
    #[serde( default = "default_string" )]
    user: String,
    #[serde( default = "default_string" )]
    password: String,
    #[serde( default = "default_max_connections" )]
    max_connections: u32
}
impl MySqlConf {
    fn new() -> Self {
        Self {
            login_info_loc: String::new(),
            login_path: String::new(),
            host: String::new(),
            port: String::new(),
            database: String::new(),
            user: String::new(),
            password: String::new(),
            max_connections: 75,
        }
    }
}
fn default_max_connections() -> u32 {
    75
}
fn default_string() -> String {
    String::new()
}

// Weather Structures
#[derive( Clone, Deserialize, Debug, sqlx::FromRow )]
struct Wx {
    latitude: f64,
    longitude: f64,

    #[serde( with = "string_to_opt_string", default = "default_opt_string" )]
    timezone: Option<String>,

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    offset: Option<f32>,  

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    elevation: Option<f32>,

    currently: CurrentWx,
    daily: DailyWx,
}

#[derive( Clone, Deserialize, Debug, sqlx::FromRow )]
struct CurrentWx {
    #[ serde( with = "unix_epoch_to_chrono_utc_date_time" )]
    time: DateTime<Utc>,
    
    #[serde( with = "string_to_opt_string", default = "default_opt_string" )]
    summary: Option<String>,

    #[serde( with = "string_to_opt_string", default = "default_opt_string"  )]
    icon: Option<String>,

    #[serde(alias = "nearestStormDistance", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    nearest_storm_distance: Option<f32>,

    #[serde( alias = "nearestStormBearing", with = "float_to_opt_f32", default = "default_opt_f32"  )] 
    nearest_storm_bearing: Option<f32>,       // should be u16, but it wasn't working

    #[serde( alias = "precipIntensity", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    precip_intensity: Option<f32>,

    #[serde( alias = "precipProbability", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    precip_probability: Option<f32>,

    #[serde( alias = "precipIntensityError", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    precip_intensity_error: Option<f32>,

    #[ serde( alias = "precipType", with = "string_to_opt_string", default = "default_opt_string"  ) ]
    precip_type: Option<String>,

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    temperature: Option<f32>,

    #[serde( alias = "apparentTemperature", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    apparent_temperature: Option<f32>,

    #[serde( alias = "dewPoint", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    dew_point: Option<f32>,

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    humidity: Option<f32>,
    
    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    pressure: Option<f32>,

    #[serde( alias = "windSpeed", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    wind_speed: Option<f32>,

    #[serde( alias = "windGust", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    wind_gust: Option<f32>,

    #[serde( alias = "windBearing", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    wind_bearing: Option<f32>, 

    #[serde( alias = "cloudCover", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    cloud_cover: Option<f32>,

    #[serde( alias = "uvIndex", with = "float_to_opt_f32", default = "default_opt_f32"  )]
    uv_index: Option<f32>,

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    visibility: Option<f32>,

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    ozone: Option<f32>,

    #[serde( with = "float_to_opt_f32", default = "default_opt_f32" )]
    smoke: Option<f32>,

    #[serde( alias = "fireIndex", with = "float_to_opt_f32", default = "default_opt_f32" )]
    fire_index: Option<f32>,

    #[serde( alias = "feelsLike", with = "float_to_opt_f32", default = "default_opt_f32" )]
    feels_like: Option<f32>,

    #[serde( alias = "currentDayIce",with = "float_to_opt_f32", default = "default_opt_f32" )]
    current_day_ice: Option<f32>,

    #[serde( alias = "currentDayLiquid", with = "float_to_opt_f32", default = "default_opt_f32" )]
    current_day_liquid: Option<f32>,

    #[serde( alias = "currentDaySnow", with = "float_to_opt_f32", default = "default_opt_f32" )]
    current_day_snow: Option<f32>,
}

#[derive( Clone, Deserialize, Debug, sqlx::FromRow )]
struct DailyWx {
    data: Vec<DailyWxData>
}

#[derive( Clone, Deserialize, Debug, sqlx::FromRow )]
struct DailyWxData {
    #[ serde( with = "unix_epoch_to_chrono_utc_date_time" )]
    time: DateTime<Utc>,

    #[serde( alias = "dawnTime", with = "unix_epoch_to_opt_chrono_utc_date_time", default = "default_opt_utc_dt" )]
    dawn_time:Option<DateTime<Utc>>,

    #[serde( alias = "duskTime", with = "unix_epoch_to_opt_chrono_utc_date_time", default = "default_opt_utc_dt" )]
    dusk_time:Option<DateTime<Utc>>,

    #[serde( alias = "sunriseTime", with = "unix_epoch_to_opt_chrono_utc_date_time", default = "default_opt_utc_dt" )]
    sunrise_time: Option<DateTime<Utc>>,


    #[serde( alias = "sunsetTime", with = "unix_epoch_to_opt_chrono_utc_date_time", default = "default_opt_utc_dt" )]
    sunset_time: Option<DateTime<Utc>>,

    #[serde( alias = "moonPhase", with = "float_to_opt_f32", default = "default_opt_f32" )]
    moon_phase: Option<f32>,

    #[serde( alias = "precipAccumulation", with = "float_to_opt_f32", default = "default_opt_f32" )]
    precip_accumulation: Option<f32>,

    #[serde( alias = "temperatureMin", with = "float_to_opt_f32", default = "default_opt_f32" )]
    temperature_min: Option<f32>,

 
    #[serde( alias = "temperatureMinTime", with = "unix_epoch_to_opt_chrono_utc_date_time", default = "default_opt_utc_dt" )]
    temperature_min_time: Option<DateTime<Utc>>,

    #[serde( alias = "temperatureMax", with = "float_to_opt_f32", default = "default_opt_f32" )]
    temperature_max: Option<f32>,


    #[serde( alias = "temperatureMaxTime", with = "unix_epoch_to_opt_chrono_utc_date_time", default = "default_opt_utc_dt" )]
    temperature_max_time: Option<DateTime<Utc>>,
}

fn default_opt_string() -> Option<String> {
    None
}
fn default_opt_f32() -> Option<f32> {
    None
}
fn default_opt_utc_dt() -> Option<DateTime<Utc>> {
    None
}

// MODUELES
mod string_to_opt_string {
    use serde::{self, Deserialize, Deserializer};
    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            Ok(None)
        } else {
            Ok( Some( s ) )
        }
        
    }
}

mod float_to_opt_f32 {
    use serde::{self, Deserialize, Deserializer};
    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<f32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = f32::deserialize(deserializer)?;
        Ok( Some( val ) )
    }
}

mod unix_epoch_to_chrono_utc_date_time {
    use chrono::{ DateTime, Utc };
    use serde::{self, Deserialize, Deserializer, de::Error};

    use crate::ErrDeSerEpoch;

    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = i64::deserialize(deserializer)?;
        let dt = DateTime::from_timestamp(val, 0);
        match dt {
            Some(date_time) => {
                Ok( date_time )
            },
            None => Err(ErrDeSerEpoch::InvalidEpochTimestamp).map_err(D::Error::custom)
        }
    }
}

mod unix_epoch_to_opt_chrono_utc_date_time {
    use chrono::{ DateTime, Utc };
    use serde::{self, Deserialize, Deserializer};

    use crate::ErrDeSerEpoch;

    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = i64::deserialize(deserializer)?;
        Ok( DateTime::from_timestamp(val, 0) )
    }
}

mod integer_to_chrono_time_delta_ms {
    use chrono::TimeDelta;
    use serde::{self, Deserialize, Deserializer, de::Error};

    use crate::ErrDeSerEpoch;

    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<TimeDelta, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = i64::deserialize(deserializer)?;
        let td = TimeDelta::milliseconds(val);
        Ok(td)
    }
}

mod pvs6_date_format {
    use chrono::{ DateTime, NaiveDateTime, Utc };
    use serde::{self, Deserialize, Deserializer};

    const FORMAT: &'static str = "%Y,%m,%d,%H,%M,%S";

    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok( Some( NaiveDateTime::parse_from_str( &s, FORMAT ).map_err(serde::de::Error::custom )?.and_utc() ) )

    }
}

mod string_to_i32 {
    use serde::{self, Deserialize, Deserializer};
    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let val = s.parse::<i32>().map_err(serde::de::Error::custom)?;
        Ok( Some( val ) )
    }
}

mod string_to_u32 {
    use serde::{self, Deserialize, Deserializer};
    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<u32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let val = s.parse::<u32>().map_err(serde::de::Error::custom)?;
        Ok( Some( val ) )
    }
}

mod string_to_i64 {
    use serde::{self, Deserialize, Deserializer};
    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<i64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let val = s.parse::<i64>().map_err(serde::de::Error::custom)?;
        Ok( Some( val ) )
    }
}

mod string_to_f64 {
    use serde::{self, Deserialize, Deserializer};
    pub fn deserialize<'de, D>( deserializer: D, ) -> Result<Option<f64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let val = s.parse::<f64>().map_err(serde::de::Error::custom)?;
        Ok( Some( val ) )
    }
}
    
//#[tokio::main]
#[tokio::main]
async fn main() {
    
    log4rs::init_file("log_config.yml", Default::default()).unwrap();   //Need Error Handling here.  What if log doesn't unwrap
    //get sqlx mysql pool and connection
    let mut conf ;
    

    let settings = Config::builder()
        // Add in `./Settings.toml`
        .add_source(config::File::with_name("config"))
        .build().unwrap();
    let conf_res = settings.try_deserialize::<Conf>();
    match conf_res {
        Ok(c) => conf = c,
        Err(de_eff) => {
            error!("Fatal Error.  Could not deserialize Pirate WX configuration file {}", de_eff);
            panic!("Fatal Error.  Could not deserialize Pirate WX configuration file {}", de_eff);
        }
    }

    // verify config file has the required parameters. These functions do not validate that parameter values are correct to work,
    // it only verifies they exist.

    // verifies pirate wx conf data
    conf.pirate_wx = verify_pirate_wx_conf(conf.pirate_wx);
    // verifies pvs6 conf data
    verify_pvs6_conf(&conf.pvs6);
    // verifies mysql conf data
    conf.mysql = verify_mysql_conf(conf.mysql); 

    let solar_pool = get_sqlx_solar_pool(&conf.mysql).await;
    
    
    let pirate_wx_handle = spawn( pirate_wx_to_mysql(solar_pool.clone(), conf.pirate_wx.clone() ) );
    let pvs6_handle = spawn( pvs6_to_mysql( solar_pool.clone(), conf.pvs6.clone() ) );

    let _ = pvs6_handle.await;
    let _ = pirate_wx_handle.await;

}

async fn pirate_wx_to_mysql(solar_pool: Option<sqlx::Pool<sqlx::MySql>>, pirate_wx_conf: PirateWxConf) {
    let mut get_wx_interval = set_interval(&pirate_wx_conf.interval, &pirate_wx_conf.interval_unit, &pirate_wx_conf.offset);
    
    loop {
        get_wx_interval.tick().await;
        let wx_opt = get_weather( &pirate_wx_conf ).await;

        if let Some(wx) = wx_opt {
            insert_pirate_wx_to_mysql(wx, &solar_pool ).await;
        }
    }
}
async fn pvs6_to_mysql(solar_pool: Option<sqlx::Pool<sqlx::MySql>>, pvs6_conf: Pvs6Conf) {
     
    // Set the offset duration of the interval.  For fine tuning timing request.  We want the pvs6 response time for the request (ie the data_time) to be as close to the 
    // interval time we planned, but not before it.  There is some small delay in the PVS6 receiving the request and then processing it.  This negative offset is meant to 
    // fine tune that as needed.     
    // set the start time and interval used to get pvs6 device data.
    let mut get_pvs6_device_interval = set_interval(&pvs6_conf.get_device_interval, &pvs6_conf.get_device_interval_unit, &pvs6_conf.get_device_offset);
     
    loop {
        // Wait until the next tick (start time and interval)
        get_pvs6_device_interval.tick().await; 
        // get pvs6 data and upload to mysql solar database
        println!( "Run at: {}", Utc::now().to_string() ); //for loop timing testing
        debug!( "Run at: {}", Utc::now().to_string() ); //for loop timing testing
        
        
        let pvs6_opt = get_pvs6_device_data(&pvs6_conf).await;

        let mut latest_data = get_latest_pvs6_data_from_sql(&solar_pool).await;

        if let Some( pvs6_data ) = pvs6_opt {
            if let Some (deser_pvs6) = deserialize_pvs6_devices(pvs6_data) {
                //println!("{:#?}", deser_pvs6);
                let cleaned_pvs6_data = update_pvs6_old_responses(deser_pvs6, &latest_data);
                //println!("{:#?}", cleaned_pvs6_data);
                insert_pvs6_data_to_mysql( cleaned_pvs6_data, &solar_pool ).await;
            } 
        }
    }
}

fn deserialize_pvs6_devices( pvs6_data: String ) -> Option<Pvs6DevicesResponse> {
    // Function takes pvs6 devices response (from API call <host or ip>/cgi-bin/dl_cgi?Command=DeviceList")
    // and returns deserialized structure Pvs6DevicesResponse which is a structure of all devices.  Each device is typed
    // ie Supervisor, Production Meter, Consumption Meter, Inverter.  Inverters are stored as vector of inverters.  Currently
    // can handle systems with 1 supervisor, 1 production meter, 1 consumption meter and unlimited inverters.
    // structure stores serial numbers, data_time, and data (fields that change over time).  Structure ready for upload to 
    // mysql database with exception that data may be old and already uploaded to sql database.  separate function to address that.

    // Function returns Option<Pvs6Response>.  It returns a response if any of the devices are properly deserialized for upload and 
    // None if none of the devices were properly serialized  

    //Regex patterns synced lazy for compile efficiency improvement
    //Unwrapping all Regex expressions.  If it doesn't unwrap, its a bug in the hardcoded expression and needs to be caught at first runtime.
    static RE_RESULT: Lazy<Regex> = Lazy::new( || Regex::new(r#""result".*?:.*?"succeed""#).unwrap() );
    static RE_ALL_DEVICES: Lazy<Regex> = Lazy::new( || Regex::new(r#"\[.*\]"#).unwrap() );
    static RE_DEVICE: Lazy<Regex> = Lazy::new( || Regex::new(r#"\{.*?\}"#).unwrap() );
    static RE_DEVICE_TYPE: Lazy<Regex> = Lazy::new( || Regex::new(r#""DEVICE_TYPE".*?:.*?"(.*?)""#).unwrap() );
    static RE_SERIAL: Lazy<Regex> = Lazy::new( || Regex::new(r#""SERIAL".*?:.*?"(.*?)""#).unwrap() );
    static RE_STATE: Lazy<Regex> = Lazy::new( || Regex::new(r#""STATE".*?:.*?"error""#).unwrap() );
    static RE_TYPE: Lazy<Regex> = Lazy::new( || Regex::new(r#""TYPE".*?:.*?"(.*?)""#).unwrap() );

    // declared to hold deserialized values and final return structure
    let mut cur_device_data: Pvs6DevicesResponse = Pvs6DevicesResponse::new();
    let mut inverters: Vec<Inverter> = Vec::new();
    let mut supervisor: Supervisor = Supervisor::new();
    let mut production_meter: ProductionMeter = ProductionMeter::new();
    let mut consumption_meter: ConsumptionMeter = ConsumptionMeter::new();
    
    //Inverter counter for identifying which inverter in the json had errors.  Counts from 0 to last inverter -1.
    let mut inverter_cnt: i32 = -1;

    // Flags whether any devices successfully deserialized.  True if at least one is successful.
    let mut flg_device_deserialized = false;
    
    //strip out excess tabs '\t', newlines '\n' and space characters. ' '
    let pvs6_data: String = pvs6_data.chars().filter(|c| !matches!(c, '\t' | '\n' )).collect::<String>();

    // check if json response was "sucess";   
    if RE_RESULT.is_match(&pvs6_data) {
        let mat_devices = RE_ALL_DEVICES.find(&pvs6_data).unwrap();
        if ! mat_devices.is_empty() {
            let device_list: &str = mat_devices.as_str();

            // Find each indivdual device json and collect into a vector
            let devices: Vec<&str> = RE_DEVICE.find_iter(device_list).map(|m| m.as_str()).collect();

            //itterate through each device in list, get device json string, match to correct device type and deserialize
            for device in devices.iter() {
                match RE_DEVICE_TYPE.captures(&device) {
                    Some(device_type_opt) => {
                        match device_type_opt.get(1) {
                            Some(device_type) => {
                                match device_type.as_str() {
                                    SUPERVISOR => {
                                        let sup_opt: Result<Supervisor> = serde_json::from_str(&device);
                                        match sup_opt {
                                            Ok(sup) => {
                                                supervisor = sup;
                                                flg_device_deserialized = true;
                                            },
                                            Err(sup_eff) => {
                                                error!("Could not deserialize Supervisor. Err: {}", sup_eff);
                                                //add error handling
                                            },
                                        }
                                    },
                                    METER => {
                                        match RE_TYPE.captures(&device) {
                                            Some(type_opt) => {
                                                match type_opt.get(1) {
                                                    Some(type_d) => {
                                                        match type_d.as_str() {
                                                            PRODUCTION_METER => {
                                                                let pm_opt: Result<ProductionMeter> = serde_json::from_str(&device); 
                                                                match pm_opt {
                                                                    Ok(pm) => {
                                                                        production_meter = pm;
                                                                        flg_device_deserialized = true;
                                                                    },
                                                                    Err(pm_eff) => {
                                                                        error!("Could not deserialize Production Meter. Err: {}", pm_eff);
                                                                        //add error handling
                                                                    },
                                                                }
                                                            },
                                                            CONSUMPTION_METER => {
                                                                let cm_opt: Result<ConsumptionMeter> = serde_json::from_str(&device); 
                                                                match cm_opt {
                                                                    Ok(cm) => {
                                                                        consumption_meter = cm;
                                                                        flg_device_deserialized = true;
                                                                    },
                                                                    Err(cm_eff) => {
                                                                        error!("Could not deserialize Consumption Meter. Err: {}", cm_eff);
                                                                        //add error handling
                                                                    },
                                                                }
                                                            },
                                                            _ => {
                                                                error!("Meter did not match an appropriate TYPE");
                                                                //Add Error Handling that couldn't process data
                                                            },
                                                        }
                                                    },
                                                    None => {
                                                        error!("Could not find TYPE of meter.  There was no Regex capture.");
                                                        //Add Error Handling that couldn't process data
                                                    },
                                                }
                                            },
                                            None => {
                                                error!("Could not find TYPE of meter.  There was no Regex match.");
                                                //Add Error Handling that couldn't process data
                                            },
                                        }
                                    },
                                    INVERTER => {
                                        inverter_cnt +=1;
                                        //print!("Inverter # {}: {}", inverter_cnt, &device);
                                        if !RE_STATE.is_match(&device) {
                                            let inv_opt: Result<Inverter> = serde_json::from_str(&device);
                                            match inv_opt {
                                                Ok(inv) => {
                                                    inverters.push(inv);
                                                    flg_device_deserialized = true;
                                                },
                                                Err(inv_eff) => {
                                                    // Consider adding code here to check if inverter STATE was error: 
                                                    error!("Could not deserialize Inverter. Inverter was #: {} in vector. Err: {}", inverter_cnt, inv_eff);
                                                },
                                            }
                                        } else {
                                            match RE_SERIAL.captures(&device) {
                                                Some(serial_opt) => {
                                                    match serial_opt.get(1) {
                                                        Some(serial_d) => {
                                                            warn!( "Inverter {} (#{}) reported STATE:error.", serial_d.as_str(), inverter_cnt );
                                                        },
                                                        None => {
                                                            error!("Inverter # {} reported STATE:error and it's serial number could not be determined", inverter_cnt);
                                                        }
                                                    }
                                                },
                                                None => {
                                                    error!("Inverter # {} reported STATE:error and it's serial number could not be determined", inverter_cnt);
                                                }
                                            }
                                        }
                                    },
                                    _ => {
                                        error!("Device did not match an appropriate DEVICE_TYPE. Device type was {}", device_type.as_str())
                                        //Add Error Handling
                                    },

                                }
                            },
                            None => {
                                error!("Could not find DEVICE_TYPE of device.  There was no Regex capture.");
                                 //Add Error Handling that couldn't process data
                            },
                        }
                    },
                    None => {
                        error!("Could not find DEVICE_TYPE of device.  There was no Regex match.");
                        //Add Error Handling that couldn't process data
                    },
                }
            }  //end of for iter on devices json strings
            
            //println!("{:#?}",supervisor);
            
            cur_device_data = Pvs6DevicesResponse::set_values(
                supervisor, consumption_meter, production_meter, inverters
            );


        } else {
            error!("Unable to Regex match device list ie [...]");
            // Add error handling
        }
    } else {
        //Response was not successful.  log failed response and skip processing
        error!("PVS6 JSON responsed Unsucessful.");
        //Should add error here.
    }
    
    if flg_device_deserialized == true {
        debug!("Deserialized result returned from fn deserialize_pvs6-devices");
        //println!("{:#?}", cur_device_data);
        return Some( cur_device_data )
    } else {
        warn!("No devices were able to be deserialized");
        return None
    }
}

async fn get_sqlx_solar_pool(mysql_conf: &MySqlConf) -> Option<sqlx::Pool<sqlx::MySql>> {
    // gets sqlx mysql pool of connections for mysql solar db 
    

    let solar_mysql_url = format!( "mysql://{}:{}@{}:{}/{}", mysql_conf.user, mysql_conf.password, 
        mysql_conf.host, mysql_conf.port, mysql_conf.database );
    
    let sqlx_solar_pool = MySqlPoolOptions::new()
    .max_connections(mysql_conf.max_connections)
    .connect(&solar_mysql_url)
    .await;
    
    match sqlx_solar_pool {
        Ok(sqlx_solar_pool) => {
            info!("Pool Created");
            return Some(sqlx_solar_pool)
        },
        Err(pool_eff) => {
            error!("Unable to create pool for mysql db solar. Err: {}", pool_eff);
            return None
        },
    }
            
}

async fn insert_pvs6_data_to_mysql( data: Pvs6DevicesResponse, solar_sql_upload_pool: &Option<sqlx::Pool<sqlx::MySql>>) {
    // takes data from pvs6 (in Pvs6DeviceResponse Struct) and inserts it into mysql solar db tables for each type of device. 
    
    // Future addition - save any data not uploaded to db to a file (one file for each device type) to allow for future uploads once problem identified.
    
    let mut all_device_success: bool = true;  
    
    match solar_sql_upload_pool {
        Some( sql_pool ) => {
            //( serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw,
            //    p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v )
            // Upload Supervisor Data

            //println!("{:#?}", &data.supervisor);

            let sup_result = sqlx::query(SUP_INSERT_QUERY)
                .bind(&data.supervisor.serial)
                .bind(&data.supervisor.data_time)
                .bind(&data.supervisor.dl_comm_err)
                .bind(&data.supervisor.dl_cpu_load)
                .bind(&data.supervisor.dl_err_count)
                .bind(&data.supervisor.dl_flash_avail)
                .bind(&data.supervisor.dl_mem_used)
                .bind(&data.supervisor.dl_scan_time)
                .bind(&data.supervisor.dl_skipped_scans)
                .bind(&data.supervisor.dl_untransmitted)
                .bind(&data.supervisor.dl_uptime)
                .execute(sql_pool).await;

            match sup_result {
                Ok(_) => {
                    debug!(
                        "Supervisor: {} @ {:#?} uploaded to Mysql solar database", 
                        &data.supervisor.serial, format!("{}", &data.supervisor.data_time.unwrap().format("%Y-%m-%d %H:%M:%S")) 
                    );
                },
                Err(sup_eff) => {
                    let mut serial = "Unknown";
                    let mut date_time = "Unknown".to_string();

                    if !data.supervisor.serial.is_empty() {
                        serial = &data.supervisor.serial;
                    }
                    if data.supervisor.data_time.is_some() {
                        date_time = format!("{}", data.supervisor.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"));
                    }
                    error!(
                        "Supervisor: {} @ {:#?} failed to upload to Mysql solar database. Error: {}", 
                        serial, date_time, sup_eff
                    );
                    all_device_success = false;
                }, 
            }
                //( serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw,
                //    p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v )
                // Upload Consumption Meter Data
            let cons_meter_result = sqlx::query(CM_INSERT_QUERY)
                .bind(&data.cons_meter.serial)
                .bind(&data.cons_meter.data_time)
                .bind(&data.cons_meter.freq_hz)
                .bind(&data.cons_meter.i1_a)
                .bind(&data.cons_meter.i2_a)
                .bind(&data.cons_meter.neg_ltea_3phsum_kwh)
                .bind(&data.cons_meter.net_ltea_3phsum_kwh)
                .bind(&data.cons_meter.p_3phsum_kw)
                .bind(&data.cons_meter.p1_kw)
                .bind(&data.cons_meter.p2_kw)
                .bind(&data.cons_meter.pos_ltea_3phsum_kwh)
                .bind(&data.cons_meter.q_3phsum_kvar)
                .bind(&data.cons_meter.s_3phsum_kva)
                .bind(&data.cons_meter.tot_pf_rto)
                .bind(&data.cons_meter.v12_v)
                .bind(&data.cons_meter.v1n_v)
                .bind(&data.cons_meter.v2n_v)
                .execute(sql_pool).await;

            match cons_meter_result {
                Ok(_) => {
                    debug!(
                    "Consumption Meter: {} @ {:#?} uploaded to Mysql solar database", 
                        &data.cons_meter.serial, format!("{}", &data.cons_meter.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"))
                    );
                },
                Err(cons_meter_eff) => {
                    let mut serial = "Unknown";
                    let mut date_time = "Unknown".to_string();

                    if !data.cons_meter.serial.is_empty() {
                        serial = &data.cons_meter.serial;
                    }
                    if data.cons_meter.data_time.is_some() {
                        date_time = format!("{}", data.cons_meter.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"));
                    }
                    error!(
                        "Consumption Meter: {} @ {:#?} failed to upload to Mysql solar database. Error: {}", 
                        serial, date_time, cons_meter_eff
                    );
                    all_device_success = false;
                }, 
            }
        

            //( serial, data_time, freq_hz, i_a, net_ltea_3phsum_kwh, 
            //    p_3phsum_kw, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v )
            // Upload Production Meter Data
            let prod_meter_result = sqlx::query(PM_INSERT_QUERY)
                .bind(&data.prod_meter.serial)
                .bind(&data.prod_meter.data_time)
                .bind(&data.prod_meter.freq_hz)
                .bind(&data.prod_meter.i_a)
                .bind(&data.prod_meter.net_ltea_3phsum_kwh)
                .bind(&data.prod_meter.p_3phsum_kw)
                .bind(&data.prod_meter.q_3phsum_kvar)
                .bind(&data.prod_meter.s_3phsum_kva)
                .bind(&data.prod_meter.tot_pf_rto)
                .bind(&data.prod_meter.v12_v)
                .execute(sql_pool).await;

            match prod_meter_result {
                Ok(_) => {
                    debug!(
                    "Production Meter: {} @ {:#?} uploaded to Mysql solar database", 
                        &data.prod_meter.serial, format!("{}", &data.prod_meter.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"))
                    );
                },
                Err(prod_meter_eff) => {
                    let mut serial = "Unknown";
                    let mut date_time = "Unknown".to_string();

                    if !data.prod_meter.serial.is_empty() {
                        serial = &data.prod_meter.serial;
                    }
                    if data.prod_meter.data_time.is_some() {
                        date_time = format!("{}", data.prod_meter.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"));
                    }
                    error!(
                        "Production Meter: {} @ {:#?} failed to upload to Mysql solar database. Error: {}", 
                        serial, date_time, prod_meter_eff
                    );
                    all_device_success = false;
                }, 
            }

            //( serial, data_time, freq_hz, i_3phsum_a, i_mppt1_a, ltea_3phsum_kwh, p_3phsum_kw, 
            //p_mppt1_kw, stat_ind, t_htsnk_degc, v_mppt1_v, vln_3phavg_v )
            // Upload Inverter data
            for inv in data.inverters.iter() {
                let inv_result = sqlx::query(INV_INSERT_QUERY)
                    .bind(&inv.serial)
                    .bind(&inv.data_time)
                    .bind(&inv.freq_hz)
                    .bind(&inv.i_3phsum_a)
                    .bind(&inv.i_mppt1_a)
                    .bind(&inv.ltea_3phsum_kwh)
                    .bind(&inv.p_3phsum_kw)
                    .bind(&inv.p_mppt1_kw)
                    .bind(&inv.stat_ind)
                    .bind(&inv.t_htsnk_degc)
                    .bind(&inv.v_mppt1_v)
                    .bind(&inv.vln_3phavg_v)
                    .execute(sql_pool).await;

                match inv_result {
                    Ok(_) => {
                        debug!(
                        "Inverter: {} @ {} uploaded to Mysql solar database", 
                            &inv.serial, format!("{}", &inv.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"))
                        );
                    },
                    Err(inv_eff) => {
                        let mut serial = "Unknown";
                        let mut date_time = "Unknown".to_string();

                        if !inv.serial.is_empty() {
                            serial = &inv.serial;
                        }
                        if inv.data_time.is_some() {
                            date_time = format!("{}", inv.data_time.unwrap().format("%Y-%m-%d %H:%M:%S"));
                        }
                        error!(
                            "Inverter: {} @ {:#?} failed to upload to Mysql solar database. Error: {}", 
                            serial, date_time,inv_eff
                        );
                        all_device_success = false;
                    }, 
                }
            }
            match all_device_success {
                true => {
                    info!("All devices uploaded to mysql db solar");
                    //return Ok(())
                }
                false => {
                    warn!("Some devices not uploaded to mysql db solar. See specific device errors.");
                    //return Err(())
                }
            }
        },
        None => error!("Couldn't get sql pool"),
    }
        
}

async fn get_latest_pvs6_data_from_sql( solar_sql_upload_pool: &Option<sqlx::Pool<sqlx::MySql>> ) -> Pvs6DevicesResponse {
    let mut latest_sql_pvs6_data: Pvs6DevicesResponse = Pvs6DevicesResponse::new();
    
    match solar_sql_upload_pool {
        Some( sql_pool) => {
            let sup = sqlx::query_as::<_, Supervisor>(QUERY_GET_LATEST_SUP_DATA)
            .fetch_all(sql_pool).await;

            let cm = sqlx::query_as::<_, ConsumptionMeter>(QUERY_GET_LATEST_CM_DATA)
            .fetch_all(sql_pool).await;

            let pm = sqlx::query_as::<_, ProductionMeter>(QUERY_GET_LATEST_PM_DATA)
            .fetch_all(sql_pool).await;

            let inv = sqlx::query_as::<_, Inverter>(QUERY_GET_LATEST_INV_DATA)
            .fetch_all(sql_pool).await;

            latest_sql_pvs6_data = Pvs6DevicesResponse::set_values(
                match sup {
                    Ok(sup_v) => {
                        match sup_v.len().cmp(&1) {
                            Ordering::Less => {
                                warn!("solar db did not return latest supervisor data");
                                Supervisor::new()
                            },
                            Ordering::Equal => sup_v[0].clone(),
                            Ordering::Greater => {
                                warn!("solar db returned more than one lastest supervisor data.  Took the first ");
                                sup_v[0].clone()
                            },
                        }
                    },
                    Err(sup_eff) => {
                        error!("{}",sup_eff);
                        Supervisor::new()
                    },
                },
                match cm {
                    Ok(cm_v) => {
                        match cm_v.len().cmp(&1) {
                            Ordering::Less => {
                                warn!("solar db did not return latest consumption meter data");
                                ConsumptionMeter::new()
                            },
                            Ordering::Equal => cm_v[0].clone(),
                            Ordering::Greater => {
                                warn!("solar db returned more than one lastest consumption meter data.  Took the first ");
                                cm_v[0].clone()
                            },
                        }
                    },
                    Err(cm_eff) => {
                        error!("{}",cm_eff);
                        ConsumptionMeter::new()
                    },
                }, 
                match pm {
                    Ok(pm_v) => {
                        match pm_v.len().cmp(&1) {
                            Ordering::Less => {
                                warn!("solar db did not return latest production meter data");
                                ProductionMeter::new()
                            },
                            Ordering::Equal => pm_v[0].clone(),
                            Ordering::Greater => {
                                warn!("solar db returned more than one lastest production meter data.  Took the first ");
                                pm_v[0].clone()
                            },
                        }
                    },
                    Err(pm_eff) => {
                        error!("{}",pm_eff);
                        ProductionMeter::new()
                    },
                }, 
                match inv {
                    Ok(inv_v) => {
                        match inv_v.len().cmp(&1) {
                            Ordering::Less => {
                                warn!("solar db did not return latest inverters data");
                                Vec::new()
                            },
                            Ordering::Equal => inv_v,
                            Ordering::Greater => inv_v,
                        }
                    },
                    Err(inv_eff) => {
                        error!("{}",inv_eff);
                        Vec::new()
                    },
                }, 
            );
        },
        None => error!("Couldn't get SQL pool"),
    }    
    latest_sql_pvs6_data
}

fn set_interval(repeat_interval: &u64, units: &char, offset: &Duration) -> Interval {
    // repeat_interval: time in seconds that interval should repeat
    // units: unit of time.  Only d, h, m, s are accepted.  All others will panic
    // Set start time and interval of pvs6 data pulls.  

    // convert repeat interval to seconds.  Panic if units is not 'd' day(s), 'h' hour(s), 'm' minute(s)), or 's' second(s) 
    let repeat_interval_s =   match units {
        'd' => repeat_interval * 60 * 60 * 24,
        'h' => repeat_interval * 60 * 60,
        'm' => repeat_interval * 60,
        's' => *repeat_interval,
        other => panic!("Invalid time unit: {}. Use d, h, m, or s", other),
    };
    debug!("Repeat Interval in seconds: {}", repeat_interval_s);
    // Get current time 
    let now: DateTime<Utc> = Utc::now();

    let mut target_time: NaiveDateTime = now.naive_utc();
    debug!("Original Target Time: {}", target_time.to_string() );
    let mut next_start: Duration = chrono::Duration::seconds( 60 );
    debug!("Original next_start: {}", next_start); 

    // Round target_time (planned Start time) to nearest interval based on interval length.  Intent is to have intervals that will
    // match clock times as much as possible.  Only works if exact intervals used here.  Future changes may round repeat_interval to match options.
    if repeat_interval_s <= 60 {  // round to nearest minute
        target_time = target_time.duration_round( Duration::minutes( 1 ) ).unwrap();
        debug!("target_time rounded to nearest minute. Target Time: {}", target_time);
    }
    else if repeat_interval_s <= 60 * 5 {  // round to nearest 5 minutes
        target_time = target_time.duration_round( Duration::minutes( 5 ) ).unwrap();
        next_start = chrono::Duration::seconds ( 60 * 5 );
        debug!("target_time rounded to nearest 5 minutes. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 10 {  // round to nearest 10 minutes
        target_time = target_time.duration_round( Duration::minutes( 10 ) ).unwrap();
        next_start = chrono::Duration::seconds ( 60 * 10 );
        debug!("target_time rounded to nearest 10 minutes. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 15 {  // round to nearest 15 minutes
        target_time = target_time.duration_round( Duration::minutes( 15 ) ).unwrap();
        next_start = chrono::Duration::minutes ( 15 );
        debug!("target_time rounded to nearest 15 minutes. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 30 {  // round to nearest 30 minutes
        target_time = target_time.duration_round( Duration::minutes( 30 ) ).unwrap();
        next_start = chrono::Duration::minutes ( 30 );
        debug!("target_time rounded to nearest 30 minutes. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 60 * 1 {  // round to nearest hour
        target_time = target_time.duration_round( Duration::hours( 1 ) ).unwrap();
        next_start = chrono::Duration::hours ( 1 );
        debug!("target_time rounded to nearest hour. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 60 * 2 {  // round to nearest 2 hours
        target_time = target_time.duration_round( Duration::hours( 2 ) ).unwrap();
        next_start = chrono::Duration::hours ( 2 );
        debug!("target_time rounded to nearest 2 hours. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 60 * 3 {  // round to nearest 3 hours
        target_time = target_time.duration_round( Duration::hours( 3 ) ).unwrap();
        next_start = chrono::Duration::hours ( 3 );
        debug!("target_time rounded to nearest 3 hours. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 60 * 4 {  // round to nearest 4 hours
        target_time = target_time.duration_round( Duration::hours( 4 ) ).unwrap();
        next_start = chrono::Duration::hours ( 4 );
        debug!("target_time rounded to nearest 4 hours. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 60 * 6 {  // round to nearest 6 hours
        target_time = target_time.duration_round( Duration::hours( 6 ) ).unwrap();
        next_start = chrono::Duration::hours ( 6 );
        debug!("target_time rounded to nearest 6 hours. Target Time: {}", target_time);
    }
	else if repeat_interval_s <= 60 * 60 * 12 {  // round to nearest 12 hours
        target_time = target_time.duration_round( Duration::hours( 12 ) ).unwrap();
        next_start = chrono::Duration::hours ( 12 );
        debug!("target_time rounded to nearest 12 hours. Target Time: {}", target_time);
    }
	else {  // round to nearest day
        target_time = target_time.duration_round( Duration::days( 1 ) ).unwrap();
        next_start = chrono::Duration::days ( 1 );
        debug!("target_time rounded to nearest day. Target Time: {}", target_time);
    }
    
    target_time += *offset;  //adjust target time by user supplied offset
    
    let mut start = target_time.signed_duration_since(now.naive_utc());
    
    
    if start < chrono::Duration::milliseconds(500) {
        start = start + next_start;
    }

    interval_at(tokio::time::Instant::now() + start.to_std().unwrap(), TokioDuration::from_secs( repeat_interval_s ) )
}

async fn get_pvs6_device_data(conf: &Pvs6Conf) -> Option<String> {
    // Using direct Reqwest::get fn instead of creating client.  PVS6 loses main internet connection and 
    // will not upload data when installer port (where we are making request) connection remains open.
    //  PVS6 is known to have a bug that causes memory to fill up and crash PVS6 if data is not uploaded.

    let url_devices_api = format!("{}{}",conf.host,URL_DEVICES_API);

    let pvs6_received = get(url_devices_api).await;

    // If PVS6 responded, check response status.  Else, log error and return none
    match pvs6_received {  
        Ok(pvs6_response) => {   
            
            //if PVS6 response is ok, then get body as text. Else, log response as error and return none
            match pvs6_response.status() {   
                reqwest::StatusCode::OK => {
                    
                    //If body extracts as text, return body as Some(String) else log error and return none
                    match pvs6_response.text().await {  
                        Ok(pvs6_data) => {
                            info!("PVS6 response body extracted to text: Ok");
                            return Some(pvs6_data)
                        },
                        Err(text_eff) => {
                            error!("PVS Response code: OK, but unable to extract body text from pvs6 response. Err: {:#?}", text_eff);
                            return None
                        },
                    };
                },
                other => {
                    error!("PVS6 returned error code: {}", other);
                    return None
                },
            };
        },
        Err(response_eff) => {
            warn!("PVS6 did not respond. Error Code: {}", response_eff);
            return None
        },
    }
}

fn update_pvs6_old_responses (cur_data: Pvs6DevicesResponse, latest_sql_data: &Pvs6DevicesResponse) -> Pvs6DevicesResponse {
    let mut check_dts: Vec<&Option<DateTime<Utc>>> = 
        vec![ &cur_data.supervisor.data_time, &cur_data.prod_meter.data_time, &cur_data.cons_meter.data_time ];
    for inv in cur_data.inverters.iter() {
        check_dts.push(&inv.data_time);
    }
    
    let greatest_cur_dt: Option<DateTime<Utc>> = greater_option_dt(check_dts);

    let sup: Supervisor;
    let pm: ProductionMeter;
    let cm: ConsumptionMeter;
    let mut invs: Vec<Inverter> = Vec::new();
    // check if supervisor serial and data_time are same for cur_data and latest_sql_data (ie already in sql)
    // if so, set data_time to greatest current time, set serial to serial and set all other values to None.
    if cur_data.supervisor.serial == latest_sql_data.supervisor.serial && 
        cur_data.supervisor.data_time == latest_sql_data.supervisor.data_time {

        sup = Supervisor::set_values(cur_data.supervisor.serial, greatest_cur_dt, 
            None, None, None, None, None,
             None, None, None, None );
    } else {
        sup = cur_data.supervisor;
    }
    // check if production meter serial and data_time are same for cur_data and latest_sql_data (ie already in sql)
    // if so, set data_time to greatest current time, set serial to serial and set all other values to None.
    if cur_data.prod_meter.serial == latest_sql_data.prod_meter.serial &&
        cur_data.prod_meter.data_time == latest_sql_data.prod_meter.data_time {

        pm = ProductionMeter::set_values(&cur_data.prod_meter.serial, greatest_cur_dt, None, 
            None, None, None, None,  
            None, None, None );
    } else {
        pm = cur_data.prod_meter;
    }
    // check if consumption meters serial and data_time are same for cur_data and latest_sql_data (ie already in sql)
    // if so, set data_time to greatest current time, set serial to serial and set all other values to None.
    if cur_data.cons_meter.serial == latest_sql_data.cons_meter.serial &&
        cur_data.cons_meter.data_time == latest_sql_data.cons_meter.data_time {

        cm = ConsumptionMeter::set_values(cur_data.cons_meter.serial, greatest_cur_dt, 
            None, None, None, None, None, 
            None, None, None, None,  
            None, None, None, None, 
            None, None, )
    } else {
        cm = cur_data.cons_meter;
    }
    // check if inverters serial and data_time are same for cur_data and latest_sql_data (ie already in sql)
    // if so, set data_time to greatest current time, set serial to serial and set all other values to None.
    for cur_inv in cur_data.inverters.iter() {
        let mut inv_found_flg = false;
        // for each inverter in current set, iterate through latest inverter data from sql database
        for latest_inv in latest_sql_data.inverters.iter() {
            // check if curent inverter serial and latest inverter from solar db are same
            if cur_inv.serial == latest_inv.serial {
                println!( "cur serial: {}, latest serial: {}.  Same", cur_inv.serial, latest_inv.serial );
                inv_found_flg = true;
                // if date_time also match, data alread in system. 
                println!( "{}: cur dt: {:#?}, {}: latest dt: {:#?}.", cur_inv.serial, Some(cur_inv.data_time),  latest_inv.serial, Some(latest_inv.data_time) ); 
                if Some( cur_inv.data_time ) == Some( latest_inv.data_time ) {
                    println!("They are the same");
                    // update date_time to latest current date time and set data to none.  Add to data to be uploaded to system
                    invs.push( Inverter::set_values(&cur_inv.serial, greatest_cur_dt, 
                        None, None, None, None, 
                        None, None, None, None, 
                        None, None) );
                }
                // if date-time are not the same, new data to add to system.
                else {
                    invs.push(cur_inv.clone());
                    println!("They are the same");
                }
                // matched cur inverter serial to latest inverter serial.  don't need to keep looping through latest to
                // to find the match.  break to next current inverter.
                break;
            }
        }
        // if current inverter was not found in latest inverters table from solar db,  
        // Add it and warn log
        if !inv_found_flg {
            invs.push(cur_inv.clone());
            warn!("Inverter {} serial number was not found in latest data from solar db inverters_data table.  \
                That's an oddity to look into.", cur_inv.serial);
        }
    }

    Pvs6DevicesResponse::set_values( sup, cm, pm, invs )
    
}

fn greater_option_dt( dt_vec: Vec<&Option<DateTime<Utc>>> ) -> Option<DateTime<Utc>> {
    let mut greatest_dt: Option<DateTime<Utc>> = None;
    match dt_vec.len().cmp(&1) {
        //If Vector is empty, make no changes (return None)
        Ordering::Less => warn!("No date_time values provided."),
        //If only one item in vector, its the biggest
        Ordering::Equal => greatest_dt = *dt_vec[0],
        // Enough items in vector to compare
        Ordering::Greater => {
            // Set first item in vector to the biggest
            greatest_dt = *dt_vec[0]; 
            println!( "greatest_dt is first item {:#?}", greatest_dt );
            // iterate through vector, skipping first item
            for dt in dt_vec.iter().skip(1) {
                
                if greatest_dt.is_none() && dt.is_some()  {
                    // The item with some is greater than the item with none
                    println!( "greatest_dt is None: {:#?} and dt is some: {:#?}.", greatest_dt, **dt );
                    greatest_dt = **dt;
                    println!( "greatest_dt is now: {:#?}", greatest_dt );
                } 
                else if greatest_dt.is_some() && dt.is_some() {
                    println!( "greatest_dt is Some: {:#?} and dt is some: {:#?}.", greatest_dt, **dt );
                    if Some( **dt ) >  Some( greatest_dt ) {
                        println!( "dt is bigger" );
                        // when they both have a value, make dt the greatest if it is bigger than the current greatest
                        greatest_dt = **dt;
                        println!( "greatest_dt is now: {:#?}", greatest_dt );
                    }
                } else {
                    println!( "greatest_dt is ?: {:#?} and dt is ?: {:#?}.", greatest_dt, **dt );
                    println!( "No changes.  greatest_dt is: {:#?}", greatest_dt )
                }
                // otherwise, don't make any changes to greatest ie (new item is none, both are none, new item is <= curent greatest)
            }
        } 
    }
    greatest_dt
}

async fn get_weather(wx_conf: &PirateWxConf) -> Option<Wx> {
    // create pirate_weather API url with pirate_wx_conf parameters
    let pirate_wx_api_url= format!("https://api.pirateweather.net/forecast/{}/{},{}?exclude=minutely,hourly,alerts&units={}&version=2", 
        wx_conf.api_key, wx_conf.lat, wx_conf.long, wx_conf.units ) ;
    // create new client for keep alive connections
    let pirate_wx_client = reqwest::Client::new();
    //send api request
    let pirate_wx_response = pirate_wx_client.get(&pirate_wx_api_url)
        .send()
        .await;
    match pirate_wx_response {
        Ok(response) => {
            //handle pirate wx responses.  API has clearly document response codes for different issues, but none are addressable from within program?
            // so handle ok or all other responses, no need to break down. 
            match response.status() {
                reqwest::StatusCode::OK => {
                    //deserializse json to Wx structure
                    match response.json::<Wx>().await {
                        Ok(data) => {
                            info!( "Pirate WX json retrieved and deserialized." );
                            Some( data )   
                        },
                        Err(text_eff) => {
                            //couldn't deserialize
                            error!("Pirate WX Response code: OK, but unable to deserialize json response. Err: {:#?}", text_eff);
                            return None
                        },
                    }
                },
                other => {
                    // Pirate WX response code not 200
                    error!("Pirate WX returned error code: {}", other);
                    return None
                },
            }
        },
        Err(response_eff) => {
            // Didn't get to the point of having a response code from Pirate WX
            error!("Error in initial response from Pirate_WX. Err: {}", response_eff);
            None
        }
    }
}

async fn insert_pirate_wx_to_mysql(wx: Wx, sql_pool_opt: &Option<sqlx::Pool<sqlx::MySql>> ) {
    
    if let Some(sql_pool) = sql_pool_opt {
        let cur_wx_result = sqlx::query( INSERT_CURRENT_WX_QUERY, )
            .bind(&wx.latitude)
            .bind(&wx.longitude)
            .bind(&wx.currently.time)
            .bind(&wx.currently.summary)
            .bind(&wx.currently.icon)
            .bind(&wx.currently.nearest_storm_distance)
            .bind(&wx.currently.nearest_storm_bearing)
            .bind(&wx.currently.precip_intensity)
            .bind(&wx.currently.precip_probability)
            .bind(&wx.currently.precip_intensity_error)
            .bind(&wx.currently.precip_type)
            .bind(&wx.currently.temperature)
            .bind(&wx.currently.apparent_temperature)
            .bind(&wx.currently.dew_point)
            .bind(&wx.currently.humidity)
            .bind(&wx.currently.pressure)
            .bind(&wx.currently.wind_speed)
            .bind(&wx.currently.wind_gust)
            .bind(&wx.currently.wind_bearing)
            .bind(&wx.currently.cloud_cover)
            .bind(&wx.currently.uv_index)
            .bind(&wx.currently.visibility)
            .bind(&wx.currently.ozone)
            .bind(&wx.currently.smoke)
            .bind(&wx.currently.fire_index)
            .bind(&wx.currently.feels_like)
            .bind(&wx.currently.current_day_ice)
            .bind(&wx.currently.current_day_liquid)
            .bind(&wx.currently.current_day_snow)
            .execute( sql_pool ).await;

        match cur_wx_result {
            Ok(_) => info!("Current Wx uploaded to MySql solar db current_wx table."),

            Err(cur_wx_eff) => error!("Current Wx failed to upload to Mysql solar db current_wx table. Error: {}", cur_wx_eff),
        }

        let daily_wx_result = sqlx::query(REPLACE_DAILY_WX_QUERY)
            .bind(&wx.latitude)
            .bind(&wx.longitude)
            .bind(&wx.daily.data[0].time)
            .bind(&wx.daily.data[0].sunrise_time)
            .bind(&wx.daily.data[0].dawn_time)
            .bind(&wx.daily.data[0].sunset_time)
            .bind(&wx.daily.data[0].dusk_time)
            .bind(&wx.daily.data[0].moon_phase)
            .bind(&wx.daily.data[0].precip_accumulation)
            .bind(&wx.daily.data[0].temperature_min)
            .bind(&wx.daily.data[0].temperature_min_time)
            .bind(&wx.daily.data[0].temperature_max)
            .bind(&wx.daily.data[0].temperature_max_time)
            .execute(sql_pool).await;
        
        match daily_wx_result {
            Ok(res) => {
                println!("{}", res.rows_affected());
                match res.rows_affected() {
                0 => warn!("No rows of daily_wx table changed. Row should have been added (1 row affected) or replaced 
                    (2 rows affected).  Response: {:#?}", res),
                1 => info!("Daily Wx added to MySql solar db daily_wx table. {:#?}", res),
                2 => info!("Daily Wx replaced existing weather for day in MySql solar db daily_wx table. {:#?}", res),
                3_u64..=u64::MAX => warn!("More than 2 rows of daily_wx table changed. Row should have been added (1 row affected) or replaced 
                (2 rows affected).  Response: {:#?}", res),
                }
            },

            Err(daily_wx_eff) => error!("Daily Wx failed to upload to Mysql solar db daily_wx table. Error: {}", daily_wx_eff),
        }
    }
}

fn verify_pirate_wx_conf(pirate_wx_conf: PirateWxConf ) -> PirateWxConf {
    // confirms that lat, long, and units are all present in conf file (does not validate parameters are correct, only that they exist.)
    // confirms that either API key or file path to API key are included in file (does not validatey that are correct)
    // uses API key if it is in file,  if not, attempts to read API key from file.  
    // Logs error and panics if above conditions are not met

    let mut wx_conf = pirate_wx_conf;
    // if lat, long, and units don't exist in conf file, log error and panic.
    if wx_conf.lat.is_empty() || wx_conf.long.is_empty() || wx_conf.units.is_empty() {
        error!("Pirate Weather configuration file parameters latitute, longitute, or units are missing or empty");
        panic!("Missing Pirate Weather Config Parameters");
    }
    // if API key and API Key file path don't exist in conf file, log error and panic.
    else if wx_conf.api_key.is_empty() && wx_conf.api_key_path.is_empty() {
        error!("Pirate Weather configuration file parameters ap i_key_path and api_key are both
                missing or empty.  Must have at least one");
        panic!("Missing Pirate Weather Config Parameters");
    } 
    else if  !matches!( wx_conf.interval_unit, 'd' | 'h' | 'm' | 's') {
        // if interval units is not d, h, m or s, log error and panic
        error!("Pirate Weather configuration file parameters interval_units is incorrect value. Must be 'd', 'h', 'm', or 's'.");
        panic!("Incorrect Pirate WX interval_units value.");
    } else {
        // if api Key exists in file, use it and return data
        if !wx_conf.api_key.is_empty() {
            return wx_conf
        } else {
            // otherwise, get API key from file path.
            let api_key_res = fs::read_to_string( &wx_conf.api_key_path );
            match api_key_res {
                // success reading from file, save value to API Key and return data
                Ok(api) => {
                    wx_conf.api_key = api;
                    return wx_conf
                },
                // Error reading from file, log error and panic.
                Err(api_eff) => {
                    error!( "Unable to retrieve Pirate Weather API key from file at {}. Err: {}", wx_conf.api_key_path, api_eff );
                    panic!("Couldn't get Pirate WX API Key");
                }
            }
            
        }
    }
}

fn verify_pvs6_conf(pvs6_conf: &Pvs6Conf) {
    // verifies if host provided.  if not, logs error and panics
    // verifies if get_device)interval_units is 'd', 'h', 'm' or 's'
    if pvs6_conf.host.is_empty() {
        error!("PVS6 configuration file parameter host is missing or empty");
        panic!("Missing PVS6 Config Parameters");
    } else if !matches!( pvs6_conf.get_device_interval_unit, 'd' | 'h' | 'm' | 's') {
        error!("PVS6 configuration file parameters interval_units is incorrect value. Must be 'd', 'h', 'm', or 's'.");
        panic!("Incorrect PVS6 interval_units value.");
    }
}

fn verify_mysql_conf(mysql_conf: MySqlConf ) -> MySqlConf {
    // confirms that database is present in conf file (does not validate parameters are correct, only that they exist.)
    // uses host, user, and password values if provided over login_info_loc
    // Logs error and panics if above conditions are not met

    //check if database provided.  Log error and panic if not.
    if mysql_conf.database.is_empty() {
        error!("MySql configuration file parameter database is missing or empty");
        panic!("Missing MySql Config Parameter database");      
    }
    //check if host, user, and password provided.  If so, return conf file and be done
    if !mysql_conf.host.is_empty() && !mysql_conf.user.is_empty() && !mysql_conf.password.is_empty() && !mysql_conf.port.is_empty() {
        return mysql_conf
    } 

    // if login_info_loc or login_path are empty, log error and crash
    if mysql_conf.login_info_loc.is_empty() || mysql_conf.login_path.is_empty() {
        error!("MySql configuration file parameter login_info_loc or login_path is missing or empty and at least one of host, 
                user, passowrd or port is empty.");
        panic!("Missing MySql Config Parameters. MySql configuration file parameter login_info_loc or login_path is missing or 
                empty and at least one of host, user, passowrd or port is empty.");     
    }
    //create mutable Pvs6Conf variable to save values from 
    let mut conf = mysql_conf;
    
    //try to parse login_info_loc file.  If can't parse, log error and panic. 
    let my_login_file_path = PathBuf::from( &conf.login_info_loc );

    match fs::exists(&my_login_file_path) {
        Ok(file_exists) => {
            if file_exists == true {
                let mysql_login = myloginrs_parse(
                    &conf.login_path, 
                    Some(&my_login_file_path)
                );
                if conf.host.is_empty() {
                    match mysql_login.get("host") {
                        Some(h) => conf.host = h.to_owned(),
                        None => {
                            error!("MySql configuration file parameter host is missing and not included in .cnf file.");
                            panic!("MySql configuration file parameter host is missing and not included in .cnf file.");
                        },
                    }
                }
                if conf.port.is_empty() {
                    match mysql_login.get("port") {
                        Some(p) => conf.port = p.to_owned(),
                        None => {
                            error!("MySql configuration file parameter port is missing and not included in .cnf file.");
                            panic!("MySql configuration file parameter port is missing and not included in .cnf file.");
                        },
                    }
                }
                if conf.user.is_empty() {
                    match mysql_login.get("user") {
                        Some(u) => conf.user = u.to_owned(),
                        None => {
                            error!("MySql configuration file parameter user is missing and not included in .cnf file.");
                            panic!("MySql configuration file parameter user is missing and not included in .cnf file.");
                        },
                    }
                }
                if conf.password.is_empty() {
                    match mysql_login.get("password") {
                        Some(pw) => conf.password = pw.to_owned(),
                        None => {
                            error!("MySql configuration file parameter password is missing and not included in .cnf file.");
                            panic!("MySql configuration file parameter password is missing and not included in .cnf file.");
                        },
                    }
                }
            }
            else {
                error!("Error: {} Login Credential file does not exist", &conf.login_info_loc);
                panic!("Error: {} Login Credential file does not exist", &conf.login_info_loc)
            }
        },
        Err(fp_eff) => {
            error!("Error: Mysql login file exists but can't be accessed.  
            May have incorrect permissions. Err: {}",fp_eff);
            panic!("Error: Mysql login file exists but can't be accessed.  
            May have incorrect permissions. Err: {}",fp_eff);
        }
    }
    conf
}
