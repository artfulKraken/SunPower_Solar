/*
Program completes the following actions:
1)  At a regular interval connects to Sunpower PVS6 Supervisor via http api call and collects pvs6 data.
    Processes data into a usable format and then uploads data to mysql database (solar).

2)  Future: Processes energy produced and energy consumed data (in solar db) to tabular format for data analysis  
*/

// USE STATEMENTS
    use reqwest::get;
    use regex::Regex;
    use once_cell::sync::Lazy;
    use myloginrs::parse as myloginrs_parse;
    use tokio::time::{ Interval, interval_at, Duration as TokioDuration };
    use std::{ str, fs, path::PathBuf, env, cmp::Ordering };
    use log::{ debug, error, info, warn };
    use log4rs;
    use chrono::{ DateTime, Utc, NaiveDateTime, Duration, DurationRound };
    use sqlx::mysql::MySqlPoolOptions;
    use serde::Deserialize;
    use serde_json::Result;

// CONSTANTS
    const URL_DEVICES_API: &str = "https://solarpi.artfulkraken.com/cgi-bin/dl_cgi?Command=DeviceList";
    const LOGIN_INFO_LOC: &str= "/home/solarnodered/.mylogin.cnf";
    const DATABASE: &str = "solar";
    const MYSQL_SERVER_PORT: &str = "3306";
    const LOGIN_INFO_LOC_MAC_TESTING: &str= "/Users/daveboggs/.mylogin.cnf";
    const PVS6_GET_DEVICES_INTERVAL: u64 = 5; 
    const PVS6_GET_DEVICES_INTERVAL_UNITS: char = 'm';
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

// ENUMS, STRUCTURES AND IMPLEMENTATIONS
enum DeviceType {
    Inverter,
    ConsumptionMeter,
    ProductionMeter,
    Supervisor
}
    
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
    fn new() -> Self {
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
    }   
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

// MODUELES
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

    let mut solar_pool = get_sqlx_solar_pool().await;

    // Set the offset duration of the interval.  For fine tuning timing request.  We want the pvs6 response time for the request (ie the data_time) to be as close to the 
    // interval time we planned, but not before it.  There is some small delay in the PVS6 receiving the request and then processing it.  This negative offset is meant to 
    // fine tune that as needed.     
    let offset_dur = chrono::Duration::milliseconds( -200 );
    // set the start time and interval used to get pvs6 device data.
    let mut get_pvs6_device_interval = set_interval(PVS6_GET_DEVICES_INTERVAL, PVS6_GET_DEVICES_INTERVAL_UNITS, offset_dur);
    pvs6_to_mysql( &mut solar_pool ).await;
    /*loop {
        // Wait until the next tick (start time and interval)
        get_pvs6_device_interval.tick().await; 
        // get pvs6 data and upload to mysql solar database
        debug!( "Run at: {}", Utc::now().to_string() ); //for loop timing testing
        pvs6_to_mysql( &mut solar_pool ).await;
    }*/
}

async fn pvs6_to_mysql(solar_pool: &mut Option<sqlx::Pool<sqlx::MySql>>) {
    let pvs6_opt = get_pvs6_device_data().await;
    let mut latest_data = Pvs6DevicesResponse::new();

    if let Some( pool ) = solar_pool {
        latest_data = get_latest_pvs6_data_from_sql(pool).await;
    }
    if let Some( pvs6_data ) = pvs6_opt {
        if let Some (deser_pvs6) = deserialize_pvs6_devices(pvs6_data) {
            //println!("{:#?}", deser_pvs6);
            let cleaned_pvs6_data = update_pvs6_old_responses(deser_pvs6, &latest_data);
            //println!("{:#?}", cleaned_pvs6_data);
            insert_pvs6_data_to_mysql( cleaned_pvs6_data, &solar_pool.as_ref().expect("Getting solar_pool crashed.  It shouldn't have") ).await;
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
    static RE_TYPE: Lazy<Regex> = Lazy::new( || Regex::new(r#""TYPE".*?:.*?"(.*?)""#).unwrap() );
    
    
    //vector of inverters to hold inverters that are deserialized.
    let mut type_of_device: DeviceType;

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
                                        type_of_device = DeviceType::Supervisor;
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
                                                                type_of_device = DeviceType::ProductionMeter;
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
                                                                type_of_device = DeviceType::ConsumptionMeter;
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
                                        type_of_device = DeviceType::Inverter;
                                        let inv_opt: Result<Inverter> = serde_json::from_str(&device);
                                        match inv_opt {
                                            Ok(inv) => {
                                                inverters.push(inv);
                                                flg_device_deserialized = true;
                                            },
                                            Err(inv_eff) => {
                                                error!("Could not deserialize Inverter. Inverter was #: {} in vector. Err: {}", inverter_cnt, inv_eff);
                                                //add error handling
                                            },
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

async fn get_sqlx_solar_pool() -> Option<sqlx::Pool<sqlx::MySql>> {
    // gets sqlx mysql pool of connections for mysql solar db using local credential file.
    
    // /*FOR DB TESTING  - Allows for connection from remote device.  Block out and unblock production line for final use.
    let filepath: String;// = String::new(); 

    if env::consts::OS == "macos" {
        filepath = LOGIN_INFO_LOC_MAC_TESTING.to_string();
    } else {
        filepath = LOGIN_INFO_LOC.to_string();
    }
    
    let my_login_file_path = PathBuf::from( filepath );

    //  */FOR DEB TESTING ENDS HERE

    //FOR PRODUCTION
    //let my_login_file_path = PathBuf::from( LOGIN_INFO_LOC );

    match fs::exists(&my_login_file_path) {
        Ok(file_exists) => {
            if file_exists == true {
                let mysql_client_info = myloginrs_parse("client", Some(&my_login_file_path));
                let solar_mysql_url = format!( "mysql://{}:{}@{}:{}/{}", &mysql_client_info["user"], &mysql_client_info["password"], 
                    &mysql_client_info["host"], MYSQL_SERVER_PORT, DATABASE );
                
                let sqlx_solar_pool = MySqlPoolOptions::new()
                .max_connections(10)
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
            else {
                error!("Error: {} Login Credential file does not exist on local client device", LOGIN_INFO_LOC);
                return None
            }
        },
        Err(fp_eff) => {
            error!("Error: Mysql login file exists but can't be accessed.  
            May have incorrect permissions. Err: {}",fp_eff);
            return None
        }
    }
}

async fn insert_pvs6_data_to_mysql( data: Pvs6DevicesResponse, solar_sql_upload_conn: &sqlx::Pool<sqlx::MySql> ) {
    // takes data from pvs6 (in Pvs6DeviceResponse Struct) and inserts it into mysql solar db tables for each type of device. 
    
    // Future addition - save any data not uploaded to db to a file (one file for each device type) to allow for future uploads once problem identified.
    
    let mut all_device_success: bool = true;  
    
    const SUP_INSERT_QUERY: &str = r#"
        INSERT INTO supervisors_data 
            ( serial, data_time, dl_comm_err, dl_cpu_load, dl_err_count, dl_flash_avail, dl_mem_used, 
                dl_scan_time, dl_skipped_scans, dl_untransmitted, dl_uptime ) 
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#;
    const PM_INSERT_QUERY: &str = r#"
        INSERT INTO production_meters_data
            ( serial, data_time, freq_hz, i_a, net_ltea_3phsum_kwh, 
                p_3phsum_kw, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#;

    const CM_INSERT_QUERY: &str = r#"
        INSERT INTO consumption_meters_data
            ( serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw,
                p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v )
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    "#;
    const INV_INSERT_QUERY: &str = "
        INSERT INTO inverters_data
            ( serial, data_time, freq_hz, i_3phsum_a, i_mppt1_a, ltea_3phsum_kwh, p_3phsum_kw, 
                p_mppt1_kw, stat_ind, t_htsnk_degc, v_mppt1_v, vln_3phavg_v )
            VALUE ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
    ";
    
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
        .execute(solar_sql_upload_conn).await;

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
        .execute(solar_sql_upload_conn).await;

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
        .execute(solar_sql_upload_conn).await;

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
            .execute(solar_sql_upload_conn).await;

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
        
}

async fn get_latest_pvs6_data_from_sql( solar_sql_upload_pool: &sqlx::Pool<sqlx::MySql> ) -> Pvs6DevicesResponse {
    let mut latest_sql_pvs6_data = Pvs6DevicesResponse::new();

    let sup = sqlx::query_as::<_, Supervisor>(QUERY_GET_LATEST_SUP_DATA)
    .fetch_all(solar_sql_upload_pool).await;

    let cm = sqlx::query_as::<_, ConsumptionMeter>(QUERY_GET_LATEST_CM_DATA)
    .fetch_all(solar_sql_upload_pool).await;

    let pm = sqlx::query_as::<_, ProductionMeter>(QUERY_GET_LATEST_PM_DATA)
    .fetch_all(solar_sql_upload_pool).await;

    let inv = sqlx::query_as::<_, Inverter>(QUERY_GET_LATEST_INV_DATA)
    .fetch_all(solar_sql_upload_pool).await;

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
    
    latest_sql_pvs6_data
}

fn set_interval(repeat_interval: u64, units: char, offset: Duration) -> Interval {
    // repeat_interval: time in seconds that interval should repeat
    // units: unit of time.  Only d, h, m, s are accepted.  All others will panic
    // Set start time and interval of pvs6 data pulls.  

    // convert repeat interval to seconds.  Panic if units is not 'd' day(s), 'h' hour(s), 'm' minute(s)), or 's' second(s) 
    let repeat_interval_s: u64 =   match units {
        'd' => repeat_interval * 60 * 60 * 24,
        'h' => repeat_interval * 60 * 60,
        'm' => repeat_interval * 60,
        's' => repeat_interval,
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
    
    target_time += offset;  //adjust target time by user supplied offset
    
    let mut start = target_time.signed_duration_since(now.naive_utc());
    
    
    if start < chrono::Duration::milliseconds(500) {
        start = start + next_start;
    }

    interval_at(tokio::time::Instant::now() + start.to_std().unwrap(), TokioDuration::from_secs( repeat_interval_s ) )
}

async fn get_pvs6_device_data() -> Option<String> {
    // Using direct Reqwest::get fn instead of creating client.  PVS6 loses main internet connection and 
    // will not upload data when installer port (where we are making request) connection remains open.
    //  PVS6 is known to have a bug that causes memory to fill up and crash PVS6 if data is not uploaded.

    let pvs6_received = get(URL_DEVICES_API).await;

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
    
    let greatest_cur_dt: Option<DateTime<Utc>> = greater_option_dt(&check_dts);

    let mut sup = Supervisor::new();
    let mut pm = ProductionMeter::new();
    let mut cm = ConsumptionMeter::new();
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
                inv_found_flg = true;
                // if date_time also match, data alread in system.  
                if cur_inv.data_time == latest_inv.data_time {
                    // update date_time to latest current date time and set data to none.  Add to data to be uploaded to system
                    invs.push( Inverter::set_values(&cur_inv.serial, greatest_cur_dt, 
                        None, None, None, None, 
                        None, None, None, None, 
                        None, None) );
                    break;
                }
                // if date-time are not the same, new data to add to system.
                else {
                    invs.push(cur_inv.clone());
                    break;
                }
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

fn greater_option_dt( dt_vec: &Vec<&Option<DateTime<Utc>>> ) -> Option<DateTime<Utc>> {
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
            // iterate through vector, skipping first item
            for dt in dt_vec.iter().skip(1) {
                
                if greatest_dt.is_none() && dt.is_some()  {
                    // The item with some is greater than the item with none
                    greatest_dt = **dt;
                } 
                else if greatest_dt.is_some() && dt.is_some() {
                    if Some( dt ) >  Some( &&greatest_dt ) {
                        // when they both have a value, make dt the greatest if it is bigger than the current greatest
                        greatest_dt = **dt;
                    }
                }
                // otherwise, don't make any changes to greatest ie (new item is none, both are none, new item is <= curent greatest)
            }
        } 
    }
    greatest_dt
}

