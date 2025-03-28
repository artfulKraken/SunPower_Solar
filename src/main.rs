/*
Program completes the following actions:
1)  At a regular interval connects to Sunpower PVS6 Supervisor via http api call and collects pvs6 data.
    Processes data into a usable format and then uploads data to mysql database (solar).

2)  Future: Processes energy produced and energy consumed data (in solar db) to tabular format for data analysis  
*/


//use reqwest;
use reqwest::get;
use regex::Regex;
use once_cell::sync::Lazy;
use mysql::*;
//use mysql::prelude::Queryable;
use mysql::prelude::*;
use myloginrs::parse as myloginrs_parse;
use tokio::time::{Interval, interval_at, Duration as TokioDuration};
use std::{str, fs, path::PathBuf};
use log::{debug, error, info, warn};
use log4rs;
use chrono::{prelude::*, Duration, DurationRound, NaiveDateTime};



const URL_DEVICES_API: &str = "https://solarpi.artfulkraken.com/cgi-bin/dl_cgi?Command=DeviceList";
const LOGIN_INFO_LOC: &str= "/home/solarnodered/.mylogin.cnf";
const PVS6_GET_DEVICES_INTERVAL: u64 = 5; // Time in minutes
const PVS6_GET_DEVICES_INTERVAL_UNITS: char = 'm';
const SUPERVISOR: &str = "PVS";
const METER: &str = "Power Meter";
const INVERTER: &str = "Inverter";
const POWER_METER: &str = "GROSS_PRODUCTION_SITE";
const CONSUMPTION_METER: &str = "NET_CONSUMPTION_LOADSIDE";


const INV_QUERY_P1: &str = 
    "SELECT \
        serial, \
        MAX(data_time), \
        SUM(CASE WHEN parameter = 'freq_hz' THEN data ELSE NULL END) AS freq_hz, \
        SUM(CASE WHEN parameter = 'i_3phsum_a' THEN data ELSE NULL END) AS i_3phsum_a, \
        SUM(CASE WHEN parameter = 'i_mppt1_a' THEN data ELSE NULL END) AS i_mppt1_a, \
        SUM(CASE WHEN parameter = 'ltea_3phsum_kwh' THEN data ELSE NULL END) AS ltea_3phsum_kwh,\
        SUM(CASE WHEN parameter = 'p_3phsum_kw' THEN data ELSE NULL END) AS p_3phsum_kw, \
        SUM(CASE WHEN parameter = 'p_mppt1_kw' THEN data ELSE NULL END) AS p_mppt1_kw, \
        SUM(CASE WHEN parameter = 'stat_ind' THEN data ELSE NULL END) AS stat_ind, \
        SUM(CASE WHEN parameter = 't_htsnk_degc' THEN data ELSE NULL END) AS t_htsnk_degc, \
        SUM(CASE WHEN parameter = 'v_mppt1_v' THEN data ELSE NULL END) AS v_mppt1_v, \
        SUM(CASE WHEN parameter = 'vln_3phavg_v' THEN data ELSE NULL END) AS vln_3phavg_v ";

const SUP_QUERY_P1: &str = 
    "SELECT \
        serial, \
        MAX(data_time), \
        SUM(CASE WHEN parameter = 'dl_comm_err' THEN data ELSE NULL END) AS dl_comm_err, \
        SUM(CASE WHEN parameter = 'dl_cpu_load' THEN data ELSE NULL END) AS dl_cpu_load, \
        SUM(CASE WHEN parameter = 'dl_err_count' THEN data ELSE NULL END) AS dl_err_count, \
        SUM(CASE WHEN parameter = 'dl_flash_avail' THEN data ELSE NULL END) AS dl_flash_avail,\
        SUM(CASE WHEN parameter = 'dl_mem_used' THEN data ELSE NULL END) AS dl_mem_used, \
        SUM(CASE WHEN parameter = 'dl_scan_time' THEN data ELSE NULL END) AS dl_scan_time, \
        SUM(CASE WHEN parameter = 'stat_ind' THEN data ELSE NULL END) AS stat_ind, \
        SUM(CASE WHEN parameter = 'dl_skipped_scans' THEN data ELSE NULL END) AS dl_skipped_scans, \
        SUM(CASE WHEN parameter = 'dl_untransmitted' THEN data ELSE NULL END) AS dl_untransmitted, \
        SUM(CASE WHEN parameter = 'dl_uptime' THEN data ELSE NULL END) AS dl_uptime ";

const CONSUMP_QUERY_P1: &str = 
    "SELECT \
        serial, \
        MAX(data_time), \
        SUM(CASE WHEN parameter = 'freq_hz' THEN data ELSE NULL END) AS freq_hz, \
        SUM(CASE WHEN parameter = 'i1_a' THEN data ELSE NULL END) AS i1_a, \
        SUM(CASE WHEN parameter = 'i2_a' THEN data ELSE NULL END) AS i2_a, \
        SUM(CASE WHEN parameter = 'neg_ltea_3phsum_kwh' THEN data ELSE NULL END) AS neg_ltea_3phsum_kwh,\
        SUM(CASE WHEN parameter = 'net_ltea_3phsum_kwh' THEN data ELSE NULL END) AS net_ltea_3phsum_kwh, \
        SUM(CASE WHEN parameter = 'p_3phsum_kw' THEN data ELSE NULL END) AS p_3phsum_kw, \
        SUM(CASE WHEN parameter = 'p1_kw' THEN data ELSE NULL END) AS p1_kw, \
        SUM(CASE WHEN parameter = 'p2_kw' THEN data ELSE NULL END) AS p2_kw, \
        SUM(CASE WHEN parameter = 'pos_ltea_3phsum_kwh' THEN data ELSE NULL END) AS pos_ltea_3phsum_kwh, \
        SUM(CASE WHEN parameter = 'q_3phsum_kvar' THEN data ELSE NULL END) AS q_3phsum_kvar, \
        SUM(CASE WHEN parameter = 's_3phsum_kva' THEN data ELSE NULL END) AS s_3phsum_kva, \
        SUM(CASE WHEN parameter = 'tot_pf_rto' THEN data ELSE NULL END) AS tot_pf_rto, \
        SUM(CASE WHEN parameter = 'v12_v' THEN data ELSE NULL END) AS v12_v, \
        SUM(CASE WHEN parameter = 'v1n_v' THEN data ELSE NULL END) AS v1n_v, \
        SUM(CASE WHEN parameter = 'v2n_v' THEN data ELSE NULL END) AS v2n_v ";

const PRODUCT_QUERY_P1: & str = 
    "SELECT \
        serial, \
        MAX(data_time), \
        SUM(CASE WHEN parameter = 'freq_hz' THEN data ELSE NULL END) AS freq_hz, \
        SUM(CASE WHEN parameter = 'i_a' THEN data ELSE NULL END) AS i_a, \
        SUM(CASE WHEN parameter = 'net_ltea_3phsum_kwh' THEN data ELSE NULL END) AS net_ltea_3phsum_kwh, \
        SUM(CASE WHEN parameter = 'p_3phsum_kw' THEN data ELSE NULL END) AS p_3phsum_kw,\
        SUM(CASE WHEN parameter = 'q_3phsum_kvar' THEN data ELSE NULL END) AS q_3phsum_kvar, \
        SUM(CASE WHEN parameter = 's_3phsum_kva' THEN data ELSE NULL END) AS s_3phsum_kva, \
        SUM(CASE WHEN parameter = 'tot_pf_rto' THEN data ELSE NULL END) AS tot_pf_rto, \
        SUM(CASE WHEN parameter = 'v12_v' THEN data ELSE NULL END) AS v12_v ";
    
const DEV_QUERY_P2: & str =
    "FROM ( \
        SELECT serial, parameter, data_time, data \
        FROM pvs6_data \
        WHERE serial = '";

const DEV_QUERY_P3: & str =
    "' AND data_time = ( \
        SELECT MAX(data_time) from pvs6_data WHERE serial = '";

const DEV_QUERY_P4: & str = "' ) ) AS subquery GROUP BY serial";

#[derive(Clone)] 
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

#[derive(Clone)] 
struct Inverter {
    serial: String,
    data_time: String,
    freq_hz: Option<String>,
    i_3phsum_a: Option<String>,
    i_mppt1_a: Option<String>,
    ltea_3phsum_kwh: Option<String>,
    p_3phsum_kw: Option<String>,
    p_mppt1_kw: Option<String>,
    stat_ind: Option<String>,
    t_htsnk_degc: Option<String>,
    v_mppt1_v: Option<String>,
    vln_3phavg_v: Option<String>,

}

impl Inverter {
    fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: String::new(),
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
        serial: &str, data_time: &str, freq_hz: Option<&str>, i_3phsum_a: Option<&str>, i_mppt1_a: Option<&str>,
        ltea_3phsum_kwh: Option<&str>, p_3phsum_kw: Option<&str>, p_mppt1_kw: Option<&str>, stat_ind: Option<&str>,
        t_htsnk_degc: Option<&str>, v_mppt1_v: Option<&str>, vln_3phavg_v: Option<&str>,
    ) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time: data_time.to_owned(),
            freq_hz: freq_hz.map( |s| s.to_owned() ),
            i_3phsum_a: i_3phsum_a.map( |s| s.to_owned() ),
            i_mppt1_a: i_mppt1_a.map( |s| s.to_owned() ),
            ltea_3phsum_kwh: ltea_3phsum_kwh.map( |s| s.to_owned() ),
            p_3phsum_kw: p_3phsum_kw.map( |s| s.to_owned() ),
            p_mppt1_kw: p_mppt1_kw.map( |s| s.to_owned() ),
            stat_ind: stat_ind.map( |s| s.to_owned() ),
            t_htsnk_degc: t_htsnk_degc.map( |s| s.to_owned() ),
            v_mppt1_v: v_mppt1_v.map( |s| s.to_owned() ),
            vln_3phavg_v: vln_3phavg_v.map( |s| s.to_owned() ),
        }
    }
} 

#[derive(Clone)] 
struct ProductionMeter {
    serial: String,
    data_time: String,
    freq_hz: Option<String>,
    i_a: Option<String>,
    net_ltea_3phsum_kwh: Option<String>,
    p_3phsum_kw: Option<String>,
    q_3phsum_kvar: Option<String>,
    s_3phsum_kva: Option<String>,
    tot_pf_rto: Option<String>,
    v12_v: Option<String>,
}
impl ProductionMeter {
    fn new() -> Self {
        Self { 
            serial: String::new(),
            data_time: String::new(),
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
        serial: &str, data_time: &str, freq_hz: Option<&str>, i_a: Option<&str>, net_ltea_3phsum_kwh: Option<&str>, p_3phsum_kw: Option<&str>,
        q_3phsum_kvar: Option<&str>, s_3phsum_kva: Option<&str>, tot_pf_rto: Option<&str>, v12_v: Option<&str>,) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time: data_time.to_owned(),
            freq_hz: freq_hz.map( |s| s.to_owned() ),
            i_a: i_a.map( |s| s.to_owned() ),
            net_ltea_3phsum_kwh: net_ltea_3phsum_kwh.map( |s| s.to_owned() ),
            p_3phsum_kw: p_3phsum_kw.map( |s| s.to_owned() ),
            q_3phsum_kvar: q_3phsum_kvar.map( |s| s.to_owned() ),
            s_3phsum_kva: s_3phsum_kva.map( |s| s.to_owned() ),
            tot_pf_rto: tot_pf_rto.map( |s| s.to_owned() ),
            v12_v: v12_v.map( |s| s.to_owned() ),
        }
    }
}

#[derive(Clone)] 
struct ConsumptionMeter {
    serial: String,
    data_time: NaiveDateTime,
    freq_hz: Option<f64>,
    i1_a: Option<f64>,
    i2_a: Option<f64>,
    neg_ltea_3phsum_kwh: Option<f64>,
    net_ltea_3phsum_kwh: Option<f64>,
    p_3phsum_kw: Option<f64>,
    p1_kw: Option<f64>,
    p2_kw: Option<f64>,
    pos_ltea_3phsum_kwh: Option<f64>,
    q_3phsum_kvar: Option<f64>,
    s_3phsum_kva: Option<f64>,
    tot_pf_rto: Option<f64>,
    v12_v: Option<f64>,
    v1n_v: Option<f64>,
    v2n_v: Option<f64>,
}
impl ConsumptionMeter {
    fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: NaiveDateTime::new(NaiveDate::from_ymd_opt(2016, 7, 8).unwrap(), NaiveTime::from_hms_opt(12, 59, 59).unwrap()),
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

    fn set_values(
        serial: String, data_time: NaiveDateTime, freq_hz: Option<f64>, i1_a: Option<f64>, i2_a: Option<f64>,
        neg_ltea_3phsum_kwh: Option<f64>, net_ltea_3phsum_kwh: Option<f64>, p_3phsum_kw: Option<f64>, p1_kw: Option<f64>,
        p2_kw: Option<f64>, pos_ltea_3phsum_kwh: Option<f64>, q_3phsum_kvar: Option<f64>, s_3phsum_kva: Option<f64>,
        tot_pf_rto: Option<f64>, v12_v: Option<f64>, v1n_v: Option<f64>, v2n_v: Option<f64>,
    ) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time: data_time,
            freq_hz: freq_hz.map( |opt_float| opt_float ),
            i1_a: i1_a.map( |opt_float| opt_float ),
            i2_a: i2_a.map( |opt_float| opt_float ),
            neg_ltea_3phsum_kwh: neg_ltea_3phsum_kwh.map( |opt_float| opt_float ),
            net_ltea_3phsum_kwh: net_ltea_3phsum_kwh.map( |opt_float| opt_float ),
            p_3phsum_kw: p_3phsum_kw.map( |opt_float| opt_float ),
            p1_kw: p1_kw.map( |opt_float| opt_float ),
            p2_kw: p2_kw.map( |opt_float| opt_float ),
            pos_ltea_3phsum_kwh: pos_ltea_3phsum_kwh.map( |opt_float| opt_float ),
            q_3phsum_kvar: q_3phsum_kvar.map( |opt_float| opt_float ),
            s_3phsum_kva: s_3phsum_kva.map( |opt_float| opt_float ),
            tot_pf_rto: tot_pf_rto.map( |opt_float| opt_float ),
            v12_v: v12_v.map( |opt_float| opt_float ),
            v1n_v: v1n_v.map( |opt_float| opt_float ),
            v2n_v: v2n_v.map( |opt_float| opt_float ),
        }
    }

}

#[derive(Clone)] 
struct Supervisor {
    serial: String,
    data_time: String,
    dl_comm_err: Option<String>,
    dl_cpu_load: Option<String>,
    dl_err_count: Option<String>,
    dl_flash_avail: Option<String>,
    dl_mem_used: Option<String>,
    dl_scan_time: Option<String>,
    dl_skipped_scans: Option<String>,
    dl_untransmitted: Option<String>,
    dl_uptime: Option<String>,
}
impl Supervisor {
    fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: String::new(),
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
        serial: String,  data_time: String,  dl_comm_err: Option<String>,  dl_cpu_load: Option<String>,  dl_err_count: Option<String>,
        dl_flash_avail: Option<String>,  dl_mem_used: Option<String>,  dl_scan_time: Option<String>,  dl_skipped_scans: Option<String>,
        dl_untransmitted: Option<String>,  dl_uptime: Option<String>,
    ) -> Self {  
        Self {
            serial: serial.to_owned(),
            data_time: data_time.to_owned(),
            dl_comm_err:  dl_comm_err.map( |s| s.to_owned() ),
            dl_cpu_load:  dl_cpu_load.map( |s| s.to_owned() ),
            dl_err_count:  dl_err_count.map( |s| s.to_owned() ),
            dl_flash_avail:  dl_flash_avail.map( |s| s.to_owned() ),
            dl_mem_used:  dl_mem_used.map( |s| s.to_owned() ),
            dl_scan_time:  dl_scan_time.map( |s| s.to_owned() ),
            dl_skipped_scans:  dl_skipped_scans.map( |s| s.to_owned() ),
            dl_untransmitted:  dl_untransmitted.map( |s| s.to_owned() ),
            dl_uptime:  dl_uptime.map( |s| s.to_owned() ),
        }
    }
}

struct Pvs6DataPoint {
    serial: String,
    data_time: String,
    parameter: String,
    data: Option<String>,
}
impl Pvs6DataPoint {

    fn new() -> Self {
        Self {
            serial: String::new(),
            data_time: String::new(),
            parameter: String::new(),
            data: None,
        }
    }

    fn set_values( serial: &str, data_time: &str, parameter: &str, data: Option<&str> ) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time: data_time.to_owned(),
            parameter: parameter.to_owned(),
            data: data.map(|s| s.to_owned()),
        }
    }
}

impl Clone for Pvs6DataPoint {
    fn clone(&self) -> Pvs6DataPoint {
        Pvs6DataPoint::set_values( &self.serial, &self.data_time, &self.parameter, self.data.as_deref() )
    }
}

//#[tokio::main]
#[tokio::main]
async fn main() {
    
    log4rs::init_file("log_config.yml", Default::default()).unwrap();   //Need Error Handling here.  What if log doesn't unwrap

    let mut device_ts_data: Vec<Vec<Pvs6DataPoint>> = Vec::new();
    let mut last_device_data: Pvs6DevicesResponse = Pvs6DevicesResponse::new();
    

    ////// Need to rework sql pool so it is before this statement.  This has to come before getting pvs6 data
    
    
    
    //get sql pool and connection
    let result_sql_pool = get_mysql_pool();
    match result_sql_pool {
        Ok(solar_sql_pool) => {
            let conn = solar_sql_pool.get_conn();
            match conn {
                Ok(solar_sql_upload_conn) => {
                   let mut solar_sql_upload_conn = solar_sql_upload_conn; 
    
                  let last_device_data_res = get_sql_last_device_data(&mut solar_sql_upload_conn);
                  match last_device_data_res {
                    Ok(data) => last_device_data = data,
                    Err(_) => warn!("Could not get last data from devices in mysql solar db"),
                  }

                   // Set start time and interval of pvs6 data pulls.  
                    let offset_dur = chrono::Duration::milliseconds( -250 );
                    let mut get_pvs6_device_interval = set_interval(PVS6_GET_DEVICES_INTERVAL, PVS6_GET_DEVICES_INTERVAL_UNITS, offset_dur);

                    loop {
                        get_pvs6_device_interval.tick().await; // Wait until the next tick
                        // get pvs6 data and upload to mysql solar database
                        debug!( "Run at: {}", Utc::now().to_string() ); //for loop timing testing
                        pvs6_to_mysql( &mut device_ts_data, &mut solar_sql_upload_conn, &mut last_device_data).await;
                        
                    }    
                },
                Err(sql_conn_err) => {
                    error!("Unable to get solar sql conn. Err: {}", sql_conn_err);
                    panic!("Panicing. Unable to get solar sql conn.");
                },
            }
        },
        Err(e) => {
            error!( "Unable to get solar sql pool.  Err: {}", e );
            panic!( "Panicing. Unable to get solar sql pool." );
        },
    }

    
}

async fn pvs6_to_mysql( device_ts_data: &mut Vec<Vec<Pvs6DataPoint>>, solar_sql_upload_conn: &mut PooledConn, last_device_data: &mut Pvs6DevicesResponse ) {
    let pvs6_result = get_pvs6_device_data().await;
    match pvs6_result {
        Some(pvs6_data) => {
            process_pvs6_devices_output( pvs6_data, device_ts_data, last_device_data );  //Function needs some error handling and response to determine if we should move to
            // mysql upload step.
            import_to_mysql( device_ts_data, solar_sql_upload_conn, last_device_data );

        },
        None => warn!("Incorrect or no PVS6 response. No data to process or upload "),
    };
}

fn process_pvs6_devices_output(pvs6_data: String, device_ts_data: &mut Vec<Vec<Pvs6DataPoint>>, last_device_data: &mut Pvs6DevicesResponse  ) {
    //Regex patterns synced lazy for compile efficiency improvement
    //Unwrapping all Regex expressions.  If it doesn't unwrap, its a bug in the hardcoded expression and needs to be caught at first runtime.
    static RE_DEVICES: Lazy<Regex> = Lazy::new( || Regex::new(r"\[.*\]").unwrap() );
    static RE_RESULT: Lazy<Regex> = Lazy::new( || Regex::new(r"\}\],result:succeed").unwrap() );
    static RE_SERIAL: Lazy<Regex> = Lazy::new( || Regex::new(r"SERIAL:([^,]*)").unwrap() );
    static RE_DEVICE_TYPE: Lazy<Regex> = Lazy::new( || Regex::new(r"DEVICE_TYPE:([^,]*)").unwrap() );
    static RE_SUBTYPE: Lazy<Regex> = Lazy::new( || Regex::new(r"subtype:([^,]*)").unwrap() );
    static RE_DATA_TIME: Lazy<Regex> = Lazy::new( || Regex::new(r"DATATIME:([0-9]{4},[0-9]{2},[0-9]{2},[0-9]{2},[0-9]{2},[0-9]{2})").unwrap() );
    static RE_PARAMETER: Lazy<Regex> = Lazy::new( || Regex::new(r",([a-z_0-9]*):([^,]*)").unwrap() );
    static RE_EXCLUDED_PARAMETER: Lazy<Regex> = Lazy::new( || Regex::new(r"interface|subtype|origin|hw_version|slave|panid|ct_scl_fctr").unwrap() );
    
    let mut repeat_flg: bool = false;
    
    let pvs6_data: String = pvs6_data.chars().filter(|c| !matches!(c, '\"' | '\t' | '\n' | ' ' )).collect::<String>();
    let mat_result = RE_RESULT.find(&pvs6_data).unwrap();
    
    if ! mat_result.is_empty() {
        let mat_devices = RE_DEVICES.find(&pvs6_data).unwrap();
        if ! mat_devices.is_empty() {
            let devices: &str = mat_devices.as_str();
            //println!("Devices: {:?}", devices);
            let mut find_data_time: Option<&str> = None;
            let mut max_data_time = String::new();
            let mut data_point: Pvs6DataPoint = Pvs6DataPoint::new();
            
            for data_time_cap in RE_DATA_TIME.captures_iter(&devices) {

                if find_data_time.is_none() {
                    find_data_time = Some( data_time_cap.get(1).unwrap().as_str() );
                } 
                else if find_data_time < Some( data_time_cap.get(1).unwrap().as_str() ) {
                    find_data_time = Some( data_time_cap.get(2).unwrap().as_str() );    
                }
            }

            match find_data_time {
                Some(dt) => {
                    max_data_time = to_sql_timestamp( dt );
                },
                None => error!("Couldn't find a max data_time from PVS6 response"),
            };
            
            
            let mut data_points: Vec<Pvs6DataPoint>= Vec::new();

            let device_list = devices.split("},{");
            for device in device_list {
                repeat_flg = false;
                //println!("Device: {}", &device);
                let serial = RE_SERIAL.captures(&device).unwrap().get(1).unwrap().as_str(); //first unwap may cause crash as error
                let device_type = RE_DEVICE_TYPE.captures(&device).unwrap().get(1).unwrap().as_str();  //first unwap may cause crash as error
                let data_time = to_sql_timestamp( 
                    RE_DATA_TIME
                    .captures(&device)
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .as_str()
                );  //first unwap may cause crash as error

                match device_type {
                    SUPERVISOR => {
                        if last_device_data.supervisor.data_time == data_time && last_device_data.supervisor.serial == serial {
                            repeat_flg = true;
                        }
                    },
                    METER => {
                        match RE_SUBTYPE.captures(&device).unwrap().get(1).unwrap().as_str() {
                            POWER_METER => {
                                if last_device_data.prod_meter.data_time == data_time && last_device_data.prod_meter.serial == serial {
                                    repeat_flg = true;
                                }
                            },
                            CONSUMPTION_METER => {
                                if last_device_data.cons_meter.data_time == NaiveDateTime::parse_from_str(&data_time, "%Y-%m-%d %H:%M:%S").unwrap()  && last_device_data.cons_meter.serial == serial {
                                    repeat_flg = true;
                                }
                            },
                            _ => error!("Device did not match an appropriate device type"),
                        }
                    },
                    INVERTER => {
                        for inv in last_device_data.inverters.iter() {
                            if inv.data_time == data_time && inv.serial == serial {
                                repeat_flg = true;
                            }
                        }
                    },
                    _ => error!("Device did not match an appropriate device type"),
                }

                for caps in RE_PARAMETER.captures_iter(device) {
                    if ! RE_EXCLUDED_PARAMETER.is_match( caps.get(1).unwrap().as_str() ) {
                        let parameter = caps.get(1).unwrap().as_str();
                        let data = caps.get(2).unwrap().as_str();
                        if repeat_flg {        
                            data_point = Pvs6DataPoint::set_values( serial, &max_data_time, parameter, None );
                        } 
                        else {
                            data_point = Pvs6DataPoint::set_values( serial, &data_time, parameter, Some(data) );
                        }
                    }
                        data_points.push(data_point.clone());
                }
                device_ts_data.push(data_points.clone());
                data_points.clear();
                
            }
            //for device in device_ts_data.iter() {
            //    for dp in device.iter() {
            //        println!("Serial: {} Time: {} Parameter: {} Data: {}", dp.serial, dp.data_time, dp.parameter, dp.data);
            //    }
            //}
        }
    }
    //for dp in data_points.iter() {
    //    println!("Serial:{}  Date: {}  Parameter: {}  Data: {}", dp.serial, dp.data_time, dp.parameter, dp.data);
    //}
}

fn to_sql_timestamp(str_timestamp: &str) -> String {
    let ts_values: Vec<&str> = str_timestamp.split(',').collect();
    ts_values[0].to_owned() + "-" + ts_values[1] + "-" + ts_values[2] + " " + ts_values[3] + ":" + ts_values[4] + ":" + ts_values[5]
}

fn get_mysql_pool() -> Result<Pool, Box<dyn std::error::Error>> {
    let my_login_file_path = PathBuf::from( LOGIN_INFO_LOC );

    match fs::exists(&my_login_file_path) {
        Ok(file_exists) => {
            if file_exists == true {
                let mysql_client_info = myloginrs_parse("client", Some(&my_login_file_path));
                let solar_db_opts = OptsBuilder::new()
                    .ip_or_hostname(Some(&mysql_client_info["host"]))
                    .db_name(Some("solar"))
                    .user(Some(&mysql_client_info["user"]))
                    .pass(Some(&mysql_client_info["password"]));
                
                let pool = Pool::new(solar_db_opts);
                match pool {
                    Ok(solar_pool) => {
                        info!("Pool Created");
                        return Ok(solar_pool)
                    },
                    Err(err_pool) => {
                        error!("Unable to create pool for mysql db solar. Err: {}", err_pool);
                        return Err( "Unable to create pool for mysql db solar.".into() )
                    }
                }
   
            } else {
                error!("Error: {} Login Credential file does not exist on local client device", LOGIN_INFO_LOC);
                return Err( "Login Credential file not found on local client device".into() )
            };
        },
        Err(fp_eff) => {
            error!("Error: Mysql login file exists but can't be accessed.  
            May have incorrect permissions. Err: {}",fp_eff);
            return Err( "Login Credential file can't be accessed. Potential file permission issues".into() )
        }
        
    }
}

fn import_to_mysql( device_ts_data: &mut Vec<Vec<Pvs6DataPoint>>, solar_sql_upload_conn: &mut PooledConn, last_device_data: &mut  Pvs6DevicesResponse ) {
    let mut all_device_success: bool = true;  
    let mut index_delete: Vec<usize> = Vec::new();  
    for (index, datapoints) in device_ts_data.iter_mut().enumerate() {

        // Now let's insert data to the database
        let upload_success = solar_sql_upload_conn.exec_batch(
            r"INSERT INTO pvs6_data (serial, parameter, data_time, data)
            VALUES (:serial, :parameter, :data_time, :data)",
            datapoints.iter().map(|dp| params! {
                "serial" => &dp.serial,
                "parameter" => &dp.parameter,
                "data_time" => &dp.data_time,
                "data" => &dp.data,
            })
        );
        match upload_success {
            Ok(_) => {
                debug!("Device: {} @ {} uploaded to Mysql solar database", datapoints[0].serial, datapoints[0].data_time );
                //debug!("Index is {}.  Device is: {}", index, datapoints[0].serial);
                index_delete.push(index);

            },
            Err(e) => {
                error!("Device: {} @ {} failed to upload to Mysql solar database. Error: {}", datapoints[0].serial, datapoints[0].data_time, e);
                //debug!("Index is {}.  Device is: {}", index, datapoints[0].serial);
                index_delete.push(index);
                all_device_success = false;
            }, 
        }
    }
    for dex in index_delete.iter().rev() {
        debug!("Removed Device: {} @ {}", device_ts_data[*dex][0].serial, device_ts_data[*dex][0].data_time);
        device_ts_data.remove(*dex);
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

fn round_time_to_sql_timestamp(str_timestamp: &str) -> String {
    // Rounds time to nearest minute from string timestamp in format: YYYY,MM,dd,HH,mm,ss and outputs in mysql timestamp
    // format of YYYY-MM-dd hh:mm:ss.  uses naive datetime and assumes datetime is in utc or other non daylight savings time 
    // changing format.
    let mut dt = NaiveDateTime::parse_from_str(str_timestamp, "%Y,%m,%d,%H,%M,%S").unwrap();

    //let dt = NaiveDate::from_ymd_opt(time[0], time[1], time[2]).unwrap()
    //.and_hms_milli_opt(time[3], time[4], time[5]).unwrap();

    let seconds: i64 = dt.second() as i64;

    if seconds >= 30 {
        // Round up to the next minute
        dt += chrono::Duration::seconds(60 - seconds);
    } else {
        // Round down to the current minute
        dt -= chrono::Duration::seconds(seconds);
    }
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
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

async fn get_pvs6_device_data( ) -> Option<String> {
    // Using direct Reqwest::get fn instead of creating client.  PVS6 loses main internet connection and will not upload data when installer port (where we are making request) connection
    //  remains open.  PVS6 is known to have a bug that causes memory to fill up and crash PVS6 if data is not uploaded.
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

fn get_sql_last_device_data( solar_sql_upload_conn: &mut PooledConn ) -> Result<Pvs6DevicesResponse, Box<dyn std::error::Error>> {
    
    let serials_result: Result<Vec<(String, String, Option<String>)>, Error> = solar_sql_upload_conn.query_map(
        r"SELECT serial, device_type, type FROM devices",
        |(serial, device_type, r#type)| (serial, device_type, r#type),
    );

    
    let mut inverters: Vec<Inverter> = Vec::new();
    let mut prod_meter: ProductionMeter = ProductionMeter::new();
    let mut sup: Supervisor = Supervisor::new();
    let mut consump_meter: ConsumptionMeter = ConsumptionMeter::new();
    
    match serials_result {
        Ok(serials) => {
            for s in serials.iter() {
                match &s.1 {
                    val if *val ==  INVERTER => {
                        let inv_query = format!("{}{}{}{}{}{}", INV_QUERY_P1, DEV_QUERY_P2, s.0, DEV_QUERY_P3, s.0, DEV_QUERY_P4 );

                        let inverters_res = solar_sql_upload_conn.query_map(
                            inv_query,
                            |(serial, data_time, freq_hz, i_3phsum_a, 
                                i_mppt1_a, ltea_3phsum_kwh, p_3phsum_kw, p_mppt1_kw,
                                stat_ind, t_htsnk_degc, v_mppt1_v, vln_3phavg_v)| 
                                { Inverter {serial, data_time, freq_hz, i_3phsum_a, i_mppt1_a, ltea_3phsum_kwh, p_3phsum_kw, p_mppt1_kw,
                                    stat_ind, t_htsnk_degc, v_mppt1_v, vln_3phavg_v} },
                        );
                        match inverters_res {
                            Ok(inverter) => {
                                let mut inv_cnt = 0;
                                for inv in inverter.iter() {
                                    inverters.push(inv.clone());
                                    inv_cnt += 1;
                                    debug!( "Inverter Count: {}.  Should be 1", inv_cnt);
                                }
                                if inv_cnt > 1 {
                                    warn!("Query should have returned only one inverter with serial: {}.  {} were returned", s.0, inv_cnt)
                                }
                            },
                            Err(effed) => {
                                error!("No inverter matching device serial {} found. Err: {}", s.0, effed);
                                return Err( format!("No inverter matching device serial {} found. Err: {} ", s.0, effed).into() )
                            }
                        }
                    },
                    val if *val == METER => {
                        match &s.2 {
                            Some(meter_type) => {
                                match meter_type {
                                    val if *val == "PVS5-METER-C" => {
                                        let consump_query = format!("{}{}{}{}{}{}", CONSUMP_QUERY_P1, DEV_QUERY_P2, s.0, DEV_QUERY_P3, s.0, DEV_QUERY_P4 );
                                        /*type RowType = HList!(String, NaiveDateTime, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>,
                                            Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>);
                                        let consump = solar_sql_upload_conn.query_map( consump_query, |row: RowType| {
                                            let hlist_pat![cm0, cm1, cm2, cm3, cm4, cm5, cm6, cm7, cm8, cm9, cm10, cm11, cm12, cm13, cm14, cm15, cm16] = row;
                                            (cm0, cm1, cm2, cm3, cm4, cm5, cm6, cm7, cm8, cm9, cm10, cm11, cm12, cm13, cm14, cm15, cm16)
                                        });
                                        
                                        let consump_data = consump.unwrap();
                                        println!("Len: {}", consump_data.len());
                                        println!("Head: {}", consump_data.head());
                                        //et serial_string: String = consump_data[0];
                                        //let timeString: String = format!("{}", consump_data.1.format("%Y-%m-%d %H:%M:%S"));

*/
                                        /*
                                        let consumption_res = solar_sql_upload_conn.query_map(
                                            consump_query,
                                            | ( serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw, 
                                                p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v ) |
                                                { ConsumptionMeter { serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw, 
                                                    p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v }
                                                },
                                            );

                                            

                                                    

                                        
                                        let consumption_res = solar_sql_upload_conn.exec_first(consump_query, ())
                                        .map( |row| {
                                            row.map(| ( serial, data_time, freq_hz, i1_a, i2_a, neg_ltea_3phsum_kwh, net_ltea_3phsum_kwh, p_3phsum_kw, 
                                                p1_kw, p2_kw, pos_ltea_3phsum_kwh, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v, v1n_v, v2n_v ) |
                                                ConsumptionMeter {
                                                    serial: serial,
                                                    data_time: data_time,
                                                    freq_hz: freq_hz,
                                                    i1_a: i1_a, 
                                                    i2_a: i2_a, 
                                                    neg_ltea_3phsum_kwh: neg_ltea_3phsum_kwh, 
                                                    net_ltea_3phsum_kwh: net_ltea_3phsum_kwh, 
                                                    p_3phsum_kw: p_3phsum_kw, 
                                                    p1_kw: p1_kw, 
                                                    p2_kw: p2_kw, 
                                                    pos_ltea_3phsum_kwh: pos_ltea_3phsum_kwh, 
                                                    q_3phsum_kvar: q_3phsum_kvar, 
                                                    s_3phsum_kva: s_3phsum_kva, 
                                                    tot_pf_rto: tot_pf_rto, 
                                                    v12_v: v12_v, 
                                                    v1n_v: v1n_v, 
                                                    v2n_v: v2n_v,
                                                })
                                            }
                                        );
                                        */
                                        
                                        let consumption_res: Result<Option<Row>,Error> = solar_sql_upload_conn.exec_first(consump_query, ());
                                        match consumption_res {
                                            Ok(row) => {
                                                match row {
                                                    Some(cm) => {
                                                        print!("{:#?}", cm[0]);
                                                        print!("{:#?}", cm[1]);
                                                        let dt = from_value::<chrono::NaiveDateTime>(cm[1].clone());
                                                        //let st = format!("{}", dt.format("%Y-%m-%d %H:%M:%S"));
                                                        //println!("TimeString: {}", st); 
                                                        consump_meter = ConsumptionMeter::set_values(
                                                            from_value::<String>(cm[0].clone()),
                                                            NaiveDate::from_ymd_opt(2016, 7, 8).unwrap().and_hms_opt(9, 10, 11).unwrap(),
                                                            if cm[2] == "NULL".into() {None} else {Some( from_value::<f64>(cm[3].clone()) )}, 
                                                            if cm[3] == "NULL".into() {None} else {Some( from_value::<f64>(cm[3].clone()) )}, 
                                                            if cm[4] == "NULL".into() {None} else {Some( from_value::<f64>(cm[4].clone()) )},
                                                            if cm[5] == "NULL".into() {None} else {Some( from_value::<f64>(cm[5].clone()) )},  
                                                            if cm[6] == "NULL".into() {None} else {Some( from_value::<f64>(cm[6].clone()) )}, 
                                                            if cm[7] == "NULL".into() {None} else {Some( from_value::<f64>(cm[7].clone()) )}, 
                                                            if cm[8] == "NULL".into() {None} else {Some( from_value::<f64>(cm[8].clone()) )},
                                                            if cm[9] == "NULL".into() {None} else {Some( from_value::<f64>(cm[9].clone()) )},
                                                            if cm[10] == "NULL".into() {None} else {Some( from_value::<f64>(cm[10].clone()) )},
                                                            if cm[11] == "NULL".into() {None} else {Some( from_value::<f64>(cm[11].clone()) )},
                                                            if cm[12] == "NULL".into() {None} else {Some( from_value::<f64>(cm[12].clone()) )},
                                                            if cm[13] == "NULL".into() {None} else {Some( from_value::<f64>(cm[13].clone()) )},
                                                            if cm[14] == "NULL".into() {None} else {Some( from_value::<f64>(cm[14].clone()) )},
                                                            if cm[15] == "NULL".into() {None} else {Some( from_value::<f64>(cm[15].clone()) )},
                                                            if cm[16] == "NULL".into() {None} else {Some( from_value::<f64>(cm[16].clone()) )},
                                                        )
                                                    },
                                                    None => {
                                                        error!("No consumption meter matching device serial {} found.", s.0);
                                                        return Err( format!("No iconsumption meter matching device serial {} found.", s.0).into() )
                                                    },
                                                }
                                                println!("Made IT through, panicing now");
                                                panic!("Good Stuff");
                                            },
                                            Err(con_eff) => {
                                                error!("{}", con_eff);
                                                return Err( format!("{}", con_eff).into() )
                                            },
                                        }
                                    },
                                    val if *val == "PVS5-METER-P" => {
                                        let production_query = format!("{}{}{}{}{}{}", PRODUCT_QUERY_P1, DEV_QUERY_P2, s.0, DEV_QUERY_P3, s.0, DEV_QUERY_P4 );

                                        let production_res = solar_sql_upload_conn.query_map(
                                            production_query,
                                            | ( serial, data_time, freq_hz, i_a, net_ltea_3phsum_kwh, p_3phsum_kw, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v ) |
                                                { ProductionMeter { serial, data_time, freq_hz, i_a, net_ltea_3phsum_kwh, p_3phsum_kw, q_3phsum_kvar, s_3phsum_kva, tot_pf_rto, v12_v }
                                                },
                                            );
                                        match production_res {
                                            Ok(production_meter) => {
                                                prod_meter = production_meter[0].clone();
                                                if production_meter.len() > 1 {
                                                    warn!("Query should have returned only one production meter with serial: {}.  {} were returned", s.0, production_meter.len());
                                                }
                                            },
                                            Err(prod_eff) => {
                                                error!("{}", prod_eff);
                                                return Err( format!("{}", prod_eff).into() )
                                            },
                                        }
                                    },
                                    _ => {
                                        error!("Not a valid power meter type");
                                        return Err( "Not a valid power meter type".into() )
                                    },
                                }
                            },
                            None => {
                                error!("Not a valid power meter type");
                                return Err( "Not a valid power meter type".into() )
                            },
                        }
                    },
                    val if *val == SUPERVISOR => {
                        let sup_query = format!("{}{}{}{}{}{}", SUP_QUERY_P1, DEV_QUERY_P2, s.0, DEV_QUERY_P3, s.0, DEV_QUERY_P4 );

                        let supervisor_res = solar_sql_upload_conn.query_map(
                            sup_query,
                            |( serial, data_time, dl_comm_err, dl_cpu_load, dl_err_count, dl_flash_avail, dl_mem_used, dl_scan_time, 
                                dl_skipped_scans, dl_untransmitted, dl_uptime ) | {
                                Supervisor { serial, data_time, dl_comm_err, dl_cpu_load, dl_err_count, dl_flash_avail, dl_mem_used, dl_scan_time, 
                                dl_skipped_scans, dl_untransmitted, dl_uptime
                                }
                            },
                        );
                        match supervisor_res {
                            Ok(supervisor) => {
                                sup = supervisor[0].clone();
                                if supervisor.len() > 1 {
                                    warn!("Query should have returned only one supervisor with serial: {}.  {} were returned", s.0, supervisor.len());
                                }
                            },
                            Err(sup_eff) => {
                                error!("{}", sup_eff);
                                return Err( format!("{}", sup_eff).into() )
                            },
                        }
                    },
                    _ => {
                        error!("Not a valid device type");
                        return Err( "Not a valid device type".into() )
                    },
                }
            }
        let last_data: Pvs6DevicesResponse = Pvs6DevicesResponse::set_values(sup, consump_meter, prod_meter, inverters);
        println!("Print 1st");
        println!("Serial: {} Time: {}", last_data.supervisor.serial, last_data.supervisor.data_time);
        println!("Print 2nd");
        println!("Serial: {} Time: {}", last_data.cons_meter.serial, last_data.cons_meter.data_time);
        println!("Print 3rd");
        println!("Serial: {} Time: {}", last_data.prod_meter.serial, last_data.prod_meter.data_time);
        println!("Print last");
        let mut test_cnt = 1;
        for inv in last_data.inverters.iter() {
            println!("#: {} Serial: {} Time: {}",test_cnt, inv.serial, inv.data_time);
            test_cnt += 1;
        }
        return Ok(last_data)
        },
        Err(eff) => {
            error!("{}", eff);
            return Err( format!( "{}", eff ).into() )
        },
    }

}
