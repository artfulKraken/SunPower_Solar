use reqwest;
use reqwest::Client;
use regex::Regex;
use once_cell::sync::Lazy;
use mysql::*;
use mysql::prelude::*;
use myloginrs::parse as myloginrs_parse;
use tokio::time::{Interval, interval_at, Duration as TokioDuration};
use std::{str, fs, path::PathBuf};
use log::{debug, error, info, warn};
use log4rs;
use chrono::{prelude::*, Duration, DurationRound};


const DEVICES_API: &str = "https://solarpi.artfulkraken.com/cgi-bin/dl_cgi?Command=DeviceList";
const LOGIN_INFO_LOC: &str= "/home/solarnodered/.mylogin.cnf";
const PVS6_GET_DEVICES_INTERVAL: u64 = 5; // Time in minutes
const PVS6_GET_DEVICES_INTERVAL_UNITS: char = 'm';
 

struct Pvs6DataPoint {
    serial: String,
    data_time: String,
    parameter: String,
    data: String,
}

impl Pvs6DataPoint {
    fn new( serial: &str, data_time: &str, parameter: &str, data: &str ) -> Self {
        Self {
            serial: serial.to_owned(),
            data_time: data_time.to_owned(),
            parameter: parameter.to_owned(),
            data: data.to_owned(),
        }
    }
}

impl Clone for Pvs6DataPoint {
    fn clone(&self) -> Pvs6DataPoint {
        Pvs6DataPoint::new(&self.serial, &self.data_time, &self.parameter, &self.data)
    }
}

//#[tokio::main]
#[tokio::main]
async fn main() {
    
    log4rs::init_file("log_config.yml", Default::default()).unwrap();   //Need Error Handling here.  What if log doesn't unwrap

    let mut device_ts_data: Vec<Vec<Pvs6DataPoint>> = Vec::new();
    let solarpi_client: Client = Client::new();
    
    //get sql pool and connection
    let result_sql_pool = get_mysql_pool();
    match result_sql_pool {
        Ok(solar_sql_pool) => {
            let conn = solar_sql_pool.get_conn();
            match conn {
                Ok(solar_sql_upload_conn) => {
                   let mut solar_sql_upload_conn = solar_sql_upload_conn; 

                   // Set start time and interval of pvs6 data pulls.  
                    let offset_dur = chrono::Duration::milliseconds( -250 );
                    let mut get_pvs6_device_interval = set_interval(PVS6_GET_DEVICES_INTERVAL, PVS6_GET_DEVICES_INTERVAL_UNITS, offset_dur);

                    loop {
                        get_pvs6_device_interval.tick().await; // Wait until the next tick
                        // get pvs6 data and upload to mysql solar database
                        pvs6_to_mysql(&solarpi_client, &mut device_ts_data, &mut solar_sql_upload_conn).await;
                        println!( "Run at: {}", Utc::now().to_string() ); //for loop timing testing
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

async fn pvs6_to_mysql( solarpi_client: &Client, device_ts_data: &mut Vec<Vec<Pvs6DataPoint>>, solar_sql_upload_conn: &mut PooledConn ) {

    let pvs6_received = solarpi_client.get(DEVICES_API)
        .send()
        //.header(ACCEPT, "application/json")
        .await;
        match pvs6_received {
            Ok(pvs6_response) => {
                match pvs6_response.status() {
                    reqwest::StatusCode::OK => {
                        // on success, parse our JSON to an APIResponse
                        info!("PVS6 response code: Ok");
                        match pvs6_response.text().await {
                            Ok(pvs6_data) => {
                                
                                process_pvs6_devices_output( pvs6_data, device_ts_data );
                                import_to_mysql( device_ts_data, solar_sql_upload_conn );
                
                            },
                            Err(text_eff) => error!("Unable to extract json body from pvs6 response. Err: {:#?}", text_eff),
                        };
                    }
                    other => {
                        error!("PVS6 returned error code: {}", other);
                    }
                };
            },
            Err(response_eff) => warn!("PVS6 did not respond. Error Code: {}", response_eff),
        }
}



fn process_pvs6_devices_output(pvs6_data: String, device_ts_data: &mut Vec<Vec<Pvs6DataPoint>> ) {
    //Regex patterns synced lazy for compile efficiency improvement
    //Unwrapping all Regex expressions.  If it doesn't unwrap, its a bug in my expression.
    static RE_DEVICES: Lazy<Regex> = Lazy::new( || Regex::new(r"\[.*\]").unwrap() );
    static RE_RESULT: Lazy<Regex> = Lazy::new( || Regex::new(r"\}\],result:succeed").unwrap() );
    static RE_SERIAL: Lazy<Regex> = Lazy::new( || Regex::new(r"SERIAL:([^,]*)").unwrap() );
    static RE_DATA_TIME: Lazy<Regex> = Lazy::new( || Regex::new(r"DATATIME:([0-9]{4},[0-9]{2},[0-9]{2},[0-9]{2},[0-9]{2},[0-9]{2})").unwrap() );
    static RE_PARAMETER: Lazy<Regex> = Lazy::new( || Regex::new(r",([a-z_0-9]*):([^,]*)").unwrap() );
    static RE_EXCLUDED_PARAMETER: Lazy<Regex> = Lazy::new( || Regex::new(r"interface|subtype|origin|hw_version|slave").unwrap() );
    
    let pvs6_data: String = pvs6_data.chars().filter(|c| !matches!(c, '\"' | '\t' | '\n' | ' ' )).collect::<String>();
    let mat_result = RE_RESULT.find(&pvs6_data).unwrap();
    
    if ! mat_result.is_empty() {
        let mat_devices = RE_DEVICES.find(&pvs6_data).unwrap();
        if ! mat_devices.is_empty() {
            let devices: &str = mat_devices.as_str();
            //println!("Devices: {:?}", devices);
            
            let mut data_points: Vec<Pvs6DataPoint>= Vec::new();

            let device_list = devices.split("},{");
            for device in device_list {
                
                //println!("Device: {}", &device);
                let serial = RE_SERIAL.captures(&device).unwrap().get(1).unwrap().as_str();  //first unwap may cause crash as error
                let data_time = to_sql_timestamp( 
                    RE_DATA_TIME
                    .captures(&device)
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .as_str()
                );  //first unwap may cause crash as error
                for caps in RE_PARAMETER.captures_iter(device) {
                    if ! RE_EXCLUDED_PARAMETER.is_match( caps.get(1).unwrap().as_str() ) {
                        let parameter = caps.get(1).unwrap().as_str();
                        let data = caps.get(2).unwrap().as_str();
                        
                        let data_point = Pvs6DataPoint::new( 
                            serial, &data_time, parameter, data
                        );
                        data_points.push(data_point);
                    }
                }
                
                device_ts_data.push(data_points.clone());
                data_points.clear();
                
            }
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

fn import_to_mysql( device_ts_data: &mut Vec<Vec<Pvs6DataPoint>>, solar_sql_upload_conn: &mut PooledConn ) {
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
                debug!("Index is {}.  Device is: {}", index, datapoints[0].serial);
                index_delete.push(index);

            },
            Err(e) => {
                error!("Device: {} @ {} failed to upload to Mysql solar database. Error: {}", datapoints[0].serial, datapoints[0].data_time, e);
                debug!("Index is {}.  Device is: {}", index, datapoints[0].serial);
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
