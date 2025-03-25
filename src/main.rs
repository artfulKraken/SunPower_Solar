use reqwest;
use reqwest::Client;
use regex::Regex;
use once_cell::sync::Lazy;
use mysql::*;
use mysql::prelude::*;
use myloginrs::parse as myloginrs_parse;
use tokio::time::{self, error::Elapsed, interval_at, Duration as TokioDuration};
use std::{str,  fs, path::PathBuf};
use log::{debug, error, info, trace, warn};
use log4rs;
use chrono::prelude::*;


const DEVICES_API: &str = "https://solarpi.artfulkraken.com/cgi-bin/dl_cgi?Command=DeviceList";
const LOGIN_INFO_LOC: &str= "/home/solarnodered/.mylogin.cnf";
//const MYSQL_USER: &str = "pvs6_data";



//struct Pvs6Response {
//    devices: String,
//    result: String,
//}

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

    let now: DateTime<Utc> = Utc::now();
    //let start_time = Instant::now();
    
    /* 
    let now_minutes = now.minutes();
    let target_minutes = if now_minutes < 14 || ( now_minutes == 29 && now_seconds < 54 ) {
        14;
    } else if now_minutes < 29 || ( now_minutes == 29 && now_seconds < 54 ) {
        29;
    } else if now_minutes < 44 || ( now_minutes == 29 && now_seconds < 54 ) {
        44;
    } else {
        59;
    };
    */
    //println!("Current Time: {}", now.to_string() );

    let target_time = now.date_naive().and_hms_opt(now.hour(), now.minute(), 55).unwrap(); // Set target_time to date, cur hour, min and 55 seconds (could be before or after actual current time)
    //println!("Target Time: {}", target_time.to_string() );
    let mut start = target_time.signed_duration_since(now.naive_utc());
    //println!("Start: {}", start.to_string() );
    //println!("chrono Duration: {}", chrono::Duration::seconds(1).to_string());
    
    if start < chrono::Duration::seconds(1) {
        start = start + chrono::Duration::minutes(1);
        //println!("Start with 1 min: {}", start.to_string() );
    }

    let mut interval = interval_at(tokio::time::Instant::now() + start.to_std().unwrap(), TokioDuration::from_secs( 60 ) );

    //let mut interval = time::interval(TokioDuration::from_secs(60)); // Set interval to 1 minute
    //let start_time = Instant::now(); // Record the start time

    //let response = solarpi_client.get(DEVICES_API)
    //    .send()
    //    //.header(ACCEPT, "application/json")
    //    .await;
    //match response {
    //    Ok(_) => println!("No Error Here"),
    //    Err(eff) => println!("Error: {}", eff),
    //}
    //for dp in data_points.iter() {
    //    println!("Serial:{}  Date: {}  Parameter: {}  Data: {}", dp.serial, dp.data_time, dp.parameter, dp.data);
    //}

    loop {
        interval.tick().await; // Wait until the next tick
        pvs6_to_mysql(&solarpi_client, &mut device_ts_data).await;
        //let elapsed = start_time.elapsed().as_millis(); // Calculate elapsed time
        //println!("Task executed after {} milliseconds", elapsed);
    }    
}

async fn pvs6_to_mysql( solarpi_client: &Client, device_ts_data: &mut Vec<Vec<Pvs6DataPoint>> ) {

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
                                let sql_response = import_to_mysql( device_ts_data );
                                match sql_response {
                                    Ok(_) => {
                                        info!("Mysql database: solar successfully updated");
                                    },
                                    Err(sql_eff) => warn!("Warning Uploading to mysql. Err: {:#?}", sql_eff),
                                }
            
                            },
                            Err(text_eff) => error!("Hm, the response didn't match the shape we expected. Err: {:#?}", text_eff),
                        };
                    }
                    other => {
                        error!("PVS6 returned error code: {}", other)
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
                let data_time = round_time_to_sql_timestamp( 
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
            }
            device_ts_data.push(data_points.clone());
            data_points.clear();
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


fn import_to_mysql( device_ts_data: &mut Vec<Vec<Pvs6DataPoint>>  ) -> std::result::Result<(), Box<dyn std::error::Error>> {

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
                
                let pool = Pool::new(solar_db_opts)?;
                let mut conn = pool.get_conn()?;
                

                for datapoints in device_ts_data.iter_mut() {

                    // Now let's insert data to the database
                    let upload_success = conn.exec_batch(
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
                            debug!("Device: {} @ {} uploaded to Mysql solar database", datapoints[0].serial, datapoints[0].data_time )
                        },
                        Err(e) => error!("Device: {} @ {} failed to upload to Mysql solar database. Error: {}", datapoints[0].serial, datapoints[0].data_time, e), 
                    }
                }
            } else {
                error!("Error: {} File does not exist on local client device", LOGIN_INFO_LOC);
            };
        },
        Err(fp_eff) => error!("Error: Mysql login file exists but can't be accessed.  
            May have incorrect permissions. Err: {}",fp_eff),
        
    }
    
    Ok(())  //Incorrect format, always returns ok.  Need to return error when not able to upload file
     
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
