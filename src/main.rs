use reqwest;
use reqwest::Client;
use regex::Regex;
use once_cell::sync::Lazy;
use mysql::*;
use mysql::prelude::*;
use myloginrs::*;
use tokio::time::{self, Duration};
use std::{time::Instant, fs, path::PathBuf};


const DEVICES_API: &str = "https://solarpi.artfulkraken.com/cgi-bin/dl_cgi?Command=DeviceList";
const LOGIN_INFO_LOC: &str= "/_home/solarnodered/.mylogin.cnf";
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

//#[tokio::main]
#[tokio::main]
async fn main() {
    
    let mut data_points: Vec<Pvs6DataPoint> = Vec::new();
    let solarpi_client: Client = Client::new();

    let mut interval = time::interval(Duration::from_secs(60)); // Set interval to 1 minute
    let start = Instant::now(); // Record the start time

    //let response = solarpi_client.get(DEVICES_API)
    //    .send()
    //    //.header(ACCEPT, "application/json")
    //    .await;
    //match response {
    //    Ok(_) => println!("No Error Here"),
    //    Err(eff) => println!("Error: {}", eff),
    //}

    pvs6_to_mysql(&solarpi_client, &mut data_points).await;
    
    for dp in data_points.iter() {
        println!("Serial:{}  Date: {}  Parameter: {}  Data: {}", dp.serial, dp.data_time, dp.parameter, dp.data);
    }

    //loop {
    //    interval.tick().await; // Wait until the next tick
    //    let elapsed = start.elapsed().as_millis(); // Calculate elapsed time
    //    println!("Task executed after {} milliseconds", elapsed);
    //}

    

    
}

async fn pvs6_to_mysql( solarpi_client: &Client, data_points: &mut Vec<Pvs6DataPoint> ) {

    let pvs6_received = solarpi_client.get(DEVICES_API)
        .send()
        //.header(ACCEPT, "application/json")
        .await;

        match pvs6_received {
            Ok(pvs6_response) => {
                match pvs6_response.status() {
                    reqwest::StatusCode::OK => {
                        // on success, parse our JSON to an APIResponse
                        match pvs6_response.text().await {
                            Ok(pvs6_data) => {
            
                                process_pvs6_devices_output( pvs6_data, data_points );
                                let sql_response = import_to_mysql( data_points );
                                match sql_response {
                                    Ok(_) => data_points.clear(),
                                    Err(sql_eff) => println!("Error Uploading to mysql. Err: {:#?}", sql_eff),
                                }
            
                            },
                            Err(text_eff) => println!("Hm, the response didn't match the shape we expected. Err: {:#?}", text_eff),
                        };
                    }
                    reqwest::StatusCode::UNAUTHORIZED => {
                        println!("Need to grab a new token");
                    }
                    other => {
                        panic!("Uh oh! Something unexpected happened: {:?}", other);
                    }
                };
            },
            Err(response_eff) => println!("Error: {}", response_eff),
        }
}



fn process_pvs6_devices_output(pvs6_data: String, data_points: &mut Vec<Pvs6DataPoint> ) {
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


fn import_to_mysql( data_points: &mut Vec<Pvs6DataPoint>  ) -> std::result::Result<(), Box<dyn std::error::Error>> {

    let my_login_file_path = PathBuf::from( LOGIN_INFO_LOC );
    match fs::exists(&my_login_file_path) {
        Ok(file_exists) => {
            if file_exists == true {
                let mysql_client_info = myloginrs::parse("client", Some(&my_login_file_path));
                let solar_db_opts = OptsBuilder::new()
                    .ip_or_hostname(Some(&mysql_client_info["host"]))
                    .db_name(Some("solar"))
                    .user(Some(&mysql_client_info["user"]))
                    .pass(Some(&mysql_client_info["password"]));
                
                let pool = Pool::new(solar_db_opts)?;
                let mut conn = pool.get_conn()?;
                
                // Now let's insert data to the database
                conn.exec_batch(
                    r"INSERT INTO pvs6_data (serial, parameter, data_time, data)
                    VALUES (:serial, :parameter, :data_time, :data)",
                    data_points.iter().map(|dp| params! {
                        "serial" => &dp.serial,
                        "parameter" => &dp.parameter,
                        "data_time" => &dp.data_time,
                        "data" => &dp.data,
                    })
                )?;
            } else {
                println!("Error: {} File does not exist on local client device", LOGIN_INFO_LOC);
            };
        },
        Err(fp_eff) => println!("Error: {}",fp_eff),
    }
    
    Ok(())
     
}
