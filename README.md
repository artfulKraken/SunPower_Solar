# Sunpower PVS6 to mysql data retrieval
This project pulls data from a Sunpower PVS6 Supervisor device and saves it to mysql database.  Data is displayed through Grafana dashboards

## PVS6 Setup
The Sunpower PVS6 is accessed through the installer port on the PVS6 device. My personal setup uses a Rasperry Pi Zero W with a microUSB to ethernet (RJ45) adapter.  The RJ45 port is connected to the PVS6 Supervisor installer port and managed via a DHCP server on the PVS6 Supervisor.  The Raspberry Pi Zero W is connected via WiFi to my local network.  API calls from devices on the local network to the Raspberry PI Zero W are forwarded via HA Proxy to the PVS6 Supervisor and the PVS6 data is returned to the device on the local network.

There are a number of great resourses for setting this up.
- [Starreveld Guide - https://starreveld.com/PVS6%20Access%20and%20API.pdf](https://starreveld.com/PVS6%20Access%20and%20API.pdf)
  - Comprehensive setup! Most detailed guide I've run across.  I used this as my main guide for setting up and Accessing PVS6 data.
- https://github.com/ginoledesma/sunpower-pvs-exporter/blob/master/sunpower_pvs_notes.md
  - Most setups based on this Github project
  - very good detail of API calls and data returned.  Some updated guides note minor mistakes in this original guide.
- [Scott Gruby's Blog](https://blog.gruby.com/2020/04/28/monitoring-a-sunpower-solar-system/)
  - The first one I found, pretty good detail
- [Nelsons Log](https://nelsonslog.wordpress.com/2021/12/02/getting-local-access-to-sunpower-pvs6-data/)
  - Setup description - running directly off RaspPi, no ha proxy - different set up, not very detailed blog
- [brett.durrett.net](https://brett.durrett.net/getting-administrator-access-to-sunpower-pvs6-with-no-ethernet-port/)
- Getting Administrator Access to SunPower PVS6 with no Ethernet Port (USB Port).  For the newer version of the PVS6.  I don't have this version.  

## mysql Database
Self-hosted MySql server and database for storage of solar system and other relevant data.  MySql user (provided to Rust program ) must have minimum priveledges of SELECT and INSERT.  

## PVS6 Rust Program
- Sends API call to PVS6 Supervisor in a regular interval, receives data, processes and cleans data, and uploads it to MySql database
- Currently configuration settings for PVS6 Host, MySqlServer, etc are hard coded.  Future update to include a config file for settings.


## Grafana 
- Self-hosted Grafana server displays data and trends from the MySql database.  Grafana Dashboards jsons, queries for the dashboard panels, and images for the canvas panels are available in the Grafana folder.



