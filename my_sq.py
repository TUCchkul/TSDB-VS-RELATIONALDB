import mysql.connector
import numpy as np
from datetime import timedelta, datetime
import time, csv, logging, os
import matplotlib.pyplot as plt

batch_size_ = 1000

if not os.path.exists("logs"):
    os.mkdir("logs")

if not os.path.exists("graphs_MySQL"):
    os.mkdir("graphs_MySQL")


class RouteLogfile:
    """
    Reads the simulation results file created by RoadHopper
    """

    def read(self, fname):
        self.time = []
        self.lat = []
        self.lon = []
        self.elevation = []
        self.speed = []
        self.acceleration = []
        self.heading = []

        with open(fname, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter='\t')
            line_count = 0
            for row in csv_reader:
                self.time.append(float(row["time"]) / 1000.0)  # time is in milliseconds
                self.lat.append(float(row["lat"]))
                self.lon.append(float(row["lon"]))
                self.elevation.append(float(row["elevation"]))
                self.acceleration.append(float(row["accel"]))
                self.heading.append(float(row["heading"]))
                self.speed.append(float(row["speed"]) * 3.6)  # speed is in m/s

                line_count += 1
            print(f'MySQL starts to read {line_count} data points.')


class DBStore:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234",
            database="storage_module_mysql"
        )
        self.cursor = self.conn.cursor()
        self.table_name = "ds_drive_sensor_values"
        #self.cursor.execute(f"drop table if exists {self.table_name}")
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} (TS TIMESTAMP, driveId VARCHAR(100), sensorId VARCHAR(100), latitude FLOAT, longitude FLOAT,  elevation FLOAT, speed FLOAT,acceleration FLOAT, heading FLOAT)")

    def insert_sensor_values_using_batch_mysql(self, driveid, sensorid, timestamps, values_lat, values_lon,
                                                values_elevation,values_speed, values_acceleration,
                                               values_heading):
        batch_size = 1000
        total_data_points = len(values_lon)
        data = []

        for i in range(total_data_points):
            sv_drv_id = driveid
            sv_sen_id = sensorid
            timestamp_str = timestamps[i].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Convert timestamp to string in the format 'YYYY-MM-DD HH:MM:SS.sss'
            data.append(
                (timestamp_str, sv_drv_id, sv_sen_id, values_lat[i], values_lon[i], values_elevation[i],values_speed[i],
                  values_acceleration[i], values_heading[i]))
        insert_statement = f"INSERT INTO {self.table_name} (TS, driveId, sensorId, latitude, longitude,  elevation,speed, acceleration, heading) VALUES"
        for i in range(0, total_data_points, batch_size):
            batch = data[i:i + batch_size]
           
            values = ','.join(
                [f"('{value[0]}', '{value[1]}', '{value[2]}', {value[3]}, {value[4]}, {value[5]}, {value[6]}, {value[7]}, {value[8]})" for
                 value in batch])
            
            query = f"{insert_statement} {values}"
            #print(f"Query has done.")
            self.cursor.execute(query)

            self.conn.commit()
    def read_sensor_values_mysql(self):
        total_time = {}
        average_time = {}
        sensor_columns = ["latitude", "longitude",  "elevation","speed", "acceleration", "heading"]
        query = f"SELECT {', '.join(sensor_columns)} FROM ds_drive_sensor_values"
        start_time = time.time()
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        end_time = time.time()
        total_time = {column: end_time - start_time for column in sensor_columns}
        print(f"total time mysql:{total_time}")
        average_time = {column: total_time[column] / len(results) for column in sensor_columns}
        print(f"average time mysql:{average_time}")
        return total_time, average_time
    ##############disk space calculations########
    def disk_space(self):
        query = "SELECT  TABLE_NAME AS `ds_drive_sensor_values`, \
                ROUND(((DATA_LENGTH + INDEX_LENGTH) \
                / 1024 / 1024),2) AS `Size (MB)` \
                FROM information_schema.TABLES WHERE \
                TABLE_SCHEMA = 'storage_module_mysql' ORDER BY \
                (DATA_LENGTH + INDEX_LENGTH) DESC;"

        self.cursor.execute(query)
        myresult = self.cursor.fetchall()
        
        for item in myresult:
            return item[-1]
    
################insert sensor value in mysql##########################
def insert_values_mysql(driveId, sensorId, timestamps, values_lat,values_lon,   values_elevation,values_speed,
                       values_acceleration, values_heading):
    begin_time = time.time()
    all_the_time_stamps = {}
    logging.basicConfig(filename=f"logs/MySQL_insert-time.log", level=logging.INFO)
    for sensor_name in ["latitude", "longitude",  "elevation","speed", "acceleration", "heading"]:
        all_the_time_stamps[sensor_name] = []

    for i in range(len(values_lat)):
        start_time = time.time()
        try:
            mysql_obj.insert_sensor_values_using_batch_mysql(driveId, sensorId, [timestamps[i]], [values_lat[i]],
                                                            [values_lon[i]],[values_elevation[i]], [values_speed[i]],
                                                             [values_acceleration[i]],
                                                            [values_heading[i]])
        except Exception as e:
            print(f"Error occurred while inserting data in MySQL at index {i}: {e}")
            pass
        end_time = time.time()
        insertion_time = end_time - start_time
        all_the_time_stamps["latitude"].append(end_time - start_time)
        all_the_time_stamps["longitude"].append(end_time - start_time)
        all_the_time_stamps["elevation"].append(end_time - start_time)
        all_the_time_stamps["speed"].append(end_time - start_time)
        all_the_time_stamps["acceleration"].append(end_time - start_time)
        all_the_time_stamps["heading"].append(end_time - start_time)
        logging.info(f"\nInsertion time for MySQL at index {i}: {insertion_time}")

    final_time = time.time()
    mysql_total_time = final_time - begin_time

    logging.info(f"\nTotal time to insert in MySQL values: {mysql_total_time}")
    average_times = {}
    for sensor_name, time_list in all_the_time_stamps.items():
        average_time = sum(time_list) / len(time_list)
        average_times[sensor_name] = average_time
        logging.info(f"\nAverage time to insert {sensor_name} values in MySQL: {average_time}")

    return mysql_total_time,average_times
log = RouteLogfile()
log.read("1.log")

date = datetime.now()
starttime = datetime(
    year=date.year,
    month=date.month,
    day=date.day,
    hour=date.hour,
    minute=date.minute,
    second=date.second
)

route = log
values_lat,values_lon,  values_speed, values_elevation, values_acceleration, values_heading = [], [], [], [], [], []
timestamps = []
time_difference = timedelta(milliseconds=1)
for idx in range(0, len(route.time)):
    curtime = starttime + timedelta(seconds=route.time[idx]) + (idx * time_difference)
    #curtime = starttime + timedelta(seconds=route.time[idx])
    timestamps.append(curtime)
    values_lat.append(route.lat[idx])
    values_lon.append(route.lon[idx])
    values_speed.append(route.speed[idx])
    values_elevation.append(route.elevation[idx])
    values_acceleration.append(route.acceleration[idx])
    values_heading.append(route.heading[idx])

mysql_obj = DBStore()
sensorId = "fe0c2889-b8b5-45d1-bc8d-0759930df075"
driveId = "02cc6b37-6686-4ed6-8b69-bf3183fdb3f9"
print("Start insert sensor value in MySQL")
mysql_total_time, mysql_average_time = insert_values_mysql(driveId, sensorId, timestamps, values_lat, values_lon,values_speed, values_elevation, values_acceleration,values_heading)
print("Start reading of each sensor value in MySQL")
mysql_total_reading_time, mysql_average_reading_time = mysql_obj.read_sensor_values_mysql()
disk_space = mysql_obj.disk_space()
disk_space_in_bytes = float(disk_space)*1024*1024
logging.basicConfig(filename=f"logs/mysql_disk-space_{str(time.time())}.log", level=logging.INFO)
logging.info(f"Total disk space - MySQL = {disk_space_in_bytes} Bytes")
