from taos import *
from datetime import timedelta, datetime
import time, csv, logging, os
import matplotlib.pyplot as plt
import numpy as np, psutil
from my_sq import DBStore,mysql_total_time, mysql_average_time,mysql_total_reading_time, mysql_average_reading_time
batch_size_= 1000

if not os.path.exists("logs"):
    os.mkdir("logs")

if not os.path.exists("graphs_TDEngine"):
    os.mkdir("graphs_TDEngine")


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
            print(f'TDEngine starts to read {line_count} data points.')
        #print(f"The time from roadhoper:{self.time}")


class TsdbStore:
    def __init__(self):
        self.conn = connect()
        self.cursor = self.conn.cursor()
        self.dbname = "storage_module_TDEngine"       
        self.conn.execute("create database if not exists %s" % self.dbname)
        self.conn.select_db(self.dbname)
        self.conn.execute("drop table if exists ds_drive_sensor_values")
        self.conn.execute(
            "create table if not exists ds_drive_sensor_values(TS TIMESTAMP, driveId NCHAR(100), sensorId NCHAR(100), latitude FLOAT, longitude FLOAT, elevation FLOAT, speed FLOAT, acceleration FLOAT, heading FLOAT)")

    def insert_sensor_values_using_batch_tdengine1(self, driveid, sensorid, timestamps, values_lat, values_lon,
                                               values_elevation, values_speed, values_acceleration,
                                               values_heading):
        batch_size = 10000
        total_data_points = len(values_lon)
        data = []
        for i in range(total_data_points):
            sv_drv_id = driveid
            sv_sen_id = sensorid
            timestamp_str = timestamps[i].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Convert timestamp to string in the format 'YYYY-MM-DD HH:MM:SS.sss'
            try:
                latitude = float(values_lat[i])
                longitude = float(values_lon[i])
                elevation = float(values_elevation[i])
                speed = float(values_speed[i])
                acceleration = float(values_acceleration[i])
                heading = float(values_heading[i])
                data.append((timestamp_str, sv_drv_id, sv_sen_id, latitude, longitude, elevation, speed, acceleration, heading))
            except ValueError as e:
                print(f"Error occurred while converting float values: {e}")
                continue

        insert_statement = "INSERT INTO ds_drive_sensor_values (TS, driveId, sensorId, latitude, longitude, elevation, speed, acceleration, heading) VALUES"
        for i in range(0, total_data_points, batch_size):
            batch = data[i:i + batch_size]
            values = ','.join([f"('{value[0]}', '{value[1]}', '{value[2]}', {value[3]}, {value[4]}, {value[5]}, {value[6]}, {value[7]},{value[8]})" for value in batch])
            query = f"{insert_statement} {values}"
            try:
                self.cursor.execute(query)
            except Exception as e:
                print(f"Error occurred while executing the query: {e}")
                continue
        self.conn.commit()
       
##################Disk Space Calculation################
    def disk_space(self):
        columns_query = "DESCRIBE ds_drive_sensor_values"
        columns_result = self.conn.query(columns_query)

        count_query = f"SELECT COUNT(*) FROM ds_drive_sensor_values"
        count_result = self.conn.query(count_query)
        num_rows = count_result.fetch_all()[0][0]

        column_sizes = []
        for column in columns_result:
            print(f"Column name in td_engine:{column}")
            column_size = column[2]
            column_sizes.append(column_size)
        total_size = sum(column_sizes) * num_rows
        return total_size
    ##################Reading performence in tdengine#############
    def read_sensor_values_tdengine(self):
        total_time = {}
        average_time = {}
        sensor_columns = ["latitude", "longitude",  "elevation","speed", "acceleration", "heading"]
        query = f"SELECT {', '.join(sensor_columns)} FROM ds_drive_sensor_values"
        start_time = time.time()
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        end_time = time.time()
        total_time = {column: end_time - start_time for column in sensor_columns}
        print(f"total time tdengine:{total_time}")
        average_time = {column: total_time[column] / len(results) for column in sensor_columns}
        print(f"average time tdengine:{average_time}")
        return total_time, average_time
    

#################insert data in tdengine ####################
def insert_values_tdengine(driveId, sensorId, timestamps, values_lat, values_lon, values_speed, values_elevation,
                           values_acceleration, values_heading):
    begin_time = time.time()
    all_the_time_stamps = {}
    logging.basicConfig(filename=f"logs/TDEngine_insert-time.log", level=logging.INFO)
    for sensor_name in ["latitude", "longitude", "elevation","speed", "acceleration", "heading"]:
        all_the_time_stamps[sensor_name] = []
    for i in range(len(values_lat)):
        start_time = time.time()
        try:
            tdengine_obj.insert_sensor_values_using_batch_tdengine1(driveId, sensorId, [timestamps[i]], [values_lat[i]],
                                                                   [values_lon[i]], [values_speed[i]],
                                                                   [values_elevation[i]], [values_acceleration[i]],
                                                                   [values_heading[i]])
        except Exception as e:
            print(f"Error occurred while inserting data in TDEngine at index {i}: {e}")
            pass
        end_time = time.time()
        insertion_time = end_time - start_time
        all_the_time_stamps["latitude"].append(end_time - start_time)
        all_the_time_stamps["longitude"].append(end_time - start_time)
        all_the_time_stamps["elevation"].append(end_time - start_time)
        all_the_time_stamps["speed"].append(end_time - start_time)
        all_the_time_stamps["acceleration"].append(end_time - start_time)
        all_the_time_stamps["heading"].append(end_time - start_time)
        logging.info(f"\nInsertion time for TDengine at index {i}: {insertion_time}")
    
    final_time = time.time()
    tdengine_total_time = final_time - begin_time
    logging.info(f"\nTotal time to insert in TDEngine values: {tdengine_total_time}")
    average_times = {}
    
    for sensor_name, time_list in all_the_time_stamps.items():
        average_time = sum(time_list) / len(time_list)
        average_times[sensor_name] = average_time
        logging.info(f"\nAverage time to insert {sensor_name} in TDEngine values: {average_time}")
    #print(f"average time:{average_times}")
    return tdengine_total_time, average_times

def total_insertion_time_tdengine_vs_mysql(tdengine_total_time, mysql_total_time):
    tdengine_total_times = {"latitude": tdengine_total_time,
                            "longitude": tdengine_total_time,
                            "elevation": tdengine_total_time,
                            "speed": tdengine_total_time,
                            "acceleration": tdengine_total_time,
                            "heading": tdengine_total_time}
    mysql_total_times = {"latitude": mysql_total_time,
                         "longitude": mysql_total_time,
                         "elevation": mysql_total_time,
                         "speed": mysql_total_time,
                         "acceleration": mysql_total_time,
                         "heading": mysql_total_time}
    x = np.arange(len(tdengine_total_times))
    width = 0.35
    fig, ax = plt.subplots(figsize=(12, 6))
    rects1 = ax.bar(x - width / 2, list(tdengine_total_times.values()), width, label='TDengine')
    rects2 = ax.bar(x + width / 2, list(mysql_total_times.values()), width, label='MySQL')
    ax.set_xlabel('Sensor Values')
    ax.set_ylabel('Total Time (s)')
    ax.set_title('Total Time for Batch Insertion - TDengine vs MySQL')
    ax.set_xticks(x)
    ax.set_xticklabels(tdengine_total_times.keys())
    ax.legend()
    fig.tight_layout()
    plt.savefig(f"graphs_TDEngine/total_insertion_time{str(time.time())}.png", dpi=300, bbox_inches='tight')
    plt.close()


def avg_insertion_time_tdengine_vs_mysql(tdengine_average_time, mysql_average_time):
   
    x = np.arange(len(tdengine_average_time))
    width = 0.35
    fig, ax = plt.subplots(figsize=(12, 6))
    rects1 = ax.bar(x - width / 2, [tdengine_average_time[sensor_name] for sensor_name in tdengine_average_time], width, label='TDengine')
    rects2 = ax.bar(x + width / 2, [mysql_average_time[sensor_name] for sensor_name in mysql_average_time], width, label='MySQL')
    ax.set_xlabel('Sensor Values')
    ax.set_ylabel('Average Time (s)')
    ax.set_title('Average Time for Batch Insertion - TDengine vs MySQL')
    ax.set_xticks(x)
    ax.set_xticklabels(tdengine_average_time.keys())
    ax.legend()
    fig.tight_layout()
    plt.savefig(f"graphs_TDEngine/average_insertion_time{str(time.time())}.png", dpi=300, bbox_inches='tight')
    plt.close()
def total_reading_time_tdengine_mysql(tdengine_total_reading_time, mysql_total_reading_time):
    sensor_values = list(tdengine_total_reading_time.keys())
    x = np.arange(len(sensor_values))
    width = 0.35
    fig, ax = plt.subplots(figsize=(12, 6))
    rects1 = ax.bar(x - width / 2, list(tdengine_total_reading_time.values()), width, label='TDengine')
    rects2 = ax.bar(x + width / 2, list(mysql_total_reading_time.values()), width, label='MySQL')
    ax.set_xlabel('Sensor Values')
    ax.set_ylabel('Total Time (s)')
    ax.set_title('Total Reading Time for Each Sensor Value - TDengine vs MySQL')
    ax.set_xticks(x)
    ax.set_xticklabels(sensor_values)
    ax.legend()
    fig.tight_layout()
    plt.savefig(f"graphs_TDEngine/total_reading_time{str(time.time())}.png", dpi=300, bbox_inches='tight')
    plt.close()

def average_reading_time_tdengine_mysql(tdengine_average_reading_time, mysql_average_reading_time):
    sensor_values = list(tdengine_average_reading_time.keys())
    x = np.arange(len(sensor_values))
    width = 0.35
    fig, ax = plt.subplots(figsize=(12, 6))
    rects1 = ax.bar(x - width / 2, list(tdengine_average_reading_time.values()), width, label='TDengine')
    rects2 = ax.bar(x + width / 2, list(mysql_average_reading_time.values()), width, label='MySQL')

    ax.set_xlabel('Sensor Values')
    ax.set_ylabel('Average Time (s)')
    ax.set_title('Average Reading Time for Each Sensor Value - TDengine vs MySQL')
    ax.set_xticks(x)
    ax.set_xticklabels(sensor_values)
    ax.legend()
    fig.tight_layout()
    plt.savefig(f"graphs_TDEngine/average_reading_time{str(time.time())}.png", dpi=300, bbox_inches='tight')
    plt.close()
def bar_graph_disk_space(td_engine):
    mysql = DBStore()
    my_sql=mysql.disk_space()
    print(f"disk_sopace:{my_sql}")
    mysql_disk_space= float(my_sql)*1024*1024
    space = [td_engine, mysql_disk_space]
    labels = ['TDEngine', 'MySQL']
    plt.bar(labels, space)
    plt.xlabel("Database")
    plt.ylabel("Disk Space in Bytes")
    plt.title(f"Disk Space for MySQL vs TDEngine")
    plt.savefig(f"graphs_TDEngine/insert_bar_graph_disk_space{str(time.time())}.png", dpi=300, bbox_inches='tight')
    plt.close()

log = RouteLogfile()
log.read("routeor.log")
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
values_lat,values_lon, values_elevation,values_speed,  values_acceleration, values_heading = [], [], [], [], [], []
timestamps = []
time_difference = timedelta(milliseconds=1)
for idx in range(0, len(route.time)):
    # Generate a unique timestamp for each data point
    curtime = starttime + timedelta(seconds=route.time[idx]) + (idx * time_difference)
    timestamps.append(curtime)
    values_lon.append(route.lon[idx])
    values_lat.append(route.lat[idx])
    values_speed.append(route.speed[idx])
    values_elevation.append(route.elevation[idx])
    values_acceleration.append(route.acceleration[idx])
    values_heading.append(route.heading[idx])
#print(f"values latitude:{values_lat}")
tdengine_obj = TsdbStore()
sensorId = "fe0c2889-b8b5-45d1-bc8d-0759930df075"
driveId = "02cc6b37-6686-4ed6-8b69-bf3183fdb3f9"

# Insert values
print("Start insert sensor value in TDEngine")
tdengine_total_time, tdengine_average_time = insert_values_tdengine(driveId, sensorId, timestamps, values_lat,values_lon, values_elevation,values_speed,  values_acceleration, values_heading)
total_insertion_time_tdengine_vs_mysql(tdengine_total_time, mysql_total_time)
avg_insertion_time_tdengine_vs_mysql(tdengine_average_time,mysql_average_time)

#total_insertion_time_tdengine_vs_mysql(tdengine_time_stamps, mysql_time_stamps)
###############as an example   
print("Start reading sensor value in TDEngine")
tdengine_total_reading_time, tdengine_average_reading_time = tdengine_obj.read_sensor_values_tdengine()
total_reading_time_tdengine_mysql(tdengine_total_reading_time,mysql_total_reading_time)
average_reading_time_tdengine_mysql(tdengine_average_reading_time,mysql_average_reading_time)
disk_space = tdengine_obj.disk_space() 
print("Start calculating disk space")
bar_graph_disk_space(disk_space)
logging.basicConfig(filename=f"logs/tdengine_disk-space_{str(time.time())}.log", level=logging.INFO)
logging.info(f"Total disk space - TDEngine = {disk_space} Bytes")
