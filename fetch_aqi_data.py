import boto3
import requests
import pandas as pd
import pymysql
from datetime import datetime


# RDS details
rds_host = 'database-1.cv2q6e4qezd1.us-east-1.rds.amazonaws.com'
rds_user = 'admin'
rds_password = 'adminajith'
rds_db_name = 'DB_API'

# Establish connection to the RDS MySQL database
connection = pymysql.connect(
    host=rds_host,
    user=rds_user,
    password=rds_password,
    database=rds_db_name,
    cursorclass=pymysql.cursors.DictCursor
)

def process_aqi_data(response):
    if response.status_code != 200:
        print(f'Error: Unable to fetch data (status code: {response.status_code}).')
        return None
    try:
        data = response.json()
    except:
        print(f'Error: Unable to parse response as JSON.')
        return None
    if 'data' not in data:
        print("Data key not found in response.")
        return None
    aqi_data = data['data']
    data_dict = {
        'date': datetime.strptime(aqi_data['time']['s'], '%Y-%m-%d %H:%M:%S').date().strftime('%Y-%m-%d'),
        'time': aqi_data['time']['s'][11:19],
        'city': aqi_data['city']['name'],
        'pm25': aqi_data['iaqi'].get('pm25', {}).get('v', 'N/A'),
        'pm10': aqi_data['iaqi'].get('pm10', {}).get('v', 'N/A'),
        'no': aqi_data['iaqi'].get('no', {}).get('v', 'N/A'),
        'no2': aqi_data['iaqi'].get('no2', {}).get('v', 'N/A'),
        'nox': 'N/A' if 'nox' not in aqi_data['iaqi'] else aqi_data['iaqi'].get('nox', {}).get('v', 'N/A'),
        'nh3': aqi_data['iaqi'].get('nh3', {}).get('v', 'N/A'),
        'co': aqi_data['iaqi'].get('co', {}).get('v', 'N/A'),
        'so2': aqi_data['iaqi'].get('so2', {}).get('v', 'N/A'),
        'o3': aqi_data['iaqi'].get('o3', {}).get('v', 'N/A'),
    }
    df = pd.DataFrame(data_dict, index=[0])  
    return df

# Initialize an empty DataFrame to store all data
all_data = pd.DataFrame()
api_key = "aad8fd32a82a6e5fa7e5238750d6eeafd10f9114"
cities = ["Bangalore","Hyderabad","Coimbatore","Bangalore","Thiruvananthapuram","Delhi"]

for city in cities:
    url = f'https://api.waqi.info/feed/{city}/?token={api_key}'
    response = requests.get(url)
    df = process_aqi_data(response)

    # Check if data was successfully retrieved (df is not None)
    if df is not None:
        # Append the DataFrame for this city to the all_data DataFrame
        all_data = pd.concat([all_data, df], ignore_index=True)

def store_data_in_rds(df, connection):
    try:
        with connection.cursor() as cursor:
            data = [tuple(x) for x in df.to_numpy()]
            cursor.executemany("""
                INSERT INTO AQI_Hourly_Data (date, time, city, pm25, pm10, no, no2, nox, nh3, co, so2, o3)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, data)
        connection.commit()
        print(f"Successfully stored data for {df.shape[0]} cities.")
    except Exception as e:
        print(f"Error storing data in RDS: {e}")

# Store the data in RDS
store_data_in_rds(all_data, connection)

# Close the connection
connection.close()
