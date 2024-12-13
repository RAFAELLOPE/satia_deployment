import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd
import re
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import requests
import json
from typing import Optional


def get_aws_credentials() -> dict:
    # Retrieve environment variables
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

    return {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
            'AWS_DEFAULT_REGION': AWS_DEFAULT_REGION}


def get_meto_data(lat:str, 
                  lon:str,  
                  timezone:str = 'Europe/Madrid') -> Optional[pd.DataFrame]:
    
    API_KEY = os.getenv("METOSOURCE_API_KEY")
    endpoint = 'https://www.meteosource.com/api/v1/flexi/' + f'point'
    payload = {'lat' : lat,
               'lon' : lon,
               'timezone': timezone,
               'units': 'metric',
               'language': 'en',
               'key': API_KEY}

    try:
        res = requests.get(endpoint, params=payload)
        data = json.loads(res.content)
        df = pd.json_normalize(data['hourly']['data'], 
                               meta=['date', 
                                     'temperature', 
                                     'pressure', 
                                     'cape', 
                                     'irradiance', 
                                     'humidity',
                                     ['wind', 'speed'],
                                     ['wind', 'angle'],
                                     ['cloud_cover', 'total'],
                                     ['precipitation','total']])

        df = df[['date', 
                 'temperature', 
                 'pressure', 
                 'cape', 
                 'irradiance', 
                 'humidity',
                 'wind.speed',
                 'wind.angle',
                 'cloud_cover.total',
                 'precipitation.total']]

        df['date'] = df['date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
        df.set_index('date', inplace=True)
        return df
    except requests.exceptions.RequestException as e:
        raise e


def get_mongodb_data():
   
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   user = os.getenv("MONGODB_USER")
   pwd = os.getenv("MONGODB_PASSWORD")
   cluster = os.getenv("MONGODB_CLUSTER")
   dbname = os.getenv("MONGODB_PROD_DB")
   MONGODB_CONNECTION_STRING = f'mongodb+srv://{user}:{pwd}@{cluster}.mongodb.net/'

   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(MONGODB_CONNECTION_STRING)

   # Create the database for our example (we will use the same database throughout the tutorial
   return client[dbname]


def process_inverterdatas(inverterdatas:dict) -> pd.DataFrame:
    dev_data = {'date':[],
                'active_power':[],
                'dev_temperature':[],
                'ac_voltage':[],
                'dc_voltage':[]}
    
    ac_voltage = 0
    for item in inverterdatas:
        for l in item.keys():
            if re.match("L\d+Data", l):
                ac_voltage += item[l]['acVoltage']
        
        dev_data['date'].append(item['date'])
        dev_data['active_power'].append(item['totalActivePower'])
        dev_data['dev_temperature'].append(item['temperature'])
        dev_data['ac_voltage'].append(ac_voltage)
        dev_data['dc_voltage'].append(item['dcVoltage'])
        ac_voltage = 0
    
    df = pd.DataFrame(dev_data)
    if not df.empty:
        df.fillna(0, inplace=True)
    return df

def process_data2inference(df_inv: pd.DataFrame, 
                           df_meteo: pd.DataFrame) -> pd.DataFrame:
    df_inv_c = df_inv.copy()
    df_meteo_c = df_meteo.copy()
    df_meteo_c = df_meteo_c.resample('5min').mean().interpolate(method='cubic')
    df_inv_c = df_inv_c.resample('5min').mean()
    df_r = df_meteo_c.join(df_inv_c, how='left')
    df_r.fillna(0, inplace=True)
    cols = [c.replace('.', '_') for c in df_r.columns]
    df_r.columns = cols
    df_r = df_r[['active_power', 
                 'dev_temperature', 
                 'ac_voltage', 
                 'dc_voltage',
                 'temperature', 
                 'pressure', 
                 'cape', 
                 'irradiance', 
                 'humidity',
                 'wind_speed', 
                 'wind_angle', 
                 'cloud_cover_total', 
                 'precipitation_total']]
    # Save to see how output looks like
    return df_inv_c, df_meteo, df_meteo_c, df_r


def filter_data(df:pd.DataFrame, 
                var:pd.DataFrame,
                wind:int = 5) -> pd.DataFrame:
    df_c = df.copy()
    df_c[var] = df_c[var].rolling(wind, center=True).mean()
    df_c[var].fillna(0, inplace=True)
    return df_c

# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":    
   # Get the database
   env_path = os.path.abspath(os.path.join(os.getcwd(), 'keys.env'))
   load_dotenv(env_path)
   database = get_mongodb_data()

   start_date = datetime.today().replace(year=2024, month=8, day=25, minute=0, hour=0, second=0)  # Start date (inclusive)
   end_date = start_date + timedelta(days=1)  # End date (inclusive)
   tomorrow = datetime.today() + timedelta(days=1)

   inverter = ObjectId("673f92e7cf2a88fc6f8d53be")
   lon = "3.8488378294343217W"
   lat = "37.35199875174232N"

   query = {"$and": [{"date": 
                            {"$gte": start_date,  
                             "$lte": end_date}},
                     {"inverter" : inverter}]
           }
   
   print(query)
   item_details = database.inverterdatas.find(query)
   df_inv = process_inverterdatas(item_details)
   df_inv['date'] = df_inv['date'].apply(lambda x: x.replace(month = tomorrow.month, day = tomorrow.day))
   df_inv.set_index('date', inplace=True)
   df_meteo = get_meto_data(lat, lon)
   df_inv_c, df_meteo, df_meteo_c, df_r = process_data2inference(df_inv, df_meteo)
   
   start_filter_date = tomorrow.replace(hour=0, minute=0, second=0)
   end_filter_date = tomorrow.replace(day=tomorrow.day + 1, hour=0, minute=0, second=0)
   
   df_r = df_r.loc[start_filter_date:end_filter_date]
   
    
   df_r = filter_data(df_r, 'active_power', 30)
   df_r = filter_data(df_r, 'ac_voltage', 30)

#    df_inv_c.to_csv(os.path.abspath(os.path.join(os.getcwd(), 'data/inverter_resampled.csv')))
#    df_meteo.to_csv(os.path.abspath(os.path.join(os.getcwd(), 'data/meteodatas.csv')))
#    df_meteo_c.to_csv(os.path.abspath(os.path.join(os.getcwd(), 'data/meteo_resampled.csv')))
#    df_r.to_csv(os.path.abspath(os.path.join(os.getcwd(), 'data/joined.csv')))

