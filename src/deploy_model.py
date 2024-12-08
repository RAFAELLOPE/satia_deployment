import os
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd
import re
from datetime import datetime, timedelta
from bson.objectid import ObjectId


def get_aws_credentials() -> dict:
    # Retrieve environment variables
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

    return {'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
            'AWS_DEFAULT_REGION': AWS_DEFAULT_REGION}




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
        df.set_index('date', inplace=True)
        df.to_csv(os.path.abspath(os.path.join(os.getcwd(), 'data/inverterdatas.csv')))
    return df

        

# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":    
   # Get the database
   env_path = os.path.abspath(os.path.join(os.getcwd(), 'keys.env'))
   load_dotenv(env_path)
   database = get_mongodb_data()

   start_date = datetime.today().replace(year=2024, month=8, day=25, minute=0, hour=0, second=0)  # Start date (inclusive)
   end_date = start_date + timedelta(days=1)  # End date (inclusive)
   inverter = ObjectId("673f92e7cf2a88fc6f8d53be")

   query = {"$and": [{"date": 
                            {"$gte": start_date,  
                             "$lte": end_date}},
                     {"inverter" : inverter}]
           }
   
   print(query)
   item_details = database.inverterdatas.find(query)
   process_inverterdatas(item_details)