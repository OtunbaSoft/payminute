import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from utils.helper import create_bucket
from configparser import ConfigParser
from utils.constants import db_tables


# creating instance of configparser
config = ConfigParser()
config.read('.env')

# Declaring variables
region = config['AWS']['region']
bucket_name = config['AWS']['bucket_name']

access_key = config['AWS']['access_key']
secret_access_key = config['AWS']['secret_access_key']

host = config['DB_CRED']['host']
user = config['DB_CRED']['username']
password = config['DB_CRED']['password']
database = config['DB_CRED']['database']



# Step 1: create a bucket using boto3
# create_bucket(access_key, secret_access_key, bucket_name)

#  Step 2: Extract from Database to Data lake (s3)
# conn = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:9355/{database}')
conn = create_engine('postgresql+psycopg2://admin:admin@localhost:9355/payminutefintench')
def create_conn():
    return  psycopg2.connect(
            database = database,
            user = user,
            password = password,
            host = host,
            port = 9355
    )

cur = create_conn().cursor()
s3_path = 's3://{}/{}.csv'

# for table in db_tables:
#     query = f'SELECT * FROM {table}'
#     df = pd.read_sql(query,conn)
#     print(df.head(4))
    
#     df.to_csv(
#         s3_path.format(bucket_name, table)
#         , index= False
#         , storage_options={
#             'key': access_key
#             , 'secret': secret_access_key
#         }
#     )
    
for table in db_tables:
    query = f'SELECT * FROM {table}'
    cur.execute(query)
    result = cur.fetchall()
    # print(result)
    df = pd.DataFrame(result)
    s3_path = 's3://{}/{}.csv'

    df.to_csv(
            s3_path.format(bucket_name, table)
            , index= False
            , storage_options={
                'key': access_key
                , 'secret': secret_access_key
            }
        )
    
    


