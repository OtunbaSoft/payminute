import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from utils.helper import create_bucket, connect_to_dwh
from configparser import ConfigParser
from utils.constants import db_tables
from sql_statements.create import raw_data_tables, transformed_tables
from sql_statements.transform import transformation_queries

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

dwh_host = config['DWH']['host']
dwh_user = config['DWH']['username']
dwh_password = config['DWH']['password']
dwh_database = config['DWH']['database']
role = config['DWH']['role']


# Step 1: create a bucket using boto3
create_bucket(access_key, secret_access_key, bucket_name)

#  Step 2: Extract from Database to Data lake (s3)
# con = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:9355/{database}')
# con = create_engine('postgresql+psycopg2://admin:admin@localhost:9355/payminutefintench')
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
#     df = pd.read_sql(query,con)
#     print(df.head(4))
    
#     df.to_csv(
#         s3_path.format(bucket_name, table)
#         , index= False
#         , storage_options={
#             'key': access_key
#             , 'secret': secret_access_key
#         }
#     )
    
# Alternatively, using psycopg2:
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
    
# Step 3: Moving data from S3 to the DWH   
#  Create Raw Schema in DWH
conn_details = {
    'host': dwh_host
    , 'user': dwh_user
    , 'password': dwh_password
    , 'database': dwh_database
}


conn = connect_to_dwh(conn_details)
print('conn successful') 
schema = 'raw_data'

cursor = conn.cursor()

# ---- Create the dev schema
create_dev_schema_query = 'CREATE SCHEMA raw_data;'
cursor.execute(create_dev_schema_query)

# # ----- Creating the raw tables
for query in raw_data_tables:
    print(f'=================== {query[:50]}')
    cursor.execute(query)
    conn.commit()


# # copy from s3 to redshift  
for table in db_tables:
    query = f'''
        copy {schema}.{table} 
        from '{s3_path.format(bucket_name, table)}'
        iam_role '{role}'
        delimiter ','
        ignoreheader 1;
    '''
    cursor.execute(query)
    conn.commit()   



# Step 4: Create facts and Dimension table for the transformed schema


# ----- creating schema
staging_schema = 'staging'
create_dev_schema_query = f'CREATE SCHEMA {staging_schema};'
cursor.execute(create_dev_schema_query)

# ------ create star schema tables (facts and dimension)
for query in transformed_tables:
    print(f'=================== {query[:50]}')    
    cursor.execute(query)
    conn.commit()


# # ------ insert the data into the facts and dimension
for query in transformation_queries:
    print(f'=================== {query[:50]}')    
    cursor.execute(query)
    conn.commit()


# Step 5: Data Quality check
staging_tables = ['dim_customers','dim_banks', 'dim_dates', 'dim_wallets' ,'ft_customer_transactions']
query = 'SELECT COUNT(*) FROM staging.{}'

for table in staging_tables:
    cursor.execute(query.format(table))
    print(f'Table {table} has {cursor.fetchall()} rows')

cursor.close()
conn.close()
