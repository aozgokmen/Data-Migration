import pandas as pd
import psycopg2 
import numpy as np
import time
import os
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.bool_, psycopg2._psycopg.AsIs)
from google.cloud import bigquery_storage
from google.cloud import bigquery
from psycopg2 import extras
from configparser import ConfigParser
# Create a BigQuery client and a BigQuery Storage API client with the same
# credentials to avoid authenticating twice.

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'localdevelopment.json'

srctable = "`local-development-372108.destination.test`"
dsttable = "public.listing_analytics"


config_object = ConfigParser()


#Read config.ini file

config_object.read("config.ini")


param_dic = config_object["POSTRESQLİNFO"]
db_host = param_dic["host"]
db_database = param_dic["database"]
db_user = param_dic["user"]
db_pass = param_dic["password"]


def connect_bq():
    client = bigquery.Client()

    dataset_id = 'destination'
    dataset = client.dataset(dataset_id) 

    tbl = dataset.table('test')
    
    return client,tbl

params_dic = {
    
    "host"      : db_host,
    "database"  : db_database,
    "user"      : db_user,
    "password"  : db_pass
}

def connect_postgres(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def insert_values(conn, df, dsttable):
    df = df.replace({np.nan: None})

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s (%s) VALUES %%s" % (dsttable, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    
print("Connections are succesfully!")


def create_partitions():
    partition_query = f"SELECT distinct cast(TIMESTAMP_TRUNC(created_at, HOUR) as string) as  period FROM {srctable} ORDER BY 1"
    df = client.query(partition_query).to_dataframe()
    start = time.time()
    stop = time.time()
    print(str(np.round((stop-start),2)) + " " + "partion query done!")
    print(" ")
    print("The number of partion = " + str(len(df)))

    return df


def get_partition_data(conn ,df):
    for i in range(0,len(df)):
        print(i)
        query = f"SELECT * FROM {srctable} WHERE TIMESTAMP_TRUNC(created_at, HOUR) = '{df.iloc[i].period}' "

        #data pull insertion query  dataframe to sql table
        start = time.time()
        df_source = client.query(query).to_dataframe()
        stop = time.time()
        print(str(np.round((stop-start),2))+ " " + "data pulled!")
       # query insertion
       # Run the execute_many strateg
        start = time.time()
        insert_values(conn, df_source, dsttable)
        stop = time.time()
        print(str(np.round((stop-start),2)) + " " + "execute_values done")
        print(str(i) + " " + " partion done\n")
                
                
                
client,tbl = connect_bq()
conn = connect_postgres(params_dic)
df = create_partitions()
get_partition_data(conn,df)
conn.close()
                