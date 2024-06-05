import pandas as pd
import psycopg2.extras as extras    
import psycopg2
import numpy as np
import argparse
import time
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--srctable", type=str ,help="write table name!")
parser.add_argument("--dsttable",type=str, help="write destination table!")
parser.add_argument("--count",type=int, help="write count!")
parser.add_argument("--coln",type=str, help="write colunmn name!")
args = parser.parse_args()



#fizbot postresql details
param_dic = {
}

def connect(params_dic):



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

def pull_data(conn,query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    liste = cursor.fetchall()
    df_list = pd.DataFrame(liste,columns=[i[0] for i in cursor.description])
    df = pd.DataFrame(df_list)
    return df

def execute_values(conn, df, table):
    df = df.replace({np.nan: None})

   
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    table = args.dsttable
    query  = "INSERT INTO %s (%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    

    # Connect to the database
conn = connect(param_dic)


partition_query = """SELECT part_number, min(id) as min_id, max(id) as max_id, count(id) as count_listing
    FROM(
    select ntile(cnt) over(order by id) as part_number, id
        from %s
        inner join (
                select (count(id)/200)::integer+1 as cnt
                from %s
                where id < %s
                ) as sq on 1=1
        where id < %s
        ) as sq2
        GROUP BY part_number
        ORDER BY part_number asc"""  % (args.srctable,args.srctable,args.count,args.count)
        
start = time.time()
df_source = pull_data(conn, partition_query)
stop = time.time()
print(str(np.round((stop-start),2)) + " " + "partion query done!")
print(" ")
print("The number of partion = " + str(len(df_source)))



import numpy as np



for i in range(0,len(df_source)):
                query = f"SELECT {args.coln} FROM {args.srctable}  where id between {df_source.iloc[i].min_id} and {df_source.iloc[i].max_id} "



                #data pull insertion query  dataframe to sql table
                start = time.time()
                df = pull_data(conn, query)
                stop = time.time()
                print(str(np.round((stop-start),2))+ " " + "data pulled!")
                # query insertion

                # Run the execute_many strateg
                start = time.time()
                table = args.dsttable
                execute_values(conn, df, table)
                stop = time.time()
                print(str(np.round((stop-start),2)) + " " + "execute_values done")
                print(str(i) + " " + " partion done\n")
                
conn.close()


