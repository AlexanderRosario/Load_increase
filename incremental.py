from sqlalchemy import create_engine
from dbfread import DBF
import json,urllib,math,pyodbc,pandas as pd
from datetime import datetime

#Se establece la coneccion a la base de datos.
def conection():

    ConnetionDB = 'Connectionlocal'

    with open('DBconfig.json') as conn:

        config = json.loads(conn.read())
        database_name = config[ConnetionDB]["DB_NAME"]
        database_user = config[ConnetionDB]["DB_USER"]
        database_password = config[ConnetionDB]["DB_PASSWORD"]
        database_server = config[ConnetionDB]["SERVER"]
        dbms_driver = config[ConnetionDB]["DRIVER"]

        ConnectionString = "DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}".format(dbms_driver, database_server, database_name, database_user, database_password)
        try:  
            
            quoted = urllib.parse.quote_plus(ConnectionString)
            engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
            return engine

        except Exception as e:
            print("Error connecting to database " + str(e)) 


def load_json():

    with open('config.json') as conn:
    
        config = json.loads(conn.read())

        return config


def load_to_sqlserver(conn,df_data,name_table,sucursal,schema):

    df_data['sucursal']= sucursal
    
    df_num_of_cols = len(df_data.columns)
    
    chunknum = math.floor(2100/df_num_of_cols)
    
    df_data.to_sql(name_table,con=conn,index=False,schema=schema,if_exists='append', chunksize=chunknum,method='multi')



    
def main():

    loaded_json = load_json()

    for num,config in enumerate(load_json()):

        dbf_frame = pd.DataFrame(DBF(config["url"]),columns = config["get_columns"])

        dbf_frame_filtered = dbf_frame[dbf_frame[config["condition_columns"]] > datetime.strptime(config["condition_value"],'%Y-%m-%d').date()].fillna(0).astype(str)
        
        if dbf_frame_filtered.empty != True:

            load_to_sqlserver(conection(),dbf_frame_filtered,config["sink"],config["sucursal"],config["schema"])

            loaded_json[num]["condition_value"] = str(dbf_frame_filtered[config["condition_columns"]].max())

            json.dump(loaded_json, open("config.json", "w"), indent = 4)


if __name__ == "__main__":
    main()