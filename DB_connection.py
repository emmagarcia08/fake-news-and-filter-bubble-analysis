from dotenv import load_dotenv
import os

import pandas as pd
import sqlite3
from tqdm import tqdm

load_dotenv()

class DB_connection:

    def __init__(self,dataset):
        # dataset is either POLIFACT, LIAR or GOSSIPCOP --> In our work, we only use GOSSIPCOP
        path_to_data = os.getenv("DATA_PATH")
        if dataset == "POLIFACT":
            self.connection = sqlite3.connect(path_to_data + "Polifact/"  + "database.db")
            self.cursor = self.connection.cursor()
        elif dataset == "GOSSIPCOP":
            self.connection = sqlite3.connect(path_to_data + "GossipCop/"  + "database.db")
            self.cursor = self.connection.cursor()
        elif dataset == "LIAR":
            self.connection = sqlite3.connect(path_to_data + "LIAR/"  + "database.db")
            self.cursor = self.connection.cursor()

    def execute(self,query, params=None, commit = True):
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if commit == True:
            self.connection.commit()    
        return cursor.fetchall()
    
    def commit(self):
        self.connection.commit()
    
    def select(self,query):
        return pd.read_sql_query(query, self.connection)
        
    def select_single_value(self,query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]

    def save_df(self,df,table_name):
        df.to_sql(table_name, self.connection, if_exists="replace", index =False)

    def close(self):
        self.connection.close()



