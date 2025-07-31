import sys
import os
from dotenv import load_dotenv
sys.path.insert(0, os.path.abspath('../..'))

import pandas as pd

from DB_connection import DB_connection

load_dotenv()

db_connection = DB_connection("GOSSIPCOP")

def load_table_into_db():
    data_path = os.getenv("DATA_PATH")
    fake_table = pd.read_csv(data_path + 'GossipCop/gossipcop_fake.csv')
    real_table = pd.read_csv(data_path + 'GossipCop/gossipcop_real.csv')

    # Add a column to indicate if the row is fake or real
    fake_table['label'] = 'fake'
    real_table['label'] = 'real'

    # Concatenate the two tables
    combined_table = pd.concat([fake_table, real_table], ignore_index=True)

    # Save the table to the database
    db_connection.save_df(combined_table, 'news')
