### This script will generate a random sample of user IDs and store it in a local DB ###

# Initial imports
import random
import pandas as pd
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
from db_config import db_config

# Generate list of 50,000 random user IDs within range (27382501 registered users as of Feb 27, 2018)
generated_ids = random.sample(range(1, 27382501), 50000)

# Convert list into Pandas Series
generated_ids = pd.DataFrame(generated_ids, columns = ["id"])

# Initialize local SQL connection to store data
cnx = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], db_config['pass'], db_config['host'], db_config['port'], db_config['db']), echo=False)

# Write data
generated_ids.to_sql(name='generated_ids', con=cnx, if_exists = 'replace', index=False)

# Print output
print("{} values written on {}".format(len(generated_ids), datetime.now()))

