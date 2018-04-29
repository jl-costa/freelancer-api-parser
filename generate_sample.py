### This script will generate a random sample of user IDs and store it in a local DB ###

# Initial imports
import random
import pandas as pd
from datetime import datetime
import pymysql
from sqlalchemy import create_engine

# Generate list of 50,000 random user IDs within range (27382501 registered users as of Feb 27, 2018)
generated_ids = random.sample(range(1, 27382501), 50000)

# Convert list into Pandas Series
generated_ids = pd.Series(generated_ids)

# Initialize local SQL connection to store data
cnx = create_engine('mysql+pymysql://opsgenie_usr:i349jdqaa38fd@localhost:3306/opsgenie', echo=False)

# Write data
generated_ids.to_sql(name='generated_ids', con=cnx, if_exists = 'replace', index=False)

