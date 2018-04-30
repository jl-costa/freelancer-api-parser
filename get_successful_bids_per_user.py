# Initial imports
import freelancer_sdk_v2 as api
import os
import json
import pandas as pd
import time
from datetime import datetime
import pymysql
from sqlalchemy import create_engine
from db_config import db_config
from collections import deque

from freelancer_sdk_v2.session import Session

from freelancer_sdk_v2.resources.projects.projects import get_bids
from freelancer_sdk_v2.resources.projects.exceptions import \
    BidsNotFoundException

# Create session
token = '***REMOVED***'
session = Session(oauth_token=token)

# Function to get 100 successful (awarded) bids for a given user
def get_successful_bids_per_user(user_id, offset):
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_OAUTH_TOKEN')
    session = Session(oauth_token=token, url=url)

    get_bids_data = {
        'user_ids': [
            user_id
        ],
        'awarded_statuses': [
            'awarded'
        ],
        'offset': offset,
        'project_details': True
    }

    try:
        bids_per_user = get_bids(session, **get_bids_data)
        return bids_per_user
    except BidsNotFoundException:
        return None        

# Recursive function to parse all successful bids for a given user
def parse_all_successful_bids_per_user(userid, offset, all_user_bids_df):
    successful_bids_per_user_data = get_successful_bids_per_user(userid, offset)
    
    while len(successful_bids_per_user_data['bids']) > 0:
        tempdf = pd.io.json.json_normalize(successful_bids_per_user_data['bids'])
        tempdf.columns = tempdf.columns.map(lambda x: x.split(".")[-1])
        all_user_bids_df = all_user_bids_df.append(tempdf)
        offset += 100
        try:
            print(userid)
            print(all_user_bids_df.min()['time_submitted'])
            print(offset)
            print("len of all_user_bids_df is {}".format(len(all_user_bids_df)))
            all_user_bids_df = parse_all_successful_bids_per_user(userid, offset, all_user_bids_df)
        except IndexError:
            return all_user_bids_df
    else:
        return all_user_bids_df

# Initialize local SQL connection
cnx = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], 
																			db_config['pass'], 
																			db_config['host'], 
																			db_config['port'], 
																			db_config['db']), 
																			echo=False)

# Import 100 user IDs whose bids still have to be parsed. Select only freelancer or hybrid profiles.
# ids_result = cnx.execute("SELECT userid FROM users WHERE (chosen_role = 'freelancer' OR chosen_role = 'both') AND userid NOT IN (SELECT userid FROM bids) LIMIT 100")
ids_result = cnx.execute("SELECT userid FROM users WHERE (chosen_role = 'freelancer' OR chosen_role = 'both') LIMIT 5")

# Initialize empty list
list_of_ids = []

# Iterate through output of SQL query, and add IDs to list
for row in ids_result:
     list_of_ids.append(row['userid'])

# Create queue of users from list of IDs
user_id_queue = deque(list_of_ids)

# Initialize empty dataframe
successful_bids_df = pd.DataFrame()

# Initialize offset
offset = 0

# Parse user_ids until queue is empty
while user_id_queue:
    userid = user_id_queue.pop()
    all_user_bids_df = pd.DataFrame()
    bids_data = parse_all_successful_bids_per_user(userid, offset, all_user_bids_df)
    
    # Process data returned by API, if bids were found
    if bids_data.empty != True:
        tempdf = pd.io.json.json_normalize(bids_data)
        tempdf.columns = tempdf.columns.map(lambda x: x.split(".")[-1])
        tempdf.insert(0, 'userid', userid)
        tempdf = tempdf.loc[:,~tempdf.columns.duplicated()]
        successful_bids_df = successful_bids_df.append(tempdf)
    
    # If bids weren't found, do nothing
    else:
        pass

    time.sleep(0.1)

# Initialize second SQL connection
cnx2 = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], 
																			db_config['pass'], 
																			db_config['host'], 
																			db_config['port'], 
																			db_config['db']), 
																			echo=False)

# Store results in DB if DF isn't empty

if successful_bids_df.empty != True:
	successful_bids_df.to_sql(name='successful_bids', con=cnx2, if_exists = 'append', index=False)


# Print output
print("{} records inserted at {}".format(len(successful_bids_df), datetime.now()))
