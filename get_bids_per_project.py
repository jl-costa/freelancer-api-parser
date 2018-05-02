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

# Function to get 100 bids for a given project
def get_bids_per_project(project_id, offset):
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_OAUTH_TOKEN')
    session = Session(oauth_token=token, url=url)
    bids_data = pd.DataFrame()

    get_bids_data = {
        'project_ids': [
            project_id
        ],
        'offset': offset,
        'project_details': True
    }

    try:
        bids_response = get_bids(session, **get_bids_data)
        if bids_response['bids']:
            bids_data = pd.io.json.json_normalize(bids_response['bids'])
            bids_data.columns = bids_data.columns.map(lambda x: x.split(".")[-1])
            return bids_data
        else:
            return bids_data
    except BidsNotFoundException:
        return bids_data

# Function to parse all bids for a given project
def parse_all_bids_per_project(project_id, offset):
    all_bids = pd.DataFrame()
    bids_data = get_bids_per_project(project_id, offset)
    
    while bids_data.empty == False:
        all_bids = all_bids.append(bids_data)
        offset += 100
        bids_data = get_bids_per_project(project_id, offset)

    return all_bids

# Initialize local SQL connection
cnx = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], 
                                                                            db_config['pass'], 
                                                                            db_config['host'], 
                                                                            db_config['port'], 
                                                                            db_config['db']), 
                                                                            echo=False)

# Import 50 project IDs whose bids still have to be parsed.
ids_result = cnx.execute("SELECT project_id FROM successful_bids WHERE project_id NOT IN (SELECT * FROM processed_projects_bids) LIMIT 50")

# Initialize empty list
list_of_project_ids = []

# Iterate through output of SQL query, and add IDs to list
for row in ids_result:
     list_of_project_ids.append(row['project_id'])

# Create queue of projects from list of IDs
project_id_queue = deque(list_of_project_ids)

# Initialize empty dataframe
all_bids_df = pd.DataFrame()

# Initialize offset
offset = 0

# Parse project_ids until queue is empty
while project_id_queue:
    project_id = project_id_queue.pop()
    bids_df = parse_all_bids_per_project(project_id, offset)
    
    # Process data returned by API, if bids were found
    if bids_df.empty != True:
        bids_df = bids_df.loc[:,~bids_df.columns.duplicated()]
        all_bids_df = all_bids_df.append(bids_df)
    
    # If bids weren't found, do nothing
    else:
        pass

    time.sleep(3)

# Store project_ids that were processed in dedicated table
pd.Series(list_of_project_ids).to_sql(name='processed_projects_bids', con=cnx, if_exists = 'append', index=False)

# Store bids in DB if bids DF isn't empty
if all_bids_df.empty != True:
    all_bids_df.to_sql(name='all_bids', con=cnx, if_exists = 'append', index=False)

# Print output
print("{} project_ids processed with {} bids found at {}".format(len(list_of_project_ids), len(all_bids_df), datetime.now()))