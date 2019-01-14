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

from freelancer_sdk_v2.resources.projects.projects import get_reviews

# Create session
token = ''
session = Session(oauth_token=token)

# Get all reviews for a project
def get_review_data(project_id):
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_OAUTH_TOKEN')
    session = Session(oauth_token=token, url=url)
    try:
        review_data = get_reviews(session, project_id)
        return review_data
    except ReviewNotFoundException:
        return None

# Initialize local SQL connection
cnx = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], 
                                                                            db_config['pass'], 
                                                                            db_config['host'], 
                                                                            db_config['port'], 
                                                                            db_config['db']), 
                                                                            echo=False)

# Import 100 project IDs that still have to be parsed
# projects_result = cnx.execute("SELECT project_id FROM successful_bids WHERE project_id NOT IN (SELECT * FROM processed_projects_reviews) LIMIT 100")
projects_result = cnx.execute("SELECT project_id FROM successful_bids LIMIT 50")

# Initialize empty list
list_of_projects = []

# Iterate through output of SQL query, and add IDs to list
for row in projects_result:
     list_of_projects.append(row['project_id'])

# Create queue of projects from list of IDs
project_queue = deque(list_of_projects)

# Initialize empty dataframe
reviewdf = pd.DataFrame()

# Parse projects until queue is empty
while project_queue:
    project_id = project_queue.pop()
    review_data = get_review_data(project_id)
    
    # Check that API call didn't fail
    if review_data is not None:
        # Check that at least one review was found
        if len(review_data['reviews']) > 0:
            # Iterate through reviews in response and add them to DF
            for review in review_data['reviews']:
                tempdf = pd.io.json.json_normalize(review)
                tempdf.columns = tempdf.columns.map(lambda x: x.split(".")[-1])
                tempdf = tempdf.loc[:,~tempdf.columns.duplicated()]

            # Convert the DF to str for successful storage in DB
            tempdf = tempdf.astype(str)
            # Append tempdf to main DF
            reviewdf = reviewdf.append(tempdf)
    
    # If project data wasn't found, do nothing
    else:
        pass

    time.sleep(2)

# Store projects that were processed in dedicated table
pd.Series(list_of_projects).to_sql(name='processed_projects_reviews', con=cnx, if_exists = 'append', index=False)

# Store reviews in DB if reviews DF isn't empty
if reviewdf.empty != True:
    # Select columns to keep
    reviewdf = reviewdf.loc[:,
    ['bid_amount', 'code', 'country', 'exchange_rate', 'id', 'is_external',
       'name', 'sign', 'description', 'featured', 'from_user_id',
       'paid_amount', 'project_id', 'rating', 'communication', 'expertise',
       'hire_again', 'professionalism', 'quality', 'on_budget_display',
       'on_time_display', 'ratings', 'context_id', 'context_name',
       'review_type', 'seo_url', 'review_project_status', 'role', 'sealed',
       'status', 'submitdate', 'time_submitted', 'to_user_id']]

    # Store in DB
    reviewdf.to_sql(name='reviews', con=cnx, if_exists = 'append', index=False)

# Print output
print("{} reviews found from {} projects and processed at {}".format(len(reviewdf), len(list_of_projects), datetime.now()))
