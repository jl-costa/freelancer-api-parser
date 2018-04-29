### This will take a list of generated user IDs, and parse profile info one at a time ###

# Initial imports
import freelancersdk as api
import os
import json
import pandas as pd
import time
from datetime import datetime
import pymysql
from sqlalchemy import create_engine

from freelancersdk.session import Session

from freelancersdk.resources.users.users import get_users
from freelancersdk.resources.users.helpers import (
    create_get_users_object, create_get_users_details_object,
)
from freelancersdk.resources.users.exceptions import \
    UsersNotFoundException

# Create session
token = '***REMOVED***'
session = Session(oauth_token=token)

# Function to get profile data for a user ID
def get_user_data(user_id):
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_OAUTH_TOKEN')
    session = Session(oauth_token=token, url=url)

    query = create_get_users_object(
        user_ids=[
            user_id
        ],
        user_details=create_get_users_details_object(
            basic=True,
            profile_description=True,
            reputation=True,
            portfolio=True,
        ),
    )

    try:
        user_data = get_users(session, query)
        return user_data
    except UsersNotFoundException:
        return None

# Initialize local SQL connection
cnx = create_engine('mysql+pymysql://parser:DoIq55NnQ8uz1@localhost:3306/freelancer', echo=False)

# Import 1000 user IDs that still have to be parsed
# WHERE id NOT IN (SELECT userid FROM users)
ids_result = cnx.execute("SELECT * FROM generated_ids LIMIT 1000")

# Initialize empty list
list_of_ids = []

# Iterate through output of SQL query, and add IDs to list
for row in ids_result:
	list_of_ids = list_of_ids.append(ids_result[generated_ids.id])

# Create queue of users from list of IDs
user_queue = deque(list_of_ids)

# Initialize empty dataframe
userdf = pd.DataFrame()

# Initialize local SQL connection to store data

# Parse users until queue is empty
while user_queue:
    userid = user_queue.pop()
    user_data = get_user_data(userid)
    
    # Process data returned by API, if user was found
    if user_data is not None:
        tempdf = pd.io.json.json_normalize(user_data)
        tempdf.columns = tempdf.columns.map(lambda x: x.split(".")[-1])
        tempdf.insert(0, 'userid', userid)
        userdf = userdf.append(tempdf)
    
    # If user wasn't found, do nothing
    else:
        pass

    time.sleep(0.1)

# Store results in DB
userdf.to_sql(name='users', con=cnx, if_exists = 'append', index=False)

# Print output
print("{} records inserted at {}".format(len(userdf), datetime.now()))