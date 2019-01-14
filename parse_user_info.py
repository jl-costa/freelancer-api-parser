### This will take a list of generated user IDs, and parse profile info one at a time ###

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

from freelancer_sdk_v2.resources.users.users import get_users
from freelancer_sdk_v2.resources.users.helpers import (
    create_get_users_object, create_get_users_details_object,
)
from freelancer_sdk_v2.resources.users.exceptions import \
    UsersNotFoundException

# Create session
token = 'Fne2Y3oFUZRHTYnHcrFTfGp9mJJH64'
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
cnx = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], db_config['pass'], db_config['host'], db_config['port'], db_config['db']), echo=False)

# Import 100 user IDs that still have to be parsed
ids_result = cnx.execute("SELECT * FROM generated_ids WHERE id NOT IN (SELECT userid FROM users) LIMIT 100")

# Initialize empty list
list_of_ids = []

# Iterate through output of SQL query, and add IDs to list
for row in ids_result:
     list_of_ids.append(row['id'])

# Create queue of users from list of IDs
user_queue = deque(list_of_ids)

# Initialize empty dataframe
userdf = pd.DataFrame()

# Parse users until queue is empty
while user_queue:
    userid = user_queue.pop()
    user_data = get_user_data(userid)
    
    # Process data returned by API, if user was found
    if user_data is not None:
        tempdf = pd.io.json.json_normalize(user_data)
        tempdf.columns = tempdf.columns.map(lambda x: x.split(".")[-1])
        tempdf.insert(0, 'userid', userid)
        tempdf = tempdf.loc[:,~tempdf.columns.duplicated()]
        userdf = userdf.append(tempdf)
    
    # If user wasn't found, do nothing
    else:
        pass

    time.sleep(3)

# If DF isn't empty, select columns and store in DB

if userdf.empty == False:
    userdf = userdf.loc[:, 
                        ['userid',
                        'chosen_role',
                        'closed',
                        'company',
                        'corporate',
                        'corporate_users',
                        'display_name',
                        'email',
                        'endorsements',
                        'first_name',
                        'force_verify',
                        'hourly_rate',
                        'id',
                        'is_active',
                        'jobs',
                        'last_name',
                        'limited_account',
                        'administrative_area',
                        'city',
                        'code',
                        'flag_url',
                        'name',
                        'person',
                        'phone_code',
                        'region_id',
                        'sanction',
                        'seo_url',
                        'full_address',
                        'membership_package',
                        'portfolio_count',
                        'country',
                        'exchange_rate',
                        'is_external',
                        'sign',
                        'primary_language',
                        'profile_description',
                        'public_name',
                        'qualifications',
                        'registration_date',
                        'earnings_score',
                        'all',
                        'communication',
                        'expertise',
                        'hire_again',
                        'professionalism',
                        'quality',
                        'complete',
                        'completion_rate',
                        'earnings',
                        'incomplete',
                        'incomplete_reviews',
                        'on_budget',
                        'on_time',
                        'overall',
                        'positive',
                        'rehire_rate',
                        'reviews',
                        'job_history',
                        'project_stats',
                        'role',
                        'user_id',
                        'responsiveness',
                        'search_languages',
                        'spam_profile',
                        'deposit_made',
                        'email_verified',
                        'facebook_connected',
                        'identity_verified',
                        'payment_verified',
                        'phone_verified',
                        'profile_complete',
                        'support_status',
                        'suspended',
                        'tagline',
                        'test_user',
                        'offset',
                        'timezone',
                        'true_location',
                        'username',
                        ]]

    # Store results in DB
    userdf.to_sql(name='users', con=cnx, if_exists = 'append', index=False)

# Print output
print("{} records inserted at {}".format(len(userdf), datetime.now()))
