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

from freelancersdk.resources.projects.projects import get_projects
from freelancersdk.resources.projects.helpers import (
    create_get_projects_object, create_get_projects_project_details_object,
    create_get_projects_user_details_object
)
from freelancersdk.resources.projects.exceptions import \
    ProjectsNotFoundException

# Get all data for a project
def get_project_data(project_id):
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_OAUTH_TOKEN')
    session = Session(oauth_token=token, url=url)

    query = create_get_projects_object(
        project_ids=[
            project_id,
        ],
        project_details=create_get_projects_project_details_object(
            full_description=True,
            jobs=True,
            qualifications=True,
        ),
        user_details=create_get_projects_user_details_object(
            basic=True,
            profile_description=True,
            reputation=True,
        ),
    )

    try:
        project_data = get_projects(session, query)
    except ProjectsNotFoundException as e:
        return None
    else:
        return project_data

# Initialize local SQL connection
cnx = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(db_config['user'], db_config['pass'], db_config['host'], db_config['port'], db_config['db']), echo=False)

# Import 100 project IDs that still have to be parsed
# ids_result = cnx.execute("SELECT project_id FROM successful_bids WHERE project_id NOT IN (SELECT * FROM processed_projects) LIMIT 100")
projects_result = cnx.execute("SELECT project_id FROM successful_bids LIMIT 10")

# Initialize empty list
list_of_projects = []

# Iterate through output of SQL query, and add IDs to list
for row in projects_result:
     list_of_projects.append(row['id'])

# Create queue of projects from list of IDs
project_queue = deque(list_of_projects)

# Initialize empty dataframe
projectdf = pd.DataFrame()

# Parse projects until queue is empty
while project_queue:
    project_id = project_queue.pop()
    project_data = get_project_data(project_id)
    
    # Process data returned by API, if project was found
    if project_data is not None:
        tempdf = pd.io.json.json_normalize(project_data['projects'][0])
        tempdf.columns = tempdf.columns.map(lambda x: x.split(".")[-1])
        tempdf = tempdf.loc[:,~tempdf.columns.duplicated()]
        tempdf.insert(0, 'project_id', project_id)

        #If skills are listed, iterate and parse skill name and category name
        if len(project_data['projects'][0]['jobs']) > 0:
	        # Initialize empty strings to parse skills and categories
	        skills_field = ''
			categories_field = ''
			for skill in project_data['projects'][0]['jobs']:
			    skills_field += skill['name'] + '; '
			    categories_field += skill['category']['name'] + '; '
	    	# Insert results into dedicated fields in DF
	    	tempdf.insert(0, 'skills', skills_field)
	    	tempdf.insert(0, 'skill_categories', categories_field)

    	# Append tempdf to main DF
        projectdf = projectdf.append(tempdf)
    
    # If project data wasn't found, do nothing
    else:
        pass

    time.sleep(3)

# Select columns to keep
projectdf = projectdf.loc[: ,
['project_id', 
'bid_avg', 
'bid_count', 
'bidperiod', 
'currency_id', 
'maximum', 
'minimum', 
'name', 
'project_type', 
'can_post_review	code', 
'country	exchange_rate', 
'id', 
'is_external', 
'name', 
'sign', 
'deleted', 
'description', 
'featured', 
'files', 
'from_user_location', 
'frontend_project_status', 
'hidebids', 
'hireme', 
'hireme_initial_bid', 
'hourly_project_info', 
'invited_freelancers', 
'jobs', 
'skills', 
'skill_categories', 
'language', 
'local', 
'administrative_area', 
'city', 
'code', 
'demonym	', 
'flag_url', 
'flag_url_cdn', 
'highres_flag_url', 
'highres_flag_url_cdn', 
'iso3', 
'language_code', 
'language_id', 
'name', 
'person', 
'phone_code', 
'region_id', 
'sanction', 
'seo_url', 
'full_address', 
'negotiated', 
'negotiated_bid', 
'nonpublic', 
'owner_id', 
'preview_description', 
'project_collaborations', 
'qualifications', 
'recommended_freelancers', 
'seo_url', 
'status', 
'sub_status', 
'submitdate', 
'support_sessions', 
'time_free_bids_expire', 
'time_submitted', 
'time_updated', 
'title', 
'track_ids', 
'true_location', 
'type', 
'NDA', 
'active_prepaid_milestone', 
'assisted', 
'featured', 
'fulltime', 
'ip_contract', 
'non_compete', 
'nonpublic', 
'pf_only', 
'project_management', 
'qualified', 
'sealed', 
'success_bundle']]

# Store projects that were processed in dedicated table
pd.Series(list_of_projects).to_sql(name='processed_projects', con=cnx, if_exists = 'append', index=False)

# Store projects in DB if projects DF isn't empty
if projectdf.empty != True:
    projectdf.to_sql(name='project_data', con=cnx, if_exists = 'append', index=False)

# Print output
print("{} projects found and processed at {}".format(len(projectdf), datetime.now()))

