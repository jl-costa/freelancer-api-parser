# freelancer-api-parser

This is a series of Python script intended to parse a sample of freelancers from freelancer.com, get their profile details, get all of the projects that they've been awarded over their career in the platform, and then parse project details for those successful projects, as well as associated reviews.

# Requirements

You need to be running Python 3.x (this is tested with Python 3.6), with the following dependencies installed:

- pandas
- pymysql
- sqlalchemy
- freelancer_sdk_v2 (a modified version of the freelancer-sdk package provided by the platform). My modified version fixes a few bugs and also adds a function to parse reviews. The modified version can be found at:

https://github.com/jl-costa/freelancer_sdk_v2

It should be cloned into the folder where the rest of your Python modules are running.

Also, this has been set up to store the parsed data into a MySQL database. The code must be run in an environment with an active MySQL engine, or alternatively it can run locally but db_config.py must contain details for a remote MySQL connection. I've only tested this with a local MySQL DB, and I'd strongly encourage it.

After setting up the database engine and creating a dedicated DB (with an associated user and password with full privileges over the DB), edit the db_config_example.py file and rename it to db_config.py - all of the scripts are configured to import your database credentials from that config file.

If you don't want to use a relational DB, you can replace the "to_sql()" functions at the end of each script with a "to_csv()" function to generate CSV files. I'd recommend using a DB though, it's neater and you can run SQL statements on the data.

# Execution

This is the recommended order of execution:

1) generate_sample.py - this will generate a random sample of integers in a range (based on the number of active accounts), and store them in a dedicated table on the DB. You should update the file to reflect the total number of accounts active in the platform (to make the range as realistic as possible), and also update the sample size to your desired number.

2) parse_user_info.py - this will take the IDs generated in the previous script, and get profile details for user accounts that exist under those randomly-generated IDs. Data will be stored in a dedicated table.

3) get_successful_bids_per_user.py - this will take user accounts, filter them to 'freelancer' or 'freelancer and employer' account types, and then try to find awarded bid (aka awarded projects) for those accounts. It will store the found bids in a dedicated table, and it will also keep track of all user_ids that have been processed in a separate table.

4) get_project_data.py - this will take project IDs from the successful bids found in the previous step, and parse additional project details into a separate table.

5) get_reviews.py - this will also take project IDs from the list of successful bids, and store all reviews associated with each project into a separate table.

(OPTIONAL) get_bids_per_project.py - this is useful in case you want to parse ALL bids associated with each project, not just the awarded bid.

# Notes

All of the scripts are set to process a limited amount of users/projects at a time (typically 100). They also have time.sleep() settings to prevent CPU overuse and to prevent the API from banning the IP too quickly. Still, I need to implement better error checking for http/ssl errors that are returned whenever the rate limit is exceeded.

Because each script only processes a bunch of IDs at a time, I have a few bash scripts to continuously execute each script on a loop. You can use them as a template if running this on a Linux environment. In my case, I'm using a CentOS 7 environment. The bash loops can be found in a dedicated folder. They're also set to store the output from each script run into a dedicated log file (and log folder).

Each script is set to first query the database for the relevant table and grab IDs that haven't been processed yet. This will not work the first time you ever run one of the scripts (as the associated DB table won't exist yet). Therefore, for the first time you run each script, remove the "WHERE ..." conditions from the SELECT statement in the code, and just run the script once. Then, put back in the "WHERE ..." conditions and leave the script running on a loop.
