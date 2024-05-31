import requests
import time
from datetime import datetime
import os
import snowflake.connector
import webbrowser
from behave import *

# Configuration
BASE_URL = 'https://matillion.dev.eu.dm.aws.novartis.net'
GROUP_NAME = 'GERMANY'
PROJECT_NAME = 'GERMANY'
VERSION_NAME = 'default'
ENVIRONMENT_NAME = 'DEV_CONN'

# API user credentials
API_USER = 'bddtester'
API_PASSWORD = os.getenv('matillion_key')

# Snowflake connection parameters
ACCOUNT = 'nvs6deveuwest1.eu-west-1'
USER = 'PONSJO2@novartis.net'
AUTHENTICATOR = 'externalbrowser'
WAREHOUSE = 'COM_IMDNA_GERMANY_WH_S'
DATABASE = 'COM_GERMANY_IM_DNA_STG'
SCHEMA = 'STG_GERMANY'

# Function to execute a Matillion script
def execute_matillion_script(job_name):
    print(f"Executing Matillion script: {job_name}")
    url = f'{BASE_URL}/rest/v1/group/name/{GROUP_NAME}/project/name/{PROJECT_NAME}/version/name/{VERSION_NAME}/job/name/{job_name}/run?environmentName={ENVIRONMENT_NAME}'
    response = requests.post(url, auth=(API_USER, API_PASSWORD), verify=False)
    
    if response.status_code == 200:
        job_response = response.json()
        job_id = job_response['id']
        print(f"Job '{job_name}' triggered successfully with ID {job_id}")
        status_url = f'{BASE_URL}/rest/v1/group/name/{GROUP_NAME}/project/name/{PROJECT_NAME}/task/history?since={datetime.now().date()}'
        
        while True:
            status_response = requests.get(status_url, auth=(API_USER, API_PASSWORD), verify=False)
            if status_response.status_code == 200:
                tasks = status_response.json()
                for task in tasks:
                    if task['id'] == job_id:
                        status = task['state']
                        if status == 'SUCCESS':
                            print(f"Job '{job_name}' completed successfully.")
                            return True
                        elif status == 'ERROR':
                            print(f"Job '{job_name}' failed.")
                            return False
            else:
                print(f"Failed to get job status for '{job_name}':")
                print(f"Status Code: {status_response.status_code}")
                print(f"Response: {status_response.text}")
                return False
            
            time.sleep(5)
    else:
        print(f"Failed to trigger job '{job_name}':")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return False


# Function to validate the structure of the tables
def check_structure():
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=ACCOUNT,
            user=USER,
            authenticator=AUTHENTICATOR,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
        print("Connected to Snowflake successfully!")
        # Creating the cursor
        cs = conn.cursor()

        print("Executing query...")
        # Check table names
        cs.execute("USE COM_GERMANY_IM_DNA_STG")
        cs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'STG_GERMANY' AND table_name IN ('prod_dim', 'rep_dim', 'fact_test');")
        tables = cs.fetchall()
        print(f"Tables found: {tables}")
        assert ('fact_test',) in tables
        assert ('rep_dim',) in tables
        assert ('prod_dim',) in tables

        # Check column names and data types for FACT_TEST
        cs.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'fact_test';")
        fact_table_columns = cs.fetchall()
        print(f"fact_test columns: {fact_table_columns}")
        assert ('rep_id', 'TEXT') in fact_table_columns
        assert ('prod_id', 'TEXT') in fact_table_columns
        assert ('price', 'NUMBER') in fact_table_columns
        assert ('quantity', 'NUMBER') in fact_table_columns
    
        # Check column names and data types for REP_DIM
        cs.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'rep_dim';")
        rep_dim_columns = cs.fetchall()
        print(f"rep_dim columns: {rep_dim_columns}")
        assert ('rep_id', 'TEXT') in rep_dim_columns
        assert ('rep_status', 'TEXT') in rep_dim_columns
        
        # Check column names and data types for PROD_DIM
        cs.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'prod_dim'")
        prod_dim_columns = cs.fetchall()
        print(f"prod_dim columns: {prod_dim_columns}")
        assert ('prod_id', 'TEXT') in prod_dim_columns
        assert ('prod_status', 'TEXT') in prod_dim_columns

    finally:
        cs.close()
        print("Cursor closed.")
        print("Passed: Tables structure is correct step")
    conn.close()
    print("Connection closed.")
    return True

# Function to validate the data inserted into the tables
def validate_data():
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=ACCOUNT,
            user=USER,
            authenticator=AUTHENTICATOR,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
        print("Connected to Snowflake successfully!")

        # Creating the cursor
        cs = conn.cursor()
        print("Executing query...")

        # Validate data in rep_dim
        cs.execute('SELECT DISTINCT "rep_status" FROM com_germany_im_dna_stg.stg_germany."rep_dim";')
        distinct_status = cs.fetchall()

        # Convert fetchall result into a plain list
        distinct_status = [status[0] for status in distinct_status]

        # Check if the distinct_status list contains only 'Active' and 'Inactive'
        assert set(distinct_status) == {'Active', 'Inactive'}, f"Error: rep_dim table has unexpected rep_status values: {distinct_status}"

    finally:
        cs.close()
        print("Cursor closed.")
        print("Passed: Data inserted is correct step")
    conn.close()
    print("Connection closed.")
    print("")
    return True

# Function to drop tables inserted into the tables
def delete_tables():
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=ACCOUNT,
            user=USER,
            authenticator=AUTHENTICATOR,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
        print("Connected to Snowflake successfully!")

        cs = conn.cursor()
        print("Executing query...")

        # Delete All tables
        cs.execute('DROP TABLE IF EXISTS com_germany_im_dna_stg.stg_germany."fact_test";')
        cs.execute('DROP TABLE IF EXISTS com_germany_im_dna_stg.stg_germany."rep_dim";')
        cs.execute('DROP TABLE IF EXISTS com_germany_im_dna_stg.stg_germany."prod_dim";')

    finally:
        cs.close()
        print("Cursor closed.")
        print("Passed: Tables deleted")
    conn.close()
    print("Connection closed.")
    print("")
    return True

def validate_drop_tables():
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=ACCOUNT,
            user=USER,
            authenticator=AUTHENTICATOR,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
        print("Connected to Snowflake successfully!")

        cs = conn.cursor()
        print("Executing query...")

        # Chcek if fact_test table still exists
        cs.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'stg_germany' AND table_name = 'fact_test';")
        assert cs.fetchone()[0] == 0, "fact_test table still exists."

        # Check if rep_dim table still exists
        cs.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'stg_germany' AND table_name = 'rep_dim';")
        assert cs.fetchone()[0] == 0, "rep_dim table still exists."

        # Check if prod_dim table still exists
        cs.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'stg_germany' AND table_name = 'prod_dim';")
        assert cs.fetchone()[0] == 0, "prod_dim table still exists."

    finally:
        cs.close()
        print("Cursor closed.")
        print("Passed: Tables validation")

    conn.close()
    print("Connection closed.")
    print("")
    return True


# Scenario: Create and verify tables structure
@given('that a Matillion Script to create tables in Snowflake is executed')
def step_create_tables(context):
    execute_matillion_script('createtables_test')

@when('I check the tables created in Snowflake')
def step_check_structure(context):
    context.check_structure_result = check_structure()

@then('the tables are correctly created')
def step_validate_structure(context):
    if not context.check_structure_result:
        assert False, "The structure created is not correct"


# Scenario: Verify that the data inserted in the tables is correct
@given('that a Matillion Script to insert data into the tables in Snowflake is executed')
def step_insert_data(context):
    execute_matillion_script('insertdata_test')

@when('I check the data of the tables in Snowflake')
def step_validate_data(context):
    context.check_validate_data = validate_data()

@then('the data inserted is correct')
def step_validate_structure(context):
    if not context.check_validate_data:
        assert False, "The data inserted is not correct"


# Scenario: Delete all tables created
@given('that all the tables created in Snowflake for the test are dropped')
def step_drop_tables(context):
    delete_tables()

@when('I check whether the tables exist in Snowflake')
def step_check_drop(context):
    context.check_validate_drop = validate_drop_tables()

@then('the data is deleted correctly')
def step_validate_drop(context):
    if not context.check_validate_drop:
        assert False, "The data inserted is not correct"