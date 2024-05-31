import snowflake.connector
import webbrowser

# Snowflake connection parameters
ACCOUNT = 'nvs6deveuwest1.eu-west-1'
USER = 'PONSJO2@novartis.net'
AUTHENTICATOR = 'externalbrowser'
#PASSWORD = '4iUUb1iqsEnl3MUO2jpILJtk1qmDZPAmp0jgN4Wx'
WAREHOUSE = 'COM_IMDNA_GERMANY_WH_S'
DATABASE = 'COM_GERMANY_IM_DNA_STG'
SCHEMA = 'STG_GERMANY'

# Establish a connection to Snowflake
try:
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        #password = PASSWORD,
        authenticator=AUTHENTICATOR,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    print("Connected to Snowflake successfully!")

    # Perform a simple query to test the connection
    cs = conn.cursor()
    try:
        print("Executing query...")
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print("Query result:", one_row[0])
    finally:
        cs.close()
        print("Cursor closed.")
    conn.close()
    print("Connection closed.")
except Exception as e:
    print("An error occurred:", e)
    webbrowser.open_new_tab(f'https://{ACCOUNT}.snowflakecomputing.com/console/login')
