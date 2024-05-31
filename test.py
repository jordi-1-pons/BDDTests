import requests
import json

# Configuration 
base_url = 'https://matillion.dev.eu.dm.aws.novartis.net' 
group_name = 'GERMANY' 
project_name = 'GERMANY' 
version_name = 'default' 
environment_name = 'DEV_CONN' 
job_name = 'insertdata_test' 
api_user = 'bddtester'  # Replace with your API user 
api_password = '4iUUb1iqsEnl3MUO2jpILJtk1qmDZPAmp0jgN4Wx'  # Replace with your API password

# Construct the URL 
url = f'{base_url}/rest/v1/group/name/{group_name}/project/name/{project_name}/version/name/{version_name}/job/name/{job_name}/run?environmentName={environment_name}' 

# Debug output 
print(f"URL: {url}") 
print(f"API User: {api_user}") 

# Make the API request 
response = requests.post(url, auth=(api_user, api_password), verify=False) 

# Check the response 
if response.status_code == 200: 
    print("Job triggered successfully") 
    print(json.dumps(response.json(), indent=4)) 
else: 
    print("Failed to trigger job:") 
    print(f"Status Code: {response.status_code}") 
    print(f"Response: {response.text}")