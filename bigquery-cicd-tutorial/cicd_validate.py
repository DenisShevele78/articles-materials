import os
import gitlab
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN = 'YOUR_GITLAB_TOKEN'
# Standard GitLab environment variables
# See: https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
SERVER = os.environ['CI_SERVER_URL']
COMMIT_SHA = os.environ['CI_COMMIT_SHA']
PROJECT_ID = os.environ['CI_PROJECT_ID']
BRANCH = os.environ['CI_COMMIT_BRANCH']
# Our custom environment variable
BQ_CRED = os.environ['BQ_KEY_test'] 

# Some characters can't be stored in a masked variable,
# including the \ character in Google service account credentials.
# We previously replaced \n with @ — now we reverse that.
private_key = BQ_CRED.replace('@', '\n')
private_key = '-----BEGIN PRIVATE KEY-----\n' + private_key + '\n-----END PRIVATE KEY-----\n'

service_account_info = { # Credentials from the BQ service account JSON key file
  "type": "service_account",
  "project_id": "YOUR_BQ_PROJECT_ID",
  "private_key_id": "****",
  "private_key": private_key, # The key variable from GitLab
  "client_email": "****",
  "client_id": "****",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "****",
  "universe_domain": "googleapis.com"
}


# Get the current commit and list of changed files
gl = gitlab.Gitlab(SERVER, private_token=TOKEN)
project = gl.projects.get(PROJECT_ID)
commit = project.commits.get(COMMIT_SHA)
changed_files = commit.diff()


# Loop through all changed files
for changed_file in changed_files:
    if changed_file['deleted_file'] is True:
        continue # Skip deleted files

    file_path = changed_file['new_path']
    print('===')
    print(file_path)
    dest = file_path.split('/') # File path in the repository
    print('dest:', dest)

    if dest[0] != 'projects': # Only process files in BQ projects folder
        print("File is not in 'projects' directory. Skipped.")
        continue
    if file_path[-4:] != '.sql':
        print('Non-SQL file. Skipped.')
        continue # Skip non-SQL files

    commit_project = dest[1]
    commit_dataset = dest[2]
    commit_table = dest[4][:-4].lower()

    # Read file content
    with open(file_path, encoding="utf-8") as f:
        file_content = f.read()
        #print(file_content)
        
    # Only process views
    if file_content[:22] != "CREATE OR REPLACE VIEW": # Must start with this command
        print("Not a view or 'CREATE OR REPLACE VIEW' missed. Skipped.")
        continue
    
    # Connect to BigQuery
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(commit_project, credentials)
    # Run a dry-run query to validate before executing
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, maximum_bytes_billed=0)
    query_result = client.query(file_content, job_config=job_config)

    # Validation checks
    print('Query result: ', query_result.state)    
    if query_result.state != 'DONE':
        print('--> Query failed. Skipped')
        continue
    if query_result.destination.project != commit_project:
        print('project in file path: ', commit_project)
        print('project in query: ', query_result.destination.project)
        print('--> Wrong project. Skipped')
        continue
    if query_result.destination.dataset_id != commit_dataset:
        print('dataset in file path: ', commit_dataset)
        print('dataset in query: ', query_result.destination.dataset_id)
        print('--> Wrong dataset. Skipped')
        continue
    if query_result.destination.table_id.lower() != commit_table.lower():
        print('table in file path: ', commit_table)
        print('table in query: ', query_result.destination.table_id)
        print('--> Wrong table. Skipped')
        continue


    # Execute the actual query to create/update the view
    work_query = client.query(file_content)
    print(work_query.state)
    print('---')


print('Done.')
