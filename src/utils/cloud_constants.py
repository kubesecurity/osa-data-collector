"""
This file contains the constants for interaction with AWS/Github Repo.
Note: Please don't add keys directly here, refer to environment variables
"""
import os

# Please AWS Bucket that you want to use to store data-collector program output.
AWS_S3_BUCKET_NAME = os.environ.get('AWS_S3_BUCKET_NAME', 'rzalavad-data-collector')

# Please set the following to point to your BQ auth credentials JSON
BIGQUERY_CREDENTIALS_FILEPATH = os.environ.get('BIGQUERY_CREDENTIALS_FILEPATH', '../../auth/bq_key.json')

# File contains repo list for each ecosystem
REPO_LIST = os.environ.get('REPO_LIST', 'src/utils/data_assets/repo-list.json')
