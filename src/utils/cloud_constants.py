"""
This file contains the constants for interaction with AWS/Github Repo.
Note: Please don't add keys directly here, refer to environment variables
"""
import os

# Please AWS Bucket that you want to use to store data-collector program output.
AWS_S3_BUCKET_NAME = os.environ.get('AWS_S3_BUCKET_NAME', 'rzalavad-data-collector')

# Please set the following to point to your BQ auth credentials JSON
BIGQUERY_CREDENTIALS_FILEPATH = os.environ.get('BIGQUERY_CREDENTIALS_FILEPATH', '../../auth/bq_key.json')

GOKUBE_REPO_LIST = os.environ.get('GOKUBE_REPO_LIST', 'src/utils/data_assets/golang-repo-list.txt')
KNATIVE_REPO_LIST = os.environ.get('KNATIVE_REPO_LIST', 'src/utils/data_assets/knative-repo-list.txt')
KUBEVIRT_REPO_LIST = os.environ.get('KUBEVIRT_REPO_LIST', 'src/utils/data_assets/kubevirt-repo-list.txt')
