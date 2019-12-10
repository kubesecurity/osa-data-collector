# -*- coding: utf-8 -*-
"""
This file contains the constants for interaction with AWS/Minio.
Note: Please don't add keys directly here, refer to environment variables
"""

import os

# Please set the following to point to your BQ auth credentials JSON
BIGQUERY_CREDENTIALS_FILEPATH = os.environ.get('BIGQUERY_CREDENTIALS_FILEPATH', '../../auth/bq_key.json')

# you probably don't need to change this
GOKUBE_REPO_LIST = './utils/data_assets/golang-repo-list.txt'
KNATIVE_REPO_LIST = './utils/data_assets/knative-repo-list.txt'
KUBEVIRT_REPO_LIST = './utils/data_assets/kubevirt-repo-list.txt'
