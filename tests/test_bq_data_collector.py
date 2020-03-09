import os
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

import src.utils.bq_client_helper as bq_client_helper
import src.utils.cloud_constants as cc
import tests.test_helper as test_helper
from src.bq_data_collector import BigQueryDataCollector


class BigDataCollectorTestCase(unittest.TestCase):

    @staticmethod
    def _mock_repo_url(**_kwargs):
        working_dir = os.path.abspath(os.getcwd())
        file_path = "{dir}/tests/utils/data_assets/sample-repo-list.txt".format(dir=working_dir)
        return file_path

    @staticmethod
    def _get_sample_repo_names(**_kwargs):
        working_dir = os.path.abspath(os.getcwd())
        sample_file_path = "{dir}/tests/utils/data_assets/sample-repo-list.txt".format(dir=working_dir)
        repo_names = bq_client_helper.get_gokube_trackable_repos(sample_file_path)
        return repo_names

    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    def test_get_gh_event_as_data_frame(self, _mock_bq_client):
        # mock few data
        _mock_bq_client().estimate_query_size.return_value = "15.5"
        file_path = test_helper.get_file_absolute_path("/tests/utils/data_assets/sample_gh_issue_data.csv")
        gh_data = pd.read_csv(file_path)
        _mock_bq_client().query_to_pandas.return_value = gh_data

        sample_query_param = test_helper.get_sample_query_param()
        bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                  repos=["openshift"], days=2)

        # call actual method and test output
        df = bq_data_collector._get_gh_event_as_data_frame(sample_query_param)

        self.assertEqual(3, len(df))
