import os
import unittest
from unittest.mock import patch

import src.utils.bq_client_helper as bq_client_helper

import tests.test_helper as test_helper


class BigQueryClientHelperTestCase(unittest.TestCase):

    @patch('src.utils.bq_client_helper.create_github_bq_client')
    def test_create_github_bq_client(self, _mock_create_github_bq_client):
        heper = bq_client_helper.create_github_bq_client()
        self.assertIsNotNone(heper)

    def test_get_gokube_trackable_repos(self):
        repo_names = test_helper.get_sample_repo_names()
        # As we have given 2 repo url as invalid out of 4 repo url, it should return 2 valid repo names
        self.assertEqual(len(repo_names), 2)

    def test_bq_add_query_params(self):
        # Get raw qyery and expected query text from the file.
        working_dir = os.path.abspath(os.getcwd())
        file_path = "{dir}/tests/utils/data_assets/raw-event-query.txt".format(dir=working_dir)
        raw_event_query = test_helper.read_file_data(file_path)
        file_path = "{dir}/tests/utils/data_assets/formatted-event-query.txt".format(dir=working_dir)
        expected_event_query = test_helper.read_file_data(file_path)

        # get sample query params
        query_params = test_helper.get_sample_query_param()

        # call actual method and assert
        event_query = bq_client_helper.bq_add_query_params(raw_event_query, query_params)
        self.assertEqual(event_query, expected_event_query)
