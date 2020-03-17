import unittest
from unittest.mock import patch

import src.utils.bq_client_helper as bq_client_helper
import tests.src.test_helper as test_helper


@patch("src.utils.cloud_constants.REPO_LIST", 'tests/src/utils/data_assets/repo-list.json')
class BigQueryClientHelperTestCase(unittest.TestCase):

    @patch('src.utils.bq_client_helper.create_github_bq_client')
    def test_create_github_bq_client(self, _mock_create_github_bq_client):
        heper = bq_client_helper.create_github_bq_client()
        self.assertIsNotNone(heper)

    def test_get_repos_names(self):
        repo_names = test_helper.get_sample_repo_names()
        # As we have given 2 repo url as invalid out of 5 repo url, it should return 3 valid repo names
        self.assertEqual(len(repo_names), 3)

    def test_bq_add_query_params(self):
        # Get raw qyery and expected query text from the file.
        raw_event_query = test_helper.read_file_data('tests/src/utils/data_assets/raw-event-query.txt')
        expected_event_query = test_helper.read_file_data('tests/src/utils/data_assets/formatted-event-query.txt')

        # get sample query params
        query_params = test_helper.get_sample_query_param()

        # call actual method and assert
        event_query = bq_client_helper.bq_add_query_params(raw_event_query, query_params)
        self.assertEqual(event_query, expected_event_query)
