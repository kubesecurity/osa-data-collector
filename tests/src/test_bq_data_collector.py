import unittest
from unittest.mock import patch, MagicMock

import arrow
import pandas as pd

import src.utils.cloud_constants as cc
import tests.src.test_helper as test_helper
from src.bq_data_collector import BigQueryDataCollector


class BigDataCollectorTestCase(unittest.TestCase):

    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    def setUp(self, mock_bq_client):
        self._mock_bq_client = mock_bq_client
        self._repo_url = 'tests/src/utils/data_assets/repo-list.json'
        self._no_of_days = 2
        self._bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                        repo_list_url=self._repo_url, ecosystems=["openshift"],
                                                        days=self._no_of_days)

    def tearDown(self):
        self._mock_bq_client = None
        self._bq_data_collector = None

    @patch('src.bq_data_collector.BigQueryDataCollector.get_issues_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/sample_gh_issue_data.csv'))
    @patch('src.bq_data_collector.BigQueryDataCollector.get_prs_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/sample_gh_pr_data.csv'))
    def test_get_github_data(self, _mock_issue, _mock_prs):
        bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                  repo_list_url=self._repo_url,
                                                  ecosystems=["openshift", "knative", "kubevirt"], days=2)

        df = bq_data_collector.get_github_data()

        # assert actual ouput (issue 3, Prs 3)
        self.assertEqual(6, len(df))

        # assert ecosystem updation based on repo_names
        self.assertEqual(4, len(df[df.ecosystem.str.contains("openshift")]))
        self.assertEqual(3, len(df[df.ecosystem.str.contains("knative")]))
        self.assertEqual(1, len(df[df.ecosystem.str.contains("kubevirt")]))

    @patch('src.bq_data_collector.BigQueryDataCollector.get_issues_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/empty_gh_issue_data.csv'))
    @patch('src.bq_data_collector.BigQueryDataCollector.get_prs_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/empty_gh_issue_data.csv'))
    def test_get_github_data_empty_response(self, _mock_issue, _mock_prs):

        df = self._bq_data_collector.get_github_data()

        # assert actual ouput
        self.assertEqual(0, len(df))

    def test_get_gh_event_as_data_frame(self):
        # mock few data
        self._mock_bq_client().estimate_query_size.return_value = "15.5"
        self._mock_bq_client().query_to_pandas.return_value = pd.read_csv(
            'tests/src/utils/data_assets/sample_gh_issue_data_with_duplicate.csv')
        sample_query_param = test_helper.get_sample_query_param()

        # call actual method
        df = self._bq_data_collector._get_gh_event_as_data_frame(sample_query_param)

        # assert actual ouput
        self.assertEqual(3, len(df))

        # As for the issue "https://github.com/golang/go/issues/33041" we are getting two row form github,
        # taking one with latest updated time
        self.assertEqual(1, len(df[df.url.eq('https://github.com/golang/go/issues/33041')]))

    def test_get_repo_by_list(self):
        repo_list = self._bq_data_collector._get_repo_by_eco_system('openshift')
        # as 2 repo url are invalid inside for openshift repos, we will get 2 valid url out of 5 total url
        self.assertEqual(3, len(repo_list))

        knative_repo_list = self._bq_data_collector._get_repo_by_eco_system('knative')
        self.assertEqual(3, len(knative_repo_list))
        kubevirt_repo_list = self._bq_data_collector._get_repo_by_eco_system('kubevirt')
        self.assertEqual(2, len(kubevirt_repo_list))

    def test_get_repo_by_eco_system_invalid_name(self):
        repo = self._bq_data_collector._get_repo_by_eco_system('invalid-repo')
        self.assertEqual(0, len(repo))

    def test_init_query_param(self):
        present_time = arrow.now()
        start_time = present_time.shift(days=-self._no_of_days)
        end_time = present_time.shift(days=-1)
        last_n_days = [dt.format('YYYYMMDD') for dt in arrow.Arrow.range('day', start_time, end_time)]
        day_list = '({days})'.format(days=', '.join(["'" + d + "'" for d in [item[2:] for item in last_n_days]]))

        # test init logic by comparing _last_n_days and _query_params
        self.assertEqual(2, len(self._bq_data_collector._last_n_days))
        self.assertEqual(self._bq_data_collector._query_params['{year_prefix_wildcard}'], '20*')
        self.assertEqual(self._bq_data_collector._query_params['{year_suffix_month_day}'], day_list)
        # as repo_list is set type, order may come differently, so need to test individual item
        self.assertTrue('apache/thrift' in self._bq_data_collector._query_params['{repo_names}'])
        self.assertTrue('square/go-jose' in self._bq_data_collector._query_params['{repo_names}'])
        self.assertTrue('golang/go' in self._bq_data_collector._query_params['{repo_names}'])
