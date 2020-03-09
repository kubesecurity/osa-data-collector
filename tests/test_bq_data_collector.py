import unittest
from unittest.mock import patch, MagicMock

import arrow
import pandas as pd

import src.utils.cloud_constants as cc
import tests.test_helper as test_helper
from src.bq_data_collector import BigQueryDataCollector


class BigDataCollectorTestCase(unittest.TestCase):

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

        # call actual method
        df = bq_data_collector._get_gh_event_as_data_frame(sample_query_param)

        # assert actual ouput
        self.assertEqual(3, len(df))
        # As for the issue "https://github.com/golang/go/issues/33041" we are getting two row form github,
        # taking one with latest updated time
        self.assertEqual(1, len(df[df.url.eq('https://github.com/golang/go/issues/33041')]))

    def test_get_repo_url(self):
        openshift_repo_url = BigQueryDataCollector._get_repo_url('openshift')
        self.assertEqual(openshift_repo_url, 'src/utils/data_assets/golang-repo-list.txt')

        openshift_repo_url = BigQueryDataCollector._get_repo_url('knative')
        self.assertEqual(openshift_repo_url, 'src/utils/data_assets/knative-repo-list.txt')

        openshift_repo_url = BigQueryDataCollector._get_repo_url('kubevirt')
        self.assertEqual(openshift_repo_url, 'src/utils/data_assets/kubevirt-repo-list.txt')

    @patch('src.bq_data_collector.BigQueryDataCollector._get_repo_url',
           return_value=test_helper.get_file_absolute_path("/tests/utils/data_assets/sample-repo-list.txt"))
    def test_get_repo_list(self, _mock_repo_url):
        repo_list = BigQueryDataCollector._get_repo_list(['openshift'])

        # as 2 repo url are invalid inside sample-repo-list.txt file, we will get 2 valid url out of 4 total url
        self.assertEqual(2, len(repo_list))

    @patch('src.bq_data_collector.BigQueryDataCollector._get_repo_url',
           return_value=test_helper.get_file_absolute_path("/tests/utils/data_assets/sample-repo-list.txt"))
    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    def test_init_query_param(self, _mock_repo_url, _mock_bq_client):
        # BigQueryDataCollector._init_query_param(eco_systems=["openshift"],days=2)
        no_of_days = 2
        bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                  repos=["openshift"], days=no_of_days)

        present_time = arrow.now()
        start_time = present_time.shift(days=-no_of_days)
        end_time = present_time.shift(days=-1)
        last_n_days = [dt.format('YYYYMMDD') for dt in arrow.Arrow.range('day', start_time, end_time)]
        day_list = '(' + ', '.join(["'" + d + "'" for d in [item[2:] for item in last_n_days]]) + ')'

        # test init logic by comparing _last_n_days and _query_params
        self.assertEqual(2, len(bq_data_collector._last_n_days))
        self.assertEqual(bq_data_collector._query_params['{repo_names}'], "('urfave/cli', 'mreiferson/go-httpclient')")
        self.assertEqual(bq_data_collector._query_params['{year_prefix_wildcard}'], '20*')
        self.assertEqual(bq_data_collector._query_params['{year_suffix_month_day}'], day_list)
