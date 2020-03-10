import unittest
from unittest.mock import patch, MagicMock

import arrow
import pandas as pd

import src.utils.cloud_constants as cc
import tests.src.test_helper as test_helper
from src.bq_data_collector import BigQueryDataCollector


@patch("src.utils.cloud_constants.GOKUBE_REPO_LIST", 'tests/src/utils/data_assets/golang-repo-list.txt')
@patch("src.utils.cloud_constants.KNATIVE_REPO_LIST", 'tests/src/utils/data_assets/knative-repo-list.txt')
@patch("src.utils.cloud_constants.KUBEVIRT_REPO_LIST", 'tests/src/utils/data_assets/kubevirt-repo-list.txt')
class BigDataCollectorTestCase(unittest.TestCase):

    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    @patch('src.bq_data_collector.BigQueryDataCollector.get_issues_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/sample_gh_issue_data.csv'))
    @patch('src.bq_data_collector.BigQueryDataCollector.get_prs_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/sample_gh_pr_data.csv'))
    def test_get_github_data(self, _mock_bq_client, _mock_issue, _mock_prs):
        bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                  repos=["openshift"], days=2)

        df = bq_data_collector.get_github_data()

        # assert actual ouput (issue 3, Prs 3)
        self.assertEqual(6, len(df))

        # assert ecosystem updation based on repo_names
        self.assertEqual(2, len(df[df.ecosystem.eq('golang')]))
        self.assertEqual(3, len(df[df.ecosystem.eq('knative')]))
        self.assertEqual(1, len(df[df.ecosystem.eq('kubevirt')]))

    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    @patch('src.bq_data_collector.BigQueryDataCollector.get_issues_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/empty_gh_issue_data.csv'))
    @patch('src.bq_data_collector.BigQueryDataCollector.get_prs_as_data_frame',
           return_value=pd.read_csv('tests/src/utils/data_assets/empty_gh_issue_data.csv'))
    def test_get_github_data_empty_response(self, _mock_bq_client, _mock_issue, _mock_prs):
        bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                  repos=["openshift"], days=2)

        df = bq_data_collector.get_github_data()

        # assert actual ouput
        self.assertEqual(0, len(df))

    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    def test_get_gh_event_as_data_frame(self, _mock_bq_client):
        # mock few data
        _mock_bq_client().estimate_query_size.return_value = "15.5"
        _mock_bq_client().query_to_pandas.return_value = pd.read_csv(
            'tests/src/utils/data_assets/sample_gh_issue_data_with_duplicate.csv')
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
        self.assertEqual(openshift_repo_url, 'tests/src/utils/data_assets/golang-repo-list.txt')
        openshift_repo_url = BigQueryDataCollector._get_repo_url('knative')
        self.assertEqual(openshift_repo_url, 'tests/src/utils/data_assets/knative-repo-list.txt')
        openshift_repo_url = BigQueryDataCollector._get_repo_url('kubevirt')
        self.assertEqual(openshift_repo_url, 'tests/src/utils/data_assets/kubevirt-repo-list.txt')

    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    def test_get_repo_by_list(self, _mock_bq_client):
        bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                                  repos=["openshift"], days=2)
        repo_list = bq_data_collector._get_repo_by_eco_system('openshift')
        # as 2 repo url are invalid inside golang-repo-list.txt file, we will get 2 valid url out of 4 total url
        self.assertEqual(2, len(repo_list))

        knative_repo_list = bq_data_collector._get_repo_by_eco_system('knative')
        self.assertEqual(3, len(knative_repo_list))
        kubevirt_repo_list = bq_data_collector._get_repo_by_eco_system('kubevirt')
        self.assertEqual(2, len(kubevirt_repo_list))

    @patch('src.bq_data_collector.BigQueryDataCollector._get_repo_url',
           return_value=test_helper.get_file_absolute_path("/tests/src/utils/data_assets/golang-repo-list.txt"))
    @patch('src.utils.bq_client_helper.create_github_bq_client', return_value=MagicMock())
    def test_init_query_param(self, _mock_repo_url, _mock_bq_client):
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
        self.assertEqual(bq_data_collector._query_params['{repo_names}'], "('apache/thrift', 'square/go-jose')")
        self.assertEqual(bq_data_collector._query_params['{year_prefix_wildcard}'], '20*')
        self.assertEqual(bq_data_collector._query_params['{year_suffix_month_day}'], day_list)
