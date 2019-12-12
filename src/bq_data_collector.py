from typing import List, Dict
import argparse
import itertools
import logging
import os
import textwrap
import warnings

import arrow
import daiquiri
import pandas as pd

from utils import bq_client_helper as bq_helper

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=Warning)

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)

class BigQueryDataCollector:
    def __init__(self, repos: str, bq_credentials_path: str = '', days: int = 3):
        self._bq_client = BigQueryDataCollector._get_bq_client(bq_credentials_path)
        self._init_query_param(repos, days)

    @classmethod
    def _get_repo_list(cls, repos: str) -> List[str]:
        repo_names = list()
        for repo in repos:
            repo_names.append(bq_helper.get_gokube_trackable_repos(repo))
        return list(itertools.chain(*repo_names))

    @classmethod
    def _get_bq_client(cls, bq_credentials_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
            os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', bq_credentials_path or '')
        return bq_helper.create_github_bq_client()

    @classmethod
    def _get_query_date_range(cls, no_of_days) -> List[str]:
        # Don't change this
        present_time = arrow.now()

        # CHANGE NEEDED
        # to get data for N days back starting from YESTERDAY
        # e.g if today is 20190528 and DURATION DAYS = 2 -> BQ will get data for 20190527, 20190526
        # We don't get data for PRESENT DAY since github data will be incomplete on the same day
        # But you can get it if you want but better not to for completeness :)

        # You can set this directly from command line using the -d or --days-since-yday argument
        duration_days = no_of_days or 3 # Gets 3 days of previous data including YESTERDAY


        # Don't change this
        # Start time for getting data
        start_time = present_time.shift(days=-duration_days)


        # Don't change this
        # End time for getting data (present_time - 1) i.e yesterday
        # you can remove -1 to get present day data
        # but it is not advised as data will be incomplete
        end_time = present_time.shift(days=-1)

        last_n_days = [dt.format('YYYYMMDD')
                       for dt in arrow.Arrow.range('day', start_time, end_time)]
        return last_n_days

    def _get_gh_event_as_data_frame(self, query_param: Dict) -> pd.DataFrame:
        event_query = r"""
        SELECT
            repo.name as repo_name,
            type as event_type,
            'golang' as ecosystem,
            JSON_EXTRACT_SCALAR(payload, '$.action') as status,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.id') as id,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.number') as number,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.url') as api_url,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.html_url') as url,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.user.login') as creator_name,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.user.html_url') as creator_url,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.created_at') as created_at,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.updated_at') as updated_at,
            JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.closed_at') as closed_at,
            TRIM(REGEXP_REPLACE(
                     REGEXP_REPLACE(
                         JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.title'),
                         r'\r\n|\r|\n',
                         ' '),
                     r'\s{2,}',
                     ' ')) as title,
            TRIM(REGEXP_REPLACE(
                     REGEXP_REPLACE(
                         JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.body'),
                         r'\r\n|\r|\n',
                         ' '),
                     r'\s{2,}',
                     ' ')) as body

        FROM `githubarchive.day.{year_prefix_wildcard}`
            WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
            AND repo.name in {repo_names}
            AND type = '{event_type}'
            """

        event_query = bq_helper.bq_add_query_params(event_query, query_param)
        qsize = self._bq_client.estimate_query_size(event_query)
        _logger.info('Retrieving GH Events. Query cost in GB={qc}'.format(qc=qsize))

        df = self._bq_client.query_to_pandas(event_query)
        if df.empty:
            _logger.warn('No Events present for given time duration.')
        else:
            _logger.info('Total Events retrieved: {n}'.format(n=len(df)))

            df.created_at = pd.to_datetime(df.created_at)
            df.updated_at = pd.to_datetime(df.updated_at)
            df.closed_at = pd.to_datetime(df.closed_at)
            df = df.loc[df.groupby(
                'url').updated_at.idxmax(skipna=False)].reset_index(drop=True)
            _logger.info(
                'Total Events after deduplication: {n}'.format(n=len(df)))

        return df

    def _get_gh_event_estimate(self) -> float:
        query = """
        SELECT  type as EventType, count(*) as Freq
                FROM `githubarchive.day.{year_prefix_wildcard}`
                WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
                AND repo.name in {repo_names}
                AND type in ('PullRequestEvent', 'IssuesEvent')
                GROUP BY type
        """
        query = bq_helper.bq_add_query_params(query, self._query_params)
        return self._bq_client.query_to_pandas(query)

    def _init_query_param(self, repos: str, days: int) -> None:
        last_n_days = BigQueryDataCollector._get_query_date_range(days)
        repo_names = BigQueryDataCollector._get_repo_list(repos)

        # Don't change this
        year_prefix = '20*'
        day_list = [item[2:] for item in last_n_days]
        query_params = {
            '{year_prefix_wildcard}': year_prefix,
            '{year_suffix_month_day}': '(' + ', '.join(["'" + d + "'" for d in day_list]) + ')',
            '{repo_names}': '(' + ', '.join(["'" + r + "'" for r in repo_names]) + ')',
        }
        self._query_params, self._last_n_days = query_params, last_n_days

    def get_issues_as_data_frame(self) -> pd.DataFrame:
        '''
        Retrives GH issues as pandas data frame
        '''
        return self._get_gh_event_as_data_frame(
            {**self._query_params,
             **{'{payload_field_name}':'issue', '{event_type}': 'IssuesEvent'}
            }
        )

    def get_prs_as_data_frame(self) -> pd.DataFrame:
        '''
        Retrives GH PRs as pandas data frame
        '''
        return self._get_gh_event_as_data_frame(
            {**self._query_params,
             **{'{payload_field_name}':'pull_request', '{event_type}': 'PullRequestEvent'}
            }
        )

def main():
    # Initial setup no need to change anything
    parser = argparse.ArgumentParser(prog='python',
                                     description=textwrap.dedent('''\
                                        This script can be used to fetch issues/PR/comments
                                        from Github BigQuery archive.
                                        '''),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent('''\
                                        The -days flag should be used with number of prev days data you want to pull
                                        '''))

    parser.add_argument('repos', metavar='N', type=str, nargs='+',
                        help='List of repos to get events')
    parser.add_argument('-c', '--bq-credentials-path', type=str, default=None,
                        help='Absolute or relative path to BigQuery Credentials file')
    parser.add_argument('-d', '--days-since-yday', type=int, default=7,
                        help='The number of days data to retrieve from GitHub including yesterday')

    args = parser.parse_args()

    bq_data = BigQueryDataCollector(bq_credentials_path=args.bq_credentials_path,
                                    repos=args.repos, days=args.days_since_yday)
    _logger.info(
        'Data will be retrieved for Last N={n} days: {days}'.format(n=len(bq_data._last_n_days),
                                                                    days=bq_data._last_n_days))

    # ======= BQ GET DATASET SIZE ESTIMATE ========
    _logger.info('----- BQ Dataset Size Estimate -----')
    _logger.info('Dataset Size for Last N={n} days:-'.format(n=len(bq_data._last_n_days)))
    _logger.info('\n{data}'.format(data=bq_data._get_gh_event_estimate()))


    # ======= BQ GITHUB DATASET RETRIEVAL & PROCESSING ========
    # (fixme) Combine 2 queries to reduce the cost
    _logger.info('----- BQ GITHUB DATASET RETRIEVAL & PROCESSING -----')
    issues_df = bq_data.get_issues_as_data_frame()
    prs_df = bq_data.get_prs_as_data_frame()

    _logger.info('Merging issues and pull requests datasets')
    cols = issues_df.columns
    data_frame = pd.concat([issues_df, prs_df], axis=0, sort=False,
                           ignore_index=True).reset_index(drop=True)
    data_frame = data_frame[cols]

    data_frame.to_csv('test_data_models.csv', index=False)

if __name__ == '__main__':
    main()
