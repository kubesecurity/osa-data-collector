import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=Warning)
from utils import bq_client_helper as bq_helper

import pandas as pd
import arrow
import daiquiri
import itertools
import logging
import os
import argparse
import textwrap

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)

def _get_repo_list(repos):
    repo_names = list()
    for r in repos:
        repo_names.append(bq_helper.get_gokube_trackable_repos(r))
    return list(itertools.chain(*repo_names))

def _get_bq_client(bq_credentials_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', bq_credentials_path or '')
    return bq_helper.create_github_bq_client()

def _get_query_date_range(no_of_days):
    # Don't change this
    PRESENT_TIME = arrow.now()

    # CHANGE NEEDED
    # to get data for N days back starting from YESTERDAY
    # e.g if today is 20190528 and DURATION DAYS = 2 -> BQ will get data for 20190527, 20190526
    # We don't get data for PRESENT DAY since github data will be incomplete on the same day
    # But you can get it if you want but better not to for completeness :)

    # You can set this directly from command line using the -d or --days-since-yday argument
    DURATION_DAYS = no_of_days or 3 # Gets 3 days of previous data including YESTERDAY


    # Don't change this
    # Start time for getting data
    START_TIME = PRESENT_TIME.shift(days=-DURATION_DAYS)


    # Don't change this
    # End time for getting data (present_time - 1) i.e yesterday
    # you can remove -1 to get present day data
    # but it is not advised as data will be incomplete
    END_TIME = PRESENT_TIME.shift(days=-1)

    LAST_N_DAYS = [dt.format('YYYYMMDD')
            for dt in arrow.Arrow.range('day', START_TIME, END_TIME)]
    return LAST_N_DAYS

def _get_gh_event_as_data_frame(bq_client, query_param):
    EVENT_QUERY = """
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
                     r'\\r\\n|\\r|\\n',
                     ' '),
                 r'\s{2,}',
                 ' ')) as title,
        TRIM(REGEXP_REPLACE(
                 REGEXP_REPLACE(
                     JSON_EXTRACT_SCALAR(payload, '$.{payload_field_name}.body'),
                     r'\\r\\n|\\r|\\n',
                     ' '),
                 r'\s{2,}',
                 ' ')) as body

    FROM `githubarchive.day.{year_prefix_wildcard}`
        WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
        AND repo.name in {repo_names}
        AND type = '{event_type}'
        """

    EVENT_QUERY = bq_helper.bq_add_query_params(EVENT_QUERY, query_param)
    qsize = bq_client.estimate_query_size(EVENT_QUERY)
    _logger.info('Retrieving GH Events. Query cost in GB={qc}'.format(qc=qsize))

    df = bq_client.query_to_pandas(EVENT_QUERY)
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

def _get_gh_event_estimate(bq_client, query_param):
    query = """
    SELECT  type as EventType, count(*) as Freq
            FROM `githubarchive.day.{year_prefix_wildcard}`
            WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
            AND repo.name in {repo_names}
            AND type in ('PullRequestEvent', 'IssuesEvent')
            GROUP BY type
    """
    query = bq_helper.bq_add_query_params(query, query_param)
    return bq_client.query_to_pandas(query)

def _get_query_param(repos, days):
    LAST_N_DAYS = _get_query_date_range(days)
    REPO_NAMES = _get_repo_list(repos)

    # Don't change this
    YEAR_PREFIX = '20*'
    DAY_LIST = [item[2:] for item in LAST_N_DAYS]
    QUERY_PARAMS = {
        '{year_prefix_wildcard}': YEAR_PREFIX,
        '{year_suffix_month_day}': '(' + ', '.join(["'" + d + "'" for d in DAY_LIST]) + ')',
        '{repo_names}': '(' + ', '.join(["'" + r + "'" for r in REPO_NAMES]) + ')',
    }
    return QUERY_PARAMS, LAST_N_DAYS

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
            help='The number of days worth of data to retrieve from GitHub including yesterday')

    args = parser.parse_args()

    QUERY_PARAMS, LAST_N_DAYS = _get_query_param(repos = args.repos, days=args.days_since_yday)
    _logger.info('Data will be retrieved for Last N={n} days: {days}'.format(n=len(LAST_N_DAYS),
        days=LAST_N_DAYS))

    # ======= BQ CLIENT SETUP FOR GETTING GITHUB BQ DATA ========
    _logger.info('----- BQ CLIENT SETUP FOR GETTING GITHUB BQ DATA -----')
    GH_BQ_CLIENT = _get_bq_client(args.bq_credentials_path)

    # ======= BQ GET DATASET SIZE ESTIMATE ========
    _logger.info('----- BQ Dataset Size Estimate -----')
    _logger.info('Dataset Size for Last N={n} days:-'.format(n=len(LAST_N_DAYS)))
    _logger.info('\n{data}'.format(data=_get_gh_event_estimate(GH_BQ_CLIENT, QUERY_PARAMS)))


    # ======= BQ GITHUB DATASET RETRIEVAL & PROCESSING ========
    # FIXME: Combine 2 queries to reduce the cost
    _logger.info('----- BQ GITHUB DATASET RETRIEVAL & PROCESSING -----')
    issues_df = _get_gh_event_as_data_frame(GH_BQ_CLIENT,
         {**QUERY_PARAMS, **{'{payload_field_name}':'issue', '{event_type}': 'IssuesEvent'}}
    )
    prs_df = _get_gh_event_as_data_frame(GH_BQ_CLIENT,
         {**QUERY_PARAMS, **{'{payload_field_name}':'pull_request', '{event_type}': 'PullRequestEvent'}}
    )

    _logger.info('Merging issues and pull requests datasets')
    cols = issues_df.columns
    df = pd.concat([issues_df, prs_df], axis=0, sort=False,
            ignore_index=True).reset_index(drop=True)
    df = df[cols]

    df.to_csv('test_data_models.csv', index=False)

if __name__ == '__main__':
    main()
