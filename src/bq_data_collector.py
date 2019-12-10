import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=Warning)
from utils import cloud_constants as cc
from utils import bq_client_helper as bq_helper

import pandas as pd
import arrow
import daiquiri
import logging
import os
import argparse
import textwrap

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

parser.add_argument("-c", "--bq-credentials-path", type=str, default=None,
                    help="Absolute or relative path to BigQuery Credentials file")
parser.add_argument("-d", "--days-since-yday", type=int, default=7,
                    help="The number of days worth of data to retrieve from GitHub including yesterday")

args = parser.parse_args()

DAYS_SINCE_YDAY = args.days_since_yday

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


# ======= BQ CLIENT SETUP FOR GETTING GITHUB BQ DATA ========
_logger.info('----- BQ CLIENT SETUP FOR GETTING GITHUB BQ DATA -----')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', args.bq_credentials_path or '')
GH_BQ_CLIENT = bq_helper.create_github_bq_client()
ECO_SYSTEM = 'openshift'
if ECO_SYSTEM == 'openshift':
    REPO_NAMES = bq_helper.get_gokube_trackable_repos(repo_dir=cc.GOKUBE_REPO_LIST)
elif ECO_SYSTEM == 'knative':
    REPO_NAMES = bq_helper.get_gokube_trackable_repos(repo_dir=cc.KNATIVE_REPO_LIST)
elif ECO_SYSTEM == 'kubevirt':
    REPO_NAMES = bq_helper.get_gokube_trackable_repos(repo_dir=cc.KUBEVIRT_REPO_LIST)

_logger.info('\n')


# ======= DATES SETUP FOR GETTING GITHUB BQ DATA ========
_logger.info('----- DATES SETUP FOR GETTING GITHUB BQ DATA -----')

# Don't change this
PRESENT_TIME = arrow.now()


# CHANGE NEEDED
# to get data for N days back starting from YESTERDAY
# e.g if today is 20190528 and DURATION DAYS = 2 -> BQ will get data for 20190527, 20190526
# We don't get data for PRESENT DAY since github data will be incomplete on the same day
# But you can get it if you want but better not to for completeness :)

# You can set this directly from command line using the -d or --days-since-yday argument
DURATION_DAYS = DAYS_SINCE_YDAY or 3 # Gets 3 days of previous data including YESTERDAY


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
_logger.info('Data will be retrieved for Last N={n} days: {days}'.format(n=DURATION_DAYS,
                                                                         days=LAST_N_DAYS))
_logger.info('\n')


# ======= BQ QUERY PARAMS SETUP FOR GETTING GITHUB BQ DATA ========
_logger.info('----- BQ QUERY PARAMS SETUP FOR GETTING GITHUB BQ DATA -----')

# Don't change this
YEAR_PREFIX = '20*'
DAY_LIST = [item[2:] for item in LAST_N_DAYS]
QUERY_PARAMS = {
    '{year_prefix_wildcard}': YEAR_PREFIX,
    '{year_suffix_month_day}': '(' + ', '.join(["'" + d + "'" for d in DAY_LIST]) + ')',
    '{repo_names}': '(' + ', '.join(["'" + r + "'" for r in REPO_NAMES]) + ')'
}

_logger.info('\n')


# ======= BQ GET DATASET SIZE ESTIMATE ========
_logger.info('----- BQ Dataset Size Estimate -----')

query = """
SELECT  type as EventType, count(*) as Freq
        FROM `githubarchive.day.{year_prefix_wildcard}`
        WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
        AND repo.name in {repo_names}
        AND type in ('PullRequestEvent', 'IssuesEvent')
        GROUP BY type
"""
query = bq_helper.bq_add_query_params(query, QUERY_PARAMS)
df = GH_BQ_CLIENT.query_to_pandas(query)
_logger.info('Dataset Size for Last N={n} days:-'.format(n=DURATION_DAYS))
_logger.info('\n{data}'.format(data=df))

_logger.info('\n')


# ======= BQ GITHUB DATASET RETRIEVAL & PROCESSING ========
_logger.info('----- BQ GITHUB DATASET RETRIEVAL & PROCESSING -----')

ISSUE_QUERY = """
SELECT
    repo.name as repo_name,
    type as event_type,
    'golang' as ecosystem,
    JSON_EXTRACT_SCALAR(payload, '$.action') as status,
    JSON_EXTRACT_SCALAR(payload, '$.issue.id') as id,
    JSON_EXTRACT_SCALAR(payload, '$.issue.number') as number,
    JSON_EXTRACT_SCALAR(payload, '$.issue.url') as api_url,
    JSON_EXTRACT_SCALAR(payload, '$.issue.html_url') as url,
    JSON_EXTRACT_SCALAR(payload, '$.issue.user.login') as creator_name,
    JSON_EXTRACT_SCALAR(payload, '$.issue.user.html_url') as creator_url,
    JSON_EXTRACT_SCALAR(payload, '$.issue.created_at') as created_at,
    JSON_EXTRACT_SCALAR(payload, '$.issue.updated_at') as updated_at,
    JSON_EXTRACT_SCALAR(payload, '$.issue.closed_at') as closed_at,
    TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
                 JSON_EXTRACT_SCALAR(payload, '$.issue.title'),
                 r'\\r\\n|\\r|\\n',
                 ' '),
             r'\s{2,}',
             ' ')) as title,
    TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
                 JSON_EXTRACT_SCALAR(payload, '$.issue.body'),
                 r'\\r\\n|\\r|\\n',
                 ' '),
             r'\s{2,}',
             ' ')) as body

FROM `githubarchive.day.{year_prefix_wildcard}`
    WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
    AND repo.name in {repo_names}
    AND type = 'IssuesEvent'
    """

ISSUE_QUERY = bq_helper.bq_add_query_params(ISSUE_QUERY, QUERY_PARAMS)
qsize = GH_BQ_CLIENT.estimate_query_size(ISSUE_QUERY)
_logger.info('Retrieving GH Issues. Query cost in GB={qc}'.format(qc=qsize))

issues_df = GH_BQ_CLIENT.query_to_pandas(ISSUE_QUERY)
if issues_df.empty:
    _logger.warn('No issues present for given time duration.')
else:
    _logger.info('Total issues retrieved: {n}'.format(n=len(issues_df)))

    issues_df.created_at = pd.to_datetime(issues_df.created_at)
    issues_df.updated_at = pd.to_datetime(issues_df.updated_at)
    issues_df.closed_at = pd.to_datetime(issues_df.closed_at)
    issues_df = issues_df.loc[issues_df.groupby(
        'url').updated_at.idxmax(skipna=False)].reset_index(drop=True)
    _logger.info(
        'Total issues after deduplication: {n}'.format(n=len(issues_df)))


PR_QUERY = """
SELECT
    repo.name as repo_name,
    type as event_type,
    'golang' as ecosystem,
    JSON_EXTRACT_SCALAR(payload, '$.action') as status,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.id') as id,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.number') as number,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.url') as api_url,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.html_url') as url,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.user.login') as creator_name,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.user.html_url') as creator_url,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.created_at') as created_at,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.updated_at') as updated_at,
    JSON_EXTRACT_SCALAR(payload, '$.pull_request.closed_at') as closed_at,
    TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
                 JSON_EXTRACT_SCALAR(payload, '$.pull_request.title'),
                 r'\\r\\n|\\r|\\n',
                 ' '),
             r'\s{2,}',
             ' ')) as title,
    TRIM(REGEXP_REPLACE(
             REGEXP_REPLACE(
                 JSON_EXTRACT_SCALAR(payload, '$.pull_request.body'),
                 r'\\r\\n|\\r|\\n',
                 ' '),
             r'\s{2,}',
             ' ')) as body

FROM `githubarchive.day.{year_prefix_wildcard}`
    WHERE _TABLE_SUFFIX IN {year_suffix_month_day}
    AND repo.name in {repo_names}
    AND type = 'PullRequestEvent'
"""

PR_QUERY = bq_helper.bq_add_query_params(PR_QUERY, QUERY_PARAMS)
qsize = GH_BQ_CLIENT.estimate_query_size(PR_QUERY)
_logger.info(
    'Retrieving GH Pull Requests. Query cost in GB={qc}'.format(qc=qsize))

prs_df = GH_BQ_CLIENT.query_to_pandas(PR_QUERY)
if prs_df.empty:
    _logger.warn('No pull requests present for given time duration.')
else:
    _logger.info('Total pull requests retrieved: {n}'.format(n=len(prs_df)))

    prs_df.created_at = pd.to_datetime(prs_df.created_at)
    prs_df.updated_at = pd.to_datetime(prs_df.updated_at)
    prs_df.closed_at = pd.to_datetime(prs_df.closed_at)
    prs_df = prs_df.loc[prs_df.groupby('url').updated_at.idxmax(
        skipna=False)].reset_index(drop=True)
    _logger.info(
        'Total pull requests after deduplication: {n}'.format(n=len(prs_df)))

_logger.info('\n')

_logger.info('Merging issues and pull requests datasets')
cols = issues_df.columns
df = pd.concat([issues_df, prs_df], axis=0, sort=False,
               ignore_index=True).reset_index(drop=True)
df = df[cols]

df.to_csv('test_data_models.csv', index=False)
