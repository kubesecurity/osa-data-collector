import argparse
import logging
import textwrap
import warnings

import arrow
import daiquiri
import pandas as pd

import src.utils.aws_utils as aws
import src.utils.cloud_constants as cc
from src.bq_data_collector import BigQueryDataCollector

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=Warning)

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


def main():
    # Initial setup no need to change anything
    parser = argparse.ArgumentParser(prog='python', description=textwrap.dedent('''\
                                        This script can be used to fetch issues/PR/comments
                                        from Github BigQuery archive.
                                        '''), formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent('''\
                                        The -days flag should be used with number of prev days data you want to pull
                                        '''))

    parser.add_argument('-eco-systems', '--eco-systems', metavar='N', type=str, nargs='+', default="openshift",
                        choices=["openshift", "knative", "kubevirt"], help="The eco-systems to monitor")
    parser.add_argument('-days', '--days-since-yday', type=int, default=7,
                        help='The number of days data to retrieve from GitHub including yesterday')

    args = parser.parse_args()

    bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                              repos=args.eco_systems, days=args.days_since_yday)
    _logger.info('Data will be retrieved for Last N={n} days: {days}'.format(n=len(bq_data_collector.last_n_days),
                                                                             days=bq_data_collector.last_n_days))

    # ======= BQ GET DATASET SIZE ESTIMATE ========
    _logger.info('----- BQ Dataset Size Estimate -----')
    _logger.info('Dataset Size for Last N={n} days:'.format(n=len(bq_data_collector.last_n_days)))
    _logger.info('\n{data}'.format(data=bq_data_collector.get_gh_event_estimate()))

    # ======= BQ GITHUB DATASET RETRIEVAL & PROCESSING ========
    # (fixme) Combine 2 queries to reduce the cost
    _logger.info('----- BQ GITHUB DATASET RETRIEVAL & PROCESSING -----')
    issues_df = bq_data_collector.get_issues_as_data_frame()
    prs_df = bq_data_collector.get_prs_as_data_frame()

    _logger.info('Merging issues and pull requests datasets')
    cols = issues_df.columns
    data_frame = pd.concat([issues_df, prs_df], axis=0, sort=False, ignore_index=True).reset_index(drop=True)
    data_frame = data_frame[cols]

    present_time = arrow.now()
    start_time = arrow.now().shift(days=-args.days_since_yday)
    end_time = present_time.shift(days=-1)
    file_name = "gh_data_{days}.csv".format(days='-'.join([start_time.format('YYYYMMDD'), end_time.format('YYYYMMDD')]))
    data_frame.to_csv('/app/gh_data/{filename}'.format(filename=file_name), index=False)

    # ======= UPLOADING DATASETS TO S3 BUCKET ========
    _logger.info('----- UPLOADING DATASETS TO S3 BUCKET  -----')
    s3_obj = aws.S3_OBJ
    bucket_name = cc.AWS_S3_BUCKET_NAME
    s3_bucket = s3_obj.Bucket(bucket_name)

    _logger.info('Uploading Github data to S3 Bucket')
    aws.s3_upload_folder(folder_path="/app/gh_data", s3_bucket_obj=s3_bucket)
    _logger.info('Upload completed')


if __name__ == '__main__':
    main()
