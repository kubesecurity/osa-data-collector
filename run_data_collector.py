import argparse
import logging
import textwrap
import warnings

import daiquiri

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

    parser.add_argument('-e', '--ecosystems', metavar='N', type=str, nargs='+', default="openshift",
                        choices=["openshift", "knative", "kubevirt"], help="The ecosystems to monitor")
    parser.add_argument('-d', '--days-since-yday', type=int, default=7,
                        help='The number of days data to retrieve from GitHub including yesterday')

    args = parser.parse_args()

    bq_data_collector = BigQueryDataCollector(bq_credentials_path=cc.BIGQUERY_CREDENTIALS_FILEPATH,
                                              ecosystems=args.ecosystems, repo_list_url=cc.REPO_LIST,
                                              days=args.days_since_yday)
    _logger.info('Data will be retrieved for Last N={n} days: {days}'.format(n=len(bq_data_collector.last_n_days),
                                                                             days=bq_data_collector.last_n_days))

    # ======= BQ GET DATASET SIZE ESTIMATE ========
    _logger.info('----- BQ Dataset Size Estimate -----')
    _logger.info('Dataset Size for Last N={n} days:'.format(n=len(bq_data_collector.last_n_days)))
    _logger.info('\n{data}'.format(data=bq_data_collector.get_gh_event_estimate()))

    # ======= BQ GITHUB DATASET RETRIEVAL & PROCESSING ========
    _logger.info('----- BQ GITHUB DATASET RETRIEVAL & PROCESSING -----')
    data_frame = bq_data_collector.get_github_data()
    bq_data_collector.save_data_to_object_store(data_frame, args.days_since_yday)


if __name__ == '__main__':
    main()
