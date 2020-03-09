import os;

import src.utils.bq_client_helper as bq_client_helper


def get_file_absolute_path(relative_path):
    working_dir = os.path.abspath(os.getcwd())
    absolute_file_path = "{dir}{relative_path}".format(dir=working_dir, relative_path=relative_path)
    return absolute_file_path


def get_sample_repo_names():
    sample_file_path = get_file_absolute_path("/tests/utils/data_assets/sample-repo-list.txt")
    repo_names = bq_client_helper.get_gokube_trackable_repos(sample_file_path)
    return repo_names


def read_file_data(file_path):
    with open(file_path) as file:
        return file.read()


def get_sample_query_param():
    repo_names = get_sample_repo_names()
    day_list = ['200303', '200304']
    year_prefix = '20*'
    query_params = {'{year_prefix_wildcard}': year_prefix,
                    '{year_suffix_month_day}': '(' + ', '.join(["'" + d + "'" for d in day_list]) + ')',
                    '{repo_names}': '(' + ', '.join(["'" + r + "'" for r in repo_names]) + ')',
                    '{payload_field_name}': 'issue', '{event_type}': 'IssuesEvent'}
    return query_params
