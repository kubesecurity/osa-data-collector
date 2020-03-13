import src.utils.bq_client_helper as bq_client_helper


def get_sample_repo_names():
    """
    Return sample repo list added as openshift repos
    """
    eco_with_repo_list = bq_client_helper.get_eco_system_with_repo_list('tests/src/utils/data_assets/repo-list.json')
    return eco_with_repo_list['openshift']


def read_file_data(file_path):
    """
    Read file and return content
    """
    with open(file_path) as file:
        return file.read()


def get_sample_query_param():
    """
    Build sample query param used in BigQueryDataCollector class
    """
    repo_names = get_sample_repo_names()
    day_list = ['200303', '200304']
    year_prefix = '20*'
    query_params = {'{year_prefix_wildcard}': year_prefix,
                    '{year_suffix_month_day}': '(' + ', '.join(["'" + d + "'" for d in day_list]) + ')',
                    '{repo_names}': '(' + ', '.join(["'" + r + "'" for r in repo_names]) + ')',
                    '{payload_field_name}': 'issue', '{event_type}': 'IssuesEvent'}
    return query_params
