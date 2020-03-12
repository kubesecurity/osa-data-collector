import json
import logging
import re

import daiquiri
import numpy as np
from bq_helper import BigQueryHelper

import src.utils.cloud_constants as cc

daiquiri.setup(level=logging.INFO)
_logger = daiquiri.getLogger(__name__)


def create_github_bq_client():
    """Create the object for BigQueryHelper"""
    gh_archive = BigQueryHelper(active_project="githubarchive", dataset_name="day")
    _logger.info('Setting up BQ Client')
    return gh_archive


def bq_add_query_params(query, params_dict):
    """
    Replace the parameters to build bigdata query
    """
    for i, j in params_dict.items():
        query = query.replace(i, j)
    return query


def get_eco_system_with_repo_list():
    """
    Read the repo-list.json file and make a dictionary that contains list with ecosystem name and repos
    """
    with open(cc.REPO_LIST) as file:
        json_array = json.load(file)
        eco_system_with_repo = []
        for item in json_array:
            repo_names = get_repos_names(item['urls'])
            eco_system_with_repo.append({'eco-system': item['eco-system'], 'repo-names': repo_names})
            _logger.info("Found {repo_count} repos to track in '{eco}' eco-system".format(repo_count=len(repo_names),
                                                                                           eco=item['eco-system']))
        return eco_system_with_repo


def get_repos_names(gh_repo_links):
    """
    Get all valid repos from the list
    """
    pattern = re.compile(r'.*?github.com/(.*)', re.I)
    repo_names = np.array(
        list(filter(None, [pattern.search(item).group(1) if pattern.search(item) else None for item in gh_repo_links])))
    return repo_names
