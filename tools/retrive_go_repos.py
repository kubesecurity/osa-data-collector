"""Get list of repo urls for a given organization with 1st level dependancy for GO language."""

import json
import logging
from datetime import datetime, timedelta
from urllib.request import urlopen, URLError

import requests
from bs4 import BeautifulSoup
from dateutil.parser import isoparse
import time

"""
Configure below variables, which are used in get repo with dependancy details for GO.
--------------------------------------------------------------------------------------------
GITHUB_ACCESS_TOKEN: Github token to query github api
REPO_UPDATED_WITHIN_N_DAYS: if None set then will retrive all repos
                            else will get only repos those updated in given N days
ORGANIZATION: organition name (Ex openshift/knative/kubevirt)

Note: We are filtering out disabled and archived repos.
"""
GITHUB_ACCESS_TOKEN = ""
REPO_UPDATED_WITHIN_N_DAYS = None
ORGANIZATION = "kubevirt"
LANGUAGE = "GO"
ARCHIEVED = False
DISABLED = False

# set logging level to INFO
logging.getLogger().setLevel(logging.INFO)


pkg_list = []
repo_urls = []
dependancy_urls = []


def check_repo_updated_date(date: str) -> bool:
    """Validate repo's updated date."""
    if REPO_UPDATED_WITHIN_N_DAYS is None:
        return True
    updated_date = isoparse(date).replace(tzinfo=None)
    date_n_days_ago = datetime.now() - timedelta(days=REPO_UPDATED_WITHIN_N_DAYS)
    return updated_date > date_n_days_ago


def get_go_pkg_data(pkg: str):
    """Get go package info."""
    raw_url = "https://pkg.go.dev/{pkg}?tab=overview"
    url = raw_url.format(pkg=pkg)
    return requests.get(url)


def get_dependancy_repo_url_from_text(text: str):
    """Get Dependancy repo url from text retrived from package details."""
    lines = list(filter(lambda x: ('Repository: <a href="https://github.com/' in x), text.split('\n')))
    for line in lines:
        soup = BeautifulSoup(line, features="html.parser")
        github_url = soup.find('a').contents[0]
        if "github.com/" in github_url and \
                github_url not in dependancy_urls and \
                is_valid_repo(get_repo_details(github_url.split("github.com/")[1])):
            dependancy_urls.append(github_url)


def get_go_pkg_github_repo_details(pkg: str):
    """Get Go package and repo details."""
    if pkg not in pkg_list:
        pkg_list.append(pkg)

        data = get_go_pkg_data(pkg)
        if 'Repository: <a href="https://github.com/' in data.text:
            get_dependancy_repo_url_from_text(data.text)


def is_valid_repo(item) -> bool:
    """Validate repo is valid to consider or not."""
    if item:
        try:
            if item and item['archived'] is False and \
                    check_repo_updated_date(item['updated_at']) and \
                    str(item['language']).upper() == LANGUAGE and \
                    item['disabled'] is False:
                return True
        except RuntimeError:
            logging.error("Error while validating repo details : {repo}".format(repo=item['full_name']))
    return False


def get_repo_details(org_repo: str):
    """Get github repo details."""
    url = "https://api.github.com/repos/{org_repo}".format(org_repo=org_repo)
    try:
        result = requests.get(url, headers={'Authorization': 'Bearer {token}'.format(token=GITHUB_ACCESS_TOKEN)})
        if result.status_code == 200:
            return result.json()
        else:
            logging.error("Repo doesn't exist : {repo}".format(repo=org_repo))
    except Exception as ex:
        logging.error("Error while retriving repo details : {repo}, msg: {msg}".format(repo=org_repo, msg=str(ex)))

    return None


def remove_unwanted_chars(line) -> str:
    """Remove unwanted characters from string."""
    return str(line).replace("\\t", "").replace("\\n'", "").replace("b'", "").strip()


def get_dependancy_data_from_go_mod_file(org_repo: str):
    """Get Dependancy repo details from go.mod file."""
    content_raw_url = "https://raw.githubusercontent.com/{org_repo}/master/go.mod".format(org_repo=org_repo)
    dependancy_section_started = False
    dependancy_section_ended = False

    for line in urlopen(content_raw_url):
        str_line = remove_unwanted_chars(line)
        if dependancy_section_started is False and "require" in str_line:
            dependancy_section_started = True

        if ")" in str_line:
            dependancy_section_ended = True

        if dependancy_section_ended and dependancy_section_started:
            break

        splited_text = str_line.split(" ")
        if len(splited_text) > 1:
            pkg = splited_text[0].strip()
            get_go_pkg_github_repo_details(pkg)


def get_dependancy_data_from_lock_file(org_repo: str):
    """Get Dependancy repo details from Gopkg.lock file."""
    content_raw_url = "https://raw.githubusercontent.com/{org_repo}/master/Gopkg.lock".format(org_repo=org_repo)
    for line in urlopen(content_raw_url):
        str_line = remove_unwanted_chars(line)
        if str_line.startswith('name = '):
            splited_text = str_line.split("=")
            if len(splited_text) > 1:
                pkg = splited_text[1].strip().replace('"', '')
                get_go_pkg_github_repo_details(pkg)


def get_dependancy_data_from_vendor_folder(org_repo: str):
    """Get Dependancy repo details from vendor folder."""
    sha = get_commit_sha(org_repo)
    vendor_folder_git_url = get_vendor_folder_git_tree_url(org_repo, sha)
    if vendor_folder_git_url:
        get_dependancy_repo_from_vendor(vendor_folder_git_url, 0, "")
    else:
        logging.error("Unble to find dependancy file (go.mod/Gopkg.lock) or Vendor folder for '{org_repo}'"
                      .format(org_repo=org_repo))


def get_dependancy_data(org_repo: str):
    """
    Get Dependancy repo details.

    After anlysing few repos, we came up with below logic to get dependant github repos for a repo.

    if go.mod file present in the repo:
        then we are taking dependant go pkg mentioned into require space.
    else if Gopkg.lock file present in the repo:
        then we are taking all the dependant pakages mentioned into that file.
    else if If Vendor folder present inside reepo:
        then we are taking deepedancy packages based on sub-folder stracture.

    Note:  Tried to use asyncio for parallel execution but we are facing issue with pkg.go.dev url,
    as its limitting no of request,  so removed that code.
    """
    try:
        get_dependancy_data_from_go_mod_file(org_repo)
    except URLError:
        try:
            get_dependancy_data_from_lock_file(org_repo)
        except URLError:
            get_dependancy_data_from_vendor_folder(org_repo)


def get_commit_sha(org_repo: str):
    """Get commit sha for a github repo."""
    commit_raw_url = "https://api.github.com/repos/{org_repo}/commits/master"
    url = commit_raw_url.format(org_repo=org_repo)
    try:
        result = requests.get(url, headers={'Authorization': 'Bearer {token}'.format(token=GITHUB_ACCESS_TOKEN)})
        if result.status_code == 200:
            return result.json()['sha']
        else:
            logging.error("repo doesn't exist : " + org_repo)
    except Exception as ex:
        logging.error("Error while retriving commit details for repo : {repo}, msg : {msg}"
                      .format(repo=org_repo, msg=str(ex)))
    return None


def get_vendor_folder_git_tree_url(org_repo: str, sha: str):
    """Get vendor folder git tree url."""
    repo_structure_raw_url = "https://api.github.com/repos/{org_repo}/git/trees/{sha}"
    url = repo_structure_raw_url.format(org_repo=org_repo, sha=sha)

    try:
        result = requests.get(url, headers={'Authorization': 'Bearer {token}'.format(token=GITHUB_ACCESS_TOKEN)})
        if result.status_code == 200:
            vendor_folder_info = list(filter(lambda x: (x['path'] == "vendor"), result.json()['tree']))
            if len(vendor_folder_info) > 0:
                return vendor_folder_info[0]['url']
        else:
            logging.error("repo doesn't exist : " + org_repo)
    except Exception as ex:
        logging.error("Error while retriving repo structure details {repo}, msg: {msg}"
                      .format(repo=org_repo, msg=str(ex)))

    return None


def get_dependancy_repo_from_vendor(git_tree_url: str, level: int, path: str):
    """Get dependancy repo list from vendor folder."""
    result = requests.get(git_tree_url, headers={'Authorization': 'Bearer {token}'.format(token=GITHUB_ACCESS_TOKEN)})
    if result.status_code == 200:
        json_data = result.json()
        if 'tree' in json_data:
            if level == 0:
                for item in result.json()['tree']:
                    get_dependancy_repo_from_vendor(item['url'], 1, item['path'])
            elif level == 1 or level == 2:
                for item in json_data['tree']:
                    pkg = path + "/" + item['path']
                    if pkg not in pkg_list:
                        pkg_list.append(pkg)

                        data = get_go_pkg_data(pkg)
                        if 'Repository: <a href="https://github.com/' in data.text:
                            get_dependancy_repo_url_from_text(data.text)
                        elif '404 Not Found' in data.text and level == 1:
                            get_dependancy_repo_from_vendor(item['url'], 2, pkg)


def save_data_into_file():
    """Save data into different json files."""
    distinct_repo_urls = list(dict.fromkeys(repo_urls))
    distinct_dependancy_urls = list(dict.fromkeys(dependancy_urls))
    distinct_repo_urls.sort()
    distinct_dependancy_urls.sort()

    # Serializing json and writing to a file
    json_object = json.dumps(distinct_repo_urls, indent=4)
    with open("repo_urls.json", "w") as outfile:
        outfile.write(json_object)

    # Serializing json and writing to a file
    json_object = json.dumps(distinct_dependancy_urls, indent=4)
    with open("dependancy_urls.json", "w") as outfile:
        outfile.write(json_object)

    # combine repo_urls and dependancy_urls
    combined_list = distinct_repo_urls + distinct_dependancy_urls
    combined_list = list(dict.fromkeys(combined_list))
    combined_list.sort()

    # Serializing json and writing to a file
    json_object = json.dumps(combined_list, indent=4)
    with open("combined_list.json", "w") as outfile:
        outfile.write(json_object)

    logging.info("No of '{ecosystem}' repos : {count}".format(ecosystem=ORGANIZATION, count=len(repo_urls)))
    logging.info("No of 1st level dependancies : {count}".format(count=len(dependancy_urls)))
    logging.info("No of combined dependancies : {count}".format(count=len(combined_list)))


def main():
    """Start of the logic."""
    page_no = 1
    do_next_call = True

    start_time = time.time()

    while do_next_call:

        raw_url = "https://api.github.com/orgs/{org}/repos?per_page=100&page={page_no}"
        url = raw_url.format(org=ORGANIZATION, page_no=page_no)
        data = requests.get(url, headers={'Authorization': 'Bearer {token}'.format(token=GITHUB_ACCESS_TOKEN)})
        logging.info("Github url: {url}, No of records : {count}".format(url=url, count=len(data.json())))
        for item in data.json():
            if is_valid_repo(item):
                repo_urls.append(item['html_url'])
                get_dependancy_data(item['full_name'])

        page_no = page_no + 1
        if len(data.json()) == 0:
            do_next_call = False

    logging.info("Total time taken to retrive dependancies {min:.2f} minutes"
                 .format(min=(time.time() - start_time / 60)))

    save_data_into_file()
    logging.info("Process completed successfully")


if __name__ == "__main__":
    main()
