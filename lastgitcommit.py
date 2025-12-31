# python
import git
from io import StringIO
from argparse import ArgumentParser
from datetime import datetime
from collections import OrderedDict
import sys
import warnings
import logging
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="numpy.dtype size changed")
    warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
    import pandas as pd


__author__ = 'jamin_shanti'
__status__ = "Development"
__creation_date__ = "Jul 23th 2018"

secret_git_user = 'Rumpelstiltskin'

# module logger
logger = logging.getLogger("lastgitcommit")


def get_git_log(_repoFilePath='.'):
    repo = git.Repo(_repoFilePath)
    git_repo = repo.git
    repo_name = repo.remotes[0].url
    # use numstat and a stable pretty format; original used tab-separated fields parsed by pandas
    git_log_output = git_repo.log('--numstat', '--no-merges', '--pretty=format:\t\t\t%h\t%ad\t%aN\t%ae')
    git_log = pd.read_csv(StringIO(git_log_output),
                          sep="\t",
                          header=None,
                          names=['additions', 'deletions', 'filename',
                                 'sha', 'timestamp', 'author', 'email_address']
                          )
    git_log = git_log[['additions', 'deletions', 'filename']].join(
        git_log[['sha', 'timestamp', 'author', 'email_address']].fillna(method='ffill'))
    git_log = git_log.dropna()
    return git_log


def handle_options(parser):
    group = parser.add_argument_group('group')
    global verbose
    global git_path
    global include_author_email_address
    global include_author_name
    global include_filelist
    global to_json
    global last_build_user
    global find_special_file

    group.add_argument("--gitPath", '-g', dest="git_path", action="store", default=".",
                       help="/Path/to/git/repo, default is CurrentDirectory.")
    group.add_argument("--authorEmailAddress", '-e', dest="include_author_email_address",
                       action="store_true", default=False, help="Returns author Email Address, default=False")
    group.add_argument("--authorName", '-a', dest="include_author_name",
                       action="store_true", default=False, help="Returns authorName, default=False")
    group.add_argument("--findFile", dest="find_special_file", action="store",
                       default=None, help="Find Specific File, default=None")
    group.add_argument("--noFileList", dest="include_filelist", action="store_false",
                       default=True, help="Returns filelist, default is to return list")
    group.add_argument("--lastBuildUser", dest="last_build_user", action="store",
                       default=secret_git_user, help="Returns commits until lastBuildUser")
    group.add_argument("--to_json", dest="to_json", action="store_true",
                       default=False, help="Returns json, default=False")
    group.add_argument("--verbose", dest="verbose", action="store_true",
                       default=False, help="Verbose Logging, default=False")

    options = parser.parse_args()
    verbose = options.verbose

    # configure logging: mirror original timestamp+level style
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logger.setLevel(log_level)

    logger.debug("Argument provided: verbose '%s'", verbose)
    git_path = options.git_path
    logger.debug("Argument provided: git_path '%s'", git_path)
    include_author_email_address = options.include_author_email_address
    logger.debug("Argument provided: include_author_email_address '%s'", include_author_email_address)
    include_author_name = options.include_author_name
    logger.debug("Argument provided: include_author_name '%s'", include_author_name)
    include_filelist = options.include_filelist
    logger.debug("Argument provided: include_filelist '%s'", include_filelist)
    to_json = options.to_json
    logger.debug("Argument provided: to_json '%s'", to_json)
    last_build_user = options.last_build_user
    logger.debug("Argument provided: last_build_user '%s'", last_build_user)
    find_special_file = options.find_special_file
    logger.debug("Argument provided: find_special_file '%s'", find_special_file)


def return_ouput():
    last_commit = my_git_log[my_git_log.sha == my_git_log['sha'].values[0]]
    sha_commits = my_git_log['sha'].tolist()
    sha_commits = OrderedDict((x, True) for x in sha_commits).keys()
    if last_build_user not in last_commit.author.values:
        if last_build_user in my_git_log.author.values:
            for i in sha_commits[1:]:
                last_commit = pd.concat(
                    [my_git_log[my_git_log.sha == i], last_commit])
                if last_build_user in last_commit.author.values:
                    break
        else:
            logger.debug("last_build_user: %s not present in my_git_log", last_build_user)
            if last_build_user != secret_git_user:
                logger.fatal("User: %s not present in Repository!", last_build_user)
                sys.exit(1)
    sha_commits = last_commit['sha'].tolist()
    sha_commits = OrderedDict((x, True) for x in sha_commits).keys()
    logger.debug("the current length of sha_commits is %s", len(list(sha_commits)))
    for i in reversed(list(sha_commits)):
        logger.debug("current sha is %s", i)
        row = last_commit[last_commit.sha == i]
        if include_author_email_address:
            print(str(row.email_address.values[0]))
        if include_author_name:
            print(str(row.author.values[0]))
        if include_filelist:
            print(row.filename.tolist())
        if to_json:
            print(row.to_json())
        if verbose:
            # timestamp may be numpy datetime or string
            ts = row.timestamp.values[0]
            print(str(ts))
        if row.author.values[0] != last_build_user and find_special_file:
            if [s for s in row.filename.tolist() if find_special_file in s]:
                logger.warning("FileName=%s file modified by non-Build User=%s",
                               find_special_file, row.author.values[0])


if __name__ == '__main__':
    handle_options(ArgumentParser())
    my_git_log = get_git_log(git_path)
    if len(my_git_log) == 0:
        print("No Recent Commits Found, Exiting...")
        sys.exit(1)
    return_ouput()
