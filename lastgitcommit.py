import git
from io import StringIO
import argparse
from datetime import datetime
from collections import OrderedDict
import sys
import warnings
import logging
import json

# Suppress warnings from pandas/numpy if they are not critical
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="numpy.dtype size changed")
    warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
    import pandas as pd

__author__ = 'jamin_shanti'
__status__ = "Development"
__creation_date__ = "Jul 23th 2018"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("lastgitcommit")

class GitLogAnalyzer:
    """
    Analyzes Git repository logs to extract commit information.
    """
    def __init__(self, git_path='.', last_build_user='Rumpelstiltskin'):
        self.git_path = git_path
        self.last_build_user = last_build_user
        self.git_log_df = pd.DataFrame()

    def get_git_log(self):
        """
        Retrieves Git log information for the specified repository path.
        """
        try:
            repo = git.Repo(self.git_path)
            git_repo = repo.git
            repo_name = repo.remotes[0].url if repo.remotes else "local_repo"
            logger.info(f"Analyzing Git repository: {repo_name} at {self.git_path}")

            # Use --raw for a more consistent format that includes file changes
            # --pretty=format: provides commit hash, author date, author name, author email
            # --numstat provides additions, deletions, filename
            # We combine these by parsing the output carefully.
            git_log_output = git_repo.log(
                '--numstat',
                '--no-merges',
                '--pretty=format:COMMIT_SEP%n%h%n%ad%n%aN%n%ae'
            )

            # Split by COMMIT_SEP to process each commit block
            commits_data = []
            for commit_block in git_log_output.split('COMMIT_SEP\n')[1:]:
                lines = commit_block.strip().split('\n')
                if not lines:
                    continue

                sha = lines[0]
                timestamp = lines[1]
                author = lines[2]
                email_address = lines[3]
                
                file_stats = lines[4:] # Remaining lines are file stats

                for stat_line in file_stats:
                    parts = stat_line.split('\t')
                    if len(parts) == 3:
                        additions, deletions, filename = parts
                        commits_data.append({
                            'sha': sha,
                            'timestamp': timestamp,
                            'author': author,
                            'email_address': email_address,
                            'additions': int(additions) if additions.isdigit() else 0,
                            'deletions': int(deletions) if deletions.isdigit() else 0,
                            'filename': filename
                        })
            
            if not commits_data:
                logger.warning("No commits found in the repository.")
                self.git_log_df = pd.DataFrame()
                return

            self.git_log_df = pd.DataFrame(commits_data)
            self.git_log_df['timestamp'] = pd.to_datetime(self.git_log_df['timestamp'])
            logger.info(f"Successfully loaded {len(self.git_log_df)} log entries.")

        except git.InvalidGitRepositoryError:
            logger.error(f"'{self.git_path}' is not a valid Git repository.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error getting Git log: {e}")
            sys.exit(1)

    def find_last_relevant_commit(self, find_special_file=None):
        """
        Finds the last commit that matches criteria, potentially stopping at last_build_user.
        """
        if self.git_log_df.empty:
            logger.warning("No Git log data to process.")
            return pd.DataFrame()

        # Sort by timestamp descending to get most recent commits first
        sorted_log = self.git_log_df.sort_values(by='timestamp', ascending=False)
        
        last_commit_group = pd.DataFrame()
        processed_shas = set()

        for _, row in sorted_log.iterrows():
            if row['sha'] in processed_shas:
                continue # Skip if this commit (SHA) has already been processed for another file

            current_commit_files = sorted_log[sorted_log['sha'] == row['sha']]
            last_commit_group = pd.concat([last_commit_group, current_commit_files], ignore_index=True)
            processed_shas.add(row['sha'])

            if self.last_build_user in current_commit_files['author'].values:
                logger.debug(f"Found commit by '{self.last_build_user}'. Stopping search.")
                break
        
        if self.last_build_user not in last_commit_group['author'].values:
            if self.last_build_user != 'Rumpelstiltskin': # Default secret user
                logger.warning(f"User '{self.last_build_user}' not found in recent commits.")
        
        # Filter by special file if requested
        if find_special_file:
            filtered_group = last_commit_group[last_commit_group['filename'].str.contains(find_special_file, na=False)]
            if not filtered_group.empty:
                for _, file_row in filtered_group.iterrows():
                    if file_row['author'] != self.last_build_user:
                        logger.warning(f"File '{find_special_file}' modified by non-build user '{file_row['author']}' in commit {file_row['sha']}")
            return filtered_group
        
        return last_commit_group

    def format_output(self, commit_df, args):
        """
        Formats and prints the output based on command-line arguments.
        """
        if commit_df.empty:
            logger.info("No relevant commits to display.")
            return

        # Get unique commits to avoid redundant info for multi-file commits
        unique_commits = commit_df.drop_duplicates(subset=['sha']).sort_values(by='timestamp', ascending=False)

        output_data = []
        for _, row in unique_commits.iterrows():
            commit_info = {}
            commit_info['sha'] = row['sha']
            commit_info['timestamp'] = row['timestamp'].isoformat()

            if args.include_author_name:
                commit_info['author'] = row['author']
            if args.include_author_email_address:
                commit_info['email_address'] = row['email_address']
            
            if args.include_filelist:
                files_in_commit = commit_df[commit_df['sha'] == row['sha']]['filename'].tolist()
                commit_info['files_changed'] = files_in_commit
            
            output_data.append(commit_info)

        if args.to_json:
            logger.info(json.dumps(output_data, indent=2))
        else:
            for commit in output_data:
                logger.info(f"Commit SHA: {commit.get('sha')}")
                logger.info(f"Timestamp: {commit.get('timestamp')}")
                if 'author' in commit:
                    logger.info(f"Author: {commit.get('author')}")
                if 'email_address' in commit:
                    logger.info(f"Email: {commit.get('email_address')}")
                if 'files_changed' in commit:
                    logger.info(f"Files Changed: {', '.join(commit.get('files_changed'))}")
                logger.info("-" * 40)


def main():
    parser = argparse.ArgumentParser(description="Analyze last Git commit(s).")
    parser.add_argument("--gitPath", '-g', dest="git_path", default=".",
                       help="Path to Git repository (default: current directory).")
    parser.add_argument("--authorEmailAddress", '-e', dest="include_author_email_address",
                       action="store_true", help="Include author email address in output.")
    parser.add_argument("--authorName", '-a', dest="include_author_name",
                       action="store_true", help="Include author name in output.")
    parser.add_argument("--findFile", dest="find_special_file",
                       help="Find commits related to a specific file (e.g., 'my_script.py').")
    parser.add_argument("--noFileList", dest="include_filelist", action="store_false",
                       default=True, help="Do not include the list of changed files.")
    parser.add_argument("--lastBuildUser", dest="last_build_user", default="Rumpelstiltskin",
                       help="Stop searching for commits when this user is encountered (default: 'Rumpelstiltskin').")
    parser.add_argument("--to_json", dest="to_json", action="store_true",
                       help="Output results in JSON format.")
    parser.add_argument("--verbose", dest="verbose", action="store_true",
                       help="Enable verbose (DEBUG) logging.")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled.")

    analyzer = GitLogAnalyzer(git_path=args.git_path, last_build_user=args.last_build_user)
    analyzer.get_git_log()
    
    if analyzer.git_log_df.empty:
        logger.info("No Git log data available. Exiting.")
        sys.exit(0)

    relevant_commits_df = analyzer.find_last_relevant_commit(find_special_file=args.find_special_file)
    analyzer.format_output(relevant_commits_df, args)


if __name__ == '__main__':
    main()
