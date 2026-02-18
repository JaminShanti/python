#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from io import StringIO
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

try:
    import git
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg") # Use non-interactive backend
    import matplotlib.pyplot as plt
except ImportError as e:
    logger.error(f"Missing dependency: {e}. Please install 'gitpython', 'pandas', and 'matplotlib'")
    sys.exit(1)


__author__ = "jamin_shanti"
__status__ = "Development"
__creation_date__ = "June 29th 2018"


class GitLogReport:
    """
    Generate git activity plots for a repository.
    """

    def __init__(self, repo_path: str = ".", since: str = "last month", outdir: str = ".") -> None:
        self.repo_path = repo_path
        self.since = since
        self.outdir = outdir
        
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)
            logger.info(f"Created output directory: {self.outdir}")

        try:
            self.repo = git.Repo(self.repo_path)
            logger.info(f"Initialized Git repo at {self.repo_path}")
        except git.InvalidGitRepositoryError:
            logger.error(f"Not a valid git repository: {self.repo_path}")
            sys.exit(1)
        except Exception as exc:
            logger.error(f"Error initializing git repo: {exc}")
            sys.exit(1)

        self.repo_name = "Unknown Repo"
        if self.repo.remotes:
            try:
                self.repo_name = os.path.basename(self.repo.remotes[0].url).replace('.git', '')
            except Exception:
                self.repo_name = os.path.basename(os.path.abspath(self.repo_path))
        else:
            self.repo_name = os.path.basename(os.path.abspath(self.repo_path))

    def get_git_log_data(self) -> pd.DataFrame:
        """
        Retrieves and parses git log data into a DataFrame.
        """
        # Format: hash, date, author name
        # We use a custom separator to avoid issues with commit messages
        # But here we are just getting stats, so we rely on --numstat
        # The original logic used a specific format string that pandas could parse
        
        # Constructing the git command arguments
        # --numstat gives: additions deletions filename
        # --pretty=format:... gives the commit metadata
        # The output is mixed, so we need to be careful.
        # The original script relied on a specific structure that might be fragile.
        # Let's try to replicate the original logic but safely.
        
        try:
            # Using a format that puts metadata on one line, followed by numstat lines
            # We will parse it manually instead of relying on read_csv directly on the raw output
            # to handle potential parsing errors better.
            
            # However, to keep it simple and close to the original "pandas read_csv" approach if it works:
            # The original used: --pretty=format:\t\t\t%h\t%ad\t%aN
            # This creates lines like:			hash	date	author
            # And numstat lines like: 1	1	filename
            # The read_csv with sep='\t' handles this by treating the empty fields in numstat lines as NaNs?
            # Actually, numstat lines have 3 columns. The metadata line has empty first 3 cols if we use the format above?
            # No, the format string "\t\t\t%h\t%ad\t%aN" creates a line starting with 3 tabs.
            # So it aligns with "additions", "deletions", "filename", "sha", "timestamp", "author"
            # where the first 3 are empty for metadata rows.
            # And numstat rows have data in first 3 cols, and empty in last 3?
            # No, numstat rows don't have the last 3 cols.
            # So pandas read_csv with names=... will fill missing cols with NaN.
            
            fmt = "\t\t\t%h\t%ad\t%aN"
            git_args = ["--since=" + self.since, "--numstat", "--no-merges", f"--pretty=format:{fmt}", "--date=iso"]
            
            raw_log = self.repo.git.log(*git_args)
            
            if not raw_log.strip():
                return pd.DataFrame()

            df = pd.read_csv(
                StringIO(raw_log),
                sep="\t",
                header=None,
                names=["additions", "deletions", "filename", "sha", "timestamp", "author"],
                engine="python"
            )

            # Forward fill the metadata (sha, timestamp, author) from the commit header line down to the file lines
            # The commit header line has NaNs in additions/deletions/filename (actually filename might be empty string)
            # The numstat lines have NaNs in sha/timestamp/author
            
            # In the original format "\t\t\t%h...", the first 3 cols are empty.
            # So additions/deletions/filename are NaN for these rows.
            # We forward fill sha/timestamp/author.
            df[["sha", "timestamp", "author"]] = df[["sha", "timestamp", "author"]].fillna(method="ffill")

            # Now drop the rows that were just headers (where filename is NaN)
            df = df.dropna(subset=["filename"])

            # Convert numeric columns
            df["additions"] = pd.to_numeric(df["additions"], errors="coerce").fillna(0)
            df["deletions"] = pd.to_numeric(df["deletions"], errors="coerce").fillna(0)

            # Convert timestamp
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

            # Filter out rows with invalid timestamps
            df = df.dropna(subset=["timestamp"])

            # Filter out automation accounts if needed
            df = df[~df["author"].astype(str).str.lower().str.contains("jenkins", na=False)]

            return df

        except Exception as e:
            logger.error(f"Error parsing git log: {e}")
            return pd.DataFrame()

    def save_plot(self, fig: plt.Figure, filename: str) -> None:
        path = os.path.join(self.outdir, filename)
        try:
            fig.savefig(path, bbox_inches="tight")
            plt.close(fig)
            logger.info(f"Saved plot to {path}")
        except Exception as e:
            logger.error(f"Failed to save plot {filename}: {e}")

    def generate_plots(self, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning("No data to plot.")
            return

        # 1. Top Authors
        top_authors = df["author"].value_counts().head(20)
        if not top_authors.empty:
            fig, ax = plt.subplots(figsize=(12, 6))
            top_authors.plot.bar(ax=ax)
            ax.set_title(f"Top Authors: {self.repo_name}")
            ax.set_ylabel("Commits (Files Changed)")
            fig.autofmt_xdate()
            self.save_plot(fig, "top20_authors.png")

        # 2. Top Filenames
        top_files = df["filename"].value_counts().head(20)
        if not top_files.empty:
            fig, ax = plt.subplots(figsize=(14, 8))
            top_files.plot.bar(ax=ax)
            ax.set_title(f"Top Modified Files: {self.repo_name}")
            ax.set_ylabel("Change Count")
            fig.autofmt_xdate()
            self.save_plot(fig, "top20_filename.png")

        # 3. Commits by Hour
        commits_by_hour = df["timestamp"].dt.hour.value_counts().sort_index()
        if not commits_by_hour.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            commits_by_hour.plot.bar(ax=ax)
            ax.set_title(f"Activity by Hour: {self.repo_name}")
            ax.set_xlabel("Hour of Day")
            ax.set_ylabel("Activity Count")
            self.save_plot(fig, "commits_per_hour.png")

        # 4. Commits by Weekday
        commits_by_day = df["timestamp"].dt.day_name().value_counts()
        # Sort by day order
        days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        commits_by_day = commits_by_day.reindex(days_order).fillna(0)
        
        if not commits_by_day.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            commits_by_day.plot.bar(ax=ax)
            ax.set_title(f"Activity by Weekday: {self.repo_name}")
            ax.set_ylabel("Activity Count")
            self.save_plot(fig, "commits_per_day.png")

        # 5. Commits by Date
        commits_by_date = df["timestamp"].dt.date.value_counts().sort_index()
        if not commits_by_date.empty:
            fig, ax = plt.subplots(figsize=(14, 6))
            commits_by_date.plot.line(ax=ax, marker='o') # Line plot often better for time series
            ax.set_title(f"Activity Over Time: {self.repo_name}")
            ax.set_ylabel("Activity Count")
            fig.autofmt_xdate()
            self.save_plot(fig, "commits_per_date.png")

    def run(self) -> int:
        logger.info(f"Analyzing repo: {self.repo_path} (Since: {self.since})")
        df = self.get_git_log_data()
        
        if df.empty:
            logger.warning(f"No commits found since '{self.since}'.")
            return 0

        logger.info(f"Processed {len(df)} file change events.")
        self.generate_plots(df)
        return 0


def main():
    parser = argparse.ArgumentParser(description="Generate Git Activity Reports")
    parser.add_argument("--repo", "-r", default=".", help="Path to git repository")
    parser.add_argument("--since", "-s", default="last month", help="Timeframe (e.g., '1 week ago', '2023-01-01')")
    parser.add_argument("--outdir", "-o", default=".", help="Output directory for reports")
    
    args = parser.parse_args()

    report = GitLogReport(repo_path=args.repo, since=args.since, outdir=args.outdir)
    sys.exit(report.run())

if __name__ == "__main__":
    main()
