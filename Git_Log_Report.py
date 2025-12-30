# python
#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from io import StringIO
from typing import Optional

try:
    import git
    import pandas as pd
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    print("Missing dependency: please install 'gitpython', 'pandas', and 'matplotlib'")
    sys.exit(1)


__author__ = "jamin_shanti"
__status__ = "Development"
__creation_date__ = "June 29th 2018"


class GitLogReport:
    """
    Generate git activity plots for a repository.

    Usage:
        report = GitLogReport(repo_path='.', since='last month', outdir='reports')
        report.run()
    """

    def __init__(self, repo_path: str = ".", since: str = "last month", outdir: str = ".") -> None:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.repo_path = repo_path
        self.since = since
        self.outdir = outdir
        os.makedirs(self.outdir, exist_ok=True)

        try:
            self.repo = git.Repo(self.repo_path)
        except Exception as exc:
            logging.error("Not a git repository: %s", self.repo_path)
            raise

        if self.repo.remotes and len(self.repo.remotes) > 0:
            try:
                self.repo_name = self.repo.remotes[0].url
            except Exception:
                self.repo_name = self.repo_path
        else:
            self.repo_name = self.repo_path

    def _raw_git_log(self) -> str:
        fmt = "--pretty=format:\t\t\t%h\t%ad\t%aN"
        args = ["--since=" + self.since, "--numstat", "--no-merges", fmt, "--date=iso"]
        # gitpython will accept these as separate args
        return self.repo.git.log(*args)

    def _load_dataframe(self) -> pd.DataFrame:
        raw = self._raw_git_log()
        if not raw or not raw.strip():
            return pd.DataFrame()

        df = pd.read_csv(
            StringIO(raw),
            sep="\t",
            header=None,
            names=["additions", "deletions", "filename", "sha", "timestamp", "author"],
            engine="python",
        )

        # Forward-fill metadata rows; handle missing columns safely
        if {"sha", "timestamp", "author"}.issubset(df.columns):
            df[["sha", "timestamp", "author"]] = df[["sha", "timestamp", "author"]].fillna(method="ffill")

        # Keep relevant columns and coerce numeric types
        df = df[["additions", "deletions", "filename", "sha", "timestamp", "author"]]
        df["additions"] = pd.to_numeric(df["additions"], errors="coerce")
        df["deletions"] = pd.to_numeric(df["deletions"], errors="coerce")
        df = df.dropna(subset=["filename", "sha", "timestamp", "author"])

        # Filter out automation accounts (example)
        df["author"] = df["author"].astype(str)
        df = df[~df["author"].str.lower().str.contains("jenkins")]

        return df

    def _save_fig(self, fig: plt.Figure, name: str, dpi: int = 100, figsize: Optional[tuple] = None) -> None:
        if figsize:
            fig.set_size_inches(*figsize)
        path = os.path.join(self.outdir, name)
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(fig)
        logging.info("Saved %s", path)

    def _plot_top_authors(self, df: pd.DataFrame) -> None:
        top20 = df["author"].value_counts().head(20)
        fig, ax = plt.subplots()
        top20.plot.bar(ax=ax)
        ax.set_title(f"Top Authors: {self.repo_name}")
        ax.set_xlabel("Authors")
        ax.set_ylabel("Number of Commits")
        fig.autofmt_xdate()
        self._save_fig(fig, "top20.png", figsize=(16.5, 8.5))

    def _plot_top_filenames(self, df: pd.DataFrame) -> None:
        top20 = df["filename"].value_counts().head(20)
        fig, ax = plt.subplots()
        top20.plot.bar(ax=ax)
        ax.set_title(f"Top Filenames: {self.repo_name}", fontsize=18)
        ax.set_xlabel("Filenames")
        ax.set_ylabel("Number of Commits")
        fig.autofmt_xdate()
        self._save_fig(fig, "top20_filename.png", figsize=(28, 18.5))

    def _plot_commits_by_hour(self, df: pd.DataFrame) -> None:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        commits_per_hour = df["timestamp"].dt.hour.value_counts(sort=False).sort_index()
        fig, ax = plt.subplots()
        commits_per_hour.plot.bar(ax=ax)
        ax.set_title(f"Commits per Hour: {self.repo_name}")
        ax.set_xlabel("Hour of Day")
        ax.set_ylabel("Number of Commits")
        fig.autofmt_xdate()
        self._save_fig(fig, "commits_per_hour.png")

    def _plot_commits_by_weekday(self, df: pd.DataFrame) -> None:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        commits_per_weekday = df["timestamp"].dt.weekday.value_counts(sort=False).sort_index()
        fig, ax = plt.subplots()
        commits_per_weekday.plot.bar(ax=ax)
        ax.set_title(f"Commits per Weekday: {self.repo_name}")
        ax.set_xlabel("Day of Week (0=Mon)")
        ax.set_ylabel("Number of Commits")
        fig.autofmt_xdate()
        self._save_fig(fig, "commits_per_day.png")

    def _plot_commits_by_date(self, df: pd.DataFrame) -> None:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        commits_per_date = df["timestamp"].dt.date.value_counts(sort=False).sort_index()
        fig, ax = plt.subplots()
        commits_per_date.plot.bar(ax=ax)
        ax.set_title(f"Commits per Day: {self.repo_name}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Number of Commits")
        fig.autofmt_xdate()
        self._save_fig(fig, "commits_per_date.png", figsize=(14.5, 6.5))

    def run(self) -> int:
        df = self._load_dataframe()
        if df.empty:
            logging.error("No recent commits found (since=%s). Exiting.", self.since)
            return 1

        self._plot_top_authors(df)
        self._plot_top_filenames(df)
        self._plot_commits_by_hour(df)
        self._plot_commits_by_weekday(df)
        self._plot_commits_by_date(df)
        logging.info("All reports generated in %s", self.outdir)
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate git activity plots.")
    parser.add_argument("--repo", "-r", default=".", help="Path to git repository (default: current dir)")
    parser.add_argument("--since", "-s", default="last month", help="git --since argument (default: 'last month')")
    parser.add_argument("--outdir", "-o", default=".", help="Output directory for images")
    args = parser.parse_args()

    report = GitLogReport(repo_path=args.repo, since=args.since, outdir=args.outdir)
    raise SystemExit(report.run())
