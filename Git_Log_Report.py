# !/usr/bin/env python
import git
import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO
import sys

__author__ = 'jamin_shanti'
__status__ = "Development"
__creation_date__ = "June 29th 2018"


repo = git.Repo('.')
git_repo = repo.git
repo_name = repo.remotes[0].url

git_log_output = git_repo.log(
    '--since=last month', '--numstat', '--no-merges', '--pretty=format:\t\t\t%h\t%ad\t%aN')
git_log = pd.read_csv(StringIO(git_log_output),
                      sep="\t",
                      header=None,
                      names=['additions', 'deletions', 'filename',
                             'sha', 'timestamp', 'author']
                      )
git_log = git_log[['additions', 'deletions', 'filename']]\
    .join(git_log[['sha', 'timestamp', 'author']].fillna(method='ffill'))


# Remove jenkins
git_log = git_log[git_log.author != 'jenkins']
git_log = git_log.dropna()

if len(git_log) == 0:
    print "No Recent Commits Found, Exiting..."
    sys.exit(1)

# top Author per change file
top20 = git_log.author.value_counts().head(20)
fig, ax = plt.subplots()
ax = top20.plot.bar()
ax.set_title("Top Authors: %s" % repo_name)
ax.set_xlabel("Authors")
ax.set_ylabel("Number of Commits")
ax.autoscale(enable=True, axis='x')
print "Generating top20.png..."
fig.autofmt_xdate()
fig.set_size_inches(16.5, 8.5)
fig.savefig('top20.png', dpi=100)

# top File per change
top20_filename = git_log.filename.value_counts().head(20)
fig, ax = plt.subplots()
ax = top20_filename.plot.bar()
ax.set_title("Top Filenames: %s" % repo_name, fontsize=35)
ax.set_xlabel("Filenames", fontsize=35)
ax.set_ylabel("Number of Commits", fontsize=35)
ax.autoscale(enable=True, axis='x')
print "Generating top20_filename.png..."
fig.autofmt_xdate()
fig.set_size_inches(28, 18.5)
fig.savefig('top20_filename.png', dpi=100)

# commits per hour
git_log.timestamp = pd.to_datetime(git_log.timestamp)
commits_per_hour = git_log.timestamp.dt.hour.value_counts(sort=False)

commits_per_hour.sort_index(inplace=True)
fig, ax = plt.subplots()
ax = commits_per_hour.plot.bar()
ax.set_title("Commits per Hour: %s" % repo_name)
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Number of Commits")
ax.autoscale(enable=True, axis='x')
print "Generating commits_per_hour.png..."
fig.autofmt_xdate()
fig.savefig('commits_per_hour.png', dpi=100)


# commits per weekday
git_log.timestamp = pd.to_datetime(git_log.timestamp)
commits_per_weekday = git_log.timestamp.dt.weekday.value_counts(sort=False)
commits_per_weekday.sort_index(inplace=True)
fig, ax = plt.subplots()
ax = commits_per_weekday.plot.bar()
ax.set_title("Commits per Weekday: %s" % repo_name)
ax.set_xlabel("Days of the Week")
ax.set_ylabel("Number of Commits")
ax.autoscale(enable=True, axis='x')
print "Generating commits_per_day.png..."
fig.autofmt_xdate()
fig.savefig('commits_per_day.png', dpi=100)

# commits per date
git_log.timestamp = pd.to_datetime(git_log.timestamp)
commits_per_date = git_log.timestamp.dt.date.value_counts(sort=False)
commits_per_date.sort_index(inplace=True)
fig, ax = plt.subplots()
ax = commits_per_date.plot.bar()
ax.set_title("Commits per Day: %s" % repo_name)
ax.set_xlabel("Dates")
ax.set_ylabel("Number of Commits")
ax.autoscale(enable=True, axis='x')
print "Generating commits_per_date.png..."
fig.autofmt_xdate()
fig.set_size_inches(14.5, 6.5)
fig.savefig('commits_per_date.png', dpi=100)
