![Jamin Shanti - GitHub Avatar](https://github.com/JaminShanti.png?size=240)

> Welcome to my retro for the `python` repo (master).

Profile: [https://www.linkedin.com/in/jamin-shanti](https://www.linkedin.com/in/jamin-shanti)  
Repo: `git@github.com:JaminShanti/python.git` (branch: `master`)

--- 

## About Me
- Software developer focused on Python projects and tooling.
- This repository contains a collection of utility scripts for automation, data analysis, and system administration.
- For full professional details see the LinkedIn profile above.

## Scripts Overview

### Automation & System Administration
- **`RecycleWebLogicServer.py`**: A robust script to manage WebLogic server instances (start, stop, restart, suspend, resume) via SSH, with parallel execution support.
- **`getWeblogicServerStatus.py`**: Retrieves the runtime status of WebLogic servers and clusters using WLST (WebLogic Scripting Tool) logic.
- **`svn_compare_f5.py`**: Compares F5 iRules deployed on a BigIP device against versions stored in an SVN repository to identify discrepancies.
- **`Backup_SFCC_S3.py`**: Automates backups or data transfers related to Salesforce Commerce Cloud (SFCC) and AWS S3.
- **`f5_node_health.py`**: Checks and reports on the health status of nodes within an F5 load balancer environment.

### Data Analysis & Reporting
- **`NYSE Trending Report.py`**: Generates a high-performance dividend report for S&P 500, 400, and 600 stocks. Uses Yahoo Finance's bulk quote API for speed and exports reports as HTML/PDF.
- **`yt_channel_compare.py`**: Tracks and compares YouTube channel view counts over time. Recently upgraded to use **Plotly** for modern, interactive visualizations.
    - Generates interactive **HTML** reports, as well as shareable **PNG** and **PDF** exports.
    - Features intelligent legend management (Top N channels) to ensure clarity in large datasets.
    - Supports standalone plot generation without re-fetching data via the `--plot` switch.
- **`rotten_tomato_user_reviews.py`**: Scrapes user reviews from Rotten Tomatoes for movies or TV shows, performs sentiment analysis (rating average), and generates word clouds.
- **`Git_Log_Report.py`**: Analyzes a Git repository's history to generate reports on commit activity, authors, and file changes.
- **`Corona_Mapping.py`**: Visualizes COVID-19 data, likely creating choropleth maps (e.g., `covid_choropleth_*.html`) to show spread or impact by region.

### Miscellaneous
- **`lastgitcommit.py`**: A utility to retrieve details about the most recent Git commit.
- **`contact_bot/`**: A directory containing a bot implementation, possibly for automated messaging or interaction (e.g., Facebook Messenger).

## Recent Commits
- **1acc85d**: Updating Code changes for the year. (35 hours ago)
- **6c2bef1**: Updating Code changes for the year. (8 weeks ago)
- **9c2b1b8**: Updating Code changes for the year. (8 weeks ago)
- **8bf3aa3**: Updating Code changes for the year. (8 weeks ago)
- **0c5a50f**: Updating Code changes for the year. (3 months ago)

## How to Run
Prerequisites:
- Windows, Python 3.13+
- `git` on PATH
- Python packages: `GitPython`, `pandas`, `matplotlib`, `plotly`, `kaleido`, `boto3`, `requests`, `bigsuds`, `PyYAML`, `yfinance`, `numpy`, `tqdm`, `pandas-datareader`, `imgkit`, `IPython`, `paramiko`, `wordcloud`, `yagmail`, `tabulate`, `html2text`, `fbchat`, `playwright`, `beautifulsoup4`

Install:
```powershell
pip install gitpython pandas matplotlib plotly kaleido boto3 requests bigsuds pyyaml yfinance numpy tqdm pandas-datareader imgkit ipython paramiko wordcloud yagmail tabulate html2text fbchat playwright beautifulsoup4
playwright install chromium
```
