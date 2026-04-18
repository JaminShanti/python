#!/usr/bin/env python3
import os, re, json, time, yaml, logging, argparse, concurrent.futures, requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from pathlib import Path
from pandas.plotting import register_matplotlib_converters

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
register_matplotlib_converters()

class YouTubeChannelCompare:
    """A class to track and compare YouTube channel views over time using daily CSV files."""
    def __init__(self, config_file='yt_channel_config.yaml', data_dir='yt_stats_daily', output_dir='yt_visuals'):
        self.config_file, self.data_dir, self.output_dir = config_file, Path(data_dir), Path(output_dir)
        self.channels = self.load_config()
        self.channel_colors = {self.extract_handle(u): i.get('color', 'white') for u, i in self.channels.items() if self.extract_handle(u)}
        self.today = dt.date.today()
        self.date_str, self.day_of_week = self.today.strftime("%Y-%m-%d"), self.today.strftime("%A")
        self.data_dir.mkdir(exist_ok=True); self.output_dir.mkdir(exist_ok=True)

    def load_config(self):
        """Loads channel configuration from a YAML file."""
        if not os.path.exists(self.config_file): return {}
        try:
            with open(self.config_file, 'r') as f: return yaml.safe_load(f) or {}
        except Exception as e: logger.error(f"Error loading config: {e}"); return {}

    def load_all_data(self):
        """Loads all daily CSV files and combines them into one DataFrame."""
        all_dfs = []
        for file in sorted(self.data_dir.glob("*.csv")):
            try: all_dfs.append(pd.read_csv(file))
            except Exception as e: logger.error(f"Error reading {file}: {e}")
        if not all_dfs: return pd.DataFrame(columns=['channel_name', 'date', 'day_of_week', 'total_views_today'])
        return pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['channel_name', 'date'], keep='last')

    def fetch_views(self, url):
        """Fetches the total view count for a given YouTube channel URL."""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        try:
            # Normalize URL to ensure it ends in /about for faster view count finding
            u = url.rstrip('/')
            if not u.endswith('/about'): u += '/about'
            response = requests.get(u, headers=headers, timeout=15)
            if response.status_code == 404 and '/about' in u:
                response = requests.get(url, headers=headers, timeout=15) # Fallback to original
            response.raise_for_status()
            match = re.search(r'\"viewCountText\":\{\"simpleText\":\"(.*?)\"\}', response.text) or re.search(r'\"viewCountText\":\{.*?\"text\":\"(.*?)\".*?\}', response.text) or re.search(r'\"viewCount\":\"(\d+)\"', response.text)
            if match:
                val = match.group(1).replace(' views', '').replace(',', '')
                return int(val)
            return None
        except Exception as e: logger.debug(f"Error fetching {url}: {e}"); return None

    def update_data(self):
        """Updates today's CSV file with current view counts."""
        if not self.channels: return
        new_rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(self.fetch_views, u): u for u in self.channels.keys()}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]; handle = self.extract_handle(url)
                try:
                    views = future.result()
                    if views is not None:
                        logger.info(f"Views for {handle}: {views}")
                        new_rows.append({'channel_name': handle, 'date': self.date_str, 'day_of_week': self.day_of_week, 'total_views_today': views})
                    else: logger.warning(f"Could not find view count for {url}")
                except Exception as e: logger.error(f"Error processing {handle}: {e}")

        if new_rows:
            today_csv, new_df = self.data_dir / f"yt_stats_{self.date_str}.csv", pd.DataFrame(new_rows)
            if today_csv.exists(): new_df = pd.concat([pd.read_csv(today_csv), new_df], ignore_index=True)
            new_df.drop_duplicates(subset=['channel_name'], keep='last').to_csv(today_csv, index=False, encoding='utf-8')
        self.data = self.load_all_data()

    def generate_plot(self, days=7):
        """Generates a plot of view changes over the last N days."""
        if self.data.empty: return
        df = self.data.copy(); df['date'] = pd.to_datetime(df['date']); df = df.sort_values(by=['channel_name', 'date'])
        df['diff'] = df.groupby('channel_name')['total_views_today'].diff().fillna(0)
        plot_df = df[df['date'] >= (pd.Timestamp(self.today) - dt.timedelta(days=days))]
        if plot_df.empty: return

        plt.style.use('dark_background'); fig, ax = plt.subplots(figsize=(16, 8)); plotted_count = 0
        for name, group in plot_df.groupby('channel_name'):
            if group['diff'].sum() > 0:
                ax.plot(group['date'], group['diff'], label=name, color=self.channel_colors.get(name, 'white'), linewidth=2.0, alpha=0.8)
                plotted_count += 1
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d')); ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, days // 10))); fig.autofmt_xdate()
        ax.set_title(f"YouTube Growth (Last {days} Days)", fontsize=14, pad=20); ax.set_ylabel("Daily View Gain")
        if 0 < plotted_count <= 15: plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout(); output_path = self.output_dir / f"yt_channel_compare_{self.date_str}.png"
        plt.savefig(output_path, bbox_inches='tight', dpi=150); plt.close(); logger.info(f"Plot saved to {output_path}")

    def run(self, days=14): self.update_data(); self.generate_plot(days)
    def extract_handle(self, url): 
        # Clean up common URL patterns
        url = url.split('?')[0].split('#')[0].rstrip('/')
        match = re.search(r'@([\w.-]+)', url)
        return match.group(1) if match else url.split('/')[-1]

if __name__ == "__main__":
    args = argparse.ArgumentParser(); args.add_argument("-d", "--days", type=int, default=14); parsed = args.parse_args()
    YouTubeChannelCompare().run(days=parsed.days)
