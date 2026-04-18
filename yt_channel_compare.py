#!/usr/bin/env python3
import os, re, json, time, yaml, logging, argparse, concurrent.futures, requests
import pandas as pd
import plotly.express as px
import datetime as dt
from pathlib import Path

# Configure logging: Set root level to WARNING to silence chatty libraries
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
# Create a specific logger for this script and set it to INFO
logger = logging.getLogger('yt_compare')
logger.setLevel(logging.INFO)

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
            with open(self.config_file, 'r') as file: return yaml.full_load(file) or {}
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
            u = url.rstrip('/')
            if not u.endswith('/about'): u += '/about'
            response = requests.get(u, headers=headers, timeout=15)
            if response.status_code == 404 and '/about' in u: response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            match = re.search(r'\"viewCountText\":\{\"simpleText\":\"(.*?)\"\}', response.text) or re.search(r'\"viewCountText\":\{.*?\"text\":\"(.*?)\".*?\}', response.text) or re.search(r'\"viewCount\":\"(\d+)\"', response.text)
            if match: return int(match.group(1).replace(' views', '').replace(',', ''))
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
                    else: logger.warning(f"Could not find view count for {url}")
                    new_rows.append({'channel_name': handle, 'date': self.date_str, 'day_of_week': self.day_of_week, 'total_views_today': views})
                except Exception as e: logger.error(f"Error processing {handle}: {e}")

        if new_rows:
            today_csv, new_df = self.data_dir / f"yt_stats_{self.date_str}.csv", pd.DataFrame(new_rows)
            if today_csv.exists(): new_df = pd.concat([pd.read_csv(today_csv), new_df], ignore_index=True)
            new_df.drop_duplicates(subset=['channel_name'], keep='last').to_csv(today_csv, index=False, encoding='utf-8')
        self.data = self.load_all_data()

    def generate_plot(self, days=14, top_n=20):
        """Generates Plotly graphs (HTML, PNG, PDF) of view changes."""
        if self.data.empty: return
        df = self.data.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['channel_name', 'date'])
        df['diff'] = df.groupby('channel_name')['total_views_today'].diff().fillna(0)
        
        plot_df = df[df['date'] >= (pd.Timestamp(self.today) - dt.timedelta(days=days))]
        if plot_df.empty: return

        # Sort channels by total growth and take Top N to keep legend clean and effective
        growth_totals = plot_df.groupby('channel_name')['diff'].sum()
        top_channels = growth_totals.sort_values(ascending=False).head(top_n).index
        plot_df = plot_df[plot_df['channel_name'].isin(top_channels)]

        fig = px.line(plot_df, 
                      x='date', 
                      y='diff', 
                      color='channel_name',
                      color_discrete_map=self.channel_colors,
                      title=f"Top {top_n} YouTube Growth (Last {days} Days)",
                      template='plotly_dark',
                      labels={'diff': 'Daily View Gain', 'date': 'Date', 'channel_name': 'Channel'})

        fig.update_layout(
            hovermode='x unified',
            legend_title_text='Channels (Click to toggle)',
            font=dict(family="Arial", size=12)
        )

        # Save Interactive HTML
        html_path = self.output_dir / 'yt_interactive_report.html'
        fig.write_html(str(html_path))
        
        # Save Static PNG/PDF
        png_path = self.output_dir / f"yt_growth_{self.date_str}.png"
        pdf_path = self.output_dir / f"yt_growth_{self.date_str}.pdf"

        try:
            fig.write_image(str(png_path), width=1200, height=800)
            fig.write_image(str(pdf_path))
            logger.info(f"Reports saved to:\n- {html_path}\n- {png_path}\n- {pdf_path}")
        except Exception as e:
            logger.warning(f"Could not save PNG/PDF: {e}. If desired, run: pip install kaleido")
            logger.info(f"Interactive HTML report saved to: {html_path}")

    def run(self, days=14, plot=False, top_n=20): 
        if plot:
            self.data = self.load_all_data()
            self.generate_plot(days, top_n=top_n)
        else:
            self.update_data()

    def extract_handle(self, url): 
        url = url.split('?')[0].split('#')[0].rstrip('/')
        match = re.search(r'@([\w.-]+)', url)
        return match.group(1) if match else url.split('/')[-1]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--days", type=int, default=14)
    parser.add_argument("--plot", action="store_true", help="Generate interactive growth plot")
    parser.add_argument("--top", type=int, default=20, help="Number of top growth channels to show in plot")
    args = parser.parse_args()
    YouTubeChannelCompare().run(days=args.days, plot=args.plot, top_n=args.top)
