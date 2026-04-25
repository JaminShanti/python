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
    def __init__(self, 
                 config_file='yt_channel_config.yaml', 
                 data_dir='yt_stats_daily', 
                 output_dir='yt_visuals',
                 video_data_dir='yt_video_stats',
                 days=14,
                 top_n=10,
                 max_workers=10,
                 timeout=15):
        # Configuration and Directories
        self.config_file = config_file
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.video_data_dir = Path(video_data_dir)
        
        # Control Values
        self.days = days
        self.top_n = top_n
        self.max_workers = max_workers
        self.timeout = timeout
        
        # Thresholds for Hot/Cold
        self.hot_threshold = 3.0
        self.cold_threshold = 0.2
        
        # Initialization
        self.channels = self.load_config()
        self.channel_colors = {self.extract_handle(u): i.get('color', 'white') for u, i in self.channels.items() if self.extract_handle(u)}
        self.today = dt.date.today()
        self.date_str, self.day_of_week = self.today.strftime("%Y-%m-%d"), self.today.strftime("%A")
        
        self.data_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.video_data_dir.mkdir(exist_ok=True)
        self.data = self.load_all_data()

    def load_config(self):
        """Loads channel configuration from a YAML file."""
        if not os.path.exists(self.config_file): return {}
        try:
            with open(self.config_file, 'r', encoding='utf-8') as file: return yaml.full_load(file) or {}
        except Exception as e: logger.error(f"Error loading config: {e}"); return {}

    def load_all_data(self):
        """Loads all daily CSV files and combines them into one DataFrame."""
        all_dfs = []
        for file in sorted(self.data_dir.glob("*.csv")):
            try: all_dfs.append(pd.read_csv(file))
            except Exception as e: logger.error(f"Error reading {file}: {e}")
        if not all_dfs: return pd.DataFrame(columns=['channel_name', 'date', 'day_of_week', 'total_views_today'])
        return pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=['channel_name', 'date'], keep='last')

    def fetch_channel_data(self, url):
        """Fetches channel data with robust multi-pattern regex for layout changes."""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'Accept-Language': 'en-US,en;q=0.9'}
        try:
            # 1. Fetch Total Views (Try About page first, fallback to main page)
            u_about = url.rstrip('/') + '/about'
            response = requests.get(u_about, headers=headers, timeout=self.timeout)
            
            if response.status_code == 404:
                # Try main URL if /about 404s (sometimes happened with new handles)
                response = requests.get(url, headers=headers, timeout=self.timeout)
                if response.status_code == 404:
                    return None, [], {"status": "DELETED", "msg": "404 - Channel confirmed deleted or URL changed."}
            
            response.raise_for_status()
            html = response.text
            
            # Robust extraction logic: Try multiple common patterns
            views = None
            patterns = [
                r'\"viewCountText\":\{\"simpleText\":\"(.*?)\"\}',
                r'\"viewCountText\":\{.*?\"text\":\"(.*?)\".*?\}',
                r'\"viewCount\":\"(\d+)\"',
                r'viewCount\\\":\\\"(\d+)\\\"',
                r'([0-9,]+) views'
            ]
            
            for p in patterns:
                match = re.search(p, html)
                if match:
                    try:
                        val = match.group(1).replace(' views', '').replace(',', '').replace('\\"', '').replace('"', '')
                        if val.isdigit():
                            views = int(val)
                            break
                    except: continue

            # 2. Fetch Recent Videos
            u_videos = url.rstrip('/') + '/videos'
            response_videos = requests.get(u_videos, headers=headers, timeout=self.timeout)
            
            video_matches = re.findall(r'\"title\":\{\"runs\":\[\{\"text\":\"(.*?)\"\}\].*?\"viewCountText\":\{\"simpleText\":\"(.*?)\"\}', response_videos.text)
            
            hot_videos = []
            for title, v_text in video_matches[:5]:
                try:
                    v_val = v_text.replace(' views', '').replace(',', '')
                    if 'K' in v_val: count = float(v_val.replace('K', '')) * 1000
                    elif 'M' in v_val: count = float(v_val.replace('M', '')) * 1000000
                    else: count = float(v_val)
                    hot_videos.append({'title': title, 'views': int(count)})
                except: continue

            error_info = None
            if views is None:
                error_info = {"status": "LAYOUT", "msg": "Page loaded but all 5 regex patterns failed. YouTube layout changed significantly."}

            return views, hot_videos, error_info
            
        except requests.exceptions.Timeout:
            return None, [], {"status": "TIMEOUT", "msg": "Connection timed out."}
        except requests.exceptions.HTTPError as e:
            return None, [], {"status": "HTTP_ERROR", "msg": f"HTTP Error: {e.response.status_code}"}
        except Exception as e:
            return None, [], {"status": "ERROR", "msg": str(e)}

    def update_data(self):
        """Updates today's CSV file and reports on errors/deleted channels."""
        if not self.channels: return
        new_rows = []
        video_logs = []
        warnings = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.fetch_channel_data, u): u for u in self.channels.keys()}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]; handle = self.extract_handle(url)
                try:
                    views, videos, err = future.result()
                    if views is not None:
                        logger.info(f"Views for {handle}: {views}")
                        new_rows.append({'channel_name': handle, 'date': self.date_str, 'day_of_week': self.day_of_week, 'total_views_today': views})
                        if videos:
                            video_logs.append({'channel': handle, 'date': self.date_str, 'videos': videos})
                    
                    if err:
                        warnings.append({"handle": handle, "url": url, "status": err['status'], "msg": err['msg']})
                        
                except Exception as e: 
                    logger.error(f"Error processing {handle}: {e}")

        # Save Channel Stats
        if new_rows:
            today_csv, new_df = self.data_dir / f"yt_stats_{self.date_str}.csv", pd.DataFrame(new_rows)
            if today_csv.exists(): new_df = pd.concat([pd.read_csv(today_csv), new_df], ignore_index=True)
            new_df.drop_duplicates(subset=['channel_name'], keep='last').to_csv(today_csv, index=False, encoding='utf-8')

        # Print Warnings Report
        if warnings:
            print(f"\n--- ⚠️ WARNINGS & ACTIONS NEEDED ---")
            for w in sorted(warnings, key=lambda x: x['status']):
                icon = "❌" if w['status'] == "DELETED" else "🔧" if w['status'] == "LAYOUT" else "⏳" if w['status'] == "TIMEOUT" else "🚨"
                print(f"{icon} {w['handle']}: {w['status']} - {w['msg']}")
                if w['status'] == "DELETED":
                    print(f"   👉 ACTION: Remove from config or update URL: {w['url']}")
                elif w['status'] == "LAYOUT":
                    print(f"   👉 ACTION: Script failed to parse views. Verify manually: {w['url']}")
                print("-" * 50)

        if video_logs:
            log_file = self.video_data_dir / f"video_trends_{self.date_str}.json"
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(video_logs, f, indent=4)

        self.data = self.load_all_data()

    def generate_plot(self):
        """Generates Plotly graphs with Hot/Cold trend detection."""
        if self.data.empty: return
        df = self.data.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['channel_name', 'date'])
        df['diff'] = df.groupby('channel_name')['total_views_today'].diff().fillna(0)
        
        plot_df = df[df['date'] >= (pd.Timestamp(self.today) - dt.timedelta(days=self.days))]
        if plot_df.empty: return

        # Calculate Momentum (Current Gain vs 7-day Avg)
        avg_gain = plot_df.groupby('channel_name')['diff'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        plot_df['momentum'] = (plot_df['diff'] / avg_gain.replace(0, 1)).round(2)
        
        # Rank by total growth
        growth_totals = plot_df.groupby('channel_name')['diff'].sum().sort_values(ascending=False)
        top_list = growth_totals.head(self.top_n).index.tolist()
        bottom_list = growth_totals.tail(self.top_n).index.tolist()
        
        plot_channels = top_list + bottom_list
        plot_df = plot_df[plot_df['channel_name'].isin(plot_channels)]
        
        # Determine status indicators
        status_map = {}
        for chan in plot_channels:
            chan_data = plot_df[plot_df['channel_name'] == chan]
            if not chan_data.empty:
                last_momentum = chan_data.iloc[-1]['momentum']
                if last_momentum >= self.hot_threshold: status_map[chan] = "🔥 HOT"
                elif last_momentum <= self.cold_threshold: status_map[chan] = "❄️ COLD"
                else: status_map[chan] = "Steady"

        fig = px.line(plot_df, 
                      x='date', 
                      y='diff', 
                      color='channel_name',
                      hover_data={'momentum': True},
                      category_orders={'channel_name': top_list + bottom_list},
                      color_discrete_map=self.channel_colors,
                      title=f"YouTube Growth Momentum (Last {self.days} Days)",
                      template='plotly_dark',
                      labels={'diff': 'Daily View Gain', 'date': 'Date', 'channel_name': 'Channel', 'momentum': 'Momentum Score'})

        # Apply styling and status indicators
        for trace in fig.data:
            name = trace.name
            status = status_map.get(name, "Steady")
            trace.name = f"{name} ({status})"
            if name in top_list:
                trace.legendgroup = '1_top'
                trace.legendgrouptitle = dict(text=f"⭐ TOP {self.top_n}")
                trace.line.width = 4
            else:
                trace.legendgroup = '2_bottom'
                trace.legendgrouptitle = dict(text=f"<br>⚠️ BOTTOM {self.top_n}")
                trace.line.dash = 'dash'
                trace.line.width = 3

        fig.update_layout(hovermode='x unified', legend_traceorder="grouped", font=dict(family="Arial", size=12))

        # Save HTML & PNG
        html_path = self.output_dir / 'yt_interactive_report.html'
        fig.write_html(str(html_path))
        png_path = self.output_dir / f"yt_growth_{self.date_str}.png"
        fig.write_image(str(png_path), width=1200, height=800)

        # Print Momentum Report
        print(f"\n--- Momentum Report (Last {self.days} Days) ---")
        print(f"\n[🔥 HOT - GROWING FAST]")
        for c in [k for k,v in status_map.items() if "HOT" in v and k in top_list]:
            print(f"🚀 {c}: {status_map[c]} (Gain: +{growth_totals[c]:,.0f})")
            
        print(f"\n[❄️ COLD - SIGNIFICANT FALL-OFF]")
        for c in [k for k,v in status_map.items() if "COLD" in v]:
            print(f"📉 {c}: {status_map[c]} (Momentum dropped significantly)")

    def run(self, plot=False): 
        if plot: self.generate_plot()
        else: self.update_data()

    def extract_handle(self, url): 
        url = url.split('?')[0].split('#')[0].rstrip('/')
        match = re.search(r'@([\w.-]+)', url)
        return match.group(1) if match else url.split('/')[-1]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--days", type=int, default=14)
    parser.add_argument("-n", "--top_n", type=int, default=10)
    parser.add_argument("--plot", action="store_true", help="Generate interactive growth plot")
    args = parser.parse_args()
    
    YouTubeChannelCompare(days=args.days, top_n=args.top_n).run(plot=args.plot)
