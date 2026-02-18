#!/usr/bin/env python3
import yaml
import requests
import json
import re
import pandas as pd
import os
import time
import logging
import argparse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
import datetime as dt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

register_matplotlib_converters()

class YouTubeChannelCompare:
    """
    A class to track and compare YouTube channel views over time.
    """
    def __init__(self, config_file='yt_channel_config.yaml', csv_file='yt_channel_compare.csv', output_dir='yt_output'):
        self.config_file = config_file
        self.csv_file = csv_file
        self.output_dir = output_dir
        self.channels = self.load_config()
        self.today = dt.date.today()
        self.date_str = self.today.strftime("%Y-%m-%d")
        self.day_of_week = self.today.strftime("%A")
        
        # Ensure output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def load_config(self):
        """
        Loads channel configuration from a YAML file.
        """
        if not os.path.exists(self.config_file):
            logger.error(f"Config file {self.config_file} not found.")
            return {}
        
        try:
            with open(self.config_file, 'r') as file:
                return yaml.full_load(file) or {}
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            return {}

    def load_data(self):
        """
        Loads existing data from CSV or creates a new DataFrame.
        """
        if os.path.exists(self.csv_file):
            logger.info(f"CSV file found: {self.csv_file}")
            try:
                return pd.read_csv(self.csv_file)
            except Exception as e:
                logger.error(f"Error reading CSV file: {e}")
                return pd.DataFrame(columns=['channel_name', 'date', 'day_of_week', 'total_views_today'])
        else:
            logger.info(f"No CSV found, creating new {self.csv_file}")
            return pd.DataFrame(columns=['channel_name', 'date', 'day_of_week', 'total_views_today'])

    def fetch_views(self, url):
        """
        Fetches the total view count for a given YouTube channel URL.
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            page_content = response.text
            
            # Try to find view count in simpleText
            match = re.search(r'\"viewCountText\":\{\"simpleText\":\"(.*?)\"\}', page_content)
            if match:
                view_text = match.group(1)
            else:
                # Fallback for different JSON structure
                match = re.search(r'\"viewCountText\":\{.*?\"text\":\"(.*?)\".*?\}', page_content)
                if match:
                    view_text = match.group(1)
                else:
                    logger.warning(f"Could not find view count for {url}")
                    return None

            # Clean up the string to get the integer
            # Remove ' views' and commas
            view_text = view_text.replace(' views', '').replace(',', '')
            return int(view_text)

        except Exception as e:
            logger.error(f"Error fetching views for {url}: {e}")
            return None

    def update_data(self):
        """
        Updates the dataset with today's view counts.
        """
        if not self.channels:
            logger.warning("No channels configured.")
            return

        df = self.load_data()
        new_rows = []

        for name, info in self.channels.items():
            url = info.get('url')
            if not url:
                logger.warning(f"No URL found for channel {name}")
                continue
            
            logger.info(f"Fetching data for {name}...")
            views = self.fetch_views(url)
            
            if views is not None:
                logger.info(f"Total views for {name}: {views}")
                new_row = {
                    'channel_name': name,
                    'date': self.date_str,
                    'day_of_week': self.day_of_week,
                    'total_views_today': views
                }
                new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # Use concat instead of append (deprecated)
            df = pd.concat([df, new_df], ignore_index=True)
            
            # Remove duplicates, keeping the latest entry for the day
            df = df.drop_duplicates(subset=['channel_name', 'date'], keep='last')
            
            # Save back to CSV
            df.to_csv(self.csv_file, index=False, encoding='utf-8')
            self.data = df
        else:
            self.data = df

    def generate_plot(self, days=7):
        """
        Generates a plot of view changes over the last N days.
        """
        if self.data.empty:
            logger.warning("No data available to plot.")
            return

        df = self.data.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate daily difference
        df = df.sort_values(by=['channel_name', 'date'])
        df['diff'] = df.groupby('channel_name')['total_views_today'].diff().fillna(0)
        
        # Filter for the requested date range
        start_date = pd.Timestamp(self.today) - dt.timedelta(days=days)
        df = df[df['date'] >= start_date]
        
        if df.empty:
            logger.warning("No data in the specified date range.")
            return

        # Pivot for plotting or iterate groups
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(16, 5))
        
        for name, group in df.groupby('channel_name'):
            color = self.channels.get(name, {}).get('color', 'white')
            ax.plot(group['date'], group['diff'], label=name, color=color, linewidth=3.0)

        # Formatting
        myFmt = mdates.DateFormatter('%m-%d')
        ax.xaxis.set_major_formatter(myFmt)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        fig.autofmt_xdate()
        
        plt.legend(bbox_to_anchor=(0.5, 1.15), ncol=4, loc='upper center', 
                   borderaxespad=0., fancybox=True, shadow=True, prop={"size": 'large'})
        
        output_path = os.path.join(self.output_dir, f"yt_channel_compare_{self.date_str}.png")
        plt.savefig(output_path, bbox_inches='tight')
        logger.info(f"Plot saved to {output_path}")
        plt.close()

    def run(self, days=7):
        """
        Main execution method.
        """
        self.update_data()
        self.generate_plot(days)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Track and compare YouTube channel views.")
    parser.add_argument("-c", "--config", default="yt_channel_config.yaml", help="Path to configuration YAML file")
    parser.add_argument("-d", "--days", type=int, default=7, help="Number of days to plot")
    parser.add_argument("-o", "--output", default="yt_output", help="Output directory for plots")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    comparator = YouTubeChannelCompare(config_file=args.config, output_dir=args.output)
    comparator.run(days=args.days)
