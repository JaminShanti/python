import yaml
import requests
import json
import re
import pandas as pd
from os import path
import time
import matplotlib.pyplot as plt

date = time.strftime("%Y-%m-%d")
day_of_week = time.strftime("%A")

with open(r'yt_channel_config.yaml') as file:
    channels = yaml.full_load(file)

csv_file_name = 'yt_channel_compare.csv'

if path.exists(csv_file_name):
    print('CSV file found: %s' % csv_file_name)
    data = pd.read_csv(csv_file_name, index_col=0, thousands=',')
else:
    print('No CSV found, creating new %s' % csv_file_name)
    data = pd.DataFrame(columns=['channel_name', 'date', 'day_of_week', 'total_views_today'])

for key, value in channels.items():
    url = value
    page = requests.get(url, headers={
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'}).text
    # yt_inital_data =json.loads(re.findall(r'window\["ytInitialData"\] = (.*);', page)[0])
    # total_views_today = yt_inital_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][5]['tabRenderer']['content']['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0]['channelAboutFullMetadataRenderer']['viewCountText']['runs'][0]['text']
    viewCountText = json.loads(re.findall(r'\"viewCountText\":({.*views\"}]}),', page)[0])
    total_views_today = viewCountText['runs'][0]['text']
    total_views_today = int(total_views_today.replace(',', ''))
    print('the total views for %s today are: %s' % (key, total_views_today))
    new_row = {'channel_name': key, 'date': date, 'day_of_week': day_of_week, 'total_views_today': total_views_today}
    data = data.append(new_row, ignore_index=True)

data = data.drop_duplicates()

data.to_csv(csv_file_name, encoding='utf-8')

data = data.sort_values(by=['channel_name', 'date'])
data['diff'] = data.groupby(['channel_name'])['total_views_today'].diff().fillna(0)
data.set_index('date', inplace=True)
data.groupby('channel_name')['diff'].plot(legend=True)
plt.xticks(rotation=70)
plt.savefig("yt_output\yt_channel_compare_%s.png" % date)
