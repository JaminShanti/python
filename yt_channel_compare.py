import yaml
import requests
import json
import re
import pandas as pd
from os import path
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
import datetime as dt

register_matplotlib_converters()

last_many_days = 7
today = dt.date.today()

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
    url = value['url']
    page = requests.get(url, headers={
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'}).text
    viewCountText = json.loads(re.findall(r'\"viewCountText\":({.*views\"}]}),', page)[0])
    total_views_today = viewCountText['runs'][0]['text']
    total_views_today = int(total_views_today.replace(',', ''))
    print('the total views for %s today are: %s' % (key, total_views_today))
    new_row = {'channel_name': key, 'date': date, 'day_of_week': day_of_week, 'total_views_today': total_views_today}
    data = data.append(new_row, ignore_index=True)

data = data.drop_duplicates(['channel_name','date'],keep='first')
data.to_csv(csv_file_name, encoding='utf-8')
data['date'] = pd.to_datetime(data['date'])

data = data.sort_values(by=['channel_name', 'date'])
data['diff'] = data.groupby(['channel_name'])['total_views_today'].diff().fillna(0)
data = data.sort_values(by=['date','channel_name'])

r = pd.date_range(start=data['date'].min(), end=data['date'].max())
channel_names = data['channel_name'].unique()
idx = pd.MultiIndex.from_product((r, channel_names), names=['date', 'channel_name'])
data = data.set_index(['date', 'channel_name']).reindex(idx, fill_value=0).reset_index()
data.set_index('date',inplace=True)
data = data.loc[today - dt.timedelta(days=last_many_days):today]
plt.style.use('dark_background')
fig, ax = plt.subplots(figsize=(16, 5))
for title, group in data.groupby('channel_name')['diff']:
    ax.plot(group, label=title, color=channels[title]['color'],linewidth=3.0)

myFmt = mdates.DateFormatter('%m-%d')
ax.xaxis.set_major_formatter(myFmt)
ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
fig.autofmt_xdate()
plt.legend(bbox_to_anchor=(0.5, 1.30), ncol=4 , loc='upper center', borderaxespad=0.,fancybox=True, shadow=True, prop={"size":'xx-large'})
plt.savefig("yt_output\yt_channel_compare_%s.png" % date,  bbox_inches='tight')
