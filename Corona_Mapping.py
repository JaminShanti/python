#!/usr/bin/env python
# coding: utf-8

# In[1]:


import plotly.express as px
import pandas as pd
import time
import json
from urllib.request import urlopen


# In[2]:


#state_names = ['Virginia','North Carolina', 'South Carolina']
state_names = ["Alabama", "Arkansas", "Arizona", "California", "Colorado", "Connecticut", "District ", "of Columbia", "Delaware", "Florida", "Georgia", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]
missing_fips = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/03-30-2020.csv')
df_sample = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/07-19-2020.csv')
df_sample['Death_Percentage'] = (df_sample['Deaths'] / df_sample['Confirmed']) *100
df_sample_r = df_sample[df_sample['Province_State'].isin(state_names)].fillna(0)
missing_fips['Death_Percentage'] = (missing_fips['Deaths'] / missing_fips['Confirmed']) *100
missing_fips_r = missing_fips[missing_fips['Province_State'].isin(state_names)].fillna(0)
missing_fips_r = missing_fips_r[~missing_fips_r.FIPS.isin(df_sample_r.FIPS)].dropna()
df_sample_r = pd.concat([df_sample_r, missing_fips_r])


# In[3]:


df_sample_r.loc[df_sample_r.Admin2 == 'Highland']


# In[4]:


date = "07-19-2020"
df_sample_r = df_sample_r[~df_sample_r.FIPS.isin([0])]
#df_sample_r = df_sample_r[df_sample_r.Confirmed > 300]
#values = df_sample_r['Confirmed'].tolist()
values = df_sample_r['Deaths'].tolist()
#values = df_sample_r['Active'].tolist()
#values = df_sample_r['Death_Percentage'].tolist()
fips = df_sample_r['FIPS'].tolist()


# In[5]:


sum(values)/len(values)


# In[6]:


colorscale = [
    'rgb(68.0, 1.0, 84.0)',
    'rgb(66.0, 64.0, 134.0)',
    'rgb(38.0, 130.0, 142.0)',
    'rgb(63.0, 188.0, 115.0)',
    'rgb(216.0, 226.0, 25.0)'
]


# In[7]:


# Load US counties GeoJSON for Plotly Express choropleth
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

# Build choropleth using Plotly Express (only change from original)
# Compute state totals for hover info
state_totals = df_sample_r.groupby('Province_State')['Deaths'].sum()
df_sample_r = df_sample_r.copy()
df_sample_r['State_Deaths_Total'] = df_sample_r['Province_State'].map(state_totals)

# Create discrete bins for improved readability
bins_edges = [1, 5, 25, 100, 500, float('inf')]
bin_labels = ['1-5', '6-25', '26-100', '101-500', '500+']
# Start with all zeros labeled explicitly
df_sample_r['Deaths_Bin'] = '0'
mask_pos = df_sample_r['Deaths'] > 0
if mask_pos.any():
    df_sample_r.loc[mask_pos, 'Deaths_Bin'] = pd.cut(
        df_sample_r.loc[mask_pos, 'Deaths'],
        bins=bins_edges,
        labels=bin_labels,
        right=True,
        include_lowest=True
    ).astype(str)

# Discrete color palette (purple -> blue -> teal -> green -> yellow), plus gray for 0
bin_order = ['0'] + bin_labels
color_map = {
    '0': '#444444',
    '1-5': '#5b2a86',
    '6-25': '#3b4cc0',
    '26-100': '#1fa187',
    '101-500': '#55c667',
    '500+': '#fde725'
}

fig = px.choropleth(
    df_sample_r,
    geojson=counties,
    locations='FIPS',
    color='Deaths_Bin',
    scope='usa',
    color_discrete_map=color_map,
    category_orders={'Deaths_Bin': bin_order},
    labels={'Deaths_Bin': 'County deaths (bins) — %s' % date},
    custom_data=['Province_State','Admin2','Deaths','FIPS','State_Deaths_Total','Deaths_Bin']
)
# Match county outline from original and enrich hover
fig.update_traces(marker_line_width=0.5, marker_line_color='rgb(255,255,255)')
fig.update_traces(hovertemplate='State: %{customdata[0]}<br>'
                               'County: %{customdata[1]}<br>'
                               'FIPS: %{customdata[3]}<br>'
                               'County deaths: %{customdata[2]:,}<br>'
                               'State deaths total: %{customdata[4]:,}'
                               '<extra></extra>')

# Switch hover to show both the exact count and the bin label; use categorical legend
fig.update_traces(hovertemplate='State: %{customdata[0]}<br>'
                               'County: %{customdata[1]}<br>'
                               'FIPS: %{customdata[3]}<br>'
                               'County deaths: %{customdata[2]:,} (%{customdata[5]})<br>'
                               'State deaths total: %{customdata[4]:,}'
                               '<extra></extra>')

# Legend styling (categorical legend for bins)
fig.update_layout(
    legend_title_text='County deaths (bins) — %s' % date,
    legend_traceorder='normal'
)


# In[8]:


fig.update_layout(
    legend_x=0,
    annotations=[{
        'x': -0.12,
        'y': 1.0,
        'xref': 'paper',
        'yref': 'paper',
        'xanchor': 'left',
        'showarrow': False
    }],
    template='plotly_dark'
)

# Export PNG image
output_path = f"covid_choropleth_{date}.png"
try:
    fig.write_image(output_path, width=1200, height=800, scale=2)
    print(f"Wrote {output_path}")
except Exception as e:
    print(f"Image export failed: {e}")


# In[9]:


df_sample_r.nlargest(25, 'Deaths')[[
    'Admin2','Province_State','Deaths'
]]


# In[ ]:



