#!/usr/bin/env python
# coding: utf-8

# In[1]:


import plotly.figure_factory as ff
import pandas as pd
import time


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


date = time.strftime("%Y-%m-%d")
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


fig = ff.create_choropleth(
    fips=fips, values=values,
    scope=state_names, county_outline={'color': 'rgb(255,255,255)', 'width': 0.5},
    #legend_title='Confirmed Corona Cases USA %s Source: John Hopkins' % date
    legend_title='Confirmed Corona Deaths USA %s Source: John Hopkins' % date
    #legend_title='Confirmed Corona Active USA %s Source: John Hopkins' % date
)


# In[8]:


fig.update_layout(
    legend_x = 0,
    annotations = {'x': -0.12, 'xanchor': 'left'},
    template='plotly_dark'
)


# In[9]:


df_sample_r.nlargest(25, 'Deaths')[['Admin2','Province_State','Deaths']]


# In[ ]:




