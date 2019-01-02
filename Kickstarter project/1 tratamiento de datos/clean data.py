import numpy as np
import pandas as pd
from datetime import datetime

df = pd.read_csv('kickstarter_joined_full5.csv')

# backers_count,blurb,category,converted_pledged_amount,country,created_at,creator,currency,currency_symbol,
# currency_trailing_code,current_currency,deadline,disable_communication,fx_rate,goal,id,is_starrable,launched_at,
# name,photo,pledged,profile,slug,source_url,spotlight,staff_pick,state,state_changed_at,static_usd_rate,urls,usd_pledged,
# usd_type,location,friends,is_backing,is_starred,permissions,start_backers_count,start_converted_pledged_amount,
# start_pledged,file_name,category_name,category_pos,category_slugs,category_subslugs,converted_goal,duration

columns = ['backers_count', 'usd_pledged', 'duration', 'staff_pick', 'state',
           'start_backers_count', 'category_pos', 'usd_goal', 'start_usd_pledged_amount', 'completed_time']

countries = pd.get_dummies(df['country'])
categories = pd.get_dummies(df['category_slugs'])

launched_months = pd.Series([datetime.utcfromtimestamp(t).month for t in df['launched_at']], name='launched_month')

ks = df[columns]

ks = pd.concat([ks, countries, categories, launched_months], axis=1)

ks = ks[(ks['state'] == 'successful') | (ks['state'] == 'failed')]
ks['state'] = ks['state'].map(lambda x: 1 if x == 'successful' else 0)
ks['staff_pick'] = ks['staff_pick'].map(lambda x: 1 if x else 0)

ks = ks[ks['usd_goal'] < np.percentile(ks['usd_goal'], 80)]

ks.to_csv('kickstarter_clean2.csv', header=True, mode='w', index=False)

# ks

