###### author details: Taegyoon Kim, taegyoon@psu.edu
###### environment: Python 3.7.6, macOS BigSur
###### purpose: This script is used to scrap timeline using "diss_morality_helper.py"
###### last edit: 20 Jun 2021


##### packages

import os
import pandas as pd
import json
import tweepy
from datetime import datetime, timedelta
from pytz import timezone


##### run helper functions

path_helper = '/Users/taegyoon/Google Drive/diss_morality/script/'
os.chdir(path_helper)
%run diss_morality_helper.py


##### load handle data

path_handle = '/Users/taegyoon/Google Drive/diss_morality/data/handles/' 
handles_117th = pd.read_csv(path_handle  + 'handles_117th.csv',
                            index_col=0
                            )


##### scrape timeline and write as csv


for i in handles_117th['handle_1']:

    try:
        
        print(f"\n\n\nWorking on {handles_117th['state'][i]} {handles_117th['handle_1'][i]} ({i+1}/{len(handles_117th)})\n")

        '''get user timeline and record current EST time'''
        all_tweets = get_all_tweets(handles_117th['handle_1'][i])
        current_time_est = datetime.now(timezone('EST')).strftime('%Y%m%d%H%M')
        
        '''write a set of tweets into json'''
        
        with open('/Users/taegyoon/Google Drive/diss_morality/all_timeline/' + str(current_time_est) + '-' + str(handles_117th['handle_1'][i]) + '-Timeline.json', 'w') as file:
            for tweet in all_tweets:
                json.dump(tweet._json, file)
                file.write('\n')

        '''transform a set of tweets into pd dataframe / write as a CSV'''
        data = timeline_to_dataframe(all_tweets)
        path_timeline = '/Users/taegyoon/Google Drive/diss_morality/data/all_timeline/'
        data.to_csv(path_timeline + str(current_time_est) + '-' + str(handles_117th['handle_1'][i]) + '-Timeline.csv')

    except tweepy.error.TweepError as e:

        print(e)
