###### author details: Taegyoon Kim, taegyoon@psu.edu
###### environment: Python 3.7.6, macOS BigSur
###### purpose: This script reads legislators' timeline tweets and subsets tweets published while in office based on the meta data for 117th Congress.
###### last edit: 14 Jun 2021



##### packages

import pandas as pd
import glob
from pytz import timezone
from datetime import datetime



##### load handle data

path_handle = '/Users/taegyoon/Google Drive/diss_morality/data/handles/' # https://github.com/taegyoon-kim/violent_political_rhetoric_morality/blob/main/data/handles_117th.csv
handles_117th = pd.read_csv(path_handle  + 'handles_117th.csv', index_col=0)



##### time format tranform for term start date and term end date

eastern = timezone('US/Eastern')
utc = timezone('UTC')

start_status_created_at_EST = []
end_status_created_at_EST = []

for i in handles_117th['start_date_term']:
  start_created_at = datetime.strptime(i, '%m/%d/%Y')
  start_est_created_at = eastern.localize(start_created_at)
  start_status_created_at_EST.append(start_est_created_at)
handles_117th['start_date_term_EST'] = start_status_created_at_EST

for i in handles_117th['end_date_term']:
  end_created_at = datetime.strptime(i, '%m/%d/%Y')
  end_est_created_at = eastern.localize(end_created_at)
  end_status_created_at_EST.append(end_est_created_at)
handles_117th['end_date_term_EST'] = end_status_created_at_EST



##### check if timelines are scraped without missing handles

path_timeline = '/Users/taegyoon/Google Drive/diss_morality/data/all_timeline/' 
timeline_files = glob.glob(path_timeline + '*.{}'.format('csv'))
timeline_files_handle = [x.split('-')[1] for x in timeline_files]
set([x for x in timeline_files_handle if timeline_files_handle.count(x) > 1])
set([x for x in handles_117th['handle_1'].tolist() if handles_117th['handle_1'].tolist().count(x) > 1])
set(handles_117th['handle_1']).difference(set(timeline_files_handle)) # 'RepChrisSmith', 'RepHastingsFL' are non-existant 
len(timeline_files_handle)
len(handles_117th['handle_1'])



##### timeline file time format tranform / subset

path_timeline_term = '/Users/taegyoon/Google Drive/diss_morality/data/all_timeline_term/'

for i in timeline_files:    

  df_timeline_max = pd.read_csv(i, 
                                index_col=0,
                                lineterminator='\n'
                                )

  eastern = timezone('US/Eastern')
  utc = timezone('UTC')
  created_at_UTC = []
  created_at_EST = []
  
  for j in df_timeline_max['created_at']:
    created_at = datetime.strptime(j, '%a %b %d %H:%M:%S +0000 %Y')
    utc_created_at = utc.localize(created_at)
    est_created_at = utc_created_at.astimezone(eastern)
    created_at_UTC.append(utc_created_at)
    created_at_EST.append(est_created_at)
  df_timeline_max['created_at_UTC'] = created_at_UTC
  df_timeline_max['created_at_EST'] = created_at_EST

  screen_name = df_timeline_max['user_screen_name'][0]
  df_timeline_max['start_date_term_EST'] = handles_117th[handles_117th['handle_1']==screen_name].iloc[0]['start_date_term_EST']
  df_timeline_max['end_date_term_EST'] = handles_117th[handles_117th['handle_1']==screen_name].iloc[0]['end_date_term_EST']
  
  df_timeline_max['created_after_term_start'] = df_timeline_max['created_at_EST'] >= df_timeline_max['start_date_term_EST'] 
  df_timeline_max['created_before_term_end'] = df_timeline_max['created_at_EST'] <= df_timeline_max['end_date_term_EST'] 
  
  df_timeline_max_during_term = df_timeline_max[(df_timeline_max.created_after_term_start == True) & (df_timeline_max.created_before_term_end == True)]
  
  df_timeline_max_during_term.to_csv(path_timeline_term + str(i).replace(".csv","") + '-term.csv')




