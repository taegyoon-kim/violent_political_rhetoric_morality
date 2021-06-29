###### author details: Taegyoon Kim, taegyoon@psu.edu
###### environment: Python 3.7.6, macOS BigSur
###### purpose: This script is a helper that defines two functions: a) continuous collection of timeline beyond 200 (pagination) b) tranforming tweet objects into pd a dataframe
###### last edit: 24 Jun 2021


##### developer credentials

consumer_key = ''
consumer_secret = ""
access_key = ""
access_secret = ""


##### timeline scrape

def get_all_tweets(screen_name):
    
    '''authorize twitter, initialize tweepy'''
    import tweepy
    auth = tweepy.OAuthHandler(consumer_key,
                               consumer_secret
                               )
    auth.set_access_token(access_key,
                          access_secret
                          )
    api = tweepy.API(auth,
                     wait_on_rate_limit=True, 
                     wait_on_rate_limit_notify=True
                     )
    
    '''initialize a list to hold all the tweepy Tweets'''
    alltweets = []  
    
    '''make initial request for most recent tweets (200 is the maximum allowed count)'''
    new_tweets = api.user_timeline(screen_name = screen_name,
                                   count=200,
                                   #exclude_replies=False,
                                   tweet_mode = "extended"
                                   )
    
    '''save most recent tweets'''
    alltweets.extend(new_tweets)
    
    '''save the id of the oldest tweet less one'''
    oldest = alltweets[-1].id - 1
    
    '''keep grabbing tweets until there are no tweets left to grab'''
    while len(new_tweets) > 0:
        print(f"getting tweets before {oldest}")
        new_tweets = api.user_timeline(screen_name = screen_name, # all subsiquent requests use the max_id param to prevent duplicates
                                       count = 200,
                                       max_id = oldest,
                                       #exclude_replies=False,
                                       tweet_mode = "extended"
                                       )
        
        alltweets.extend(new_tweets) # save most recent tweets
        
        oldest = alltweets[-1].id - 1 # update the id of the oldest tweet less one
        
        print(f"...{len(alltweets)} tweets downloaded so far")

    pass
    
    return alltweets


##### tweeet objects to dataframe

def timeline_to_dataframe(all_tweets):
    
    import numpy as np
    import pandas as pd 

    data = {'created_at':[],
            'favorite_count':[],
            'full_text':[], 
            'geo':[],
            'id':[],
            'id_str':[],
            'in_reply_to_screen_name':[],
            'in_reply_to_status_id':[],
            'in_reply_to_status_id_str':[],
            'in_reply_to_user_id':[],
            'in_reply_to_user_id_str':[],
            'lang':[],
            'place':[],
            'retweet_count':[],
            'truncated': [],
            'user_id':[],
            'user_id_str':[],
            'user_screen_name':[],
            
            'status_is_retweet':[],
            'status_is_quote':[],
            
            'retweeted_status_created_at':[],
            'retweeted_status_favorite_count':[],
            'retweeted_status_full_text':[],
            'retweeted_status_geo':[],
            'retweeted_status_id':[],
            'retweeted_status_id_str':[],
            'retweeted_status_in_reply_to_screen_name':[],
            'retweeted_status_in_reply_to_status_id':[],
            'retweeted_status_in_reply_to_status_id_str':[],
            'retweeted_status_in_reply_to_user_id':[],
            'retweeted_status_in_reply_to_user_id_str':[],
            'retweeted_status_is_quote_status':[],
            'retweeted_status_lang':[],
            'retweeted_status_place':[],
            'retweeted_status_retweet_count':[],
            'retweeted_status_truncated':[],
            'retweeted_status_user_id':[],
            'retweeted_status_user_id_str':[],
            'retweeted_status_user_screen_name':[],
            
            'quoted_status_created_at':[],
            'quoted_status_favorite_count':[],
            'quoted_status_full_text':[],
            'quoted_status_geo':[],
            'quoted_status_id':[],
            'quoted_status_id_str':[],
            'quoted_status_in_reply_to_screen_name':[],
            'quoted_status_in_reply_to_status_id':[],
            'quoted_status_in_reply_to_status_id_str':[],
            'quoted_status_in_reply_to_user_id':[],
            'quoted_status_in_reply_to_user_id_str':[],
            'quoted_status_is_quote_status':[],
            'quoted_status_lang':[],
            'quoted_status_place':[],
            'quoted_status_retweet_count':[],
            'quoted_status_truncated':[],
            'quoted_status_user_id':[],
            'quoted_status_user_id_str':[],
            'quoted_status_user_screen_name':[]}
    
    
    for tweet in all_tweets:
        
        """fields for original author/tweet"""
        try:
            data['created_at'].append(tweet._json["created_at"])
        except:
            data['created_at'].append(None)
        try:
            data['favorite_count'].append(tweet._json["favorite_count"])
        except:
            data['favorite_count'].append(None)
        try:
            data['full_text'].append(tweet._json["full_text"])
        except:
            data['full_text'].append(None)
        try:
            data['geo'].append(tweet._json["geo"])
        except:
            data['geo'].append(None)   
        try:
            data['id'].append(tweet._json["id"])
        except:
            data['id'].append(None)
        try:
            data['id_str'].append(tweet._json["id_str"])
        except:
            data['id_str'].append(None)   
        try:
            data['in_reply_to_screen_name'].append(tweet._json["in_reply_to_screen_name"])
        except:
            data['in_reply_to_screen_name'].append(None)   
        try:
            data['in_reply_to_status_id'].append(tweet._json["in_reply_to_status_id"])
        except:
            data['in_reply_to_status_id'].append(None)   
        try:
            data['in_reply_to_status_id_str'].append(tweet._json["in_reply_to_status_id_str"])
        except:
            data['in_reply_to_status_id_str'].append(None)   
        try:
            data['in_reply_to_user_id'].append(tweet._json["in_reply_to_user_id"])
        except:
            data['in_reply_to_user_id'].append(None)   
        try:
            data['in_reply_to_user_id_str'].append(tweet._json["in_reply_to_user_id_str"])
        except:
            data['in_reply_to_user_id_str'].append(None)   
        try:
            data['lang'].append(tweet._json["lang"])
        except:
            data['lang'].append(None)
        try:
            data['place'].append(tweet._json["place"])
        except:
            data['place'].append(None)
        try:
            data['retweet_count'].append(tweet._json["retweet_count"])
        except:
            data['retweet_count'].append(None)   
        try:
            data['truncated'].append(tweet._json["truncated"])
        except:
            data['truncated'].append(None)   
        try:
            data['user_id'].append(tweet._json["user"]["id"])
        except:
            data['user_id'].append(None)   
        try:
            data['user_id_str'].append(tweet._json["user"]["id_str"])
        except:
            data['user_id_str'].append(None)  
        try:
            data['user_screen_name'].append(tweet._json["user"]["screen_name"])
        except:
            data['user_screen_name'].append(None)   
        
        """retweet/quote identifiers"""
        try:
            if ('retweeted_status' in tweet._json):
                data['status_is_retweet'].append(True)
            else:
                data['status_is_retweet'].append(False)
        except:
            data['status_is_retweet'].append(None)
        try:
            data['status_is_quote'].append(tweet._json["is_quote_status"])
        except:
            data['status_is_quote'].append(None)        
        
        """fields for retweet"""
        try:
            data['retweeted_status_created_at'].append(tweet._json['retweeted_status']['created_at'])
        except:
            data['retweeted_status_created_at'].append(None)   
        try:
            data['retweeted_status_favorite_count'].append(tweet._json['retweeted_status']['created_at'])
        except:
            data['retweeted_status_favorite_count'].append(None)   
        try:
            data['retweeted_status_full_text'].append(tweet._json['retweeted_status']['full_text'])
        except:
            data['retweeted_status_full_text'].append(None)  
        try:
            data['retweeted_status_geo'].append(tweet._json['retweeted_status']['geo'])
        except:
            data['retweeted_status_geo'].append(None)   
        try:
            data['retweeted_status_id'].append(tweet._json['retweeted_status']['id'])
        except:
            data['retweeted_status_id'].append(None)   
        try:
            data['retweeted_status_id_str'].append(tweet._json['retweeted_status']['id_str'])
        except:
            data['retweeted_status_id_str'].append(None)  
        try:
            data['retweeted_status_in_reply_to_screen_name'].append(tweet._json['retweeted_status']['in_reply_to_screen_name'])
        except:
            data['retweeted_status_in_reply_to_screen_name'].append(None)   
        try:
            data['retweeted_status_in_reply_to_status_id'].append(tweet._json['retweeted_status']['in_reply_to_status_id'])
        except:
            data['retweeted_status_in_reply_to_status_id'].append(None)   
        try:
            data['retweeted_status_in_reply_to_status_id_str'].append(tweet._json['retweeted_status']['in_reply_to_status_id_str'])
        except:
            data['retweeted_status_in_reply_to_status_id_str'].append(None)  
        try:
            data['retweeted_status_in_reply_to_user_id'].append(tweet._json['retweeted_status']['in_reply_to_user_id'])
        except:
            data['retweeted_status_in_reply_to_user_id'].append(None)   
        try:
            data['retweeted_status_in_reply_to_user_id_str'].append(tweet._json['retweeted_status']['in_reply_to_user_id_str'])
        except:
            data['retweeted_status_in_reply_to_user_id_str'].append(None)   
        try:
            data['retweeted_status_is_quote_status'].append(tweet._json['retweeted_status']['in_reply_to_user_id_str'])
        except:
            data['retweeted_status_is_quote_status'].append(None)  
        try:
            data['retweeted_status_lang'].append(tweet._json['retweeted_status']['lang'])
        except:
            data['retweeted_status_lang'].append(None)   
        try:
            data['retweeted_status_place'].append(tweet._json['retweeted_status']['place'])
        except:
            data['retweeted_status_place'].append(None)   
        try:
            data['retweeted_status_retweet_count'].append(tweet._json['retweeted_status']['retweet_count'])
        except:
            data['retweeted_status_retweet_count'].append(None)  
        try:
            data['retweeted_status_truncated'].append(tweet._json['retweeted_status']['truncated'])
        except:
            data['retweeted_status_truncated'].append(None)   
        try:
            data['retweeted_status_user_id'].append(tweet._json['retweeted_status']['user']['id'])
        except:
            data['retweeted_status_user_id'].append(None)   
        try:
            data['retweeted_status_user_id_str'].append(tweet._json['retweeted_status']['user']['id_str'])
        except:
            data['retweeted_status_user_id_str'].append(None)  
        try:
            data['retweeted_status_user_screen_name'].append(tweet._json['retweeted_status']['user']['screen_name'])
        except:
            data['retweeted_status_user_screen_name'].append(None)   
            
            
        """fields for quote"""
        if ('retweeted_status' in tweet._json):
            try:
                data['quoted_status_created_at'].append(tweet._json['retweeted_status']["quoted_status"]['created_at'])
            except:
                data['quoted_status_created_at'].append(None)   
            try:
                data['quoted_status_favorite_count'].append(tweet._json['retweeted_status']["quoted_status"]['favorite_count'])
            except:
                data['quoted_status_favorite_count'].append(None)   
            try:
                data['quoted_status_full_text'].append(tweet._json['retweeted_status']["quoted_status"]['full_text'])
            except:
                data['quoted_status_full_text'].append(None)  
            try:
                data['quoted_status_geo'].append(tweet._json['retweeted_status']["quoted_status"]['geo'])
            except:
                data['quoted_status_geo'].append(None)   
            try:
                data['quoted_status_id'].append(tweet._json['retweeted_status']["quoted_status"]['id'])
            except:
                data['quoted_status_id'].append(None)   
            try:
                data['quoted_status_id_str'].append(tweet._json['retweeted_status']["quoted_status"]['id_str'])
            except:
                data['quoted_status_id_str'].append(None)  
            try:
                data['quoted_status_in_reply_to_screen_name'].append(tweet._json['retweeted_status']["quoted_status"]['in_reply_to_screen_name'])
            except:
                data['quoted_status_in_reply_to_screen_name'].append(None)   
            try:
                data['quoted_status_in_reply_to_status_id'].append(tweet._json['retweeted_status']["quoted_status"]['in_reply_to_status_id'])
            except:
                data['quoted_status_in_reply_to_status_id'].append(None)   
            try:
                data['quoted_status_in_reply_to_status_id_str'].append(tweet._json['retweeted_status']["quoted_status"]['in_reply_to_status_id_str'])
            except:
                data['quoted_status_in_reply_to_status_id_str'].append(None)  
            try:
                data['quoted_status_in_reply_to_user_id'].append(tweet._json['retweeted_status']["quoted_status"]['in_reply_to_user_id'])
            except:
                data['quoted_status_in_reply_to_user_id'].append(None)   
            try:
                data['quoted_status_in_reply_to_user_id_str'].append(tweet._json['retweeted_status']["quoted_status"]['in_reply_to_user_id_str'])
            except:
                data['quoted_status_in_reply_to_user_id_str'].append(None)   
            try:
                data['quoted_status_is_quote_status'].append(tweet._json['retweeted_status']["quoted_status"]['is_quote_status'])
            except:
                data['quoted_status_is_quote_status'].append(None)  
            try:
                data['quoted_status_lang'].append(tweet._json['retweeted_status']["quoted_status"]['lang'])
            except:
                data['quoted_status_lang'].append(None)   
            try:
                data['quoted_status_place'].append(tweet._json['retweeted_status']["quoted_status"]['place'])
            except:
                data['quoted_status_place'].append(None)   
            try:
                data['quoted_status_retweet_count'].append(tweet._json['retweeted_status']["quoted_status"]['retweet_count'])
            except:
                data['quoted_status_retweet_count'].append(None)  
            try:
                data['quoted_status_truncated'].append(tweet._json['retweeted_status']["quoted_status"]['truncated'])
            except:
                data['quoted_status_truncated'].append(None)   
            try:
                data['quoted_status_user_id'].append(tweet._json['retweeted_status']["quoted_status"]['user']['id'])
            except:
                data['quoted_status_user_id'].append(None)   
            try:
                data['quoted_status_user_id_str'].append(tweet._json['retweeted_status']["quoted_status"]['user']['id_str'])
            except:
                data['quoted_status_user_id_str'].append(None)  
            try:
                data['quoted_status_user_screen_name'].append(tweet._json['retweeted_status']["quoted_status"]['user']['screen_name'])
            except:
                data['quoted_status_user_screen_name'].append(None)    
        else:
            try:
                data['quoted_status_created_at'].append(tweet._json["quoted_status"]['created_at'])
            except:
                data['quoted_status_created_at'].append(None)   
            try:
                data['quoted_status_favorite_count'].append(tweet._json["quoted_status"]['favorite_count'])
            except:
                data['quoted_status_favorite_count'].append(None)   
            try:
                data['quoted_status_full_text'].append(tweet._json["quoted_status"]['full_text'])
            except:
                data['quoted_status_full_text'].append(None)  
            try:
                data['quoted_status_geo'].append(tweet._json["quoted_status"]['geo'])
            except:
                data['quoted_status_geo'].append(None)   
            try:
                data['quoted_status_id'].append(tweet._json["quoted_status"]['id'])
            except:
                data['quoted_status_id'].append(None)   
            try:
                data['quoted_status_id_str'].append(tweet._json["quoted_status"]['id_str'])
            except:
                data['quoted_status_id_str'].append(None)  
            try:
                data['quoted_status_in_reply_to_screen_name'].append(tweet._json["quoted_status"]['in_reply_to_screen_name'])
            except:
                data['quoted_status_in_reply_to_screen_name'].append(None)   
            try:
                data['quoted_status_in_reply_to_status_id'].append(tweet._json["quoted_status"]['in_reply_to_status_id'])
            except:
                data['quoted_status_in_reply_to_status_id'].append(None)   
            try:
                data['quoted_status_in_reply_to_status_id_str'].append(tweet._json["quoted_status"]['in_reply_to_status_id_str'])
            except:
                data['quoted_status_in_reply_to_status_id_str'].append(None)  
            try:
                data['quoted_status_in_reply_to_user_id'].append(tweet._json["quoted_status"]['in_reply_to_user_id'])
            except:
                data['quoted_status_in_reply_to_user_id'].append(None)   
            try:
                data['quoted_status_in_reply_to_user_id_str'].append(tweet._json["quoted_status"]['in_reply_to_user_id_str'])
            except:
                data['quoted_status_in_reply_to_user_id_str'].append(None)   
            try:
                data['quoted_status_is_quote_status'].append(tweet._json["quoted_status"]['is_quote_status'])
            except:
                data['quoted_status_is_quote_status'].append(None)  
            try:
                data['quoted_status_lang'].append(tweet._json["quoted_status"]['lang'])
            except:
                data['quoted_status_lang'].append(None)   
            try:
                data['quoted_status_place'].append(tweet._json["quoted_status"]['place'])
            except:
                data['quoted_status_place'].append(None)   
            try:
                data['quoted_status_retweet_count'].append(tweet._json["quoted_status"]['retweet_count'])
            except:
                data['quoted_status_retweet_count'].append(None)  
            try:
                data['quoted_status_truncated'].append(tweet._json["quoted_status"]['truncated'])
            except:
                data['quoted_status_truncated'].append(None)   
            try:
                data['quoted_status_user_id'].append(tweet._json["quoted_status"]['user']['id'])
            except:
                data['quoted_status_user_id'].append(None)   
            try:
                data['quoted_status_user_id_str'].append(tweet._json["quoted_status"]['user']['id_str'])
            except:
                data['quoted_status_user_id_str'].append(None)  
            try:
                data['quoted_status_user_screen_name'].append(tweet._json["quoted_status"]['user']['screen_name'])
            except:
                data['quoted_status_user_screen_name'].append(None)    
    
    data = pd.DataFrame(data)   
    data.fillna(value=np.nan, inplace=True)
    
    return data



