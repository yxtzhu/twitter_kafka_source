# -*- coding: utf-8 -*-
# this script is to stream data from Twitter API and push the records to kafka

#import libraries
import twitter
import re
import os
import pickle
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

# authenticate Twitter API
def get_auth():
    # read authentication information from file
    if not os.path.exists('secret_twitter_credentials.pkl'):
        Twitter={}
        Twitter['Consumer Key'] = ''
        Twitter['Consumer Secret'] = ''
        Twitter['Access Token'] = ''
        Twitter['Access Token Secret'] = ''
        with open('secret_twitter_credentials.pkl','wb') as f:
            pickle.dump(Twitter, f)
    else:
        Twitter=pickle.load(open('secret_twitter_credentials.pkl','rb'))
    
    # authenticate with Twitter API
    auth = twitter.oauth.OAuth(Twitter['Access Token'],
                           Twitter['Access Token Secret'],
                           Twitter['Consumer Key'],
                           Twitter['Consumer Secret'])
    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api


# Format datetime
def normalize_timestamp(t):
    mytime = datetime.strptime(t, "%a %b %d %H:%M:%S +0000 %Y") #Sun Apr 15 11:25:39 +0000 2018
    mytime -= timedelta(hours=4)   # tweets are timestamped in GMT timezone, while I am in -4 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S")) 
    

# remove link and special characters from tweets using regex
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

# Get tweet data
def get_tweets(twitter_api, producer, topic_name):
    #set topic and number of tweets
    twitter_query = "facebook"
    number = 100
    
    #search Twitter API
    result = twitter_api.search.tweets(q=twitter_query, result_type='mixed', lang='en', count=number, tweet_mode='extended')
    statuses = result['statuses']

    # create tweet data and push to Kafka producer
    for status in statuses:
        entry = ''
        entry += (str(status['id_str']) + ";")
        entry += (str(normalize_timestamp(str(status['created_at']))) + ";")
        entry += (str(status['user']['followers_count']) + ";")
        entry += (str(status['user']['favourites_count']) + ";")
        entry += (str(status['retweet_count']) + ";")
        entry += (str(status['user']['location']) + ";")
        entry += (str(clean_tweet(status['full_text'])) + ";")
        producer.send(topic_name, str.encode(entry))    

# set schedule for periodic fetching
def periodic_work(interval):
    twitter_api = get_auth()
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic_name = 'facebook'
    
    while True:
        get_tweets(twitter_api, producer, topic_name)
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60*1)