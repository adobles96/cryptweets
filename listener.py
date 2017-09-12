import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
from keys import *

auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_secret)

api = tweepy.API(auth)

coins = ['bitcoin', 'ethereum']

class FreqListener(StreamListener):
    """FreqListener extends StreamListener."""

    def on_data(self, data):
        try:
            #with open('python.json', 'a') as f:
            #    f.write(data)
            print(json.loads(data)['text'])
            print('retweeted_status' in json.loads(data))
            return True
        except BaseException as e:
            print("Error on_data: {}".format(str(e)))
        return True

    def on_error(self, status_code):
        print('ERROR:', status_code)
        if status_code == 420:
            return False
        return True

if __name__ == '__main__':
    twitter_stream = Stream(auth, FreqListener())
    twitter_stream.filter(track=coins)


# DATABASE DESIGN:
# tweets table:
# tweet consists of: id     date    text    user    coordinates     coin
# users table:
#
