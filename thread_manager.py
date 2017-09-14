import multiprocessing
import threading
import time
from keys import *
import tweepy
from listener import FreqListener
import db_handler as dbh
import re
from datetime import datetime





auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_secret)

api = tweepy.API(auth)

coins = ['bitcoin, ethereum']

tweet_table_template = '(id_str VARCHAR(20) NOT NULL, created_at TIME WITH TIME ZONE, text TEXT NOT NULL, user_info TEXT NOT NULL, retweets INTEGER, coins TEXT, language TEXT, lat REAL, long REAL, witheld_in_countries TEXT, place TEXT)'

# tweet_table_template = '(text TEXT)'


def _start_and_wait_analyzer():

    def analyzer_t():
        """Analyzer thread function"""
        print("Handler: running, simulating my DB work for 5 seconds.")
        time.sleep(5)
        print("Handler: told main Im done with my work")

    analyzer = threading.Thread(target=analyzer_t)
    analyzer.start()
    print("Main: waiting for analyzer to say it finished")
    analyzer.join()
    print("Main: analyzer says its done, lets start again!")


def _set_timer():

    def null_funtion():
        return

    x = datetime.today()
    # y = x.replace(day=x.day + 1, hour=1, minute=0, second=0, microsecond=0)
    y = x.replace(day=x.day, hour=x.hour, minute=x.minute, second=(x.second + 10) % 60, microsecond=x.microsecond)
    delta_t = y - x
    print(delta_t)
    secs = delta_t.seconds + 1
    t = threading.Timer(secs, null_funtion)
    t.start()
    t.join()


def run():
    """
    This is the manager thread that will manage the listener (who is the thread that listens for Twitter data and sends
    this data to our database. It will also manage the db_handler, which is the thread that does clean up on our
    database after every cycle so we limit the amount of memory stored in our database.  Check our their docs for more
    details.
    :return: None
    """

    def twitter_listener_t(_db_handler, _table_name):
        """Listener thread function"""

        def unpacker(tweet):
            # TODO check all not null

            unpacked_list = []
            unpacked_format = '('

            unpacked_list.append(tweet['id_str'])  # id_str TEXT
            unpacked_format += '%s'

            unpacked_list.append(re.search('[0-9]{2}:[0-9]{2}:[0-9]{2} \+[0-9]{4}',
                                         tweet['created_at']).group(0)) # create_at TIME WITH TIME ZONE
            unpacked_format += ',%s'

            unpacked_list.append(tweet['text'])
            unpacked_format += ',%s'

            unpacked_list.append(str(tweet['user']))  # TODO get relevant info
            unpacked_format += ',%s'

            unpacked_format += ',' + str(tweet['retweet_count'])  # TODO make function to get actual number of retweets

            #unpacked_list.append(get_coins) # TODO implement get_coins
            unpacked_list.append('[bitcoin,ethereum]')
            unpacked_format += ',%s'

            if tweet['lang'] is not None:
                unpacked_list.append(tweet['lang'])
                unpacked_format += ',%s'
            else:
                unpacked_format += 'NULL'

            if tweet['coordinates'] is not None:
                unpacked_format += ',' + str(tweet['coordinates']['coordinates'][1])  # lat
                unpacked_format += ',' + str(tweet['coordinates']['coordinates'][0])  # long
            else:
                unpacked_format += ',NULL,NULL'

            if 'withheld_in_countries' in tweet:
                unpacked_list.append(str(tweet['withheld_in_countries']))
                unpacked_format += ',%s'
            else:
                unpacked_format += ',NULL'

            if tweet['place'] is not None:
                unpacked_list.append(str(tweet['place']))
                unpacked_format += ',%s'
            else:
                unpacked_format += ',NULL'

            unpacked_format += ')'
            return unpacked_format, unpacked_list

        def unpacker_test(tweet):
            return '(%s)', [tweet['text']]

        stream = tweepy.Stream(auth, FreqListener(_db_handler, _table_name, unpacker))
        stream.filter(track=coins)

    # Pre initialization
    # timer_lock = threading.Event()
    my_db_handler = dbh.Handler('cryptweets_test', 'lfvarela')
    listener_running = False
    i = 0

    while True:
        try:
            table_name = 'tweets_day_' + str(i)

            print("Main: creating table")

            with my_db_handler:
                my_db_handler.create_table(table_name, tweet_table_template)

                listener = multiprocessing.Process(target=twitter_listener_t, args=(my_db_handler, table_name,))
                listener.start()
                listener_running = True

                print("Main: Simulating one day")
                _set_timer()

                print("Main: terminating listener")
                listener.terminate()
                print("Main: Listener terminated")
                listener.join()
                listener_running = False
                print("Main: telling analyzer to go")
                _start_and_wait_analyzer()

            if i == 5:
                break

            i += 1
        except KeyboardInterrupt:
            if listener_running:
                listener.terminate()
                listener.join()
            break


if __name__ == '__main__':
    run()
