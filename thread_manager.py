import multiprocessing
import threading
import time
from keys import *
import tweepy
from listener import FreqListener

auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_secret)

api = tweepy.API(auth)

coins = ['obama']


def run():
    """
    This is the manager thread that will manage the listener (who is the thread that listens for Twitter data and sends
    this data to our database. It will also manage the db_handler, which is the thread that does clean up on our
    database after every cycle so we limit the amount of memory stored in our database.  Check our their docs for more
    details.
    :return: None
    """

    def twitter_listener_t():
        """Listener thread function"""
        stream = tweepy.Stream(auth, FreqListener())
        stream.filter(track=coins)

    def analyzer_t():
        """Analyzer thread function"""
        print("Handler: running, simulating my DB work for 5 seconds.")
        time.sleep(5)
        print("Handler: told main Im done with my work")

    # Manager thread: --------------------------------------------------------------------------------------------------

    listener_running = False
    while True:
        try:
            listener = multiprocessing.Process(target=twitter_listener_t)
            listener.start()
            listener_running = True

            print("Main: Simulate a day goes by, sleep for 10 seconds (24 hrs)")
            time.sleep(10)

            print("Main: terminating listener")
            listener.terminate()
            listener.join()
            listener_running = False
            print("Main: telling handler to go")

            handler = threading.Thread(target=analyzer_t)
            handler.start()

            print("Main: waiting for handler to say it finished")
            handler.join()
            print("Main: handler says its done, lets start again!")

            print("Main: waiting for handler to say it finished")
            print("Main: handler says its done, lets start again!")
        except KeyboardInterrupt:
            if listener_running:
                listener.terminate()
                listener.join()
            break


if __name__ == '__main__':
    run()
