#captures Twitter stream to mongo db

import os, sys, threading, tweepy, logging, datetime, time
from pymongo import errors

from tweepy.utils import import_simplejson, urlencode_noplus
json = import_simplejson()


running_stats = {
    'bad_url_count' : 0,
    'ignored_count' : 0,
    'deleted_count' : 0,
    'missed_count'  : 0,
    'no_url_count'  : 0,
    'total_count'   : 0
}

class KonfabTweetListener(tweepy.StreamListener):

    def __init__(self, api=None, pg_con=None, mongo_db=None, hosts=[]):
        #self.api = api or tweepy.API()
        self.pg_con = pg_con
        self.mongo_db = mongo_db
        self.hosts = hosts
        self.mongo_errors = 0
        #self.logger = logging.getLogger('konfab-streaming')

        stats_filename = '/usr/tmp/konfab_tweet_counts_%s.txt' % ( int(time.time()) )
        self.stats_file = open(stats_filename, 'w')

        self.start_time = time.time()
        super(KonfabTweetListener, self).__init__(api)

    def on_connect(self):
        #self.logger.debug('[KonfabTweetListener]: Connected')
        return True # Don't kill the stream

    def on_limit(self, track):
        return True # Don't kill the stream

    # incoming tweets
    def on_data(self, data):
        global running_stats

        tweet = json.loads(data)

        running_stats['total_count'] += 1

        if 'limit' in tweet:
            running_stats['missed_count'] += tweet['limit']['track']
        elif 'delete' in tweet: running_stats['deleted_count'] += 1
        elif 'entities' not in tweet or len(tweet['entities']['urls']) == 0: running_stats['no_url_count'] += 1
        else:

            tweet['processed'] = 0
            if tweet['entities']:
                if self.mongo_db:
                    try:
                        self.mongo_db.tweets.save(tweet)
                    except errors.ConnectionFailure:
                        self.stop_streaming()
                        return False
                    except:
                        self.mongo_errors +=1
                        print "[KonfabTweetListener]: Error - writing tweet to mongo"
                        if self.mongo_errors > 20:
                            self.stop_streaming()
                            return False


        # write stats
        # TODO: replace with logging module
        if self.stats_file:
            now = time.time()
            elapsed = int(now - self.start_time)

            if elapsed > 60: # 1 min
                self.start_time = time.time()

                msg = "ts=%s, %s \n" % (
                    now,
                    ', '.join("%s=%r" % (key,val) for (key,val) in running_stats.iteritems())
                    )

                try:
                    self.stats_file.write(msg)
                    #print "[KonfabTweetListener]: %s" % msg
                except:
                    print "[KonfabTweetListener]: Error - writing stats"
                    #self.logger.error("[KonfabTweetListener]: writing stats")



        return True # Don't kill the stream


    def stop_streaming(self):
        print "[KonfabTweetListener]: Critical - too many mongo write errors or bad error code"
        #os.system('cd /home/ec2-user/konfab/ && python service.py --stop streaming')
        sys.exit(0)

    def on_status(self, status):
        return True # Don't kill the stream

    def on_error(self, status_code):
        print '[KonfabTweetListener]: Error - %s' % status_code
        #self.logger.error('[KonfabTweetListener]: %s' % status_code)
        try:
            code_int = int(status_code)
        except:
            code_int = 0

        if code_int == 401 or code_int == 420 or code_int == 406:
            self.stop_streaming()
            return False
        else:
            return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

class Streaming:
    #buffer_size = 1800
    #snooze_time = 30.0

    def start(self, pg_connection, mongo_db, keywords, hosts):

        consumer_key = os.environ['TWITTER_CONSUMER_KEY']
        consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
        access_key = os.environ['TWITTER_ACCESS_KEY']
        access_secret = os.environ['TWITTER_ACCESS_SECRET']

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret, None)
        auth.set_access_token(access_key, access_secret)

        keywords = keywords[0:400]

        hose = tweepy.Stream(auth, KonfabTweetListener(pg_con=pg_connection, mongo_db=mongo_db, hosts=hosts), buffer_size=1800, snooze_time=30.0)
        hose.filter(track=keywords)



