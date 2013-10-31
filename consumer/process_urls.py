# Pulls messages out of mongo queue to pbe processed

import sys, time, threading, pymongo
from twitter_processing import processURL

class ProcessUrls():
    def __init__(self, pg_conn, mongo_conn, count=1000, hosts=[], process_fn=None):
        self.pg_conn = pg_conn
        self.mongo_conn = mongo_conn
        self.count = count
        self.hosts = hosts
        self.process_fn = process_fn
        self.start()

    def start(self):
        # Fix AttributeError: _strptime_time
        time.strptime("20100202", "%Y%m%d")

        while 1:
            thread_max = 50
            tt = 0
            no_url_count = 0
            #for tweet in self.mongo_conn.tweets.find({'url_id':{'$exists':False}}, timeout=False).limit(self.count):
            for tweet in self.mongo_conn.tweets.find({'url_id':{'$exists':False}}, timeout=False).sort('_id',pymongo.DESCENDING).limit(self.count):

                if tt % 100 == 0:
                    print "[process_urls.processURLs]: %d total; %d threads" % (tt, threading.active_count())

                if 'entities' in tweet:

                    if self.pg_conn and self.mongo_conn:
                        #print "[process_urls.processURLs]: Sending - %s" % len(self.hosts)
                        th = threading.Thread(target=processURL, args=(self.pg_conn, self.mongo_conn, tweet, None, self.hosts, False,))
                        th.daemon = True
                        th.start()
                else:
                    print "[process_urls.processURLs]: no entities"
                    no_url_count += 1
                    self.mongo_conn.tweets.remove({'id':tweet['id']})

                tt += 1

                while threading.active_count() > thread_max:
                    time.sleep(1)

            print "[process_urls.processURLs]: Processed %d tweets." % tt
            print '[process_urls.processURLs]: Finished processing the queue.\nWait 30 seconds before continuing...'

            for i in range(30):
                time.sleep(1)

