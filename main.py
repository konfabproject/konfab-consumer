#   Copyright 2013 Kon*Fab
#
#
#   konfab-consumer is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   konfab-consumer  is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with konfab-consumer.  If not, see <http://www.gnu.org/licenses/>.
#

import os, sys
import os.path
from os.path import dirname, join
import argparse, re, time, datetime, logging, signal, time, thread
import psycopg2.extras
from pymongo import MongoClient
from bson.objectid import ObjectId

from consumer.streaming import Streaming
from consumer.db import pg_connect
from consumer.twitter_processing import processURL
from consumer.process_urls import ProcessUrls
from consumer.geocode import Geocode
from consumer.readable_text import ReadableText
from consumer.predict_nlp import PredictNLP
from consumer.media import *



keywords = []
hosts = []
pg_connection = None
mongo_db = None


# read .env file
def read_env(file_location='.env'):
    """ Pulled from Honcho code with minor updates, reads local default
        environment variables from a .env file located in the project root
        directory.
    """

    try:
        with open(file_location) as f:
            content = f.read()
    except IOError:
        content = ''

    for line in content.splitlines():
        m1 = re.match(r'\A([A-Za-z_0-9]+)=(.*)\Z', line)
        if m1:
            key, val = m1.group(1), m1.group(2)
            m2 = re.match(r"\A'(.*)'\Z", val)
            if m2:
                val = m2.group(1)
            m3 = re.match(r'\A"(.*)"\Z', val)
            if m3:
                val = re.sub(r'\\(.)', r'\1', m3.group(1))
            os.environ.setdefault(key, val)


# get our postgres connection
def getDBConnection(setIso=True):
    """Returns the connection object
    """
    conn = pg_connect(
        os.environ['POSTGRES_HOSTNAME'],
        os.environ['POSTGRES_DATABASE'],
        os.environ['POSTGRES_USERNAME'],
        os.environ['POSTGRES_PASSWORD'],
        os.environ['POSTGRES_PORT']
        )

    if setIso:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def getMongoConnection():
    #conn = pymongo.Connection("localhost", 27017)
    client = MongoClient('localhost', 27017)
    return client.stories


def getIdTimestamp(days):
    """ Get epoch time in seconds
        Convert seconds to hex with zeroes for MongoDB
    """
    sec = time.mktime((datetime.datetime.now() - datetime.timedelta(days=days)).timetuple())
    id_str = "%X0000000000000000" % sec
    return id_str



def do_streaming():
    """ start the Twitter streaming
    """
    if pg_connection and mongo_db:

        streaming = Streaming()
        streaming.start(pg_connection, mongo_db, keywords, hosts)
    else:
        pass


def do_process_urls():
    """ Gets tweets stored in Mongo that haven't been processed yet.
        Creates new threads to get the URL.
    """

    if pg_connection and mongo_db:
        print '[Main.process_urls]:'
        ProcessUrls(pg_connection, mongo_db, count=1000, hosts=hosts, process_fn=None)
    else:
        pass


def do_geocode(limit=1000):
    """ Geocodes messages
    """

    if pg_connection and mongo_db:
        print "Starting geocoding"
        Geocode(pg_connection, mongo_db, limit)
    else:
        pass


def do_predict_nlp(limit=100):
    """ Tries to figure out the topic for articles
    """
    if pg_connection:
        # this looks for a topic from the url
        #PredictNLP(pg_connection, limit,)

        # this looks for an article from the html text
        ReadableText(pg_connection, 300, './consumer/nlp_clf.pickle')
    else:
        pass


def do_remove_oldest_tweets(days=30):
    """ Removes the tweets from MongoDB that are older than the days given
    """
    if mongo_db:
        id_str = getIdTimestamp(days)
        mongo_db.tweets.remove({'_id':{'$lt':ObjectId(id_str)}})
    else:
        pass


def check_pid(pidfile, pid, force):
    """ Check that the pid file still exists
        If exists, don't run the script and exit
        Else write pid file and continue running
    """
    if force:
        pass
    elif os.path.isfile(pidfile):
        sys.exit(0)
    else:
        file(pidfile, 'w').write(pid)

# sadly not using right now
def configLogger(lg, level, name):
    f = '/usr/tmp/konfab-log-%s.log' % name
    lg.setLevel(level)
    handler = logging.FileHandler(f)
    handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    lg.addHandler(handler)

    return lg


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--stream', help='Download Twitter stream and store the tweets', action="store_true")
    parser.add_argument('--urls', help='Process urls and store into Postgres. Checks that each URL exists and adds to the database.', action="store_true")
    parser.add_argument('--geocode', help='Geocode user given locations from message', action="store_true")
    parser.add_argument('--topic', help='Tries to figure out the topic for articles', action="store_true")
    parser.add_argument('--force', help='Run even if another consumer.py process is running', action="store_true")
    args = parser.parse_args()

    # bail on no arguments
    if not args.stream and not args.urls and not args.geocode and not args.topic:
        print '[Main.py]: Error - needing an argument: --stream or --process or --urls or --downloadhtml or --geocode'
        sys.exit(0)


    # reads a .env file
    read_env()

    # set globals
    keywords, hosts, cities = get_keywords_hosts()

    # db's
    pg_connection = getDBConnection()
    mongo_db = getMongoConnection()

    loglevel = logging.DEBUG

    # start appropriate routine
    try:
        if args.stream:
            do_streaming()
        elif args.urls:
            do_process_urls()
        elif args.geocode:
            do_geocode()
        elif args.topic:
            do_predict_nlp(1000)
            do_remove_oldest_tweets(30)

    except KeyboardInterrupt:
        pass
    except:
        pass
    finally:
        sys.exit(0)

