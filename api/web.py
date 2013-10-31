# API interface

import os, sys
import os.path
import pymongo
from bson.objectid import ObjectId

import tornado.ioloop
import tornado.web

import simplejson as json
import time
import datetime
import pytz

sys.path.append(os.path.abspath('../'))
import main as konfab
from db_web import *


EXPIRATION = 10 * 60
pg_connection = None
mongo_db = None

import memcache

class KonfabRequestHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.memc = memcache.Client(['127.0.0.1:11211'], debug=1)

    def set_default_headers(self):
        self.add_header('Access-Control-Allow-Origin', self.request.headers.get('Origin', '*'))


class GetCity(KonfabRequestHandler):
    def get(self):
        arguments = self.request.arguments

        if 'id' in arguments:
            woe_id = str(int(self.get_argument('id')))
        else:
            woe_id = None

        if not woe_id:
            self.write(json.dumps({'error': 'invalid parameters'}))
            return

        if 'limit' in arguments:
            try:
                limit = int(limit)
            except:
                limit = 200
        else:
            limit = 200

        if limit > 500 or limit < 0:
            limit = 200

        if 'force' in arguments:
            skip_cache = True
        else:
            skip_cache = False

        cache_file = 'city-%s' % woe_id

        if skip_cache:
            city = None
        else:
            city = self.memc.get(cache_file)

        if city:
            self.write(json.dumps(city, use_decimal=True))
            return
        else:
            city = None
            if not pg_connection:
                self.write(json.dumps({'error': 'no database connection'}))
                return

            try:
                city = pg_get_city(pg_connection, woe_id, time_offset=24, from_time=None, limit=limit)
            except:
                city = None

            if city:
                self.memc.set(cache_file, city, EXPIRATION)
                self.write(json.dumps(city, use_decimal=True))
            else:
                self.write(json.dumps({'error': 'query failed'}))

        self.finish()

#
class GetOverall(KonfabRequestHandler):

    def get(self):
        arguments = self.request.arguments

        if 'force' in arguments:
            skip_cache = True
        else:
            skip_cache = False

        if skip_cache:
            overall = None
        else:
            overall = self.memc.get('overall')

        if overall:
            self.write(json.dumps(overall, use_decimal=True))
            return
        else:
            overall = None
            if not pg_connection:
                self.write(json.dumps({'error': 'no database connection'}))
                return

            try:
                overall = pg_get_overall(pg_connection, time_offset=24, from_time=None)
            except:
                overall = None

            if overall:
                self.memc.set('overall', overall, EXPIRATION)
                self.write(json.dumps(overall, use_decimal=True))
            else:
                self.write(json.dumps({'error': 'query failed'}))

        self.finish()

#
# kind of funky way to tell if twitter streaming is down
class GetLastTweet(KonfabRequestHandler):
    def get(self):
        #mongo_conn = main.getMongoConnection()
        if not mongo_db:
            self.write('0')
            return

        latest = list(mongo_db.tweets.find().sort([('_id',-1)]).limit(1))
        if latest:
            threshold =  int(60 * 10) # 10 minutes
            utc_tz = pytz.timezone('UTC')

            created = ObjectId(latest[0]['_id']).generation_time
            current_time = datetime.datetime.now(utc_tz)
            elapsed = current_time - created
            seconds = int(elapsed.seconds)

            if seconds < threshold:
                self.write('1')
            else:
                self.write('0')
        else:
            self.write('0')

###################################################################################################################
application = tornado.web.Application([
        ("/city", GetCity),
        ("/overall", GetOverall),
        ("/last", GetLastTweet)
    ],
    debug=True,
    gzip=True
)


def main():
    # reads a .env file
    konfab.read_env('../.env')

    global pg_connection, mongo_db
    pg_connection = konfab.getDBConnection(False)
    mongo_db = konfab.getMongoConnection()

    if pg_connection and mongo_db:
        application.listen(8080)
        tornado.ioloop.IOLoop.instance().start()
    else:
        sys.exit(0)

if __name__ == "__main__":

    main()


