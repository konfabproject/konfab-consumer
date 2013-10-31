# Database related functions for konfab-consumer
# In need of some serious clean-up

import sys
import os
import psycopg2
import psycopg2.extras
from urlparse import urlparse
from psycopg2 import DataError, InternalError, DatabaseError, ProgrammingError
import time as ttime
import datetime


def pg_connect(host, db, user, pwd, port):
    return psycopg2.connect(host=host, database=db, user=user, password=pwd, port=port)

def pg_test_connection(host, db, user, pwd, port):
    try:
        con = psycopg2.connect(host=host, database=db, user=user, password=pwd, port=port)
        cur = con.cursor()
        cur.execute('SELECT version()')
        ver = cur.fetchone()
        print "SUCCESS: %s" % ver
    except psycopg2.DatabaseError, e:
        print 'ERROR: %s' % e
        sys.exit(1)
    finally:
        if con:
            con.close()

def get_cities_dict(con):
    cities = {}
    try:
        cur = con.cursor('dict_cursor',cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * from cities"
        cur.execute(query)
        for city in cur:
            cities[city['name']] = [city['latitude'],city['longitude']]
        cur.close()
    except psycopg2.DatabaseError, e:
        print 'ERROR: %s' % e
        pass
    finally:
        return cities

def get_cities(con):
    cities = []
    try:
        cur = con.cursor('dict_cursor',cursor_factory=psycopg2.extras.DictCursor)
        query = "SELECT * from cities"
        cur.execute(query)
        for row in cur:
            cities.append(row)
        cur.close()
    except psycopg2.DatabaseError, e:
        print 'ERROR: %s' % e
        pass
    finally:
        return cities


def update_city_since_id(con, name, id):
    try:
        q = """UPDATE cities SET since_id = %s WHERE name = %s"""
        cur = con.cursor()
        cur.execute(q,[id,name])
        con.commit()
        cur.close()
    except:
        pass


def check_if_media_url(con, url):

    result = ''

    try:
        cur = con.cursor()
        o = urlparse(url)
        o = o.netloc.strip()

        if o.find('www') >= 0:
            o = o.strip('www.').strip()

        cur.execute("SELECT url FROM mediaurls WHERE url='%s'"%(o))
        row = cur.fetchone()

        if row is None:
            result = ''
        else:
            result = row[0]
        cur.close()

    except:
        pass
    finally:
        return result


def save_unkown_urls(con, url):

    try:
        cur = con.cursor()
        #q = """INSERT INTO mediaunknown (url) VALUES (%s)"""

        q = """INSERT INTO mediaunknown (url) SELECT %s WHERE NOT EXISTS (SELECT url FROM mediaunknown WHERE url=%s)"""
        cur.execute(q,[url,url])
        con.commit()
        cur.close()
    except:
        pass


def get_html_exists(con, url_id):
    print url_id

    cur = con.cursor()

    q = """SELECT url_id, url FROM htmlpages WHERE url_id = %s"""

    #try:
    cur.execute(q,[url_id])
    row = cur.fetchone()
    #except DatabaseError as error:
        #print "Database Error: %s" % error
        #return None

    cur.close()
    if row: return True
    else: return False

# retrieve html for a given url_id
def get_html(con, url_id):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT * FROM htmlpages WHERE url_id = %s"""

    #try:
    cur.execute(q,[url_id])
    row = cur.fetchone()
    #except DataError as error:
        #print "Database Error: %s" % error
        #return None

    cur.close()
    return row

# saves html for a give url, url_id
def save_html(con, url_id, url, html):

    #html = html.encode('utf8')
    cur = con.cursor()

    q = """INSERT INTO htmlpages (url_id, url, html)
           SELECT %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM htmlpages WHERE url_id = %s)"""

    cur.execute(q,[url_id, url, html, url_id])
    con.commit()
    return True

    cur.close()

# delete a url from urls table by a url_id
def delete_url(con, url_id):
    cur = con.cursor()

    q = """DELETE FROM urls WHERE id = %s"""

    cur.execute(q, [url_id])
    con.commit()

    cur.close()

def delete_message(con, message_id):
    cur = con.cursor()

    q = """DELETE FROM messages WHERE id = %s"""

    cur.execute(q, [message_id])
    con.commit()

    cur.close()

# add attributes to a url in urls table
def save_url_attributes(con, url_id, title, description, thumbnail=''):
    cur = con.cursor()

    q = """UPDATE urls SET title = %s, description = %s, thumbnail = %s WHERE id = %s"""

    cur.execute(q, [title, description, thumbnail, url_id])
    con.commit()

    cur.close()

# if a url is not valid, saves it here for further examination if desired
# TODO: delete urls older than XXX
def save_error_urls(con, url):

    cur = con.cursor()

    q = """INSERT INTO urls_error (url) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM urls_error WHERE url=%s)"""

    try:
        cur.execute(q,[url, url])
        con.commit()
        cur.close()
    except:
        pass

# insert the article url into the urls table
# will later populate with details
def save_urls(con, url):

    cur = con.cursor()

    q = """INSERT INTO urls (url) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM urls WHERE url=%s)"""

    #try:
    cur.execute(q,[url,url])
    con.commit()
    #except (DataError, InternalError, DatabaseError) as e:
        #print "Database Error: %s" % str(e)
        #return None

    cur.close()
    id_of_new_row = get_url_id(con, url)
    if id_of_new_row and len(id_of_new_row) > 0:
        return id_of_new_row[0]
    else:
        return None

def save_readabletext(con, url_id, text, method):

    cur = con.cursor()

    q = """UPDATE htmlpages SET readabletext = %s WHERE url_id = %s"""
    cur.execute(q,[text, str(url_id)])
    row = get_url(con, url_id)
    if row and len(row) > 0:
        if not row['topic_method'] or row['topic_method'] == '':
            q = """UPDATE urls SET topic_method = %s WHERE id = %s"""
            cur.execute(q,[method, str(url_id)])
    con.commit()

    cur.close()

def save_urls_readinglevel(con, url_id, readinglevel):

    cur = con.cursor()

    q = """UPDATE urls SET readinglevel = %s WHERE id = %s"""
    #try:
    cur.execute(q,[readinglevel, url_id])
    con.commit()
    #except DataError:
        #print "Database Error"

    cur.close()

def save_hide_url(con, url_id, hide):

    cur = con.cursor()

    q = """UPDATE urls SET hide = %s WHERE id = %s"""

    cur.execute(q,[hide, url_id])

    con.commit()
    cur.close()

def save_url_topic(con, url_id, topic, method):
    """ method is used to know if a topic was added by a person or by an algorithm"""
    cur = con.cursor()

    q = """UPDATE urls
              SET topic = %s,
                  topic_method = %s
            WHERE id = %s"""

    cur.execute(q,[str(topic).lower(), method, str(url_id)])

    con.commit()
    cur.close()

def get_urls_with_no_text(con, limit):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT urls.id as url_id, urls.url
             FROM urls
             JOIN htmlpages
               ON urls.id = htmlpages.url_id
            WHERE topic_method IS NULL
              AND htmlpages.readabletext IS NULL
         ORDER BY urls.total DESC
            LIMIT %s""" % limit
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_urls_with_text(con, limit, method='human'):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT urls.id as url_id, urls.url, htmlpages.readabletext AS text, urls.topic
             FROM urls as urls, htmlpages as htmlpages
            WHERE urls.topic IS NOT NULL
              AND urls.topic_method = '%s'
              AND urls.topic != ''
              AND urls.topic != 'nonenglish'
              AND urls.topic != 'broken'
              AND htmlpages.readabletext IS NOT NULL
              AND htmlpages.readabletext != ''
              AND urls.id = htmlpages.url_id
            LIMIT %s""" % (method, limit)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_urls_with_text_no_topic(con, limit=100, method='human'):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT urls.id as url_id, urls.url, htmlpages.readabletext AS text, urls.title
             FROM urls
             JOIN htmlpages
               ON urls.id = htmlpages.url_id
            WHERE htmlpages.readabletext IS NOT NULL
              AND htmlpages.readabletext != ''
            LIMIT %s""" % limit
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_urls_no_topic(con, limit=100):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT id as url_id, url
             FROM urls
            WHERE topic IS NULL
         ORDER BY total DESC
            LIMIT %s""" % limit
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

# not including thumbnails as a criteria
# if you want it, add 'or thumbnail is null' to where clause
def get_urls_with_missing_attributes(con, limit = 100):
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    q = """SELECT id, url
            from urls
            where title is null or description is null
            ORDER BY id desc limit %s
        """ % limit

    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_urls_with_description(con, limit, method='meta'):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT id as url_id, url, topic, description, title
             FROM urls
            WHERE topic IS NOT NULL
              AND topic_method = '%s'
              AND topic != ''
              AND topic != 'nonenglish'
              AND topic != 'broken'
              AND title != ''
              AND title IS NOT NULL
              AND description != ''
              AND description IS NOT NULL
            LIMIT %s""" % (method, limit)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_urls_with_empty_topic(con, limit=100):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q ="""SELECT id as url_id
             FROM urls
            WHERE topic IS NULL
              AND total > 0
         ORDER BY total DESC
            LIMIT %s""" % limit
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_url_id(con, url):

    cur = con.cursor()

    q = """SELECT id, topic, readinglevel
             FROM urls
            WHERE url = %s
              AND hide IS NOT TRUE"""
    cur.execute(q, [url])
    rows = cur.fetchone()
    cur.close()
    if rows:
        return rows
    else:
        return False

# get a url out of urls table by url_id
def get_url(con, url_id):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT * FROM urls WHERE id = %s"""
    cur.execute(q, [url_id])
    row = cur.fetchone()

    cur.close()
    return row

def get_url_id_range(con, num=0):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if num:
        limit = " LIMIT %d" % num
    else:
        limit = ""

    q = """SELECT id, url, total
             FROM urls
            WHERE readinglevel IS NULL
         ORDER BY total DESC %s""" % limit
    cur.execute(q)
    rows = cur.fetchall()

    cur.close()
    return rows

def increment_url_tweet_count(con, url_id):
    url_id = str(url_id)
    cur = con.cursor()

    q = """UPDATE urls
              SET total = total + 1,
                  tweet_count = tweet_count + 1
            WHERE id = %s"""

    cur.execute(q, [url_id])
    con.commit()

    cur.close()
    return
def increment_url_konfab_count(con, url_id):
    url_id = str(url_id)
    cur = con.cursor()

    q = """UPDATE urls
              SET total = total + 1,
                  konfab_count = konfab_count + 1
            WHERE id = %s"""

    cur.execute(q, [url_id])
    con.commit()

    cur.close()
    return

# update overall time series for a url
def increment_tweets_timeseries(con, url_id, time):
    url_id = str(url_id)

    cur = con.cursor()

    q = """UPDATE url_counts
              SET total = total + 1,
                  tweet_count = tweet_count + 1
            WHERE url_id = %s
              AND time = %s;
           INSERT INTO url_counts (url_id, time, total, tweet_count)
           SELECT %s, %s, 1, 1
            WHERE NOT EXISTS (SELECT 1 FROM url_counts WHERE url_id = %s AND time = %s);
           """

    cur.execute(q, [url_id, time, url_id, time, url_id, time])
    con.commit()

    cur.close()
    return

def increment_konfab_timeseries(con, url_id, time):
    url_id = str(url_id)

    cur = con.cursor()

    q = """UPDATE url_counts
              SET total = total + 1,
                  konfab_count = konfab_count + 1
            WHERE url_id = %s
              AND time = %s;
           INSERT INTO url_counts (url_id, time, total, konfab_count)
           SELECT %s, %s, 1, 1
            WHERE NOT EXISTS (SELECT 1 FROM url_counts WHERE url_id = %s AND time = %s);
           """

    cur.execute(q, [url_id, time, url_id, time, url_id, time])
    con.commit()

    cur.close()
    return

def get_top_recent(con, time, limit):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT url_id,
                  SUM(total) AS total
             FROM url_counts
            WHERE time >= %s
         GROUP BY url_id
         ORDER BY total DESC
            LIMIT %s"""

    cur.execute(q, [time, limit])

    rows = cur.fetchall()
    cur.close()
    return rows

# hourly increments
def increment_geo_timeseries(con, url_id, time, county=None, city=None, neighborhood=None):
    #https://beagle.whoi.edu/redmine/projects/ibt/wiki/Summarizing_time_series_data_in_PostgreSQL
    url_id = str(url_id)

    cur = con.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    psycopg2.extras.register_hstore(cur)

    places = []

    if county:
        places.append(county)
    if city:
        places.append(city)
    if neighborhood:
        places.append(neighborhood)

    if len(places):
        for woeid in places:
            q = """UPDATE url_spatial_counts"""
            q += """ SET woeids = woeids || hstore('%s', CAST( CAST(coalesce(woeids->'%s', '0') AS integer) + 1  AS text))"""
            q += """ WHERE url_id = %s and time = %s;"""
            q += """INSERT INTO url_spatial_counts (url_id, time, woeids)"""
            q += """ SELECT %s, %s, hstore('%s', '1') WHERE NOT EXISTS (SELECT 1 FROM url_spatial_counts WHERE url_id = %s AND time = %s);"""

            #UPDATE url_spatial_counts SET woeids = woeids || hstore('90000', CAST( CAST(coalesce(woeids->'90000', '0') AS integer) + 1  AS text)) WHERE url_id = '2' and time = 2;
            #INSERT INTO url_spatial_counts (url_id, time, woeids) SELECT '2', 2, hstore('90000', '1') WHERE NOT EXISTS (SELECT 1 FROM url_spatial_counts WHERE url_id = '2' AND time = 2);

            cur.execute(q, [woeid, woeid, url_id, time, url_id, time, woeid, url_id, time ])
            con.commit()

    cur.close()

def get_urls_from_list(con, ids):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT id as url_id,
                  url,
                  title,
                  description,
                  readinglevel,
                  topic,
                  thumbnail
             FROM urls
            WHERE id in %s
              AND hide IS NOT TRUE"""

    cur.execute(q, [tuple(ids)])

    rows = cur.fetchall()
    cur.close()
    return rows

# store tweet that posted a url to a news article
def save_message(con, tweet_id, time, url_id, geo_type, geo_location, lat, lon, uuid, tweet_text='', tweet_userid='', tweet_retweet=0, tweet_favorite=0):
    now = datetime.datetime.now()

    cur = con.cursor()
    if lat and lon:
        latlon = """ST_SetSRID(ST_MakePoint(%s, %s), 4326)""" % (lon, lat)
    else:
        latlon = 'NULL'

    q = """INSERT INTO messages (
                tweet_id,
                url_id,
                time,
                geo_type,
                geo_location,
                lat,
                lon,
                latlon,
                konfab_uuid,
                tweet_text,
                tweet_userid,
                tweet_retweet,
                tweet_favorite,
                inserted_at)
            SELECT %s, %s, %s, %s, %s, %s, %s,
            """ + latlon + """
            , %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT id FROM messages WHERE tweet_id = %s AND url_id = %s)"""

    #try:
    cur.execute(q, [str(tweet_id), str(url_id), time, geo_type, geo_location, lat, lon, str(uuid), tweet_text, str(tweet_userid), tweet_retweet, tweet_favorite, now, str(tweet_id), str(url_id)])
    con.commit()
    #except (DatabaseError, DataError, InternalError) as error:
        #print "Database Error: " + str(error)
        #return None

    cur.close()

    #id_of_new_row = get_message_id(con, url_id, tweet_id)
    return get_message_id(con, url_id, tweet_id)

def save_message_meta(con, user_id, name, screen_name, img_url):
    if not len(user_id):
        return 0

    q = """INSERT INTO messages_meta (
                user_id,
                name,
                screen_name,
                img_url)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT user_id FROM messages_meta WHERE user_id = %s)"""

    cur = con.cursor()
    cur.execute(q, [str(user_id), name, screen_name, img_url, str(user_id)])
    con.commit()
    cur.close()
    return 1

# returns a message id
def get_message_id(con, url_id, tweet_id):

    cur = con.cursor()

    q = """SELECT id FROM messages
           WHERE url_id = %s
             AND tweet_id = %s"""
    cur.execute(q, [str(url_id), str(tweet_id)])
    row = cur.fetchone()[0]

    cur.close()
    return row

def get_message_count(message_id):

    cur = con.cursor()

    q = """SELECT count(*) FROM messages
           WHERE id = %s"""
    cur.execute(q, [str(message_id)])
    row = cur.fetchone()[0]

    cur.close()
    return row

def get_messages_for_geocoding(con, amount=100):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT id, geo_location, time, url_id
             FROM messages
            WHERE geo_type = 'location'
              AND lat IS NULL
              AND lon IS NULL
              ORDER BY time DESC
              LIMIT %s
              """
    #try:
    cur.execute(q, [amount])
    rows = cur.fetchall()
    #except socket.error:
        #print "socket.error"
        #cur.close()
        #return None

    cur.close()
    return rows

def save_message_location(con, message_id, lat, lon):
    cur = con.cursor()

    q = """UPDATE messages
              SET lat = %s,
                  lon = %s,
                  latlon = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            WHERE id = %s"""

    cur.execute(q, [lat, lon, lon, lat, message_id])
    con.commit()
    #print 'failed to save messages: ' + str(e)
    cur.close()

def save_message_invalidlocation(con, message_id):
    cur = con.cursor()

    q = """UPDATE messages SET geo_type = 'invalidlocation' WHERE id = %s"""

    #try:
    cur.execute(q, [message_id])
    con.commit()
    #except socket.error as e:
        #print 'failed to save messages: ' + str(e)
    #finally:
    cur.close()

def get_tweets_for_url(con, url_id):

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    q = """SELECT COUNT(*) FROM messages WHERE url_id = %s"""
    cur.execute(q, [str(url_id)])
    row = cur.fetchone()[0]

    cur.close()
    return row

def save_to_db(con,q,vals):
    cur = con.cursor()
    cur.execute(q,vals)
    con.commit()
    cur.close()

def get_from_db(con, q, params=[]):
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, params)
    if q.startswith('SELECT'):
        rows = cur.fetchall()
        cur.close()
        return rows
    else:
        con.commit()
        cur.close()
        return None

def get_messages_near_point(con, lat, lon, time, limit):
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    q = """SELECT msg.id, msg.time, msg.tweet_userid, msg.url_id, msg.tweet_id, msg.lat, msg.lon, msg.tweet_text, msg.tweet_retweet, msg.tweet_favorite, meta.name, meta.screen_name, meta.img_url
             FROM messages msg, messages_meta meta
            WHERE msg.tweet_userid = meta.user_id
            AND msg.time > %s
         ORDER BY msg.latlon <-> ST_SETSRID(ST_MAKEPOINT(%s, %s), 4326)
            LIMIT %s"""

    cur.execute(q, [time, lon, lat, limit])

    rows = cur.fetchall()
    cur.close()
    return rows

def get_messages_boundingbox(con, lat1, lon1, lat2, lon2, time):
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    q = """SELECT msg.id, msg.time, msg.tweet_userid, msg.url_id, msg.tweet_id, msg.lat, msg.lon, msg.tweet_text, msg.tweet_retweet, msg.tweet_favorite, meta.name, meta.screen_name, meta.img_url
             FROM messages msg, messages_meta meta
            WHERE msg.tweet_userid = meta.user_id
              AND msg.time > %s
              AND msg.latlon && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
         ORDER BY msg.time DESC
            LIMIT 100000
            """

    cur.execute(q, [time, lon1, lat1, lon2, lat2])

    rows = cur.fetchall()
    cur.close()
    return rows

# uses flickr shapefiles to filter results
def get_messages_constrained_by_boundaries(con, lat, lon, time_begin, limit, min_place, _filter=None):
    # flickr tables to search in order of preference
    tables = ['neighbourhoods', 'localities', 'counties', 'regions', 'countries']
    if _filter and len(_filter):
        try:
            tables = tables[tables.index(_filter):]
        except ValueError:
            pass

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rows = []
    start = ttime.time();
    while len(rows) < min_place and len(tables) > 0:
        table = tables.pop(0)

        q = """SELECT msg.id, msg.time, msg.tweet_userid, msg.url_id, msg.tweet_id, msg.lat, msg.lon, msg.tweet_text, msg.tweet_retweet, msg.tweet_favorite, meta.name, meta.screen_name, meta.img_url, flickr.woe_id, flickr.label as place, flickr.country
                FROM messages as msg, messages_meta meta,
                    (SELECT country, woe_id, label, geom FROM %s WHERE ST_Within(ST_SETSRID(ST_MAKEPOINT(%s, %s), 4326), geom) LIMIT 1) as flickr
                WHERE msg.latlon && flickr.geom
                AND ST_Within(msg.latlon, flickr.geom)
                AND msg.tweet_userid = meta.user_id
                AND msg.time > %s
                ORDER BY msg.time DESC
                LIMIT 100000""" % (table, "%s", "%s", "%s")

        cur.execute(q, [lon, lat, time_begin])
        rows = list(cur.fetchall())

    cur.close()
    elapsed = ttime.time() - start

    # process results
    rsp = []
    place = ''
    country = ''
    woe_id = ''
    for row in rows:
        r = dict(row)

        # set country, place & woe_id
        # delete from results
        if 'place' in r:
            if not place:
                place = r['place']
            del r['place']

        if 'country' in r:
            if not country:
                country = r['country']
            del r['country']

        if 'woe_id' in r:
            if not woe_id:
                woe_id = r['woe_id']
            del r['woe_id']

        rsp.append(r)

    # check to see if there is place type
    if country:
        return {'place_type': table, 'label': place, 'country': country, 'woe_id': woe_id, 'items': rsp, 'query_time': elapsed}
    else:
        return {'error': 'no_results'}

def reverse_geocode(con, lat, lon, _filter=None):
    # flickr tables to search in order of preference
    tables = ['neighbourhoods', 'localities', 'counties', 'regions', 'countries']

    if _filter and len(_filter):
        try:
            tables = tables[tables.index(_filter):]
        except ValueError:
            pass

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rows = []

    while len(rows) < 1 and len(tables) > 0:
        table = tables.pop(0)

        q = """SELECT country, woe_id, label
                FROM %s
                WHERE ST_Within(ST_SETSRID(ST_MAKEPOINT(%s, %s), 4326), geom)
                LIMIT 5""" % (table, "%s", "%s")

        cur.execute(q, [lon, lat])
        rows = list(cur.fetchall())

    cur.close()

    if len(rows):
      rsp = []
      for row in rows:
        rsp.append(dict(row))

      return {'place_type': table, 'items': rsp}
    else:
      return {'error': 'could not find location'}


def getPostsForUUID(con, uuid, tally):
    if tally == False:
        extras = "urls.*, messages.lat, messages.lon, messages.time"
    else:
        print 'tally'
        extras = "urls.topic AS topic"

    q = """SELECT """ + extras + """
             FROM (SELECT url_id, lat, lon, time
                      FROM messages
                     WHERE konfab_uuid = %s
                       AND messages.time > EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '1 WEEK'))
                  ) as messages, urls
            WHERE CAST(urls.id AS TEXT) = messages.url_id
              """
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [uuid])
    rows = cur.fetchall()
    cur.close()
    return rows

def get_readabletext_for_url(con, url_id):
    if str(url_id).lower() == 'false':
        return None
    q = """SELECT readabletext, urls.url
             FROM urls, htmlpages
            WHERE urls.id = %s
              AND urls.id = htmlpages.url_id
            LIMIT 1"""
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [url_id])
    rows = cur.fetchone()
    cur.close()
    return rows

def get_topic_for_url(con, url_id):
    if str(url_id).lower() == 'false':
        return None
    q = """SELECT topic
             FROM urls
            WHERE id = %s
            LIMIT 1"""
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [url_id])
    rows = cur.fetchall()
    cur.close()
    if rows and len(rows) > 0:
        return rows[0]['topic']
    else:
        return None

def get_state_for_point(con, message_id):
    q = """SELECT postal
             FROM state_geometry
            WHERE ST_CONTAINS(
                      geom,
                      (SELECT latlon
                         FROM messages
                        WHERE id = %s
                        LIMIT 1) )
        """
    q_buffer = """SELECT postal
             FROM state_geometry
            WHERE ST_CONTAINS(
                      ST_BUFFER(geom, 0.07, 1),
                      (SELECT latlon
                         FROM messages
                        WHERE id = %s
                        LIMIT 1) )
        """

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [message_id])
    rows = cur.fetchall()
    if rows and len(rows) > 0:
        cur.close()
        return rows
    else:
        cur.execute(q_buffer, [message_id])
        rows = cur.fetchall()
        cur.close()
        if rows and len(rows) > 0:
            return rows
        return None

def get_mta_for_point(con, message_id):
    q = """SELECT mta
             FROM mta_shapes
            WHERE ST_CONTAINS(
                      geom,
                      (SELECT latlon
                         FROM messages
                        WHERE id = %s
                        LIMIT 1) )
        """

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [message_id])
    rows = cur.fetchall()
    cur.close()
    if len(rows) > 0:
        return rows[0][0]
    else:
        return None
#
def get_county_for_point(con, message_id):
    q = """SELECT woe_id
             FROM counties
            WHERE ST_CONTAINS(
                      geom,
                      (SELECT latlon
                         FROM messages
                        WHERE id = %s
                        LIMIT 1) )
        """

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [message_id])
    rows = cur.fetchall()
    cur.close()
    if len(rows) > 0:
        return rows[0][0]
    else:
        return None
#
def get_city_for_point(con, message_id):
    q = """SELECT woe_id
             FROM localities
            WHERE ST_CONTAINS(
                      geom,
                      (SELECT latlon
                         FROM messages
                        WHERE id = %s
                        LIMIT 1) )
        """

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [message_id])
    rows = cur.fetchall()
    cur.close()
    if len(rows) > 0:
        return rows[0][0]
    else:
        return None
#
def get_neighbourhood_for_point(con, message_id):
    q = """SELECT woe_id
             FROM neighbourhoods
            WHERE ST_CONTAINS(
                      geom,
                      (SELECT latlon
                         FROM messages
                        WHERE id = %s
                        LIMIT 1) )
        """

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [message_id])
    rows = cur.fetchall()
    cur.close()
    if len(rows) > 0:
        return rows[0][0]
    else:
        return None

def get_messages_with_geo(con, limit=100):
    q = """SELECT id, lat, lon
             FROM messages
            WHERE lat IS NOT NULL
              AND lon IS NOT NULL
              AND latlon IS NULL
            LIMIT %s""" % limit
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_messages_without_state(con, limit=100):
    q = """SELECT *, ST_AsText(latlon) AS latlontext
             FROM messages
            WHERE latlon IS NOT NULL
              AND lat != 0
              AND lon != 0
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND state IS NULL
         ORDER BY time DESC
            LIMIT %s
        """ % limit
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

# sets the place for a message
def save_message_region(con, message_id, county=0, city=0, neighborhood=0):
    cur = con.cursor()

    q = """UPDATE messages
              SET in_county = %s,
              in_city = %s,
              in_hood = %s
            WHERE id = %s"""

    cur.execute(q, [county, city, neighborhood, message_id])
    con.commit()
    cur.close()


def get_message_count(con, start=0, end=9999999999):
    # times in hours since epoch (not seconds)
    q = """SELECT time,
                  SUM(total) AS total
             FROM url_counts
            WHERE time >= %s
              AND time <= %s
         GROUP BY time
         ORDER BY time
            """
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [start, end])
    rows = cur.fetchall()
    cur.close()
    rows_dicts = []
    for row in rows:
        rows_dicts.append(dict(row))
    return rows_dicts

def get_geo_message_count(con, start=0, end=9999999999):
    # times in hours since epoch (not seconds)
    q = """SELECT time,
                  SUM(total) AS total,
                  SUM(region_1) AS region_1,
                  SUM(region_2) AS region_2,
                  SUM(region_3) AS region_3,
                  SUM(region_4) AS region_4,
                  SUM(region_5) AS region_5,
                  SUM(region_6) AS region_6,
                  SUM(region_7) AS region_7,
                  SUM(region_8) AS region_8,
                  SUM(region_9) AS region_9,
                  SUM(region_10) AS region_10,
                  SUM(region_11) AS region_11,
                  SUM(region_12) AS region_12,
                  SUM(region_13) AS region_13,
                  SUM(region_14) AS region_14,
                  SUM(region_15) AS region_15,
                  SUM(region_16) AS region_16,
                  SUM(mta_1) AS mta_1,
                  SUM(mta_2) AS mta_2,
                  SUM(mta_3) AS mta_3,
                  SUM(mta_4) AS mta_4,
                  SUM(mta_5) AS mta_5,
                  SUM(mta_6) AS mta_6,
                  SUM(mta_7) AS mta_7,
                  SUM(mta_8) AS mta_8,
                  SUM(mta_9) AS mta_9,
                  SUM(mta_10) AS mta_10,
                  SUM(mta_11) AS mta_11,
                  SUM(mta_12) AS mta_12,
                  SUM(mta_13) AS mta_13,
                  SUM(mta_14) AS mta_14,
                  SUM(mta_15) AS mta_15,
                  SUM(mta_16) AS mta_16,
                  SUM(mta_17) AS mta_17,
                  SUM(mta_18) AS mta_18,
                  SUM(mta_19) AS mta_19,
                  SUM(mta_20) AS mta_20,
                  SUM(mta_21) AS mta_21,
                  SUM(mta_22) AS mta_22,
                  SUM(mta_23) AS mta_23,
                  SUM(mta_24) AS mta_24,
                  SUM(mta_26) AS mta_26,
                  SUM(mta_27) AS mta_27,
                  SUM(mta_28) AS mta_28,
                  SUM(mta_29) AS mta_29,
                  SUM(mta_30) AS mta_30,
                  SUM(mta_31) AS mta_31,
                  SUM(mta_32) AS mta_32,
                  SUM(mta_33) AS mta_33,
                  SUM(mta_34) AS mta_34,
                  SUM(mta_35) AS mta_35,
                  SUM(mta_36) AS mta_36,
                  SUM(mta_37) AS mta_37,
                  SUM(mta_38) AS mta_38,
                  SUM(mta_39) AS mta_39,
                  SUM(mta_40) AS mta_40,
                  SUM(mta_41) AS mta_41,
                  SUM(mta_42) AS mta_42,
                  SUM(mta_43) AS mta_43,
                  SUM(mta_44) AS mta_44,
                  SUM(mta_45) AS mta_45,
                  SUM(mta_46) AS mta_46,
                  SUM(mta_47) AS mta_47,
                  SUM(mta_48) AS mta_48,
                  SUM(mta_49) AS mta_49
             FROM url_geo_counts
            WHERE time >= %s
              AND time <= %s
         GROUP BY time
         ORDER BY time
            """
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [start, end])
    rows = cur.fetchall()
    cur.close()
    rows_dicts = []
    for row in rows:
        rows_dicts.append(dict(row))
    return rows_dicts

def get_geo_url_count(con, url_id, start=0, end=9999999999):
    # times in hours since epoch (not seconds)
    url_id = str(url_id)
    q = """SELECT SUM(total) AS total,
                  SUM(region_1) AS region_1,
                  SUM(region_2) AS region_2,
                  SUM(region_3) AS region_3,
                  SUM(region_4) AS region_4,
                  SUM(region_5) AS region_5,
                  SUM(region_6) AS region_6,
                  SUM(region_7) AS region_7,
                  SUM(region_8) AS region_8,
                  SUM(region_9) AS region_9,
                  SUM(region_10) AS region_10,
                  SUM(region_11) AS region_11,
                  SUM(region_12) AS region_12,
                  SUM(region_13) AS region_13,
                  SUM(region_14) AS region_14,
                  SUM(region_15) AS region_15,
                  SUM(region_16) AS region_16,
                  SUM(mta_1) AS mta_1,
                  SUM(mta_2) AS mta_2,
                  SUM(mta_3) AS mta_3,
                  SUM(mta_4) AS mta_4,
                  SUM(mta_5) AS mta_5,
                  SUM(mta_6) AS mta_6,
                  SUM(mta_7) AS mta_7,
                  SUM(mta_8) AS mta_8,
                  SUM(mta_9) AS mta_9,
                  SUM(mta_10) AS mta_10,
                  SUM(mta_11) AS mta_11,
                  SUM(mta_12) AS mta_12,
                  SUM(mta_13) AS mta_13,
                  SUM(mta_14) AS mta_14,
                  SUM(mta_15) AS mta_15,
                  SUM(mta_16) AS mta_16,
                  SUM(mta_17) AS mta_17,
                  SUM(mta_18) AS mta_18,
                  SUM(mta_19) AS mta_19,
                  SUM(mta_20) AS mta_20,
                  SUM(mta_21) AS mta_21,
                  SUM(mta_22) AS mta_22,
                  SUM(mta_23) AS mta_23,
                  SUM(mta_24) AS mta_24,
                  SUM(mta_26) AS mta_26,
                  SUM(mta_27) AS mta_27,
                  SUM(mta_28) AS mta_28,
                  SUM(mta_29) AS mta_29,
                  SUM(mta_30) AS mta_30,
                  SUM(mta_31) AS mta_31,
                  SUM(mta_32) AS mta_32,
                  SUM(mta_33) AS mta_33,
                  SUM(mta_34) AS mta_34,
                  SUM(mta_35) AS mta_35,
                  SUM(mta_36) AS mta_36,
                  SUM(mta_37) AS mta_37,
                  SUM(mta_38) AS mta_38,
                  SUM(mta_39) AS mta_39,
                  SUM(mta_40) AS mta_40,
                  SUM(mta_41) AS mta_41,
                  SUM(mta_42) AS mta_42,
                  SUM(mta_43) AS mta_43,
                  SUM(mta_44) AS mta_44,
                  SUM(mta_45) AS mta_45,
                  SUM(mta_46) AS mta_46,
                  SUM(mta_47) AS mta_47,
                  SUM(mta_48) AS mta_48,
                  SUM(mta_49) AS mta_49
             FROM url_geo_counts
            WHERE url_id = %s
              AND time >= %s
              AND time <= %s
            """
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [url_id, start, end])
    rows = cur.fetchall()
    cur.close()
    rows_dicts = []
    for row in rows:
        rows_dicts.append(dict(row))
    return rows_dicts[0]

def get_topics_for_categorizing(con, limit=10, keyword=None):
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    if keyword is not None:
        keyword = '%' + str(keyword) + '%'
        q = """SELECT *
                 FROM urls
                WHERE (topic_method = ''
                   OR topic_method IS NULL)
                  AND total > 15
                  AND url ILIKE %s
             ORDER BY RANDOM()
                LIMIT %s"""
        cur.execute(q, [keyword, limit])
    else:
        q = """SELECT *
                 FROM urls
                WHERE (topic_method = ''
                   OR topic_method IS NULL)
                  AND total > 15
             ORDER BY RANDOM()
                LIMIT %s"""
        cur.execute(q, [limit])
    rows = cur.fetchall()
    cur.close()
    return rows

def get_category_counts_for_method(con):
    q = """SELECT topic,
                  topic_method,
                  COUNT(*) AS count
             FROM urls
         GROUP BY topic, topic_method"""
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

# returns names of mta regions from a list of mta id's
def get_mta_names(con,arr):
  q = """select ST_AsText(ST_Envelope(geom)) as bbox, name, mta from mta_shapes where mta = ANY(array[%s]);"""
  cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
  cur.execute(q, [arr])
  rows = cur.fetchall()
  cur.close()

  # process Geometry (better way?)
  out = []
  for row in rows:
    tl = []
    br = []
    try:
      parts = row[0].replace("POLYGON((","").replace("))","").split(",")
      tl = [float(x) for x in parts[1].split(" ")]
      br = [float(x) for x in parts[3].split(" ")]
    except:
      pass
    out.append({
      'name': row[1],
      'id': row[2],
      'bbox': {
        'tl' : tl,
        'br' : br
      }
    });
  return out

# returns a full json blob of messages w/ meta and url attributes merged in for a mta id
# TODO: optimize the 3 table join :-(
def get_messages_in_mta(con,mta,cutoff,count):
    q = """SELECT msg.mta, msg.id, msg.time, msg.tweet_userid, msg.url_id, msg.tweet_id, msg.lat, msg.lon, msg.tweet_text, msg.tweet_retweet, msg.tweet_favorite,
          meta.name, meta.screen_name, meta.img_url,
          u.url,u.title,u.description,u.readinglevel,u.topic,u.thumbnail
           FROM messages msg, messages_meta meta, urls u
          WHERE msg.mta = %s
            AND msg.tweet_userid = meta.user_id
            AND CAST(msg.url_id AS bigint) = u.id
            AND msg.time > %s
            AND u.hide IS NOT TRUE
       ORDER BY msg.time DESC
          LIMIT %s
          """

    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [mta,cutoff,count])
    rows = cur.fetchall()
    cur.close()
    return rows
