# general functions used to parse Twitter messages
# Needs some cleaning up

import os,sys
import os.path
from os.path import realpath, dirname, join, exists

import cookielib, ssl, socket, threading, Queue, magic, re, logging, tldextract
import argparse, re, time, datetime, math, lxml, mimetypes, rfc822, calendar

import urllib2, httplib2
from urllib2 import urlopen, URLError, HTTPError, HTTPCookieProcessor, Request, build_opener
from httplib import BadStatusLine, InvalidURL
from urlparse import urlparse
from BeautifulSoup import *

from db import *
from parsingTools import get_attribute, geocodeLocation

sys.path.append(os.path.abspath('../'))
from ebdata.templatemaker.articletext import article_text
from ebdata.textmining.treeutils import make_tree
from ebdata.retrieval import UnicodeRetriever


socket.setdefaulttimeout(30)

printMsg = True

# utilites
def getCurrentTime():
    return time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())

#Fri Oct 04 05:07:59 +0000 2013
def convertTwitterTime(time_string):
    """Converts from "Thu Jan 31 21:29:45 +0000 2013" to epoch seconds.
    """
    return calendar.timegm(rfc822.parsedate(time_string))
    #return int(time.mktime(time.strptime(time_string,'%a %b %d %H:%M:%S +0000 %Y')))



# controller for processing tweet
def processURL(pgConn, mongoConn, tweet, running_stats, hosts, testProcess):
    """Gets the URL from the tweet and sends it to be processed.
       If URL is bad, it removes the tweet.
       Else it adds the URL ids to the tweet.
    """

    urls = tweet['entities']['urls']
    remove = len(urls)
    url_ids = []

    for u in urls:
        url_long = u['expanded_url']

        url_id = addURL(pgConn, url_long, process_attrs=True, hosts=hosts, testProcess=testProcess)

        if url_id:
            url_ids.append(url_id)
            hour = 60 * 60
            tweet_time = convertTwitterTime(tweet['created_at'])
            hours = int(tweet_time / hour)
            if pgConn:
                increment_tweets_timeseries(pgConn, url_id, hours)
                increment_url_tweet_count(pgConn, url_id)
            #if printMsg: print 'seconds:%d; hours:%d date:%s' % (tweet_time, hours, tweet['created_at'])

        else:
            if running_stats:
                running_stats['bad_url_count'] += 1
            remove -= 1

    if remove < 0:
        if printMsg: print "[processURL]: Info - tweet had no valid urls, removing from mongo"
        if mongoConn:
            mongoConn.tweets.remove({'_id':tweet['_id']})

    else:
        if mongoConn:
            mongoConn.tweets.update({'_id':tweet['_id']}, {"$set":{"url_id":url_ids}})
            tweet['url_id'] = url_ids
            copyTweet(pgConn, mongoConn, tweet)

    return True


def test_validation(hosts, url):
    valid = False
    url_core = None
    url_netloc = None
    url_parts = getURLCore(None,url)

    if url_parts and len(url_parts):
        url_core = url_parts[0]
        url_netloc = url_parts[1]

    if url_core and url_netloc:
        print "Core: %s, Netloc: %s" % (url_core, url_netloc)
        print "---------------------------------"
        if len(hosts):
            net_loc_lower = url_netloc.lower().strip()
            ext = tldextract.extract(net_loc_lower)
            normalized_url_netloc = "%s.%s" % (ext.domain, ext.suffix)

            for host in hosts:
                host_parts = host.split('/') #speigal.de/international
                #if not valid:
                if host_parts[0] == normalized_url_netloc or host_parts[0] == normalized_url_netloc:
                    print "Match - %s, %s" % (host_parts[0], normalized_url_netloc)
                    print host_parts
                    if len(host_parts) == 2:
                        if host in url_core:
                            valid = True
                    else:
                        valid = True

                    if valid: break

    else:
        print "url unknown"

    print valid



def urlValidation(pgConn, url_long, hosts=[]):
    # TODO: fix periodically not returning  the expanded URL
    url_parts = getURLCore(pgConn, url_long) # returns list or None

    valid = False
    url_core = None
    html = None

    if not url_parts:
        print "[addURL]: %s - Error - not able to find a core: %s" % (getCurrentTime(), url_long)
        return url_core, html

    if url_parts and len(url_parts):
        url_core = url_parts[0]
        url_netloc = url_parts[1]

    if url_core and url_netloc:
        #validate here to only the media outlets in hosts

        if len(hosts):
            net_loc_lower = url_netloc.lower().strip()
            ext = tldextract.extract(net_loc_lower)
            normalized_url_netloc = "%s.%s" % (ext.domain, ext.suffix)

            for host in hosts:
                host_parts = host.split('/') #speigal.de/international

                #if not valid:
                if host_parts[0] == normalized_url_netloc or host_parts[0] == normalized_url_netloc:
                    if len(host_parts) == 2:
                        if host in url_core:
                            valid = True
                    else:
                        valid = True

                    if valid: break

        else:
            valid = True


        if not valid:
            print "[addURL]: %s - Error - url is not in the whitelist of hosts - Core: %s - Net: %s" % (getCurrentTime(), url_core, url_netloc)

        if valid:
            valid = is_url_valid(url_core)

            if not valid:
                print "[addURL]: %s - Error - url seems to be an image, video or audio file - Core: %s - Net: %s" % (getCurrentTime(), url_core, url_netloc)


        # got this far need to check if there is quality text on the page
        html = None
        if valid:
            html = getHTML(url_core)
            if html:
                tree = make_tree(html)
                lines = article_text(tree)
                if len(lines) < 1:
                    valid = False

            else:
                valid = False

            if not valid:
                print "[addURL]: %s - Error - there seems to be no paragraphs to read - Core: %s - Net: %s" % (getCurrentTime(), url_core, url_netloc)

    if not valid:
        url_core = None
        html = None

    return url_core, html


def addURL(pgConn, url_long, process_attrs=False, hosts=[], testProcess=False):
    """ Strips extras from URL
        returns False if it includes a kill list item
        Checks if URL exists already in DB
        Spins new thread to download the HTML
    """
    url_core = None
    html = None
    url_id = None

    url_core, html = urlValidation(pgConn, url_long, hosts)

    # stop here if not valid url
    if not url_core and not html:
        return None


    # save url, returns id

    url_id = save_urls(pgConn, url_core)

    if url_id:
        save_html(pgConn, url_id, url_core, html)
        if process_attrs:
            time.sleep(1)
            th = threading.Thread(target=setAttributesForUrl, args=(pgConn, url_id, html, url_core,))
            th.setDaemon(True)
            th.start()

        if printMsg: print "[addURL]: Info - url has been added: %s " % (url_core)
        return url_id
    else:
        if printMsg: print "[addURL]: Error - url is not valid: %s - %s" % (url_core, url_long)
        if pgConn:
            save_error_urls(pgConn, url_long)
        return None




def is_url_valid(url):
    baddies  = ('image', 'video', 'audio')
    url_type = mimetypes.guess_type(url)[0]
    valid = True
    for bad in baddies:
        try:
            if url_type.index(bad) >= 0: valid = False
        except:
            pass

    return valid
    #if not url_core.endswith('jpg') and not url_core.endswith('jpeg') and not url_core.endswith('png') and not url_core.endswith('gif') and not url_core.endswith('pdf') and not url_core.endswith('mov')


def getURLCore(pgConn, url_long):
    """ Strips unnecessary bits from the URL
        Tries to keep this 'core' consistent for all shares of that page.
    """
    if not url_long or url_long is None or len(url_long) == 0:
        return None

    url = getRedirectedURL(url_long)

    if not url or url is None:
        if pgConn:
            save_error_urls(pgConn, url_long)
        return None

    else:
        url_obj = urlparse(url)

        # reject urls w/o a path
        if len(url_obj.path) < 5:
            return None

        netloc = re.sub('^www\.', '', url_obj.netloc)

        # standardize the url
        url_core = 'http://' + netloc + url_obj.path

        '''
        if url_obj.netloc in querystring_domains:
            url_core = 'http://' + netloc + url_obj.path + "?" + url_obj.query
        else:
            url_core = 'http://' + netloc + url_obj.path
        '''

        #url_id = None

        # TODO: should this also include any image type?
        if is_url_valid(url_core):
            return [url_core, netloc]
        else:
            return None

#
def getRedirectedURL(url):
    """Gets the redirected URL.
    """
    try:
        opener,request = get_base_request(url)
        u = opener.open(request)
        redirected_url = u.geturl()

        return redirected_url

    except URLError:
        #if printMsg: print "[getRedirectedURL]: URLError - %s" % url
        return None
    except (HTTPError, BadStatusLine, InvalidURL):
        #if printMsg: print "[getRedirectedURL]: HTTPError - %s" % url
        return None
    except (socket.timeout, ssl.SSLError):
        #if printMsg: print "[getRedirectedURL]: Timeout - %s" % url
        return None
    except Exception as e:
        #if printMsg: print " [getRedirectedURL]: Error -  %s - %s" % (url, e)
        return None


def setAttributesForUrl(pgConn, url_id, html_str=None, url_core=None):
    """ Downloads the HTML if not found in DB
        pulls out the title, description, thumbnail, etc
        saves this meta data to postgres
    """
    printMsg = True

    html = None
    url = None
    soup = None
    lines = []

    if not pgConn:
        if printMsg: print '[setAttributesForUrl]: Error - No postgres connection'
        return False

    if url_core:
        url = url_core

    if not url:
        row = get_url(pgConn, url_id)
        if row:
            url = row['url']

    if url:
        url_obj = urlparse(url)
        if len(url_obj.path) < 5:
            url = None
    if url:
        if html_str:
            html = html_str

        if not html:
            html_row = get_html(pgConn, url_id)
            if html_row:
                html = html_row['html']
            elif url:
                html = getHTML(url)

        if html:
            tree = make_tree(html)
            lines = article_text(tree)
            soup = cleanSoup(BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES))

        if len(lines) < 1:
            html = None


    if not html or not url or not soup:
        if printMsg: print '[setAttributesForUrl]: Error - no html returned %s' % url
        delete_url(pgConn, url_id) # not sure we need to do this
        return False


    # get thumbnail
    # TODO: check to see if this is working correctly
    thumbnail = get_attribute(soup, 'img', url)

    if not len(thumbnail):
        if printMsg: print '[setAttributesForUrl]: Warning - no thumbnail returned - %s' % url

    # get title
    title = get_attribute(soup, 'title')
    if title is None: title = ''

    if not len(title):
        if printMsg: print '[setAttributesForUrl]: Warning - no title returned - %s' % url

    title = " ".join(title.strip().split())

    # get description
    description = get_attribute(soup, 'description', lines)
    if description == 'error':
        #delete_url(pgConn, url_id)
        description = None

    if description is None: description = ''

    try:
        description = " ".join(description.strip().split())
    except:
        description = description

    if printMsg:
        print ""
        print "-----------------------------------------"
        print "URL ID: %s" % url_id
        print "Title: %s" % title
        print "Desc: %s" % description
        print "IMG: %s" %  thumbnail
        print "-----------------------------------------"

    if not len(description):
        if printMsg: print '[setAttributesForUrl]: Warning - no description returned - %s' % url

    # save
    if pgConn:
        save_url_attributes(pgConn, url_id, title, description, thumbnail)

    return False


def copyTweet(pgConn, mongoConn, tweet):
    """ Save Tweet to postgres
    """
    if 'url_id' not in tweet:
        #if printMsg: print "[copyTweet]: Error - there were no no url_id's"
        return False

    row_id = tweet['_id']
    tweet_time = convertTwitterTime(tweet['created_at'])
    tweet_id = tweet['id_str']

    tweet_text = ''
    if 'text' in tweet:
        tweet_text = tweet['text']

    tweet_user_id = ''
    tweet_user_name = ''
    tweet_user_screen_name = ''
    tweet_user_img = ''
    if 'user' in tweet:
        if 'id_str' in tweet['user']:
            tweet_user_id = tweet['user']['id_str']
        if 'name' in tweet['user']:
            tweet_user_name = tweet['user']['name']
        if 'screen_name' in tweet['user']:
            tweet_user_screen_name = tweet['user']['screen_name']
        if 'profile_image_url' in tweet['user']:
            tweet_user_img = tweet['user']['profile_image_url']

    tweet_retweets = 0
    tweet_favorites = 0
    if 'retweet_count' in tweet:
        tweet_retweets = int(tweet['retweet_count'])
    if 'favorite_count' in tweet:
        tweet_favorites = int(tweet['favorite_count'])

    lat = None
    lon = None
    geo_type = ''
    geo_location = ''
    if tweet['geo']:
        coord = tweet['geo']['coordinates']
        lat = coord[0]
        lon = coord[1]
        geo_type = 'point'
    elif tweet['place']:
        if tweet['place']['bounding_box'] and tweet['place']['bounding_box']['coordinates'] and len(tweet['place']['bounding_box']['coordinates']) > 0:
            bbox = tweet['place']['bounding_box']['coordinates'][0]
            lat = (bbox[0][0] + bbox[2][0]) / 2
            lon = (bbox[0][1] + bbox[2][1]) / 2
            geo_type = 'place'
        else:
            pass

        geo_location = tweet['place']['full_name']
    elif tweet['user']['location']:
        geo_type = 'location'
        geo_location = tweet['user']['location']
    else:
        pass

    #if printMsg: print "%s, %s, %s, %s, %s, %s" % (tweet_id, tweet_time, geo_type, geo_location, str(lat), str(lon))
    url_id_array = tweet['url_id']
    message_id_array = []
    if pgConn:
        for url_id in url_id_array:
            message_id = save_message(pgConn, tweet_id, tweet_time, url_id, geo_type, geo_location, lat, lon, None, tweet_text, tweet_user_id, tweet_retweets, tweet_favorites)
            saved_meta = save_message_meta(pgConn, tweet_user_id, tweet_user_name, tweet_user_screen_name, tweet_user_img)
            #message_id = get_message_id(pgConn, url_id, tweet_id)
            #1380863485
            #1380864066
            #if printMsg: print "[copyTweet]: Success - added tweet to messages"
            print "[copyTweet]: %s - Success - added tweet to messages (msgID: %s, tweetTime: %s, urlID: %s)" % (getCurrentTime(), message_id, tweet_time, url_id)

            if message_id:
                message_id_array.append(message_id)

            tweet_time = convertTwitterTime(tweet['created_at'])

            if lat is not None and lon is not None:
                county, city, neighborhood = save_geography_for_message_id(pgConn, message_id)

                if county or city or neighborhood:
                    update_time_series(pgConn, url_id, tweet_time, county, city, neighborhood)

            elif geo_type == 'location': # spin geocoding...
                # geocodeLocation(location, geocodeDB=None, tries=0) mongoConn.geocode
                geocode_thread = threading.Thread(target=run_geocoding, args=(pgConn, mongoConn.geocode, url_id, message_id, tweet_time, geo_location,))
                geocode_thread.daemon = True
                geocode_thread.start()


        if len(message_id_array) > 0:
            mongoConn.tweets.update({'_id':row_id},
                             {'$set':{'message_id':message_id_array}},
                             True)
            return True
        else:
            return False
    else:
        return True

def run_geocoding(pgConn, mongoCollection, url_id, message_id, tweet_time, geo_location):
    try:
        result = geocodeLocation(geo_location, mongoCollection)

        if result:
            lat = result['lat']
            lon = result['lon']

            if lat is not None and lon is not None:
                print "[Geocoding]: %s - Info - Saving lat / lng for %s" % (getCurrentTime(), geo_location)
                save_message_location(pgConn, message_id, lat, lon)

                county, city, neighborhood = save_geography_for_message_id(pgConn, message_id)

                if county or city or neighborhood:
                    update_time_series(pgConn, url_id, tweet_time, county, city, neighborhood)

        else:
            print "[Geocoding]: %s - Warning - could not find lat / lng for %s " % (getCurrentTime(), geo_location)
            save_message_invalidlocation(pgConn, message_id)
    except:
        print "[Geocoding]: %s - Error - looking for %s" % (getCurrentTime(), geo_location)
    finally:
        return True


def update_time_series(pgConn, url_id, tweet_time, county, city, neighborhood):
    # add to timeseries
    hour = 60 * 60
    hours = int(tweet_time / hour)
    if county or city or neighborhood:
        increment_geo_timeseries(pgConn, url_id, hours, county=county, city=city, neighborhood=neighborhood)

#
def save_geography_for_message_id(pg_conn, message_id):
    """ point in polygon finding/saving of state for a message
    """

    county = get_county_for_point(pg_conn, message_id)
    city = get_city_for_point(pg_conn, message_id)
    neighborhood = get_neighbourhood_for_point(pg_conn, message_id)

    if not county: county = 0
    if not city: city = 0
    if not neighborhood: neighborhood = 0

    if county or city or neighborhood:
        save_message_region(pg_conn, message_id, county=county, city=city, neighborhood=neighborhood)

    return county, city, neighborhood


def get_base_request(url):
    cj = cookielib.CookieJar()
    cp = HTTPCookieProcessor(cj)
    opener = build_opener(cp)
    opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 5.1; rv:10.0.1) Gecko/20100101 Firefox/10.0.1')]
    return opener,Request(url)


def getHTML(url):
    """Get the HTML for a URL
    """
    html = None
    try:

        html = UnicodeRetriever().fetch_data(url)

    except URLError:
        #if printMsg: print "[getHTML]: Error - URLError - %s" % url
        return None
    except (HTTPError, BadStatusLine, InvalidURL):
        #if printMsg: print "[getHTML]: Error - HTTPError - %s" % url
        return None
    except (socket.timeout, ssl.SSLError):
        #if printMsg: print "[getHTML]: Error - Timeout - %s" % url
        return None
    except Exception as e:
        #if printMsg: print "[getHTML]: Error - %s - %s" % (url, e)
        return None
    finally:
        return html

#
def cleanSoup(soup):
    # get rid of javascript, noscript and css
    [[tree.extract() for tree in soup(elem)] for elem in ('g:plus','twitter','facebook','svg','canvas','link', 'script', 'noscript', 'style', 'applet', 'area', 'button', 'embed', 'iframe', 'form', 'input', 'object', 'option', 'select', 'spacer', 'textarea', 'video', 'audio')]
    # get rid of doctype
    subtree = soup.findAll(text=re.compile("DOCTYPE"))
    [tree.extract() for tree in subtree]
    # get rid of comments
    comments = soup.findAll(text=lambda text:isinstance(text,Comment))
    [comment.extract() for comment in comments]
    return soup

if __name__ == '__main__':
    pass

