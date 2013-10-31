# Parsing utilities used to process messages from Twitter

import urllib2, json, socket, time, sys, bleach
from operator import itemgetter, attrgetter

from urlparse import urlparse,urljoin

from lxml import etree
from StringIO import StringIO

import Image,ImageFile
import requests
import math
from BeautifulSoup import *

import logging

printMsg = True
ACCEPTABLE_TITLE_LENGTH = 10

def decodeUrl(url):
    url_decoded = urllib2.unquote(url).decode('utf-8')
    return url_decoded

def encodeUrl(url):
    url_encoded = urllib2.quote(url.encode('utf-8'))
    return url_encoded

def getsizes(uri):
    # get file size *and* image size (None if not known)
    #r = requests.head(url=url, auth=auth)
    try:
        req = requests.get(uri)
        im = Image.open(StringIO(req.content))

        size = req.headers.get("content-length")
        ctype = req.headers.get("content-type")

        if size: size = int(size)

        req.close()

        #if printMsg: print "[getsizes]: Info - type: %s | size: %s | im.size: %s | uri: %s" % (ctype, size, im.size, uri)

        return size, im.size, ctype
    except:
        return None, None, None


# remove tags, keep text
def strip_tags(html):
    if html:
        return bleach.clean(html, strip=True)
    else:
        return ''


# remove tags and text
def remove_html_tags(tag):
    try:
        p = re.compile(r'<.*?>')
        return p.sub('', tag)
    except:
        pass

    return tag

# remove headline blocks from markup
# while generating descriptions
def removeHeaders(soup):
    [[tree.extract() for tree in soup(elem)] for elem in ('h1','h2','h3','h4','h5','h6')]
    return soup

def missing_http(url):
    try:
        if url.index('//') == 0:
            url = "http:%s" % url
    except:
        pass

    return url

def getThumbnail(soup, base_url=''):
    # set logging levels to prevent log vomit from the requests module
    logging.basicConfig()
    logging.getLogger().setLevel(logging.ERROR)
    requests_log = logging.getLogger("requests")
    requests_log.setLevel(logging.ERROR)

    img_list = []
    thumbnail = ''

    base = soup.find('base')
    abs_href = base_url

    if base is not None:
        try:
            abs_href = base.get('href')
        except:
            pass

    for meta in soup.findAll('meta'):
        img = ''
        if 'image' in meta.get('name', '').lower():
            img = meta.get('content','').strip()
        elif 'image' in meta.get('itemprop','').lower():
            img = meta.get('content','').strip()
        elif 'image' in meta.get('property','').lower():
            img = meta.get('content','').strip()


        if img and len(img) > 5: # don't ask
            img = missing_http(img) # missing protocol
            img_list.append({'url': img, 'tag': 'meta'})


    for img in soup.findAll('img'):
        src = img.get('src','').strip()
        if src and len(src) > 5:
            if abs_href:
                img = urljoin(abs_href, src)
            else:
                img = src

            img = missing_http(img)
            img_list.append({'url': img, 'tag': 'img'})


    if not img_list:
        return thumbnail

    '''
        proceed to rank and filter imgs
    '''
    valid_thumbnail_type = ['image/jpeg','image/jpg','image/png']
    imgScores = {}
    counter = 0
    maxWidth = 0
    maxHeight = 0

    for img in img_list:

        size,dims,ctype = getsizes(img['url'])

        # TODO: deal with content-types that look like this: image/jpeg;charset=ISO-8859-1
        is_valid_img = False
        if ctype is not None:
            for t in valid_thumbnail_type:
                if ctype.find(t) >= 0:
                    is_valid_img = True

        if is_valid_img:
            if size is not None and size > 0:
                if dims and dims[0] >= 150 and dims[1] > 150:
                    counter += 1
                    if img['url'] not in imgScores:
                        # since we can always assume that an img defined in meta
                        # is more trustworthy, assign it a high score
                        if img['tag'] == 'meta':
                            start_score = 10
                        else:
                            start_score = 0

                        o = {
                            'score': start_score,
                            'dims': dims,
                            'pos': counter,
                            'ctype': ctype

                        }

                        imgScores[img['url']] = o

                        maxHeight = max(maxHeight, dims[1])
                        maxWidth = max(maxWidth, dims[0])


    if imgScores:
        for imgKey in imgScores:
            img = imgScores[imgKey]

            # favor landscapes
            img['score'] += (float(img['dims'][0]) / float(maxWidth)) * 10
            img['score'] += (float(img['dims'][1]) / float(maxHeight)) * 5

            '''
            if img['dims'][0] > 250 and img['dims'][0] < 500:
                img['score'] += 5
            elif img['dims'][0] >= 500 and img['dims'][0] < 1000:
                img['score'] += 10
            elif img['dims'][0] >= 1000:
                img['score'] += 15
            '''

            if img['pos'] < 5:
                img['score'] += 5
            elif img['pos'] < 10 and img['pos'] > 5:
                img['score'] += 2

            if img['ctype'] is 'image/jpeg' or img['ctype'] is 'image/jpg':
                img['score'] += 5



    imgScores_list = [x for x in imgScores.iteritems()]
    #sorted(imgScores_list, key=attrgetter('score', 'pos'))
    imgScores_list.sort(key=lambda x: x[1]['pos'], reverse=False)
    imgScores_list.sort(key=lambda x: x[1]['score'], reverse=True)
    #imgScores_list.reverse()

    if imgScores_list and imgScores_list[0]:
        thumbnail = imgScores_list[0][0]

    showScores = False
    if showScores:
        print "#####################################"
        for img in imgScores_list:
            print img
        print "#####################################"


    return thumbnail



def find_title(soup):
    title = ''
    for meta in soup.findAll('meta'):
        if 'title' == meta.get('name', '').lower():
            title = meta.get('content','')
        elif 'title' in meta.get('itemprop','').lower():
            title = meta.get('content','')
        elif 'title' in meta.get('property','').lower():
            title = meta.get('content','')

        if title and len(title) > ACCEPTABLE_TITLE_LENGTH:
            break

    if title:
        return title

    lk = ['h1','h2','h3']
    if soup.body:
        for l in lk:
            h = soup.body.find(l,{"id": re.compile(r'headl.?')})
            if h is not None:
                title = strip_tags(h.renderContents())
                break

        if title and len(title) > ACCEPTABLE_TITLE_LENGTH:
            return title

        for l in lk:
            h = soup.body.find(l,{"class": re.compile(r'headl.?')})
            if h is not None:
                title = strip_tags(h.renderContents())
                break


        if title and len(title) > ACCEPTABLE_TITLE_LENGTH:
            return title


        #next get first h1 || h2 || h3
        for l in lk:
            h = soup.body.find(l)
            if h is not None:
                title = strip_tags(h.renderContents())
                break

    if title and len(title) > ACCEPTABLE_TITLE_LENGTH:
        return title

    title = soup.title.string

    if title:
        return strip_tags(soup.title.string)

    return None

#
def trim_desc_length(txt):
    if not txt:
        return ''

    if len(txt) > 255:
        trimmed = txt[0:250]
        m = re.search(r"[.,!?;]",trimmed[::-1])
        if m:
            end = len(trimmed) - m.start()
            trimmed = trimmed[0:end]

        trimmed = "%s ..." % trimmed
    else:
        trimmed = txt

    return trimmed

# 1st - look in meta tags
# 2nd - construct from <p> tags
def find_description(soup, lines=None):
    description = None
    for meta in soup.findAll('meta'):
        if 'description' == meta.get('name', '').lower():
            description = meta.get('content','')
        elif 'description' in meta.get('itemprop','').lower():
            description = meta.get('content','')
        elif 'description' in meta.get('property','').lower():
            description = meta.get('content','')

        if description and len(description) > 80:
            break

    if lines:
        alt_desc = []
        alt_count = 0
        for line in lines:
            if len(line) > 100:
                alt_count += len(line)
                alt_desc.append(line)

            if alt_count > 255:
                break

        alt_desc_txt = alt_desc



    if description and len(description) < 80 and len(alt_desc_txt) > 100:
        description = alt_desc_txt


    if description:
        description = strip_tags(description)
        return trim_desc_length(description)

    try:
        body = removeHeaders(soup.body)
        text = []
        textLen = 0
        pat = re.compile(r'\s+')
        #[text for text in soup.stripped_strings]
        for tag in body.findAll('p'):

            t = remove_html_tags(tag.renderContents()).strip()
            tl = pat.sub('', t)
            if len(tl) > 100:
                textLen += len(t)
                text.append(t)

            if textLen > 255:
                break

        description = ' '.join(text)[0:255]
        m = re.search(r"[.,!?;]",description[::-1])
        if m:
            end = len(description) - m.start()
            description = description[0:end]
    except:
        pass

    if description:
        description = strip_tags(description)
        return trim_desc_length(description)

    return None



def get_attribute(soup, attr, lines=[], base_url=None):

    if attr == 'img':
        return getThumbnail(soup, base_url)

    elif attr == 'description':
        return find_description(soup,lines)

    elif attr == 'title':
        return find_title(soup)

    return ''


#http://nltk.org/api/nltk.tag.html#module-nltk.tag.stanford
def geocodeLocation(location, geocodeDB=None, tries=0):
    """Uses the OSM Nominatim (hosted by Mapquest Open) to find the location for a string
    """
    try:
        location = location.replace('.','')
        location = encodeUrl(location)
    except UnicodeDecodeError:
        pass

    #if printMsg: print "Location : %s" % location
    results = None
    count = 1

    if geocodeDB:
        row = geocodeDB.find_one({'query':location})
    else:
        row = None

    if row:
        #if printMsg: print '[geocodeLocation]: Cached. Times: {%d}' % row['count']
        results = row['results']
        count = row['count']
        geocodeDB.update({'_id':row['_id']}, {'$inc':{'count':1}})

    else:
        url = "http://open.mapquestapi.com/nominatim/v1/search.php?thumbMaps=false&format=json&limit=4&q=%s" % location
        try:
            results = json.loads(urllib2.urlopen(url).read())
        except (socket.timeout, socket.error):
            #if printMsg: print '[geocodeLocation]: Error - failed to get: %s' % url
            #if printMsg: print 'socket.timeout so waiting a few seconds before trying again'
            if tries < 3:
                time.sleep(5)
                return geocodeLocation(location, geocodeDB, tries+1)

        except urllib2.URLError:
            pass

    rsp = None

    if results:
        #print "[Geocoding]: found geocode :-)"
        if not row:
            if geocodeDB:
                geocodeDB.save({'query':location, 'results':results, 'count':count})

        if len(results) and 'lat' in results[0] and 'lon' in results[0]:
            rsp = {'lat':results[0]['lat'], 'lon':results[0]['lon']}

    else:
        pass
        #print "[Geocoding]: no geocode !!!!!!!!!!!!!!"

    return rsp


if __name__ == '__main__':
    pass
