# returns tracking keywords for Twitter stream,
# whitelist of media domains,
# and city meta data

import pymongo
from itertools import chain

# list of extras were allowing but not tracking
EXTRAS = ['arstechnica.com','bmj.com','theatlanticcities.com','cbc.ca', 'techcrunch.com', 'abcnews.go.com']

def get_keywords_hosts():
    client = pymongo.Connection("localhost", 27017)

    if not client:
        return [], [], []

    db = client.konfab_admin

    if not db:
        return [], [], []


    keywords = []
    hosts = []
    cities = []
    for media in db.media.find():
        if len(media['keywords']):
            keywords.append(media['keywords'][0]['term'])
        """
        for kw in media['keywords']:
            if kw['order'] == 1:
                keywords.append(kw['term'])
        """
        for host in media['hosts']:
            hosts.append(host['hostname'].lower().strip())


    for extra in EXTRAS:
        hosts.append(extra)

    for city in db.cities.find():
        obj = {}
        obj['name'] = city['city']
        obj['woeid'] = city['woeid']
        obj['neighborhoods'] = []
        for n in city['neighborhoods']:
            obj['neighborhoods'].append(n['neighborhood'])
        cities.append(obj)

    client.close()

    return keywords, hosts, cities




