import sys, tldextract
from media import *
from twitter_processing import getURLCore, urlValidation
from urlparse import urlparse


def main(url):
    keywords, hosts, cities = get_keywords_hosts()

    if hosts:
        url_core, html = urlValidation(None, url, hosts)

        if url_core and html:
            print 'valid'


if __name__ == '__main__':
    main(sys.argv[1])