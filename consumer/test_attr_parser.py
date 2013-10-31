from twitter_processing import getHTML
from BeautifulSoup import *
import re, magic, sys

from urlparse import urlparse
from ebdata.templatemaker.articletext import article_text
from ebdata.textmining.treeutils import make_tree
from ebdata.retrieval import UnicodeRetriever

from parsingTools import *

#
def cleanSoup(soup):
    # get rid of javascript, noscript and css
    [[tree.extract() for tree in soup(elem)] for elem in ('svg','canvas','link', 'script', 'noscript', 'style', 'applet', 'area', 'button', 'embed', 'iframe', 'form', 'input', 'object', 'option', 'select', 'spacer', 'textarea', 'video', 'audio')]
    # get rid of doctype
    subtree = soup.findAll(text=re.compile("DOCTYPE"))
    [tree.extract() for tree in subtree]
    # get rid of comments
    comments = soup.findAll(text=lambda text:isinstance(text,Comment))
    [comment.extract() for comment in comments]
    return soup


def main(url):
    if not url:
        print "No url provided"
        sys.exit()

    #url = 'http://newstatesman.com/politics/2013/10/russell-brand-on-revolution'
    #h = getHTML(url)
    html = UnicodeRetriever().fetch_data(url)
    tree = make_tree(html)
    lines = article_text(tree)


    file_type = magic.from_buffer(html, mime=True)
    print "File Type: %s" % file_type
    #print html

    url_obj = urlparse(url)
    if not url_obj.path:
        print "URL is top-level"

    if len(lines)<1:
        print "URL is top-level"



    soup = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES)
    #print get_attribute(html, 'img', url)

    img = get_attribute(soup, 'img', url)
    title = get_attribute(soup, 'title', url)
    desc = get_attribute(soup, 'description', lines)

    print "Title: %s" % title
    print "Desc: %s" % desc
    print "IMG: %s" % img





if __name__ == '__main__':
    main(sys.argv[1])



