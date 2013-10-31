import lxml
from random import shuffle
import cPickle as pickle
from readability.readability import Document
from db import *


""" Topic classifier
    http://www.clips.ua.ac.be/pages/pattern
    http://nltk.sourceforge.net/corpus.html
    http://nltk.googlecode.com/svn/trunk/doc/book/ch02.html
    http://nltk.googlecode.com/svn/trunk/doc/book/ch06.html
    http://stackoverflow.com/questions/3113428/classifying-documents-into-categories
    http://ai-depot.com/articles/the-easy-way-to-extract-useful-text-from-arbitrary-html/
    http://www.ibm.com/developerworks/web/library/os-pythonnltk/index.html

    Used to pull out readable text from HTML page
    Predicts and saves topic
"""
class ReadableText():
    def __init__(self, pg_conn, limit, pickle_file):
        self.limit = limit
        self.pg_conn = pg_conn
        self.pickle_file = pickle_file
        self.start()

    def start(self):
        nlp = pickle.load(open(self.pickle_file, "rb"))  # './consumer/nlp_clf.pickle'
        clf = nlp['clf']
        vectorizer = nlp['vectorizer']


        group_size = 500
        limit = self.limit

        while limit > 0:
            # using a smaller group_size pulls the most recent (relavent) articles to process.
            if limit > group_size:
                local_limit = group_size
            else:
                local_limit = limit

            limit -= local_limit

            rows = get_urls_no_topic(self.pg_conn, local_limit)

            if rows:
                length = len(rows)
                shuffle(rows)

            for row in rows:
                url_id = row['url_id']

                raw_text = self.getTextFromHTML(url_id)
                self.predictUrl(url_id, raw_text, clf, vectorizer)


    def getTextFromHTML(self, url_id):
        """ Runs Readability (Document) on the HTML text
        """
        html_row = get_html(self.pg_conn, url_id)

        if not html_row or 'html' not in html_row:
            return False

        if html_row['readabletext'] and html_row['readabletext'] != '':
            return html_row['readabletext']

        html = html_row['html']

        try:
            html_summary = Document(html).summary(html_partial=True)
            html_summary = html_summary.replace('\n','').replace('\t','')

            if len(html_summary) < 150 or "Something's wrong here..." in html_summary or "<h1>Not Found</h1><p>The requested URL" in html_summary or html_summary == "<html><head/></html>" or "403 Forbidden" in html_summary:
                return False

            raw_text = lxml.html.document_fromstring(html_summary).text_content()
        except:
            raw_text = False

        if raw_text:
            save_readabletext(self.pg_conn, url_id, raw_text, 'meta')
        else:
            save_readabletext(self.pg_conn, url_id, '', 'meta')

        return raw_text


    def predictUrl(self, url_id, text=None, clf=None, vectorizer=None):
        if not self.pg_conn:
            return None

        if text is None:
            row = get_readabletext_for_url(self.pg_conn, url_id)

            if row is None:
                print 'row does not exist'
                return None

            row = dict(row)
            url = row['url']
            text = row['readabletext']

            if text is None:
                text = self.getTextFromHTML(url_id)

        if text is None or text == '':
            return None

        if clf is None or vectorizer is None:
            nlp = pickle.load(open(self.pickle_file, "rb"))
            clf = nlp['clf']
            vectorizer = nlp['vectorizer']

        try:
            X_test = vectorizer.transform([text])
            pred = clf.predict(X_test)
        except:
            return None

        method = 'nlp'

        # these can't change because the classifier was mapped to this
        categories = ['sports', 'business', 'opinion', 'tech', 'politics', 'news',
              'blog', 'entertainment', 'lifestyle', 'world news', 'local news']

        topic = None
        try:
            topic = categories[pred[0]]
            print "Found topic-> id: %s, topic: %s" % (url_id, topic)
            save_url_topic(self.pg_conn, url_id, topic, method)
        except:
            print "!!!!  No topic found-> id: %s, topic: %s" % (url_id, topic)

        return topic




