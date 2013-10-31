import lxml
from random import shuffle
import cPickle as pickle
from readability.readability import Document
from twitter_processing import getHTML
import consumer, re
from flesch_kincaid import grade_level
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.linear_model import SGDClassifier

from ebdata.templatemaker.articletext import article_text
from ebdata.textmining.treeutils import make_tree
from ebdata.retrieval import UnicodeRetriever



# Not sure whats going on in here
# written by feesta


class L1LinearSVC(LinearSVC):

    def fit(self, X, y):
        # The smaller C, the stronger the regularization.
        # The more regularization, the more sparsity.
        self.transformer_ = LinearSVC(penalty="l1",
                                      dual=False, tol=1e-3)
        X = self.transformer_.fit_transform(X, y)
        return LinearSVC.fit(self, X, y)

    def predict(self, X):
        X = self.transformer_.transform(X)
        return LinearSVC.predict(self, X)


""" Deprecated: now predicting topic from URL rather than text
    Used to pull out readable text from HTML page
    Predicts and saves topic
"""
class ReadableText():
    def __init__(self, html, categories={}):
        self.html = html
        self.categories = categories
        self.start()

    def start(self):
        group_size = 500

        nlp = pickle.load(open('./nlp_clf.pickle', "rb"))
        clf = nlp['clf']
        vectorizer = nlp['vectorizer']

        raw_text = self.getTextFromHTML(self.html) # can also skip this and pass a url to predictTopic
        self.predictTopic(raw_text, clf, vectorizer)


    def getTextFromHTML(self, html):
        """ Runs Readability (Document) on the HTML text
        """
        try:
            html_summary = Document(html).summary(html_partial=True)
            html_summary = html_summary.replace('\n','').replace('\t','')
            if "Something's wrong here..." in html_summary or "<h1>Not Found</h1><p>The requested URL" in html_summary or html_summary == "<html><head/></html>" or "403 Forbidden" in html_summary:
                return False
            raw_text = lxml.html.document_fromstring(html_summary).text_content()
        except:
            raw_text = False

        return raw_text



    def predictTopic(self, text=None, clf=None, vectorizer=None):


        if text is None or text == '':
            print "No text to predict"
            return None

        if clf is None or vectorizer is None:
            nlp = pickle.load(open('./nlp_clf.pickle', "rb"))
            clf = nlp['clf']
            vectorizer = nlp['vectorizer']

        if not clf:
            print "[PredictNLP]: Error - no clf"
            return None

        if not vectorizer:
            print "[PredictNLP]: Error - no vectorizer"
            return None

        sample = [text]

        X_test = vectorizer.transform(sample)
        pred = clf.predict(X_test)

        method = 'nlp'

        # these can't change because the classifier was mapped to this
        categories = ['sports', 'business', 'opinion', 'tech', 'politics', 'news',
              'blog', 'entertainment', 'lifestyle', 'world news', 'local news']
        for s in range(0,len(sample)):
            print '%s : %s' % (s, categories[pred[s]])

        return True

def main():
    url = 'http://buzzfeed.com/michaelrusch/a-new-trailer-from-anchorman-2-is-released-and-its-awesome'
    html = UnicodeRetriever().fetch_data(url)

    ReadableText(html)

if __name__ == '__main__':
    main()





