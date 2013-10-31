#!/usr/bin/python
# encoding: utf-8

import os
import os.path
from os.path import dirname, join

import sys, time, re
import cPickle as pickle

import consumer
from flesch_kincaid import grade_level
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.linear_model import SGDClassifier

from db import *

categories = ['sports', 'business', 'opinion', 'tech', 'politics', 'news',
              'blog', 'entertainment', 'lifestyle', 'world news', 'local news'] # and: 'nonenglish', 'broken'

kill_words = ['http:', 'https:', 'com', 'edu', 'net', 'co', 'id', 'in', 'uk',
              'br', 'p', 'DOC', 'cms', 'html', 'htm', 'index']

""" Tries to figure out the topic for articles
"""

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



class PredictNLP():
    def __init__(self, pg_conn, limit):
        self.pg_conn = pg_conn
        self.limit = limit

        self.start()

    def start(self):
        t0 = time.time()
        try:
            url_to_pickle = './consumer/nlp_clf.pickle'
            nlp = pickle.load(open(url_to_pickle, "rb"))
        except:
            print "[PredictNLP]: Error - no nlp - %s" % sys.exc_info()[0]
            return None

        clf = nlp['clf']
        vectorizer = nlp['vectorizer']
        limit = self.limit

        if not clf:
            print "[PredictNLP]: Error - no clf"
            return None

        if not vectorizer:
            print "[PredictNLP]: Error - no vectorizer"
            return None

        while limit > 0:
            if limit > 1000:
                local_limit = 1000
            else:
                local_limit = limit

            limit -= local_limit

            sample = {}
            sample['data'] = []
            sample['target'] = []
            sample['url_id'] = []
            sample['url'] = []

            rows = get_urls_no_topic(self.pg_conn, local_limit)

            print '[PredictNLP]: Info - sample rows: %d' % len(rows)

            if len(rows) == 0:
                print '[PredictNLP]: Info - exiting because no rows'
                return

            for row in rows:
                sample['url_id'].append(row['url_id'])
                url = row['url']
                sample['url'].append(url)
                text = self.breakupURL(url)
                sample['data'].append(url)

            data_test = sample['data']

            X_test = vectorizer.transform(data_test)
            pred = clf.predict(X_test)


            for s in range(0,len(sample['url'])):
                method = 'nlp'
                url_id = sample['url_id'][s]
                topic = categories[pred[s]]
                save_url_topic(self.pg_conn, url_id, topic, method)

                print "[PredictNLP]: Info - %s\t%s" %(topic, sample['url'][s])

                if s % 100 == 0:
                    print '[PredictNLP]: Info - %s' % s


    def breakupURL(self, url):
        url = re.sub(r"[0-9]+", "", url)
        url_array = re.split('/|-|\.|_|,', url)
        text = ''
        for u in url_array:
            if u in kill_words or len(u) <= 2:
                continue
            text += ' ' + u
        return text



