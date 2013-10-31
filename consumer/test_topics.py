from twitter_processing import getHTML
from BeautifulSoup import *
#from bs4 import UnicodeDammit
import re, string
import nltk


regex = re.compile('[%s]' % re.escape(string.punctuation))
stop_words = nltk.corpus.stopwords.words('english')


def collect_all_words(items):
      all_words = []
      for item in items:
          for w in item['all_words']:
              all_words.append(w)
      return all_words

def identify_top_words(all_words):
  freq_dist = nltk.FreqDist(w.lower() for w in all_words)
  return freq_dist.keys()[:1000]

def normalized_words(article_text):
    words   = []
    oneline = article_text.replace('\n', ' ')
    cleaned = nltk.clean_html(oneline.strip())
    toks1   = cleaned.split()
    for t1 in toks1:
        translated = regex.sub('', t1)
        toks2 = translated.split()
        for t2 in toks2:
            t2s = t2.strip().lower()
            if t2s in stop_words:
                pass
            else:
                words.append(t2s)
    return words
#
def read_reuters_metadata(cats_file):
      f = open(cats_file, 'r')
      lines = f.readlines()
      f.close()
      return lines
#
def cleanSoup(soup):
    # get rid of javascript, noscript and css
    [[tree.extract() for tree in soup(elem)] for elem in ('link', 'script', 'noscript', 'style', 'applet', 'area', 'button', 'embed', 'iframe', 'input', 'object', 'option', 'select', 'spacer', 'textarea', 'video', 'audio')]
    # get rid of doctype
    subtree = soup.findAll(text=re.compile("DOCTYPE"))
    [tree.extract() for tree in subtree]
    # get rid of comments
    comments = soup.findAll(text=lambda text:isinstance(text,Comment))
    [comment.extract() for comment in comments]
    return soup

def main():

    print read_reuters_metadata('/Users/sconnelley/nltk_data/corpora/reuters/cats.txt')
    return
    #http://gawker.com/black-teen-detained-by-nypd-for-buying-an-expensive-bel-1450776820
    #'http://dailydot.com/news/opsyria-syrian-government-hack-interview/'
    h = getHTML('http://upworthy.com/they-said-he-was-evil-and-bad-but-hes-actually-really-really-astonishingly-good')
    soup = cleanSoup(BeautifulSoup(h))
    #print soup.html.string
    #print soup.html.get_text()

    words = normalized_words(str(soup))
    #print words

    articles = []
    articles.append({
        'all_words': words
        })

    all_words = collect_all_words(articles)
    top_words = identify_top_words(all_words)
    print top_words

if __name__ == '__main__':
    main()