# Utility to reprocess messages missing title or description

import sys, time, threading, pymongo
from twitter_processing import setAttributesForUrl
from db import *


class ReprocessAttributes():
    def __init__(self, pg_conn, process_fn, count=100, url_id=None):
        self.pg_conn = pg_conn
        self.count = count
        self.process_fn = process_fn
        self.url_id_from_args = url_id
        self.start()

    def start(self):
        thread_max = 50
        #AttributeError: _strptime_time
        time.strptime("20100202", "%Y%m%d")

        if not self.url_id_from_args:
            print "Looking for %s items" % self.count
            rows = get_urls_with_missing_attributes(self.pg_conn, self.count)

            if rows:
                for row in rows:
                    print row
                    url_id = None
                    url = None
                    if 'id' in row:
                        url_id = row['id']
                    if 'url' in row:
                        url = row['url']

                    if url and url_id:
                        th = threading.Thread(target=setAttributesForUrl, args=(self.pg_conn, url_id, None, url,))
                        th.setDaemon(True)
                        th.start()

                    while threading.active_count() > thread_max:
                        time.sleep(1)

                main_thread = threading.currentThread()
                for t in threading.enumerate():
                    if t is main_thread:
                        continue

                    t.join()


        else:
            item = get_url(self.pg_conn, self.url_id_from_args)
            if item:
                if 'url' in item:
                    print "starting thread to find attributes..."
                    url = item['url']
                    th = threading.Thread(target=self.process_fn, args=(self.pg_conn, self.url_id_from_args, None, url,))
                    th.setDaemon(True)
                    th.start()
                    th.join()



        print "Processing done..."




if __name__ == '__main__':
    sys.path.append(os.path.abspath('../'))
    import main as konfab
    try:
        urlid = sys.argv[1]
    except:
        urlid = None

    konfab.read_env('../.env')
    pg_connection = konfab.getDBConnection()

    if pg_connection:
        print "Starting reprocessing... "
        if urlid:
            ReprocessAttributes(pg_connection, setAttributesForUrl, count=1, url_id=urlid)
        else:
            ReprocessAttributes(pg_connection, setAttributesForUrl, count=500, url_id=None)
    else:
        print "No connection"

