# geocodes messages that need geocoding
import os
import sys
import os.path
from os.path import dirname, join

from db import *
from parsingTools import geocodeLocation
from twitter_processing import save_geography_for_message_id

class Geocode():
    def __init__(self, pg_conn, mongo_conn, limit=1000):
        self.pg_conn = pg_conn
        self.mongo_conn = mongo_conn
        self.limit = limit
        self.start()

    def start(self):
        total_count = 0
        error_count = 0
        success_count = 0
        failed_count = 0

        rows = get_messages_for_geocoding(self.pg_conn, self.limit)

        #print "[geocode.Geocode]: - INFO - found %s rows to geocode" % len(rows)

        for row in rows:
            total_count += 1

            location    = row['geo_location']
            message_id  = row['id']
            time        = row['time']
            url_id      = row['url_id']

            try:
                result = geocodeLocation(location, self.mongo_conn.geocode)
                #print "[geocode.Geocode]: (%d invalid; %d total; (%.1f%%)) %s\t\t%s\n" % (error_count, total_count, (float(error_count) / float(total_count) * 100), location, str(result))

                if result:
                    lat = result['lat']
                    lon = result['lon']

                    if lat is not None and lon is not None:
                        save_message_location(self.pg_conn, message_id, lat, lon)

                        county = None
                        city = None
                        neighborhood = None

                        county, city, neighborhood = save_geography_for_message_id(self.pg_conn, message_id)

                        if county or city or neighborhood:
                            # add to timeseries
                            hour = 60*60
                            hours = int(time / hour)
                            increment_geo_timeseries(self.pg_conn, url_id, hours, county=county, city=city, neighborhood=neighborhood)

                    success_count += 1

                else:
                    print '[geocode.Geocode]: - INFO - error: %s' % location
                    error_count += 1
                    save_message_invalidlocation(self.pg_conn, message_id)
            except:
                print '[geocode.Geocode]: - WARNING - failed: %s' % location
                failed_count += 1

        print "\n[geocode.Geocode]: - INFO - finished: Total: %s, Successful: %s, Errors: %s, Failed: %s" % (total_count, success_count, error_count, failed_count)