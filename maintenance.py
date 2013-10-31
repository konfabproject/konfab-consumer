#   Copyright 2013 Kon*Fab
#
#
#   konfab-consumer is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   konfab-consumer  is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with konfab-consumer.  If not, see <http://www.gnu.org/licenses/>.
#

import os,sys
from main import do_remove_oldest_tweets, getDBConnection, getMongoConnection, read_env, getIdTimestamp
import psycopg2
import psycopg2.extras
from psycopg2 import DataError, InternalError, DatabaseError

def remove_html(days=3):
    cur = pg_connection.cursor()
    q = '''DELETE FROM htmlpages USING messages
            WHERE htmlpages.url_id = CAST(messages.url_id as integer)
            AND to_timestamp(messages.time) < now() - interval '%s' day
        '''
    cur.execute(q,[days])
    pg_connection.commit()
    cur.close()

def main():
    remove_html(5)
    do_remove_oldest_tweets(5)


if __name__ == '__main__':
    read_env()

    pg_connection = getDBConnection()
    mongo_db = getMongoConnection()

    main()
