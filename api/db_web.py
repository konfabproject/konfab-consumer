# Database functions for web.py

import psycopg2
import psycopg2.extras
from psycopg2 import DataError, InternalError, DatabaseError
import csv
import simplejson as json
import time, datetime


city_map_meta = {
    'f1': 'isUnique',
    'f2': 'topic',
    'f3': 'title',
    'f4': 'desc',
    'f5': 'thumb',
    'f6': 'url'
}

city_map_twitter = {
    'f1': 'ts',
    'f2': 'userid',
    'f3': 'name',
    'f4': 'screenname',
    'f5': 'imgurl',
    'f6': 'text'
}

def city_mapping(things, map_provider):
    rsp = []
    #things = json.loads(things)

    for thing in things:
        o = {}
        for key, value in map_provider.items():
            o[value] = thing[key]
        rsp.append(o)

    return rsp

def neighborhood_counts(things, results):

    for thing in things:

        if thing not in results:
            results[thing] = 0
        results[thing] += 1
    return results

def get_hours(offset=24, from_time=None):
    start_time = time.time()

    if from_time:
        start_time = from_time

    hour = 60 * 60

    return  start_time - offset * hour


def pg_get_city(con, city_woeid, time_offset=24, from_time=None, limit=200):
    if not from_time:
        from_time = time.time()

    time_begin = get_hours(time_offset, from_time)

    q = """
        select url_id, count(*) as total,
        array_to_json(array_agg(DISTINCT ROW(m.uniqq, u.topic, u.title, u.description, u.thumbnail, u.url))) as meta,
        array_to_json(array_agg(DISTINCT ROW(m.time, m.tweet_userid, mm.name, mm.screen_name, mm.img_url, m.tweet_text))) as mentions,
        array_to_json(array_agg(DISTINCT(split_part(n.label,',',1)))) as neibs
        from (select a.*, NOT EXISTS (select 1 from messages b where a.url_id = b.url_id and a.in_city <> b.in_city group by b.in_city, b.url_id) as uniqq from messages a where a.in_city = %s and to_timestamp(a.time) >= to_timestamp(%s) and to_timestamp(a.time) <= to_timestamp(%s) and in_city is not null ) as m
        inner join (select id, title, url, description, thumbnail, topic from urls) u on u.id::text = m.url_id
        inner join (select user_id, name, screen_name, img_url from messages_meta) mm on m.tweet_userid = mm.user_id
        inner join (select label, woe_id from neighbourhoods) n on n.woe_id = m.in_hood
        group by m.url_id order by total desc limit %s;
    """

    cur = con.cursor('dict_cursor', cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [city_woeid, time_begin, from_time, limit])
    rows = cur.fetchall()
    cur.close()

    neighborhood_totals = {}

    output = {}
    output['results'] = []
    output['last-modified'] = time.time()

    for row in rows:
        r = dict(row)
        meta = city_mapping(r['meta'], city_map_meta)
        neighborhoods = r['neibs']
        neighborhood_counts(neighborhoods, neighborhood_totals)
        twitter_mentions = city_mapping(r['mentions'], city_map_twitter)

        out = {}
        for key, value in meta[0].items():
            out[key] = value

        out['urlid'] = r['url_id']
        out['popularity'] = r['total']
        out['mentions'] = twitter_mentions
        out['neighborhoods'] = neighborhoods

        output['results'].append(out)

    output['neighborhood_counts'] = neighborhood_totals

    return output

def pg_get_overall(con, time_offset=24, from_time=None):
    if not from_time:
        from_time = time.time()

    time_begin = get_hours(time_offset, from_time)

    q = """
        WITH q1 AS(select l.label, in_city, url_id, tweet_id, tweet_text, tweet_userid,
            NOT EXISTS (select 1 from messages b where a.url_id = b.url_id and a.in_city <> b.in_city group by b.in_city, b.url_id) as uniqq,
            SUBSTRING(uu.url FROM 'http://([^/]*).*') as domain
            from messages a
                inner join localities l on a.in_city = l.woe_id
                inner join urls uu on uu.id::text = a.url_id
                where in_city IN (2490383,2475687,2487956,2442047,2436704,2487889,2471390,2388929,2357536,2424766,2458833,2450022,2357024,2514815,2471217,2459115,2367105,2391585,2379574,2443945,2452078,2391279,9807)
                and CAST(to_timestamp(time) as timestamp) >= to_timestamp(%s) and CAST(to_timestamp(time) as timestamp) <= to_timestamp(%s)),
            q2 AS (select m.label, m.in_city, count(*) as total, count(CASE WHEN m.uniqq THEN 1 ELSE null END) as unique_count from q1 m group by m.in_city, m.label order by total desc
                ),
            q4 AS ( select pd.domain, pd.in_city, count(*) as domain_count from q1 pd group by pd.domain, pd.in_city order by domain_count desc
                ),
            q3 AS (
                select o.label, o.url_id, array_to_json((array_agg(row_to_json(o)))[1:1]) as pop_stories from (

                    select mm.rn, mm.label, mm.url_id, mm.total, mo.tweet_text, mo.tweet_userid, u.title, u.description, u.url, u.topic,u.thumbnail, meta.* from (select * from (
                        select row_number() over (partition by label order by count(*) desc ) as rn, label, in_city, url_id, count(*) as total from q1 a group by label, in_city, url_id order by in_city, total desc
                    ) m  where rn <= 5
                    ) mm
                    inner join messages mo on mo.url_id = mm.url_id
                    inner join urls u on u.id::text = mm.url_id
                    inner join messages_meta meta on mo.tweet_userid = meta.user_id
                    order by mm.label, mm.total desc
                ) o
                group by o.label, o.url_id order by o.label asc

            )
            select (select pdd.domain from q4 pdd where b.in_city = pdd.in_city limit 1) as pop_domain,
            (select ARRAY[ext.west, ext.south, ext.east, ext.north] as extent from (select label, st_xmin(geom) as west, st_ymin(geom) as south, st_xmax(geom) as east, st_ymax(geom) as north from localities where woe_id = b.in_city) as ext),
            b.label, b.in_city, b.total, b.unique_count, a.articles
            from ( select label, array_to_json(array_agg(pop_stories)) as articles from q3 group by label) a
            join q2 b on a.label = b.label order by b.label asc, b.total desc;
        """




    # keep around until I verify new query works
    old_q = """
        WITH q1 AS(select l.label, in_city, url_id, tweet_id, tweet_text, tweet_userid, NOT EXISTS (select 1 from messages b where a.url_id = b.url_id and a.in_city <> b.in_city group by b.in_city, b.url_id) as uniqq from messages a
        inner join localities l on a.in_city = l.woe_id
        where in_city IN (2490383,2475687,2487956,2442047,2436704,2487889,2471390,2388929,2357536,2424766,2458833,2450022,2357024,2514815,2471217,2459115,2367105,2391585,2379574,2443945,2452078,2391279,9807) and CAST(to_timestamp(time) as timestamp) >= to_timestamp(%s) and CAST(to_timestamp(time) as timestamp) <= to_timestamp(%s)),
        q2 AS (select m.label, m.in_city, count(*) as total, count(CASE WHEN m.uniqq THEN 1 ELSE null END) as unique_count from q1 m group by m.in_city, m.label order by total desc
        ),
        q3 AS (
        select o.label, o.url_id, array_to_json((array_agg(row_to_json(o)))[1:1]) as pop_stories from (

            select mm.rn, mm.label, mm.url_id, mm.total, mo.tweet_text, mo.tweet_userid, u.title, u.description, u.url, u.topic,u.thumbnail, meta.* from (select * from (
                select row_number() over (partition by label order by count(*) desc ) as rn, label, in_city, url_id, count(*) as total from q1 a group by label, in_city, url_id order by in_city, total desc
            ) m  where rn <= 5
            ) mm
            inner join messages mo on mo.url_id = mm.url_id
            inner join urls u on u.id::text = mm.url_id
            inner join messages_meta meta on mo.tweet_userid = meta.user_id
            order by mm.label, mm.total desc
        ) o
        group by o.label, o.url_id order by o.label asc

        )
        select b.label, b.in_city, b.total, b.unique_count, a.articles from ( select label, array_to_json(array_agg(pop_stories)) as articles from q3 group by label) a
        join q2 b on a.label = b.label order by b.label asc, b.total desc;
    """



    cur = con.cursor('dict_cursor', cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q, [time_begin, from_time])
    rows = cur.fetchall()
    cur.close()

    output = []

    for row in rows:
        output.append(dict(row))

    return output


