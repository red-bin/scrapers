#!/usr/bin/python3

import distance
import psycopg2
from tika import parser
from os import stat
from multiprocessing import Pool 
import re

import config

##TODO: IMPLEMENT THIS CODE.

def postgres_conn():
    connstr_template = "port=5432 dbname={} host={} user={} password={}"
    vals = (config.db_name, config.db_host, config.db_user, config.db_pass)
    connstr = connstr_template.format(*vals)
    conn = psycopg2.connect(connstr)

    return conn


def tika_compare_clean(content):
    return re.sub('[ \n\t]+',' ', content)

def compare_files(similar_fp, base_content):
    similar_content = parser.from_file(similar_fp)['content']
    similar_content = tika_compare_clean(similar_content)
   
    leven_dist = distance.nlevenshtein(base_content, similar_content) 

    if leven_dist <= .001:
        return similar_fp


def get_similar_files(filepath, size_threshold=.01, leven_threshold=.001, max_closest=100):
    file_size = stat(filepath).st_size

    max_diff = int(file_size * size_threshold)
    min_size = file_size - max_diff
    max_size = file_size + max_diff

    sqlstr = """
        SELECT file_location,file_size
        FROM pages 
        WHERE file_size BETWEEN {} AND {}
        AND url LIKE '%chicago.org%' 
        ORDER BY abs(file_size-{}::int) ;""".format(min_size, max_size, max_diff)

    curs = conn.cursor()
    curs.execute(sqlstr)

    similar = [i for i in curs.fetchall()]#[:max_closest]
    return similar

    print(len(similar))

    base_content = parser.from_file(filepath)['content']
    base_content = tika_compare_clean(base_content)

    vals = [(similar[i],base_content) for i in range(len(similar))]
    print(vals[0])

    pool = Pool(processes=24)
    results = pool.starmap(compare_files, (vals))

    return results

conn = postgres_conn()


testfile = '/opt/data/scrapes/ba31de31679654243c21097d92f60d01'

conn = postgres_conn()
res = get_similar_files(testfile)
