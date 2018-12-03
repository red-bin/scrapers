#!/usr/bin/python3

import json
from os import environ

conf_path = '{}/.scraper.conf'.format(environ['HOME'])
fh = open(conf_path, 'r')

conf = json.load(fh)

for key in ['db_user', 'db_pass', 'db_host', 'redis_host', 'scrape_dir']:
    if key not in conf:
        print("Conf is missing ", key, " key!")
        exit(1)

db_name = conf['db_name']
db_user = conf['db_user']
db_pass = conf['db_pass']
db_host = conf['db_host']
redis_host = conf['redis_host']
scrape_dir = conf['scrape_dir']
