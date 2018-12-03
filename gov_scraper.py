#!/usr/bin/python3

import csv
import json
import magic
import psycopg2
import re
import redis
import requests
import shutil

from pprint import pprint
from bs4 import BeautifulSoup
from hashlib import md5
from http.client import HTTPSConnection
from lepl.apps import rfc3696
from os import makedirs, path, stat
from threading import Thread
from tika import parser
from time import sleep
from urllib.parse import urlparse

import config

def postgres_conn():
    connstr_template = "port=5432 dbname={} host={} user={} password={}"
    vals = (config.db_name, config.db_host, config.db_user, config.db_pass)
    connstr = connstr_template.format(*vals)
    conn = psycopg2.connect(connstr)

    return conn

conn = postgres_conn()

def save_page_info(run_id, url, file_type, headers, file_size,
                   md5, file_location, tika_location, tika_md5, 
                   scrape_depth, http_code,time):

    curs = conn.cursor()
    sqlstr = """INSERT INTO pages 
                (run_id, url, file_type, headers, file_size,
                 md5, file_location, tika_location, tika_md5, 
                 scrape_depth, http_code, time)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 RETURNING id"""

    values = (run_id, url, file_type, headers, file_size,
        md5, file_location, tika_location, tika_md5, 
        scrape_depth, http_code, time)

    curs.execute(sqlstr, values)

    page_id = curs.fetchall()
    conn.commit()

    return page_id

def download_url(url):
    response = requests.get(url, stream=True)
    response.raw.decode_content = True

    file_name = md5(url.encode("utf-8")).hexdigest()
    savepath = '{}/{}'.format(config.scrape_dir, file_name)


    with open(savepath, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)

    page_data = {'run_id': None,
                 'url': url,
                 'file_type':magic.from_file(savepath, mime=True),
                 'headers': json.dumps(dict(response.headers)),
                 'file_size': stat(savepath).st_size,
                 'md5': md5(open(savepath,'rb').read()).hexdigest(),
                 'file_location': savepath,
                 'tika_md5': None,
                 'tika_location': None,
                 'scrape_depth': 1,
                 'http_code': response.status_code,
                 'time': 'Jan 1 1970 00:00:00'}

    return page_data

def insert_email_addrs(email_addrs, url):
    curs = conn.cursor()
    sqlstr = """
        INSERT INTO email_addresses (email_address, url)
        VALUES (%s, %s) 
        RETURNING ID"""

    values = zip(email_addrs, [url for i in range(len(email_addrs))])

    curs.executemany(sqlstr, values)
    conn.commit()

    return

def clean_href(href, url):
    parsed_url = urlparse(url)
    if 'http' not in href:
        if href.startswith('./'):
            href = href[2:]

        href = href.lstrip('/')
        href = '%s://%s/%s' % (parsed_url.scheme, parsed_url.netloc, href)

    if '/../' in href:
        href = href.replace('/../', '/')

    return href

def get_old_md5s():
    curs = conn.cursor()
    sqlstr = "SELECT DISTINCT(md5) from pages"

    curs.execute(sqlstr)
    md5s = [i[0] for i in curs.fetchall()]

    return md5s

def get_old_urls():
    curs = conn.cursor()
    sqlstr = "SELECT DISTINCT(url) from pages"

    curs.execute(sqlstr)
    urls = [i[0] for i in curs.fetchall()]

    return urls

def get_old_tika_md5s():
    curs = conn.cursor()
    sqlstr = "SELECT DISTINCT(tika_md5) from pages"

    curs.execute(sqlstr)
    tika_md5s = [i[0] for i in curs.fetchall()]

    return tika_md5s 

def extract_domain(url):
    return '.'.join(urlparse(url).netloc.split('.')[-2:])

def http_to_https(url):
    return url.replace('http://', 'https://')

def netloc_has_https(netloc):
    port = 443
    if ':' in netloc:
        after_colon = netloc.split(':')[-1]
        if after_colon.isdigit():
            port = int(after_colon)

    https_conn = HTTPSConnection(host=netloc, timeout=1, port=port)
    try:
        https_conn.connect()
        https_conn.close()
        return True

    except Exception as e:
        print(netloc, " doesn't have https!")
        return False, e.args[1]

def emails_from_tika_parse(tika_location):
    validator = rfc3696.Email()
    if not tika_location:
        return []
    fh = open(tika_location, 'r')

    parsed_content = fh.read()
    fh.close()

    if not parsed_content:
        return []

    split_parsed = re.split('[$%?\n\t"\'/ (),:;<>\[\]\\\]+', str(parsed_content))
    split_parsed = [i for i in split_parsed if '@' in i]
    email_addrs = [i for i in split_parsed if validator(i)]

    return email_addrs

def extract_info_from_html(url_data):
    fh = open(url_data['file_location'], 'rb')
    lxml_soup = BeautifulSoup(fh.read(), 'lxml')
    fh.close()

    email_addrs = []
    urls = []

    email_validator = rfc3696.Email()
    for a_tag in lxml_soup.findAll('a'):
        href = a_tag.get('href')

        if not href:
            continue

        elif href.startswith('mailto:'):
            email_addr = href.replace('mailto:','')
            if email_validator(email_addr):
                email_addrs.append(email_addr)

        else:
            cleaned_href = clean_href(href, url_data['url'])
            urls.append(cleaned_href)


    urls = list(set(map(str.lower, urls)))
    email_addrs = list(set(map(str.lower, email_addrs)))

    return {'urls': urls, 'email_addrs':email_addrs}

class SiteScraper():
    def __init__(self, site, stay_on_domain=True, no_queries=True,
                 no_fragments=True, https_first=True, use_cache=False,
                 clear_redis=True, thread_count=1):

        def init_redis():
            redis_conn = redis.Redis(host=config.redis_host)

            if clear_redis:
                redis_conn.delete('pending_urls')
                redis_conn.delete('unqueued')
                redis_conn.delete('freshly_scraped')

            return redis_conn

        self.redis_conn = init_redis()
 
        self.starting_site = site
        self.stay_on_domain = stay_on_domain
        self.no_queries = no_queries
        self.no_fragments = no_fragments
        self.https_first = https_first

        self.thread_count = thread_count

        self.http_domains = []
        self.threads = []

        self.email_addrs = []

        if use_cache:
            self.old_md5s = get_old_md5s()
            self.old_urls = get_old_urls()
            self.old_tika_md5s = get_old_tika_md5s()

        else:
           self.old_md5s = []
           self.old_urls = []
           self.old_tika_md5s = []

    def process_scraped(self):
        freshly_scraped = self.pull_freshly_scraped()
        for fresh_scrape in freshly_scraped:

            #if md5s already exists, don't add urls to any queue
            #checks for both tikafied md5 and raw download
            if fresh_scrape['md5'] in self.old_md5s:
                continue
            else:
                self.old_md5s.append(fresh_scrape['md5'])

            if fresh_scrape['tika_md5'] in self.old_tika_md5s:
                continue
            else:
                self.old_tika_md5s.append(fresh_scrape['tika_md5'])

            for new_url in fresh_scrape['extracted']['urls']:
                new_url = new_url.rstrip('#')
                if self.url_needed(new_url, fresh_scrape['url']):
                    if new_url not in self.old_urls:
                        self.old_urls.append(new_url)
                    self.redis_conn.rpush('pending_urls', new_url)

            email_addrs = fresh_scrape['extracted']['email_addrs']
            email_addrs = list(set(map(str.lower, email_addrs)))

            insert_email_addrs(email_addrs, fresh_scrape['url'])

    def pull_freshly_scraped(self):
        fresh_scrapes = []
        while True:
            scraped_raw = self.redis_conn.rpop('freshly_scraped')

            if not scraped_raw:
                break

            fresh_scrapes.append(json.loads(scraped_raw.decode('utf-8')))

        return fresh_scrapes
 
    def pull_unqueued(self):
        unqueued = []
        while True:
            unqueued_raw = self.redis_conn.rpop('unqueued')
            if not unqueued_raw:
                break

            unqueued.append(unqueued_raw)

        return list(set(unqueued))

    def url_needed(self, url, parent_url=None):
        if url in self.old_urls:
            return False

        if self.no_queries and urlparse(url).query:
            return False

        if self.no_fragments and urlparse(url).fragment:
            return False

        if parent_url:
            parent_domain = extract_domain(parent_url)
            url_domain = extract_domain(url)

            #isn't needed if domain not the same as parent's domain
            if self.stay_on_domain and parent_domain != url_domain:
                return False

        return True

    def save_tika_parse(self, file_location):
        tika_parse = parser.from_file(file_location)
        tika_content = parser.from_file(file_location)['content']

        if not tika_content:
            return None, None

        tika_md5 = md5(tika_content.encode('utf8')).hexdigest()

        save_template = '{}/tika_scrapes/{}.tika'
        tika_savepath = save_template.format(config.scrape_dir, tika_md5)

        tika_fh = open(tika_savepath, 'w')
        tika_fh.write(tika_content)
        
        return tika_md5, tika_savepath

    def post_parse(self, url_data):
        file_location = url_data['file_location']
        tika_md5, tika_savepath = self.save_tika_parse(file_location)

        if not tika_md5:
            print("NOT TIKA-ABLE: ", url_data['url'])

        url_data['tika_md5'] = tika_md5
        url_data['tika_location'] = tika_savepath

        page_id = save_page_info(**url_data)
        url_data['id'] = page_id
        url_data['extracted'] = self.extract_from_downloaded(url_data)

        return url_data

    def extract_from_downloaded(self, url_data):
        extracted = {'urls':[], 'email_addrs':[]} 

        if url_data['file_type'] == 'text/html':
            html_extraction = extract_info_from_html(url_data)
            extracted['urls'] += html_extraction['urls']
            extracted['email_addrs'] += html_extraction['email_addrs']

        elif url_data['file_type'] == 'application/pdf':
            pdf_emails = emails_from_tika_parse(url_data['tika_location'])
            extracted['email_addrs'] += pdf_emails

        return extracted

    def refresh_dead_threads(self):
        """check if any threads died, and bring some up if so."""
        self.threads = [s for s in scraper.threads if s.isAlive()]
        self.start_threads()

    def url_scraper(self, url):
        downloaded_data = download_url(url)
        url_data = self.post_parse(downloaded_data)
        
        self.redis_conn.rpush('freshly_scraped', json.dumps(url_data))

    def url_scraper_thread(self):
       while True:
            next_url = self.redis_conn.rpop('pending_urls')
            if next_url:
               next_url = next_url.decode('utf-8')
               self.url_scraper(next_url)
            else:
               sleep(.1)

    def start_threads(self):
        threads_needed = self.thread_count - len(self.threads)

        for i in range(threads_needed):
            worker = Thread(target=self.url_scraper_thread)
            worker.setDaemon(True)
            worker.start()

            self.threads.append(worker)

        return threads_needed

    def start_scraping(self):
        self.redis_conn.rpush('pending_urls', self.starting_site)
        self.start_threads()
        while True:
            self.process_scraped()
            sleep(.1)
            self.refresh_dead_threads()

if __name__ == '__main__':
    scraper = SiteScraper('https://cityofchicago.org', thread_count=30)
    scraper.start_scraping()
