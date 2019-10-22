#!/usr/bin/env python
# vim: set fileencoding=utf-8 tw=0:
"""
Simple website crawler.

Reads urls from a text file, downloads recursively all websites, saves pages to the database for further processing.
"""

from __future__ import print_function

import logging
import os
import re
import subprocess
import threading
import time
import urllib
import urlparse

import HTMLParser
import httplib2
from sqlite3 import dbapi2 as sqlite3
from sqlite3 import OperationalError


DB_PATH = "spider.db"

URL_BLACKLIST = re.compile(r'.*\.(jpg|gif|png|pdf|avi|mp4|doc|xls|zip)$', re.I)

MAX_LEVEL = 1

CHILDREN = 5


class Database(object):
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, timeout=60)
        self.conn.text_factory = str

    def __del__(self):
        self.conn.commit()

    def query(self, query, params=None):
        for attempt in range(10):
            try:
                ts = time.time()
                cur = self.conn.cursor()
                cur.execute(query, params or [])
                dur = time.time() - ts
                if dur >= 0.5:
                    logging.debug('SQL %04.1f slow: %s; params: %s' % (dur, query, params))
                return cur
            except OperationalError, e:
                if 'database is locked'  in e.message:
                    logging.warning('database is locking, will retry in 5 seconds; query: %s; params: %s' % (query, params))
                    time.sleep(5)
                    continue
                raise

        raise RuntimeError('database is locked')

    def insert(self, table, values):
        fields = []
        qmarks = []
        params = []

        for k, v in values.items():
            fields.append("`%s`" % k)
            qmarks.append("?")
            params.append(v)

        query = "INSERT INTO `%s` (%s) VALUES (%s)" % (table,
            ", ".join(fields), ", ".join(qmarks))

        cur = self.query(query, params)
        rowid = cur.lastrowid
        cur.close()

        return rowid

    def fetch(self, query, params=None):
        cur = self.query(query, params)
        rows = cur.fetchall()
        cur.close()
        return rows

    def fetchone(self, query, params=None):
        cur = self.query(query, params)
        rows = cur.fetchone()
        cur.close()
        return rows

    def update(self, table, values, conditions):
        sets = []
        wheres = []
        params = []

        for k, v in values.items():
            sets.append("`%s` = ?" % k)
            params.append(v)

        for k, v in conditions.items():
            wheres.append('`%s` = ?' % k)
            params.append(v)

        sets = ", ".join(sets)
        wheres = " AND ".join(wheres)

        query = "UPDATE `%s` SET %s WHERE %s" % (table, sets, wheres)

        cur = self.query(query, params)
        cur.close()

    def begin(self):
        cur = self.query('begin')
        cur.close()

    def commit(self):
        self.conn.commit()


class Spider(object):
    def __init__(self):
        self.db = Database()
        self.html = HTMLParser.HTMLParser()

    def add_url(self, url, level):
        """Add a new url to the queue.

        Does nothing if the url is already there.
        """
        row = self.db.fetch('SELECT id FROM urls WHERE url = ?', [url])
        if not row:
            self.db.insert("urls", {
                "url": url,
                "level": level,
            })

    def show_stats(self):
        """Logs percentage of processed urls, periodically."""
        total = self.db.fetchone('SELECT COUNT(1) FROM `urls` WHERE level <= ?', [MAX_LEVEL])[0]
        ready = self.db.fetchone('SELECT COUNT(1) FROM `urls` WHERE `status` IS NOT NULL AND level <= ?', [MAX_LEVEL])[0]

        if ready % 10 == 0:
            pc = ready * 100 / total
            logging.info('         stats: %u/%u urls processed, %u%%' % (ready, total, pc))
            self.db.conn.commit()

    def go_work(self):
        """Process the queue, url by url.

        Picks a random URL from the queue, fetches the page, finds links in it,
        adds new links to the queue, repeats until the queue's not empty.
        """

        while True:
            url = self.get_random_url()
            if not url:
                logging.info('nothing to do.')
                break

            self.db.update('urls', {'status': 999}, {'id': url['id']})  # hide it from other threads
            self.db.commit()

            self.process_url(url)

    def get_random_url(self):
        """Return a random unprocessed URL from the queue."""
        row = self.db.fetchone('SELECT id, url, level FROM urls WHERE status IS NULL AND level <= ? ORDER BY level, id LIMIT 1', [MAX_LEVEL])
        if not row:
            self.db.query('UPDATE urls SET status = NULL WHERE status IN (503, 999)')
            row = self.db.fetchone('SELECT id, url, level FROM urls WHERE status IS NULL AND level <= ? ORDER BY level, id LIMIT 1', [MAX_LEVEL])

        if not row:
            return None

        return {
            'id': int(row[0]),
            'url': row[1],
            'level': int(row[2]),
        }

    def process_url(self, url):
        """Reads a page, adds links to new pages, saves the body."""
        new_urls = []

        try:
            status, body = self.fetch_html(url['url'])

            if body:
                new_urls = self.find_urls(body, url['url'])
        except Exception, e:
            logging.info('503 %s -- %s' % (url['url'], e))

            status = 999
            body = None
        except KeyboardInterrupt:
            logging.info('interrupted by ctrl-c')
            exit(1)

        body = self.process_body(body)

        self.db.begin()

        self.db.update("urls", {
            "status": status,
            "body": body,
        }, {
            "id": url['id'],
        })

        for new_url in new_urls:
            self.add_url(new_url, url['level'] + 1)

        self.show_stats()

        self.db.commit()

    def fetch_html(self, url):
        """Reads the HTML document, returns (status, body)."""
        if URL_BLACKLIST.match(url):
            logging.info('406 00.0 %s -- blacklisted' % url)
            return 200, None

        ts = time.time()

        try:
            http = httplib2.Http()
            res = http.request(url, headers={
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0',
            })

            dur = time.time() - ts

            status = int(res[0]['status'])
            body = res[1]

            ct = res[0]['content-type'].split(';')[0]
            if ct != 'text/html':
                status = 406  # not acceptable

            real_url = res[0].get('content-location', url)
            if real_url != url:
                if real_url.startswith(self.get_base_url(url)):
                    pass  # logging.info('      >> %s' % real_url)
                elif real_url.startswith(self.get_base_url(url).replace('http://', 'https://')):
                    pass  # logging.info('         redirected to %s -- ok' % real_url)
                else:
                    logging.info('301 %04.1f %s -- external, skip' % (dur, real_url))
                    return 301, None

        except httplib2.ServerNotFoundError, e:
            status = 404
            dur = time.time() - ts

        except Exception, e:
            dur = time.time() - ts
            status = 503
            logging.info('%3u %04.1f %s -- %s' % (status, dur, url, e.__class__.__name__))
            return status, None

        logging.info('%3u %04.1f %s' % (status, dur, url))

        if status != 200:
            return status, None

        # TODO: fix charset?

        return status, body

    def find_urls(self, html, page_url):
        """Returns a list of all links within the HTML document."""
        urls = set()

        base_url = self.get_base_url(page_url)

        tags = re.findall(r'<a[^>]+>', html)
        for tag in tags:
            attrs = self.parse_attrs(tag)
            url = self.get_page_url(attrs, page_url)
            if url is None:
                continue
            if not url.startswith(base_url):
                continue

            if URL_BLACKLIST.match(url):
                continue

            url = self.cleanup_url(url)
            urls.add(url)

        return list(urls)

    def parse_attrs(self, tag):
        attrs = {}

        for m in re.finditer(r'([a-z-]+)="([^"]+)', tag):
            attrs[m.group(1)] = m.group(2)

        for m in re.finditer(r"([a-z-]+)='([^']+)", tag):
            attrs[m.group(1)] = m.group(2)

        return attrs

    def get_page_url(self, attrs, page_url):
        if 'href' not in attrs:
            return None

        href = attrs['href']

        if isinstance(href, str):
            href = href.decode('utf-8')
        try:
            href = self.html.unescape(href)
        except Exception, e:
            print('error unescaping:', href, href.__class__, e)

        if isinstance(href, unicode):
            href = href.encode('utf-8')

        href = href.split('#')[0]

        # Fix unicode.
        href = urllib.quote(href, '/:?&=')

        if re.match(r'^(mailto|tel|ftp)', href):
            return None

        if href.startswith('http://'):
            return href

        if href.startswith('https://'):
            return href

        url = urlparse.urljoin(page_url, href)
        return url

    def get_base_url(self, url):
        """Returns the base URL of the web site."""
        parts = url.split('/')
        base = '/'.join(parts[0:3]) + '/'
        return base

    def process_body(self, body):
        """Pre-process body before saving.

        Whole pages would take lots of space, just pick the emails and save a comma-separated list.
        """
        emails = self.find_emails(body)
        body = ', '.join(emails) if emails else None
        return body

    def find_emails(self, html):
        """Returns a list of email addresses found in the HTML document."""
        emails = set()

        if html:
            tags = re.findall(r'<a[^>]+>', html)
            for tag in tags:
                attrs = self.parse_attrs(tag)
                if 'href' in attrs:
                    href = attrs['href']
                    if href.startswith('mailto:'):
                        emails.add(href[7:])

        return list(emails)

    def cleanup_url(self, url):
        """Remove suspiciour arguments from the URL, e.g. PHPSESSID"""
        url = url.split('#')[0]

        url = re.sub(r'PHPSESSID=[0-9a-z]+', '', url)
        url = url.replace('?&', '?')
        url = url.rstrip('?&')

        return url

    def maintain_db(self):
        """Database maintenance.

        Has the code which you need to run befor processing the queue.
        Helpful when you find you need to change the collected data to update the algo.
        """
        pass

    @classmethod
    def thread(cls):
        me = cls()
        me.go_work()


def setup_logging():
    fmt = logging.Formatter('%(asctime)s %(message)s')

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fh = logging.FileHandler('spider.log')
    fh.setFormatter(fmt)
    root.addHandler(fh)

    con = logging.StreamHandler()
    con.setFormatter(fmt)
    root.addHandler(con)


def main():
    setup_logging()

    urls = []
    with open('spider.txt', 'r') as f:
        for url in f:
            urls.append(url.strip())

    spider = Spider()
    for url in urls:
        spider.add_url(url, 0)
    spider.maintain_db()

    row = spider.db.fetchone('SELECT COUNT(1) FROM urls WHERE status IS NULL')
    logging.info('have %u urls to process, starting threads...' % row[0])

    spider.db.commit()

    threads = []
    for i in range(CHILDREN):
        t = threading.Thread(target=Spider.thread)
        threads.append(t)
        t.start()
        time.sleep(5)

    # wait for all threads to finish
    main_thread = threading.currentThread()
    for t in threading.enumerate():
        if t is not main_thread:
            t.join()

    try:
        subprocess.Popen(['telega', 'spider.py: finished with the queue.'])
    except:
        pass


if __name__ == '__main__':
    main()
