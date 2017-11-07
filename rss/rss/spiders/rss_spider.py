import scrapy
import json
import sqlite3
import urllib3 as urllib
from pprint import pprint
from objdict import ObjDict
from dateutil.parser import parse
from langdetect import detect

from parser.article_parser import ArticleParser

class RssSpider(scrapy.Spider):
    name="rss"

    def __init__(self):
        self.conn = sqlite3.connect('ds.db', isolation_level=None)
        self.conn.execute('CREATE TABLE IF NOT EXISTS store (hash text)')

    def start_requests(self):
        with open('rss_config.json') as configFile:    
            config = json.load(configFile)
        for rss in config['rss']:
            if rss['enabled']:
                for url in rss['urls']:
                    domain = self.get_domain(url)
                    request = scrapy.Request(url, self.parse)
                    request.meta['domain'] = domain
                    request.meta['contentCss'] = rss['contentCss']
                    yield request

    def parse(self, response):
        domain = response.meta['domain']
        for _item in response.css('item'):
            item = ObjDict();
            item.reference = _item.css('link::text').extract_first()
            if domain == self.get_domain(item.reference):
                result = self.conn.execute("SELECT COUNT(*) FROM store WHERE hash='"+item.reference+"'").fetchone()
                if result[0] == 0:
                    self.conn.execute("INSERT INTO store VALUES ('"+item.reference+"')")
                    item.title = _item.css('title::text').extract_first()
                    item.abstract = _item.css('description::text').extract_first()
                    timestamp = parse(_item.css('pubDate::text').extract_first()).strftime('%Y-%m-%d %H:%M:%S')
                    item['@timestamp'] = timestamp
                    item.language = detect(item.abstract)
                    item.domain = domain

                    parser = ArticleParser(contentCss=response.meta['contentCss'])
                    request = scrapy.Request(item.reference, parser.process)
                    request.meta['item'] = item
                    yield request

    def get_domain(self, _url):
        url = urllib.util.parse_url(_url)
        parts = url.hostname.split('.')
        return parts[-2]
