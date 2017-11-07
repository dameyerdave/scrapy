# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import socket
import json
import sys

class RssPipeline(object):
    def process_item(self, item, spider):
        return item

class LogstashPipeline(object):
    def __init__(self, logstash_host, logstash_port):
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            logstash_host=crawler.settings.get('LOGSTASH_HOST'),
            logstash_port=crawler.settings.get('LOGSTASH_PORT')
        )

    #def open_spider(self, spider):

    #def close_spider(self, spider):

    def process_item(self, item, spider):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.sock.connect((self.logstash_host, self.logstash_port))
        self.sock.sendall(json.dumps(dict(item)) + "\n")
        self.sock.close()
        return item

