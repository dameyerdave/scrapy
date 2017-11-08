# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import socket
import json
import sys
import pika

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

class MessageQueuePipeline(object):
    def __init__(self, rabbitmq_host, rabbitmq_port, rabbitmq_queue):
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rabbitmq_port
        self.rabbitmq_queue = rabbitmq_queue

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            rabbitmq_host=crawler.settings.get('RABBITMQ_HOST'),
            rabbitmq_port=crawler.settings.get('RABBITMQ_PORT'),
            rabbitmq_queue=crawler.settings.get('RABBITMQ_QUEUE'),
        )

    def process_item(self, item, spider):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host = self.rabbitmq_host))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.rabbitmq_queue, durable=True)

        self.channel.basic_publish(
            exchange = '',
            routing_key = self.rabbitmq_queue,
            body = json.dumps(dict(item)) + "\n")
        self.connection.close()
        return item