import os
import json
import requests
import argparse
import logging
from elasticsearch import Elasticsearch
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, date

class Config(object):
    FROM_TIME = (datetime.utcnow() - timedelta(minutes=15))
    TO_TIME = datetime.utcnow()
    PRESTO_HOST = 'localhost'
    PRESTO_SSL = False
    PRESTO_PORT = 8080
    PRESTO_QUERIES_ENDPOINT = 'v1/query'
    PRESTO_USERNAME = 'user'
    PRESTO_PASSWORD = 'password'
    ES_HOST = 'localhost'
    ES_PORT = 9200

class Parse(object):
    def __init__(self, config):
        self.config = config

    def get_presto_url(self):
        if self.config.PRESTO_SSL:
            protocol = "https"
        else:
            protocol = "http"

        url = "%s://%s:%s" % (protocol, self.config.PRESTO_HOST, self.config.PRESTO_PORT)
        return os.path.join(url, self.config.PRESTO_QUERIES_ENDPOINT)

    def get_queries_json(self):
        url = self.get_presto_url()
        resp = requests.get(url, auth=HTTPBasicAuth(self.config.PRESTO_USERNAME, self.config.PRESTO_PASSWORD))
        return resp.json()

    def send_to_elasticsearch(self, query_dict):
        connection_es = "%s:%s" % (self.config.ES_HOST, self.config.ES_PORT)
        es = Elasticsearch(connection_es)
        index_name = "presto-%s-%s" % (self.config.PRESTO_HOST, date.today().strftime('%Y.%m.%d'))
        es.indices.create(index=index_name, ignore=400) # 400 is index already created
        query_dict['timestamp'] = datetime.now()
        es.index(index=index_name, doc_type='query', body=query_dict)

    def time_in_range(self, start, end, x):
        """Return true if x is in the range [start, end]"""
        if start <= end:
            return start <= x <= end
        else:
            return start <= x or x <= end

    def parse_queries(self):
        json_dict = self.get_queries_json()
        queries = {}
        for query in json_dict:
            t = datetime.strptime(query['queryStats']['createTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
            if self.time_in_range(self.config.FROM_TIME, self.config.TO_TIME, t):
                query_json = {}
                query_json['query_id'] = query['queryId']
                query_json['query'] = query['query']
                query_json['query_start_time'] = query['queryStats']['createTime']
                query_json['time_taken'] = query['queryStats']['elapsedTime']
                query_json['user'] = query['session']['user']
                query_json['source'] = query['session']['source'] if 'source' in query['session'] else None
                query_json['user_agent'] = query['session']['userAgent'] if 'userAgent' in query['session'] else None
                query_json['state'] = query['state']
                query_json['memory_pool'] = query['memoryPool']
                self.send_to_elasticsearch(query_json)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="turn on logging level to DEBUG",
                    action="store_true")
    parser.add_argument("--es-host", help="Elasticsearch Host to send the logs to")
    parser.add_argument("--es-port", type=int, help="Elasticsearch Port to send the logs to")
    parser.add_argument("--presto-host", help="Presto Host to send the logs to")
    parser.add_argument("--presto-port", type=int, help="Presto Port to send the logs to")
    parser.add_argument("--presto-ssl", help="Presto has SSL or not", action="store_true")
    parser.add_argument("--presto-endpoint", help="Presto Queries ENDPOINT")
    parser.add_argument("--presto-username", help="Presto Username")
    parser.add_argument("--presto-password", help="Presto Password")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    config = Config()
    if args.es_host:
        config.ES_HOST = args.es_host

    if args.es_port:
        config.ES_PORT = args.es_port

    if args.presto_ssl:
        config.PRESTO_SSL = args.presto_ssl

    if args.presto_host:
        config.PRESTO_HOST = args.presto_host

    if args.presto_port:
        config.PRESTO_PORT = args.presto_port

    if args.presto_endpoint:
        config.PRESTO_QUERIES_ENDPOINT = args.presto_endpoint

    if args.presto_username:
        config.PRESTO_USERNAME = args.presto_username

    if args.presto_password:
        config.PRESTO_PASSWORD = args.presto_password

    logging.debug('Using ES {}:{}'.format(config.ES_HOST, config.ES_PORT))
    logging.debug('Connecting to PRESTO {}:{}'.format(config.PRESTO_HOST, config.PRESTO_PORT))

    parse = Parse(config)

    parse.parse_queries()
