import glob
import os
import re
import time
import logging
import socket
from datetime import datetime, timedelta, date
from elasticsearch import Elasticsearch

class Config(object):
    LOG_DIR = '/var/log/hive/'
    GLOB_PATTERN = 'hadoop-cmf-CD-HIVE-*-HIVESERVER2-*.ec2.internal.log.out*'
    FROM_TIME = (datetime.today() - timedelta(minutes=15))
    TO_TIME = datetime.today().now()
    ES_HOST = os.environ['ES_HOST']
    ES_PORT = os.environ['ES_PORT']

class Parse(object):

    def __init__(self, config):
        self.config = config

    def time_in_range(self, start, end, x):
        """Return true if x is in the range [start, end]"""
        if start <= end:
            return start <= x <= end
        else:
            return start <= x or x <= end

    def find_files(self):
        hive_log_files_pattern = os.path.join(self.config.LOG_DIR, self.config.GLOB_PATTERN)
        hive_log_files = glob.glob(hive_log_files_pattern)

	selected = []

        for hive_log_file in hive_log_files:
            t = datetime.fromtimestamp(os.path.getmtime(hive_log_file))
            if self.time_in_range(self.config.FROM_TIME, self.config.TO_TIME, t):
                logging.info("Getting file: {} - {}".format(t, hive_log_file))
                selected.append(hive_log_file)

        return selected

    def send_to_elasticsearch(self, query_dict):
        connection_es = "%s:%s" % (self.config.ES_HOST, self.config.ES_PORT)
        es = Elasticsearch(connection_es)
        index_name = "hive_%s_%s" % (socket.gethostname(), date.today())
        es.indices.create(index=index_name, ignore=400) # 400 is index already created
        query_dict['timestamp'] = datetime.now()
        es.index(index=index_name, doc_type='query', body=query_dict)

    def parse_logs(self):
        hive_log_files = self.find_files()

        regex_log_line = re.compile('^(?P<date>[12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))\ (?P<time>\d{2}\:\d{2}\:\d{2}\,\d{3})\ (?P<level>INFO|WARN|ERROR)\ \ (?P<class>[a-zA-Z.]+)\:\ \[(?P<pool>[a-zA-Z0-9-.]+)\:\ (?P<thread>[a-zA-Z0-9-.]+)\]\: (?P<message>.*)$')

        regex_completed_compiling = re.compile('^Completed compiling command\(queryId=(?P<query_id>.+)\); Time taken\: (?P<time>.*)$')
        regex_query_command = re.compile('^Executing command\(queryId=(?P<query_id>.+)\)\:(?P<query>.*)$')
        regex_completed_command = re.compile('^Completed executing command\(queryId=(?P<query_id>.+)\)\; Time taken\: (?P<time>.*)$')
        regex_queue_user = re.compile('Setting queue name to: \'(?P<queue>.+)\' for user \'(?P<user>.+)\'')

        thread_info = {}
        incommand = False
        thread_id = None

        for hive_log_file in hive_log_files:
            file = open(hive_log_file)

            for line in file:
                match = re.search(regex_log_line, line)
                if match:
                    log_time = datetime.strptime(match.group('date') + ' ' + match.group('time'), '%Y-%m-%d %H:%M:%S,%f')
                    if self.time_in_range(self.config.FROM_TIME, self.config.TO_TIME, log_time):
                        message = match.group('message')
                        thread = match.group('thread')
                        if thread and thread not in thread_info:
                            # starting dict for next thread
                            thread_info[thread] = {}
                        if incommand:
                            incommand = False
                            logging.debug('query = {}'.format(thread_info[thread_id]['query']))
                            # maybe broken file can mess this up, I think
                            if 'queue'in thread_info[thread]: logging.debug('queue = {}'.format(thread_info[thread]['queue']))
                            if 'user' in thread_info[thread]: logging.debug('user = {}'.format(thread_info[thread]['user']))
                            thread_id = None
                        if match.group('class') == 'org.apache.hadoop.hive.schshim.FairSchedulerShim':
                            queue_user = re.search(regex_queue_user, message)
                            if queue_user:
                                thread_info[thread]['queue'] = queue_user.group('queue')
                                thread_info[thread]['user'] = queue_user.group('user')
                        if match.group('class') == 'org.apache.hadoop.hive.ql.Driver':
                            # removing this cause tables with ALTER and DROP wasn't taking into account
                            # if this is intended, maybe we need to remove this
                            #if match.group('pool') == 'HiveServer2-Handler-Pool':
                            compile = re.search(regex_completed_compiling, message)
                            if compile:
                                thread_info[thread]['query_id'] = compile.group('query_id')
                                thread_info[thread]['compile_time'] = compile.group('time')
                            #elif match.group('pool') == 'HiveServer2-Background-Pool':
                            command = re.search(regex_query_command, message)
                            if command:
                                logging.debug('query_id = {}'.format(command.group('query_id')))
                                logging.debug('thread_id = {}'.format(thread))
                                thread_id = thread
                                incommand = True
                                # cases when the query is in the same line as the command log
                                query = command.group('query')
                                if query:
                                    thread_info[thread]['query'] = query
                                    thread_info[thread]['query_start_time'] = log_time
                            finish_command = re.search(regex_completed_command, message)
                            if finish_command:
                                time_taken = finish_command.group('time')
                                logging.debug('time taken = {} {}'.format(time_taken, finish_command.group('query_id')))
                                # maybe I should check for query_id in every step
                                thread_info[thread]['time_taken'] = time_taken
                                # since command finshed, sending to ES
                                self.send_to_elasticsearch(thread_info[thread])
                                del thread_info[thread]
                elif incommand and thread_id:
                    # multline query
                    if 'query' in thread_info[thread_id]:
                        thread_info[thread_id]['query'] += line
                    else:
                        thread_info[thread_id] = {
                                'query': line
                                }


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    config = Config()
    parse = Parse(config)

    parse.parse_logs()
