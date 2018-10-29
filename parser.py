import glob
import os
import re
import time
import logging
from datetime import datetime, timedelta

class Config(object):
    LOG_DIR = './'
    GLOB_PATTERN = 'hadoop-cmf-CD-HIVE-*-HIVESERVER2-*.ec2.internal.log.out*'
    FROM_TIME = (datetime.today() - timedelta(hours=35))
    TO_TIME = datetime.today().now()


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

    def parse_logs(self):
        hive_log_files = self.find_files()

        regex_log_line = re.compile('^(?P<date>[12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))\ (?P<time>\d{2}\:\d{2}\:\d{2}\,\d{3})\ (?P<level>INFO|WARN|ERROR)\ \ (?P<class>[a-zA-Z.]+)\:\ \[(?P<pool>[a-zA-Z0-9-.]+)\:\ (?P<thread>[a-zA-Z0-9-.]+)\]\:')

        for hive_log_file in hive_log_files:
            file = open(hive_log_file)

            for line in file:
                match = re.search(regex_log_line, line)
                if match:
                    log_time = datetime.strptime(match.group('date') + ' ' + match.group('time'), '%Y-%m-%d %H:%M:%S,%f')
                    if self.time_in_range(self.config.FROM_TIME, self.config.TO_TIME, log_time):
                        print match.group('level'), match.group('class'),  match.group('pool'), match.group('thread')



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config = Config()
    parse = Parse(config)

    parse.parse_logs()
