import glob
import os
import time
import logging
from datetime import datetime, timedelta

class Config(object):
    LOG_DIR = '/var/log/hive'
    GLOB_PATTERN = 'hadoop-cmf-CD-HIVE-*-HIVESERVER2-*.ec2.internal.log.out*'
    FROM_TIME = (datetime.today() - timedelta(minutes=5))
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
                logging.info("Getting file: {} - {}", t, hive_log_file)
                selected.append(hive_log_file)

        return selected


if __name__ == '__main__':
    config = Config()
    parse = Parse(config)

    parse.find_files()
