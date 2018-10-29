import glob
import os
import time

class Config(object):
    LOG_DIR = '/var/log/hive'
    GLOB_PATTERN = 'hadoop-cmf-CD-HIVE-*-HIVESERVER2-*.ec2.internal.log.out*'


class Parse(object):

    def __init__(self, config):
        self.config = config

    def find_files(self):
        hive_log_files_pattern = os.path.join(self.config.LOG_DIR, self.config.GLOB_PATTERN)
        hive_log_files = glob.glob(hive_log_files_pattern)

        for hive_log_file in hive_log_files:
            t = time.ctime(os.path.getmtime(hive_log_file))
            print t


if __name__ == '__main__':
    config = Config()
    parse = Parse(config)

    parse.find_files()
