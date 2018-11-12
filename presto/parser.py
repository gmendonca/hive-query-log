from elasticsearch import Elasticsearch


class Config(object):
    LOG_DIR = '/var/log/hive/'
    GLOB_PATTERN = 'hadoop-cmf-CD-HIVE-*-HIVESERVER2-*.ec2.internal.log.out*'
    FROM_TIME = (datetime.today() - timedelta(minutes=15))
    TO_TIME = datetime.today().now()
    ES_HOST = 'localhost'
    ES_PORT = 9200

class Parse(object):


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="turn on logging level to DEBUG",
                    action="store_true")
    parser.add_argument("--es-host", help="Elasticsearch Host to send the logs to")
    parser.add_argument("--es-port", type=int, help="Elasticsearch Port to send the logs to")

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

    logging.debug('Using ES {}:{}'.format(config.ES_HOST, config.ES_PORT))

    parse = Parse(config)

    parse.parse_logs()
