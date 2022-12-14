# configuration
from configparser import ConfigParser
import sys

# logging
import logging
import logging.handlers

# scheduling
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler

# loader modules
from kotel_loader import KotelLoader
from influx_loader import InfluxLoader


def prepare_logging(config: ConfigParser):
    if 'logger' not in config:
        print('Missing section logger.')
        config['logger'] = {}
    logger_config = config['logger']
    logger_level = logger_config.get('level', 'INFO')
    numeric_level = getattr(logging, logger_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % logger_level)

    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    return logger


def prepare_kotel(config: ConfigParser, logger: logging.Logger):
    if 'kotel' not in config:
        logger.error('Missing section kotel.')
        sys.exit(1)
    kotel_config = config['kotel']
    kotel_domain = kotel_config.get('domain')
    kotel_password = kotel_config.get('password', '00000000')
    kotel_type_override = {'__R23658_UDINT_u': float}
    loader = KotelLoader(kotel_domain, kotel_password, kotel_type_override, logger)
    return loader


def prepare_influx(config: ConfigParser, logger: logging.Logger):
    if 'influx' not in config:
        logger.error('Missing section influx.')
        sys.exit(1)
    influx_config = config['influx']
    influx_hostname = influx_config.get('hostname')
    influx_port = influx_config.getint('port', 8086)
    influx_username = influx_config.get('username')
    influx_password = influx_config.get('password')
    influx_database = influx_config.get('database')
    influx_prefix = influx_config.get('prefix', '')
    loader = InfluxLoader(influx_hostname, influx_port, influx_username, influx_password,
                          influx_database, influx_prefix, logger)
    return loader


def prepare_scheduler(_config: ConfigParser, logger: logging.Logger):
    scheduler = BlockingScheduler()

    def log_event(event):
        if event.exception:
            logger.error('The job crashed with exception', exc_info=event.exception)
        else:
            logger.info('The job finished successfully')

    scheduler.add_listener(log_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    return scheduler


def prepare_job(config: ConfigParser, logger: logging.Logger, kl: KotelLoader,
                il: InfluxLoader, scheduler: BlockingScheduler):
    if 'job' not in config:
        logger.error('Missing section job.')
        sys.exit(1)
    job_config = config['job']
    job_interval = int(job_config.get('interval_seconds', '60'))

    def job():
        data = kl.load_pages()
        il.load(data)

    scheduler.add_job(job, 'interval', seconds=job_interval)


def start(config: ConfigParser):
    logger = prepare_logging(config)
    kl = prepare_kotel(config, logger)
    il = prepare_influx(config, logger)
    scheduler = prepare_scheduler(config, logger)
    prepare_job(config, logger, kl, il, scheduler)
    scheduler.start()
    # print(kl.load_pages())


def main():
    if len(sys.argv) != 2:
        print('One argument is expected.')
        sys.exit(1)

    config = ConfigParser()
    config.read(sys.argv[1])
    start(config)


if __name__ == '__main__':
    main()
