import pathlib
import logging

from Decorators import service

@service
class LoggingService:
    def __init__(self):
        path = pathlib.Path(__file__).parent.resolve()

        logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    filename=f'{path}/logfile',
                    filemode='a')

        self.logger = logging.getLogger(__name__)

    def info(self, message):
        self.logger.info(message)

    def warn(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)