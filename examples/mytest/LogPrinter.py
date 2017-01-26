__author__ = 'jim.graf'

import logging

class LogPrinter:

    def __init__(self, log_name=None):
        self.logger = logging.getLogger(log_name)

    def consume(self, item):
        self.logger.info(item)