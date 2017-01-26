__author__ = 'jim.graf'

import logging

class Consumer:

    def __init__(self, target=None):
        self.target = target

    def consume(self, item):
        logging.info("Consuming {item}".format(item=item))