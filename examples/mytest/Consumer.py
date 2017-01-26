__author__ = 'jim.graf'

import logging
import Queue

class Consumer:

    def __init__(self, target=None, log_queue=None):
        self.target = target
        self.log_queue = log_queue

    def consume(self, item):
        try:
            self.log_queue.put(item="Consuming {item}".format(item=item), block=True, timeout=60)
        except Queue.full:
            logging.fatal("Unable to write to logging queue")
            exit(1)