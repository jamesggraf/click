import logging
import threading
import time
import random


class ProducerThread(threading.Thread):

    def __init__(self, name=None, queue=None,
                 producer=None):
        super(ProducerThread,self).__init__()
        self.name = name
        self.queue = queue
        self.producer = producer
        self.running = True

    def run(self):
        q = self.queue
        while self.producer.has_more_items():
            if not q.full():
                item = self.producer.get_item()
                q.put(item)
                logging.info('{name} PUTting {item} : {size} items in queue'.format(name=self.name, item=str(item),
                             size=str(q.qsize())))
            else:
                time.sleep(2)

        self.running = False
        return