import logging
import threading
import time
import random


class ProducerThread(threading.Thread):

    MAX_ITEMS = 20

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None, queue=None):
        super(ProducerThread,self).__init__()
        self.target = target
        self.name = name
        self.queue = queue
        self.item_count = self.MAX_ITEMS
        self.running = True

    def run(self):
        q = self.queue
        while self.item_count > 0:
            if not q.full():
                item = random.randint(1,10)
                q.put(item)
                self.item_count = self.item_count - 1
                logging.info('{name} PUTting {item} : {size} items in queue'.format(name=self.name, item=str(item),
                             size=str(q.qsize())))
                time.sleep(random.random())
            else:
                time.sleep(2)

        self.running = False
        return