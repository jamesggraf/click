import logging
import random
import threading
import time

class ConsumerThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None, queue=None):
        super(ConsumerThread,self).__init__()
        self.target = target
        self.name = name
        self.queue = queue
        self.running = True
        return

    def run(self):
        q = self.queue
        while self.running:
            if not q.empty():
                item = q.get()
                logging.info('{name} GETting {item} : {size} items in queue'.format(name=self.name, item=str(item), size=str(q.qsize())))
                time.sleep(random.random())
        return