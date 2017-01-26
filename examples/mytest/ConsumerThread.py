import logging
import random
import threading
import time

class ConsumerThread(threading.Thread):
    def __init__(self, name=None,
                 queue=None, consumer=None):
        super(ConsumerThread,self).__init__()
        self.name = name
        self.queue = queue
        self.consumer = consumer
        self.running = True
        return

    def run(self):
        q = self.queue
        while self.running or not q.empty():
            if not q.empty():
                item = q.get()
                logging.info('{name} GETting {item} : {size} items in queue'.format(name=self.name, item=str(item), size=str(q.qsize())))
                self.consumer.consume(item)
                time.sleep(random.random())
        return

    def stop(self):
        self.running = False