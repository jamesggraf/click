__author__ = 'jim.graf'

import logging
import random

class Producer:
    MAX_ITEMS = 20

    def __init__(self, target=None):
        self.target = target
        self.item_count = self.MAX_ITEMS

    def get_item(self):
        self.item_count -= 1
        item = random.randint(1,10)
        logging.info("Producing {item}".format(item=item))
        return item

    def has_more_items(self):
        return self.item_count > 0