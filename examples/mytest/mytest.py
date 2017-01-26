import click
import click_log
import ConsumerThread
import csv
import logging
import ProducerThread

import Queue
import threading
import random
import time


from boto.s3.connection import S3Connection
from write_to_s3 import write_file_to_s3
from config_logging import configure_logging

configure_logging(__name__)

NUM_PRODUCERS = 2
NUM_CONSUMERS = 2
MAX_QUEUE_SIZE = 2
q = Queue.Queue(MAX_QUEUE_SIZE)

@click.command()
@click.argument('csv_file', type=click.File('rU'))
@click.argument('bucket_name', type=click.STRING)
@click_log.simple_verbosity_option()
@click_log.init(__name__)
def cli(csv_file, bucket_name):
    """This is an experimental script for reading a file and writing it to S3
    """
    logging.info('Reading input file {filename} and writing to S3 bucket {bucket_name}'.format(filename=csv_file.name, bucket_name=bucket_name))

    producers=[]
    for x in range(0, NUM_PRODUCERS):
        producers.append(ProducerThread.ProducerThread(name='producer-{number}'.format(number=x), queue=q))
        producers[x].start()
        time.sleep(2)

    consumers=[]
    for x in range(0, NUM_CONSUMERS):
        consumers.append(ConsumerThread.ConsumerThread(name='consumer-{number}'.format(number=x), queue=q))
        consumers[x].start()
        time.sleep(2)

    while True:
        if len(producers) > 0:
            for producer in producers:
                if not producer.running:
                    logging.info("Killing producer")
                    producer.join()
                    producers.remove(producer)
            time.sleep(10)
        else:
            break

    while True:
        if q.qsize() > 0:
            time.sleep(10)
        else:
            for consumer in consumers:
                logging.info("Killing consumer")
                consumer.running = False
                consumer.join()
            break

    # conn = S3Connection()
    #
    # # Check if the bucket exists. If not exit 1
    # if not conn.lookup(bucket_name):
    #     logging.error("Bucket {bucket_name} does NOT exist".format(bucket_name=bucket_name))
    #     exit(1)
    #
    # bucket = conn.get_bucket(bucket_name)
    #
    # field_names = ['version', 'field', 'type', 'description', 'file']
    #
    # with csv_file as open_file:
    #     csv_reader = csv.DictReader(open_file, dialect=csv.excel, fieldnames=field_names)
    #     for row in csv_reader:
    #         stripped_filename = 'data/{filename}'.format(filename=row['file'].strip())
    #
    #         try:
    #             q.put((stripped_filename, bucket))
    #             # write_file_to_s3(stripped_filename, bucket)
    #         except IOError:
    #             logging.error('Error writing file {filename}'.format(filename=stripped_filename))

# class ConsumerThread(threading.Thread):
#     def __init__(self, group=None, target=None, name=None,
#                  args=(), kwargs=None, verbose=None):
#         super(ConsumerThread,self).__init__()
#         self.target = target
#         self.name = name
#         return
#
#     def run(self):
#         while True:
#             if not q.empty():
#                 item = q.get()
#                 logging.info('Getting ' + str(item)
#                               + ' : ' + str(q.qsize()) + ' items in queue')
#                 time.sleep(random.random())
#         return