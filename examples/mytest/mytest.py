import click
import click_log
import Consumer
import ConsumerThread
import csv
import logging
import LogPrinter
import Producer
import ProducerThread

import Queue
import random
import threading
import time
import sys


from boto.s3.connection import S3Connection
from write_to_s3 import write_file_to_s3
from config_logging import configure_logging

configure_logging(__name__)

@click.command()
@click.argument('csv_file', type=click.File('rU'))
@click.argument('bucket_name', type=click.STRING)
@click.option('-p', '--max-producers', default=3, help='The maximum number of producers to run')
@click.option('-c', '--max-consumers', default=5, help='The maximum number of consumers to run')
@click.option('-q', '--max-queue-size', default=32, help='The maximum number of items the producers can put in the queue before pausing to let the consumers catch up')
@click.option('-l', '--max-log-queue-size', default=1000, help='The maximum number of items to log in the print queue')
@click_log.simple_verbosity_option()
@click_log.init(__name__)
def cli(csv_file, bucket_name, max_producers, max_consumers, max_queue_size, max_log_queue_size):
    """This is an experimental script for reading a file and writing it to S3
    """
    logging.info('Reading input file {filename} and writing to S3 bucket {bucket_name}'.format(filename=csv_file.name, bucket_name=bucket_name))

    work_queue = Queue.Queue(maxsize=max_queue_size)
    log_queue = Queue.Queue(maxsize=max_log_queue_size)

    producers=[]
    for x in range(0, max_producers):
        producer = Producer.Producer(csv_file)
        producers.append(ProducerThread.ProducerThread(name='producer-{number}'.format(number=x), queue=work_queue, producer=producer))
        producers[x].start()
        time.sleep(1)

    logger=LogPrinter.LogPrinter(log_name='Consumer-log')
    logger_thread=ConsumerThread.ConsumerThread(name='logger-thread', queue=log_queue, consumer=logger)
    logger_thread.start()
    time.sleep(1)

    consumers=[]
    for x in range(0, max_consumers):
        consumer = Consumer.Consumer(log_queue=log_queue)
        consumers.append(ConsumerThread.ConsumerThread(name='consumer-{number}'.format(number=x), queue=work_queue, consumer=consumer))
        consumers[x].start()
        time.sleep(1)

    while True:
        if len(producers) > 0:
            for producer in producers:
                if not producer.running:
                    logging.info("Sending kill signal to {name}".format(name=producer.name))
                    producer.join()
                    producers.remove(producer)
            time.sleep(5)
        else:
            break

    while True:
        if work_queue.qsize() > 0:
            time.sleep(5)
        else:
            for consumer in consumers:
                logging.info("Sending kill signal to {name}".format(name=consumer.name))
                consumer.stop()
                consumer.join()
            logging.info("Sending kill signal to LogPrinter")
            logger_thread.stop()
            logger_thread.join()
            exit(0)

if __name__ == '__main__':
    cli(sys.argv[1:])
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