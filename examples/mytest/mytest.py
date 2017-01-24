import click
import click_log
import boto
import boto.s3
import sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from config_logging import configure_logging

logger = configure_logging(__name__)

@click.command()
@click.argument('input', type=click.File('rb'))
@click.argument('bucket_name', type=click.STRING)
@click_log.simple_verbosity_option()
@click_log.init(__name__)
def cli(input, bucket_name):
    """This is an experimental script for reading a file and writing it to S3
    """
    logger.info('Reading input file %s and writing to S3 bucket %s' % (input, bucket_name))
    conn = S3Connection()

    # Check if the bucket exists. If not exit 1
    if not conn.lookup(bucket_name):
        logger.error("Bucket %s does NOT exist" % bucket_name)
        exit(1)

    bucket = conn.get_bucket(bucket_name)

    with input as f:
        for filename in f:
            stripped_filename = 'data/%s' % filename.strip()

            try:
                write_file_to_s3(stripped_filename, bucket)
            except IOError:
                logger.error('Error writing file %s' % stripped_filename)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def write_file_to_s3(filename, bucket):
    logger.info('Writing file: %s' % filename)
    s3key = Key(bucket)
    s3key.key = filename
    s3key.set_contents_from_filename(filename, cb=percent_cb, num_cb=10)
    s3key.make_public()
    sys.stdout.write('\n')
    sys.stdout.flush()