import click
import click_log
import logging
import boto
import boto.s3
import sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key


def setup_logging():
    # set up logging to file
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=__name__ + '.log',
                        filemode='w')
    root_logger = logging.getLogger('')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    root_logger.addHandler(console)

    errorFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    error = logging.FileHandler('error.log')
    error.setLevel(logging.ERROR)
    error.setFormatter(errorFormatter)
    root_logger.addHandler(error)

    return logging.getLogger('')

logger = setup_logging()


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
                # write_file_to_s3(stripped_filename, bucket)
                logger.info('Write here')
            except IOError:
                logger.error('Error writing file %s' % stripped_filename)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def write_file_to_s3(filename, bucket):
    logger.info('\nWriting file: %s' % filename)
    s3key = Key(bucket)
    s3key.key = filename
    s3key.set_contents_from_filename(filename, cb=percent_cb, num_cb=10)