__author__ = 'jim.graf'

import logging
import os
import sys

from boto.s3.key import Key


def write_file_to_s3(filename, bucket):
    logging.info('Writing file: {filename}'.format(filename=filename))
    mb_size = os.path.getsize(filename) / 1e6
    if mb_size < 60:
        _standard_upload(filename, bucket)
    else:
        _multipart_upload(filename, bucket)


def _percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def _standard_upload(filename, bucket):
    logging.info('Standard - Writing file: {filename}'.format(filename=filename))
    s3key = Key(bucket)
    s3key.key = filename
    s3key.set_contents_from_filename(filename, cb=_percent_cb, num_cb=10)
    s3key.make_public()
    sys.stdout.write('\n')
    sys.stdout.flush()


def _multipart_upload(filename, bucket):
    logging.info('Multipart - Writing file: {filename}'.format(filename=filename))