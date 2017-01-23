import click

from boto.s3.connection import S3Connection

@click.command()
@click.argument('input', type=click.File('rb'))
@click.argument('output', type=click.STRING)
def cli(input, output):
    """This is an experimental script for reading a file and writing it to S3
    """

    with input as f:
        for filename in f:
            read_file(filename.strip())
    # inspection = input.name
    # print('Reading: %(input)s. %(inspection)s.' % locals())
    # print('Connecting...')
    # conn = S3Connection()

def read_file(filename):
    print(filename)
    file = open(filename, 'r')

    print(file.read().strip())

    file.close()
