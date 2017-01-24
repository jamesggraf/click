import logging

__author__ = 'jim.graf'

def configure_logging(name):
    # set up logging to file
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='%s.log' % name,
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
    error = logging.FileHandler('%s-error.log' % name)
    error.setLevel(logging.ERROR)
    error.setFormatter(errorFormatter)
    root_logger.addHandler(error)

    return logging.getLogger('')