import logging

def createLogger(loggerName):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(message)')
    
    stream = logging.StreamHandler()
    stream.setLevel(logging.ERROR)

    fh = logging.FileHandler(loggerName)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(stream)
    logger.addHandler(fh)

    return logger

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.info('test1')
    logger.error('test2')
