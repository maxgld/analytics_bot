import logging
import otherMod2
import sys


# def main():
#     """
#     The main entry point of the application
#     """
#
#     logger = logging.getLogger('exampleApp')
#     logger.setLevel(logging.INFO)
#
#     # create the logging file handler
#     fh = logging.FileHandler('new_snake.log')
#
#     #'%(filename)-20s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s'
#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     fh.setFormatter(formatter)
#
#     # add handler to logger object
#     logger.addHandler(fh)
#
#     logger.info('Program started')
#     result = otherMod2.add(7, 8)
#     logger.info('Done!')
#
#
# if __name__ == '__main__':
#     sys.exit(main())

# logging.basicConfig(filename='logger.log', level=logging.INFO,
#                     format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
#
# if __name__ == '__main__':
#     logger = logging.getLogger(__name__)
#     logger.info('Started script 1')
#     otherMod2.somefunc()
#     logger.info('Finished script 1')

# logging:
#   console: false
#   file: /opt/www/log/django-catalog.log
#   request_log: /opt/www/log/django-catalog-requests.log


logger = logging.getLogger('logger_test')#__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(filename)-20s %(levelname)-8s %(message)s')

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(formatter)

filehandler = logging.FileHandler('logger.log')
filehandler.setLevel(logging.DEBUG)
filehandler.setFormatter(formatter)

logger.addHandler(console)
logger.addHandler(filehandler)

if __name__ == '__main__':
    logger.info('Started logging')
    # эта строка пойдёт и в файл и на консоль
    logger.warning('Started logging to console and log')
    logger.info('Finished logging')
    otherMod2.somefunc()

