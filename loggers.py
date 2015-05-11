__author__ = 'Ammar Akhtar'

"""
This file creates loggers for the DiffEngine class in the extract.py module
"""

import logging
import logging.handlers

# set up logs
LOG_FILENAME = 'logs/logs.out'
formatter = logging.Formatter('%(levelname)s [%(asctime)s] [%(name)s:%(lineno)s] %(message)s')

loghandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1000000, backupCount=15)
iohandler = logging.handlers.RotatingFileHandler('logs/iologs.out', maxBytes=100000, backupCount=15)
classifierhandler = logging.handlers.RotatingFileHandler('logs/classifierlogs.out', maxBytes=1000000, backupCount=150)
final_stats_handler = logging.handlers.RotatingFileHandler('logs/final_stats.out', maxBytes=2500000, backupCount=15)

ch = logging.StreamHandler()

loghandler.setFormatter(formatter)
iohandler.setFormatter(formatter)
classifierhandler.setFormatter(formatter)
final_stats_handler.setFormatter(formatter)

ch.setFormatter(formatter)
ch.setLevel(logging.INFO)

stream_logger = logging.getLogger('stream_logger')
stream_logger.setLevel(logging.DEBUG)
stream_logger.addHandler(ch)

main_logger = logging.getLogger('main_logger')
main_logger.setLevel(logging.INFO)
main_logger.addHandler(loghandler)
main_logger.addHandler(ch)

iologger = logging.getLogger('iologger')
iologger.setLevel(logging.DEBUG)
iologger.addHandler(iohandler)
iologger.addHandler(ch)

classifier_logger = logging.getLogger('classifier_logger')
classifier_logger.setLevel(logging.INFO)
classifier_logger.addHandler(classifierhandler)
# classifier_logger.addHandler(ch)

final_stats = logging.getLogger('final_stats')
final_stats.setLevel(logging.INFO)
final_stats.addHandler(final_stats_handler)
# end set up logs