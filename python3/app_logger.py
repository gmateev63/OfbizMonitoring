import logging
import logging.handlers
import sys

LOG_FILENAME = "/var/iodata/log/lv_export"

fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger('lv_export')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
# Add the log message handler to the logger
#ch = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=2000000, backupCount=5)
ch.setFormatter(fmt)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

logger.setLevel(logging.INFO)
