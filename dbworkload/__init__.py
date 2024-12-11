import logging
import time
from importlib import metadata

try:
    __version__ = metadata.version(__package__)
except:
    __version__ = "#N/A"

del metadata  # optional, avoids polluting the results of dir(__package__)

logger = logging.getLogger("dbworkload")

sh = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] (%(processName)s %(threadName)s) %(module)s:%(lineno)d: %(message)s",
)

# set the formatter to use UTC and show microseconds
formatter.converter = time.gmtime
formatter.default_msec_format = "%s.%06d"

sh.setFormatter(formatter)
logger.addHandler(sh)
