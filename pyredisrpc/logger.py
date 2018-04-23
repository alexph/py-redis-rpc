import coloredlogs
import logging


logger = logging.getLogger(__name__)

coloredlogs.install(level=logging.DEBUG, milliseconds=True)
