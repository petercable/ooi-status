import datetime
import logging

log = logging.getLogger(__name__)


class StopWatch(object):
    """
    Easily measure elapsed time
    """
    def __init__(self, message=None):
        self.start_time = datetime.datetime.now()
        self.message = message

    def __repr__(self):
        stop = datetime.datetime.now()
        r = str(stop - self.start_time)
        if self.message:
            return self.message + ' ' + r
        return r

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug(self)
