import datetime
import functools
from get_logger import get_logger

log = get_logger(__name__)


class stopwatch(object):
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

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug(self)

    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            self.start_time = datetime.datetime.now()
            if self.message is None:
                self.message = 'function %r finished in :'
            with self:
                return f(*args, **kwargs)
        return decorated
